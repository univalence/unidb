package io.univalence.unidb.client

import io.univalence.unidb.db.RemoteStoreSpace

import scala.util.{Failure, Success, Try, Using}
import scala.util.hashing.MurmurHash3

@main def streaming_main(): Unit =
  import streaming.*

  val topic = "message-topic"
  val group = "group-1"

//  init(topic, group)
//  Using(new Producer("localhost", 19040)) { producer =>
//    println("produce messages...")
//    producer.send(ProducerRecord(topic = topic, key = "a", value = ujson.Str("hello"))).get
//    producer.send(ProducerRecord(topic = topic, key = "b", value = ujson.Str("world"))).get
//  }.get
  Using(new Consumer("localhost", 19040, group)) { consumer =>
    println("consume messages...")
    consumer.fetch(topic).get.foreach(println)
  }.get

def init(topic: String, group: String): Unit = {
  import streaming.*

  Using(RemoteStoreSpace("rdb", ???).get) { rdb =>
    (for {
      topicTable <- rdb.createStore("__topic")
      _ <-
        topicTable.put(
          topic,
          ujson.Obj(
            "partition-count" -> ujson.Num(4)
          )
        )
    } yield ()).get
    (for {
      producerPartitionTable <- rdb.createStore("__producer-partition-table")
//      _ <- (1 to 4).map(p => producerPartitionTable.put(s"$topic-$p", ujson.Num(0))).toList.sequence
    } yield ()).get
    (for {
      consumerPartitionTable <- rdb.createStore("__consumer-partition-table")
      _ <-
        (1 to 4)
          .map(p =>
            consumerPartitionTable.put(
              s"$group-$topic-$p",
              ujson.Obj(
                "partition" -> ujson.Num(p),
                "offset"    -> ujson.Num(0)
              )
            )
          )
          .toList
          .sequence
    } yield ()).get
  }.get
}

case class ProducerRecord(topic: String, key: String, value: ujson.Value)
case class ConsumerRecord(topic: String, partition: Int, offset: Int, key: String, value: ujson.Value)
case class TopicPartition(topic: String, partition: Int)

class Producer(host: String, port: Int) extends AutoCloseable {
  import streaming.*

  val rdb = RemoteStoreSpace("rdb", null).get

  override def close(): Unit = rdb.close()

  def send(record: ProducerRecord): Try[Unit] =
    for {
      topicTable <- rdb.getStore("__topic")
      topic      <- topicTable.get(record.topic)
      partitionCount  = topic.obj("partition-count").num.toInt
      partitionNumber = (Math.abs(MurmurHash3.stringHash(record.key)) % partitionCount) + 1
      partitionName   = s"${record.topic}-$partitionNumber"
      partitionTable <- rdb.getStore("__producer-partition-table")
      lastOffset = partitionTable.get(partitionName).map(_.num.toInt + 1).getOrElse(0)
      partition <- rdb.getStore(partitionName)
      _         <- Try(println(s"partition: count: $partitionCount - number: $partitionNumber - name: $partitionName"))
      _         <- Try(println(s"last offset: $lastOffset"))
      _ <-
        partition.put(
          streaming.leftpad(lastOffset.toString, 5, '0'),
          ujson.Obj(
            "offset"    -> ujson.Num(lastOffset),
            "partition" -> ujson.Num(partitionNumber),
            "key"       -> ujson.Str(record.key),
            "value"     -> record.value
          )
        )
      _ <- partitionTable.put(partitionName, ujson.Num(lastOffset))
    } yield ()

}

class Consumer(host: String, port: Int, group: String) extends AutoCloseable {
  import streaming.*

  val rdb = RemoteStoreSpace("rdb", null).get

  override def close(): Unit = rdb.close()

  def fetch(topic: String): Try[List[ConsumerRecord]] =
    for {
      topicTable <- rdb.getStore("__topic")

      topicObj <- topicTable.get(topic)

      partitionCount = topicObj.obj("partition-count").num.toInt
      partitionNames = (1 to partitionCount).map(p => TopicPartition(topic, p))

      consumerTable <- rdb.getStore("__consumer-partition-table")

      consumerOffsets: Map[TopicPartition, Int] <-
        consumerTable
          .getPrefix(s"$group-$topic")
          .map(
            _.toList
              .map { r =>
                val partition = r.value.obj("partition").num.toInt
                val offset    = r.value.obj("offset").num.toInt
                TopicPartition(topic, partition) -> offset
              }
              .toMap
          )

      partitionTables <-
        partitionNames
          .map(tp => rdb.getStore(s"${tp.topic}-${tp.partition}").map(tp -> _))
          .toList
          .sequence

      result: List[ConsumerRecord] <-
        partitionTables
          .map { (tp, table) =>
            val offset: Int = consumerOffsets(tp)

            val r: Try[List[ConsumerRecord]] =
              table
                .getFrom(leftpad(offset.toString, 5, '0'))
                .map { records =>
                  val consumerRecords: List[ConsumerRecord] =
                    records.toList
                      .map(r =>
                        ConsumerRecord(
                          topic     = tp.topic,
                          partition = tp.partition,
                          offset    = offset,
                          key       = r.key,
                          value     = r.value.obj("value")
                        )
                      )

                  consumerRecords
                }

            r
          }
          .filter(_.isSuccess)
          .sequence
          .map(_.flatten)

      _ <-
        result
          .groupBy(_.partition)
          .map((p, records) => p -> records.map(_.offset).max)
          .map { (p, lastOffset) =>
            consumerTable.put(
              s"$group-$topic-$p",
              ujson.Obj(
                "partition" -> ujson.Num(p),
                "offset"    -> ujson.Num(lastOffset + 1)
              )
            )
          }
          .toList
          .sequence
    } yield result
}

object streaming {
  def leftpad(s: String, size: Int, c: Char): String = c.toString * Math.max(0, size - s.length) + s

  extension [A](l: List[Try[A]])
    def sequence: Try[List[A]] =
      l.foldLeft(Try(List.empty[A])) {
        case (Success(l), Success(r)) => Success(l :+ r)
        case (Failure(e), _)          => Failure[List[A]](e)
        case (_, Failure(e))          => Failure[List[A]](e)
      }

}
