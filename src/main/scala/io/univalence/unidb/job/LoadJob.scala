package io.univalence.unidb.job

import io.univalence.unidb.StoreSpaceManagerService
import io.univalence.unidb.arg.ApplicationOption
import io.univalence.unidb.db.StoreName

import zio.*
import zio.stream.*

import scala.collection.immutable.{ListMap, TreeMap}
import scala.io.Source

import java.nio.file.{Path, Paths}

case class LoadJob(defaultStoreDir: Path, defaultKeyDelim: String) extends Job[Any, ApplicationOption.LoadOption] {

  override def run(option: ApplicationOption.LoadOption): RIO[Any, Unit] =
    val storeDir   = option.storeDir.getOrElse(defaultStoreDir)
    val keyDelim   = option.keyDelim.getOrElse(defaultKeyDelim)
    val fileToLoad = option.fromFile

    ZIO
      .scoped {
        for {
          file <- ZIO.fromAutoCloseable(ZIO.attempt(Source.fromFile(fileToLoad.toFile)))
          streams <-
            ZIO.attempt {
              val stream: ZStream[Any, Throwable, String]       = ZStream.fromIterator(file.getLines())
              val headerStream: ZStream[Any, Throwable, String] = stream.take(1)

              (stream, headerStream)
            }
          stream       = streams._1
          headerStream = streams._2
          header: List[String] <-
            headerStream
              .run(ZSink.head)
              .flatMap(o => if (o.isEmpty) ZIO.fail(new RuntimeException("empty file")) else ZIO.attempt(o.get))
              .map(_.split(",").toList)
          _          <- Console.printLine("columns: " + header.mkString(", "))
          storeSpace <- StoreSpaceManagerService.getOrCreatePersistent(option.toStore.storeSpace)
          store      <- storeSpace.getOrCreateStore(option.toStore.store)
          _ <-
            stream
              .map { line =>
                val fields = line.split(",").toList
                val record = ListMap.from(header.zip(fields))
                val key    = option.keyFields.map(k => record(k)).mkString(keyDelim)
                val json   = ujson.Obj.from(record.map((h, v) => h -> ujson.Str(v)).toList)
                key -> json
              }
              .run(ZSink.foreach((key, value) => store.put(key, value)))
        } yield ()
      }
      .provide(StoreSpaceManagerService.layer(storeDir))
}
