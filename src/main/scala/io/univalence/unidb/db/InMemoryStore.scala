package io.univalence.unidb.db

import ujson.Value

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import java.time.Instant

class InMemoryStoreSpace(name: String) extends StoreSpace(name) { storeSpace =>
  val stores: mutable.Map[String, InMemoryStore] = mutable.Map.empty

  override def createStore(name: String): Try[Store] =
    if (stores.contains(name))
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name}: store $name already exists"
        )
      )
    else
      val store = new InMemoryStore(name, storeSpace)
      stores.update(name, store)

      Success(store)

  override def getStore(name: String): Try[Store] =
    if (stores.contains(name))
      Success(stores(name))
    else
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name}: store $name does not exist"
        )
      )

  override def getOrCreateStore(name: String): Try[Store] =
    if (stores.contains(name)) Success(stores(name))
    else
      val store = new InMemoryStore(name, storeSpace)
      stores.update(name, store)

      Success(store)

  override def drop(name: String): Try[Unit] =
    if (stores.contains(name)) Success(stores.remove(name))
    else
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name}: store $name does not exist"
        )
      )

  override def close(): Unit = ()

}
class InMemoryStore private[db] (name: String, storeSpace: InMemoryStoreSpace) extends Store { store =>
  val data: mutable.TreeMap[String, Record] = mutable.TreeMap.empty

  override def put(key: String, value: ujson.Value): Try[Unit] =
    val timestamp = Instant.now().toEpochMilli
    Try(data.update(key, Record(key = key, value = value, timestamp = timestamp, deleted = false)))

  override def delete(key: String): Try[Unit] = Try(data.remove(key))

  override def get(key: String): Try[ujson.Value] =
    if (data.contains(key))
      Try(data(key).value)
    else
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name},store=${store.name}: key $key does not exists"
        )
      )

  override def getFrom(key: String): Try[Iterator[Record]] = Try(data.iteratorFrom(key).map(_._2))

  override def getPrefix(prefix: String): Try[Iterator[Record]] =
    Try {
      data
        .iteratorFrom(prefix)
        .filter((key, _) => key.startsWith(prefix))
        .map(_._2)
    }

  override def scan(): Try[Iterator[Record]] = Try(data.iterator.map(_._2))

}
