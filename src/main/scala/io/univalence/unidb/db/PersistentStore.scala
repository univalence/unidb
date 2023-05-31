package io.univalence.unidb.db

import ujson.Value

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import java.nio.file.{Files, NoSuchFileException, Path}
import java.time.Instant

class PersistentStoreSpace private[db] (name: String, storeSpaceDir: Path, storePath: Map[String, Path])
    extends StoreSpace(name) { storeSpace =>

  val stores: mutable.Map[String, PersistentStore] =
    mutable.Map.from(storePath.map((name, path) => name -> new PersistentStore(name, path, storeSpace)))

  override def createStore(name: String): Try[Store] =
    if (stores.contains(name))
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name}: store $name already exists"
        )
      )
    else
      val store = new PersistentStore(name, storeSpaceDir.resolve(name), storeSpace)
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
      val store = new PersistentStore(name, storeSpaceDir.resolve(name), storeSpace)
      stores.update(name, store)

      Success(store)

  override def drop(name: String): Try[Unit] =
    if (stores.contains(name))
      Try {
        Files.delete(stores(name).file)
        stores.remove(name)
      }
    else
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name}: store $name does not exist"
        )
      )

  def getAll: Try[Iterator[String]] = Try(stores.view.keys.iterator)

  override def close(): Unit = ()
}

object PersistentStoreSpace {
  def apply(name: String, baseDir: Path): Try[PersistentStoreSpace] =
    if (!baseDir.toFile.exists())
      Failure(new NoSuchFileException(s"store space=$name: base directory not found: $baseDir"))
    else
      val storeSpaceDir = baseDir.resolve(name)
      if (!storeSpaceDir.toFile.exists())
        Files.createDirectories(storeSpaceDir)

      val storePaths: Map[String, Path] =
        storeSpaceDir.toFile
          .listFiles()
          .toList
          .filter(f => f.isFile)
          .map { file =>
            val storeName = file.getName

            storeName -> file.toPath
          }
          .toMap

      Success(new PersistentStoreSpace(name, storeSpaceDir, storePaths))
}

class PersistentStore private[db] (val name: String, val file: Path, storeSpace: PersistentStoreSpace) extends Store {
  store =>

  val commitLog: CommitLog = new CommitLog(file)
  lazy val data: mutable.TreeMap[String, Record] =
    mutable.TreeMap
      .from(
        commitLog.readAll.get
          .filterNot(_.deleted)
          .map(r => r.key -> Record(r.key, r.value, r.timestamp, r.deleted))
      )

  override def put(key: String, value: Value): Try[Unit] =
    val timestamp = Instant.now().toEpochMilli
    val record    = CLRecord(key, value, timestamp, false)
    for {
      _ <- commitLog.add(record)
      _ <- Try(data.update(key, Record(key, value, timestamp, false)))
    } yield ()

  override def delete(key: String): Try[Unit] =
    if (data.contains(key))
      for {
        record <- Try(data(key))
        deletedRecord = CLRecord(key = record.key, value = record.value, timestamp = record.timestamp, deleted = true)
        _ <- commitLog.add(deletedRecord)
        _ <- Try(data.remove(key))
      } yield ()
    else
      Failure(
        new IllegalAccessException(
          s"store space=${storeSpace.name},store=${store.name}: key $key does not exists"
        )
      )

  override def get(key: String): Try[Value] =
    if (data.contains(key))
      Success(data(key).value)
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
