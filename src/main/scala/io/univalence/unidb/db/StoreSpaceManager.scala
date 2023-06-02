package io.univalence.unidb.db

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import java.nio.file.Path

class StoreSpaceManager(baseDir: Path) extends AutoCloseable {
  // TODO manage closed store spaces
  val storeSpaces: mutable.Map[String, StoreSpace] = mutable.Map.empty

  def getPersistent(name: String): Try[StoreSpace] =
    if (!storeSpaces.contains(name))
      for {
        storeSpace <- PersistentStoreSpace(name, baseDir, shouldCreate = false)
        _          <- Try(storeSpaces.update(name, storeSpace))
      } yield storeSpace
    else
      Success(storeSpaces(name))

  def getOrCreateInMemory(name: String): Try[StoreSpace] =
    if (storeSpaces.contains(name))
      Try(storeSpaces(name))
    else
      Try {
        val storeSpace = new InMemoryStoreSpace(name)
        storeSpaces.update(name, storeSpace)
        storeSpace
      }

  def getOrCreatePersistent(name: String): Try[StoreSpace] =
    if (storeSpaces.contains(name))
      Try(storeSpaces(name))
    else
      for {
        storeSpace <- PersistentStoreSpace(name, baseDir)
        _          <- Try(storeSpaces.update(name, storeSpace))
      } yield storeSpace

  def getOrOpenRemote(name: String, host: String, port: Int): Try[StoreSpace] =
    if (storeSpaces.contains(name))
      Try(storeSpaces(name))
    else
      for {
        storeSpace <- RemoteStoreSpace(name, host, port)
        _          <- Try(storeSpaces.update(name, storeSpace))
      } yield storeSpace

  def getAllSpaces: Try[Iterator[String]] = Try(storeSpaces.view.keys.iterator)

  override def close(): Unit = for (storeSpace <- storeSpaces.values) storeSpace.close()
}
