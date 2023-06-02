package io.univalence.unidb

import io.univalence.unidb.db
import io.univalence.unidb.db.{Store, StoreSpace, StoreSpaceManager}
import io.univalence.unidb.wrapper.ZWrapped

import zio.*

import java.nio.file.Path

trait StoreSpaceManagerService {
  def getPersistent(name: String): Task[ZStoreSpace]
  def getOrCreateInMemory(name: String): Task[ZStoreSpace]
  def getOrCreatePersistent(name: String): Task[ZStoreSpace]
  def getOrOpenRemote(name: String, host: String, port: Int): Task[ZStoreSpace]
  def getAllSpaces: Task[Iterator[String]]
}
object StoreSpaceManagerService {
  def getPersistent(name: String): RIO[StoreSpaceManagerService, ZStoreSpace] =
    ZIO.serviceWithZIO[StoreSpaceManagerService](_.getPersistent(name))
  def getOrCreateInMemory(name: String): RIO[StoreSpaceManagerService, ZStoreSpace] =
    ZIO.serviceWithZIO[StoreSpaceManagerService](_.getOrCreateInMemory(name))
  def getOrCreatePersistent(name: String): RIO[StoreSpaceManagerService, ZStoreSpace] =
    ZIO.serviceWithZIO[StoreSpaceManagerService](_.getOrCreatePersistent(name))
  def getOrOpenRemote(name: String, host: String, port: Int): RIO[StoreSpaceManagerService, ZStoreSpace] =
    ZIO.serviceWithZIO[StoreSpaceManagerService](_.getOrOpenRemote(name, host, port))
  def getAllSpaces: RIO[StoreSpaceManagerService, Iterator[String]] =
    ZIO.serviceWithZIO[StoreSpaceManagerService](_.getAllSpaces)

  def layer(baseDir: Path): ZLayer[Any, Throwable, StoreSpaceManagerService] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable(ZIO.attempt(new StoreSpaceManagerServiceLive(baseDir)))
    }
}
class StoreSpaceManagerServiceLive(baseDir: Path) extends StoreSpaceManagerService with AutoCloseable {
  val manager = new StoreSpaceManager(baseDir)

  override def getPersistent(name: String): Task[ZStoreSpace] =
    ZIO.fromTry(manager.getPersistent(name)).map(ZStoreSpace.apply)

  override def getOrCreateInMemory(name: String): Task[ZStoreSpace] =
    ZIO.fromTry(manager.getOrCreateInMemory(name)).map(ZStoreSpace.apply)

  override def getOrCreatePersistent(name: String): Task[ZStoreSpace] =
    ZIO.fromTry(manager.getOrCreatePersistent(name)).map(ZStoreSpace.apply)

  override def getOrOpenRemote(name: String, host: String, port: RuntimeFlags): Task[ZStoreSpace] =
    ZIO.fromTry(manager.getOrOpenRemote(name, host, port)).map(ZStoreSpace.apply)

  override def getAllSpaces: Task[Iterator[String]] = ZIO.fromTry(manager.getAllNames)

  override def close(): Unit = manager.close()
}

case class ZStoreSpace(storeSpace: StoreSpace) {
  def createStore(name: String): Task[ZStore]      = ZIO.fromTry(storeSpace.createStore(name)).map(ZStore.apply)
  def getStore(name: String): Task[ZStore]         = ZIO.fromTry(storeSpace.getStore(name)).map(ZStore.apply)
  def getOrCreateStore(name: String): Task[ZStore] = ZIO.fromTry(storeSpace.getOrCreateStore(name)).map(ZStore.apply)
  def dropStore(name: String): Task[Unit]          = ZIO.fromTry(storeSpace.drop(name))
}

case class ZStore(store: Store) {
  def put(key: String, value: ujson.Value): Task[Unit]     = ZIO.fromTry(store.put(key, value))
  def delete(key: String): Task[Unit]                      = ZIO.fromTry(store.delete(key))
  def get(key: String): Task[ujson.Value]                  = ZIO.fromTry(store.get(key))
  def getFrom(key: String): Task[Iterator[db.Record]]      = ZIO.fromTry(store.getFrom(key))
  def getPrefix(prefix: String): Task[Iterator[db.Record]] = ZIO.fromTry(store.getPrefix(prefix))
  def scan(): Task[Iterator[db.Record]]                    = ZIO.fromTry(store.scan())
}
