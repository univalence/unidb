package io.univalence.unidb

import io.univalence.unidb.command.StoreType
import io.univalence.unidb.db.StoreName
import io.univalence.unidb.job.RunningState

import zio.*

object UniInterpreter {

  trait Interpreter {
    def get(storeName: StoreName, key: String): Task[RunningState]
    def put(storeName: StoreName, key: String, value: ujson.Value): Task[RunningState]
    def delete(storeName: StoreName, key: String): Task[RunningState]
    def getFrom(storeName: StoreName, key: String, limit: Option[Int]): Task[RunningState]
    def getWithPrefix(storeName: StoreName, prefix: String, limit: Option[Int]): Task[RunningState]
    def getAll(storeName: StoreName, limit: Option[Int]): Task[RunningState]

    def createStore(storeName: StoreName): Task[RunningState]
    def getStore(storeName: StoreName): Task[RunningState]
    def getOrCreateStore(storeName: StoreName): Task[RunningState]
    def dropStore(storeName: StoreName): Task[RunningState]

    def openStoreSpace(name: String, storeSpaceType: StoreType): Task[RunningState]
    def showStoreSpaces(): Task[RunningState]
    def showStores(storeSpace: String): Task[RunningState]
  }

  object Interpreter {
    def get(storeName: StoreName, key: String): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.get(storeName, key))

    def put(storeName: StoreName, key: String, value: ujson.Value): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.put(storeName, key, value))

    def delete(storeName: StoreName, key: String): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.delete(storeName, key))

    def getFrom(storeName: StoreName, key: String, limit: Option[Int]): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.getFrom(storeName, key, limit))

    def getWithPrefix(
        storeName: StoreName,
        prefix: String,
        limit: Option[Int]
    ): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.getWithPrefix(storeName, prefix, limit))

    def getAll(storeName: StoreName, limit: Option[Int]): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.getAll(storeName, limit))

    def createStore(storeName: StoreName): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.createStore(storeName))

    def dropStore(storeName: StoreName): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.dropStore(storeName))

    def getStore(storeName: StoreName): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.getStore(storeName))

    def getOrCreateStore(storeName: StoreName): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.getOrCreateStore(storeName))

    def openStoreSpace(name: String, storeSpaceType: StoreType): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.openStoreSpace(name, storeSpaceType))

    def showStoreSpaces(): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.showStoreSpaces())

    def showStores(storeSpace: String): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.showStores(storeSpace))
  }

  class InterpreterLive(manager: StoreSpaceManagerService, console: UniDBConsole.Console) extends Interpreter {
    override def get(storeName: StoreName, key: String): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        value      <- store.get(key)
        _          <- console.response(s"$key -> ${value.toString}")
      } yield RunningState.Continue

    override def put(storeName: StoreName, key: String, value: ujson.Value): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        _          <- store.put(key, value)
      } yield RunningState.Continue

    override def delete(storeName: StoreName, key: String): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        _          <- store.delete(key)
      } yield RunningState.Continue

    override def getFrom(storeName: StoreName, prefix: String, limit: Option[Int]): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        records    <- limit.fold(store.getFrom(prefix))(l => store.getFrom(prefix).map(_.take(l)))
        recordList = records.toList
        _ <-
          ZIO.foreachDiscard(recordList) { record =>
            console.response(s"${record.key} -> ${record.value.toString}")
          }
        _ <-
          console.response(
            if (recordList.isEmpty) "No records"
            else if (recordList.size == 1) "1 record"
            else s"${recordList.size} records"
          )
      } yield RunningState.Continue

    override def getWithPrefix(storeName: StoreName, prefix: String, limit: Option[Int]): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        records    <- limit.fold(store.getPrefix(prefix))(l => store.getPrefix(prefix).map(_.take(l)))
        recordList = records.toList
        _ <-
          ZIO.foreachDiscard(recordList) { record =>
            console.response(s"${record.key} -> ${record.value.toString}")
          }
        _ <-
          console.response(
            if (recordList.isEmpty) "No records"
            else if (recordList.size == 1) "1 record"
            else s"${recordList.size} records"
          )
      } yield RunningState.Continue

    override def getAll(storeName: StoreName, limit: Option[Int]): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        records    <- limit.fold(store.scan())(l => store.scan().map(_.take(l)))
        recordList = records.toList
        _ <-
          ZIO.foreachDiscard(recordList) { record =>
            console.response(s"${record.key} -> ${record.value.toString}")
          }
        _ <-
          console.response(
            if (recordList.isEmpty) "No records"
            else if (recordList.size == 1) "1 record"
            else s"${recordList.size} records"
          )
      } yield RunningState.Continue

    override def createStore(storeName: StoreName): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        _          <- storeSpace.createStore(storeName.store)
      } yield RunningState.Continue

    override def getStore(storeName: StoreName): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        _          <- storeSpace.getStore(storeName.store)
      } yield RunningState.Continue

    override def getOrCreateStore(storeName: StoreName): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        _          <- storeSpace.getOrCreateStore(storeName.store)
      } yield RunningState.Continue

    override def dropStore(storeName: StoreName): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        _          <- storeSpace.dropStore(storeName.store)
      } yield RunningState.Continue

    override def openStoreSpace(name: String, storeSpaceType: StoreType): Task[RunningState] =
      val result =
        storeSpaceType match {
          case StoreType.InMemory =>
            manager.getOrCreateInMemory(name)
          case StoreType.Persistent =>
            manager.getOrCreatePersistent(name)
          case StoreType.Remote(host, port) =>
            manager.getOrOpenRemote(name, host, port)
        }

      result *> RunningState.ContinueM

    override def showStoreSpaces(): Task[RunningState] =
      for {
        names <- manager.getAllSpaces
        _     <- ZIO.foreachDiscard(names.toList)(name => console.response(name))
      } yield RunningState.Continue

    override def showStores(storeSpaceName: String): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeSpaceName)
        names      <- storeSpace.getAllStores
        _          <- ZIO.foreachDiscard(names.toList)(name => console.response(name))
      } yield RunningState.Continue

  }

  val layer: ZLayer[UniDBConsole.Console & StoreSpaceManagerService, Throwable, Interpreter] =
    ZLayer(
      for {
        console <- ZIO.service[UniDBConsole.Console]
        manager <- ZIO.service[StoreSpaceManagerService]
      } yield new InterpreterLive(manager, console)
    )
}
