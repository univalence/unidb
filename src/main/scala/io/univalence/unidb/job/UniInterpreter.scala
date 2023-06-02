package io.univalence.unidb.job

import io.univalence.unidb.command.StoreType
import io.univalence.unidb.db.{Record, StoreName}

import zio.*

import scala.io.AnsiColor

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
    def closeStoreSpace(name: String): Task[RunningState]
    def showStoreSpaces(): Task[RunningState]
    def showStores(storeSpace: String): Task[RunningState]

    def close(): Task[Unit]
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

    def closeStoreSpace(name: String): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.closeStoreSpace(name))

    def showStoreSpaces(): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.showStoreSpaces())

    def showStores(storeSpace: String): ZIO[Interpreter, Throwable, RunningState] =
      ZIO.serviceWithZIO[Interpreter](_.showStores(storeSpace))

    def close(): ZIO[Interpreter, Throwable, Unit] = ZIO.serviceWithZIO[Interpreter](_.close())
  }

  class InterpreterLive(manager: StoreSpaceManagerService, console: UniDBConsole.Console) extends Interpreter {
    override def get(storeName: StoreName, key: String): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        value      <- store.get(key)
        _          <- printRecords(List(Record(key, value, 0L, false)))
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
        records    <- store.getFrom(prefix, limit)
        _          <- printRecords(records.toList)
      } yield RunningState.Continue

    override def getWithPrefix(storeName: StoreName, prefix: String, limit: Option[Int]): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        records    <- store.getPrefix(prefix, limit)
        _          <- printRecords(records.toList)
      } yield RunningState.Continue

    override def getAll(storeName: StoreName, limit: Option[Int]): Task[RunningState] =
      for {
        storeSpace <- manager.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getOrCreateStore(storeName.store)
        records    <- store.scan(limit)
        _          <- printRecords(records.toList)
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

    override def openStoreSpace(name: String, storeSpaceType: StoreType): Task[RunningState] = {
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
    }

    override def closeStoreSpace(name: String): Task[RunningState] =
      manager.closeStoreSpace(name) *> RunningState.ContinueM

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

    private def printRecords(records: List[Record]): Task[Unit] = {
      val (maxkl, maxvl) =
        (
          records
            .map(r => (r.key.length, r.value.toString.length))
            :+ ("key".length, "value".length)
        )
          .foldLeft((0, 0)) { case ((maxkl, maxvl), (kl, vl)) =>
            (Math.max(maxkl, kl), Math.max(maxvl, vl))
          }

      for {
        _ <-
          console.response(
            AnsiColor.BOLD
              + "key".padTo(maxkl, ' ')
              + " | value"
              + AnsiColor.RESET
          )
        _ <- console.response(("-" * maxkl) + "-|-" + ("-" * maxvl))
        _ <-
          ZIO.foreachDiscard(records) { record =>
            console.response(record.key.padTo(maxkl, ' ') + " | " + record.value.toString)
          }
        recordCount =
          if (records.isEmpty) "No record"
          else if (records.size == 1) "1 record"
          else s"${records.size} records"
        _ <- console.response(AnsiColor.BOLD + recordCount + AnsiColor.RESET)
      } yield ()
    }

    override def close(): Task[Unit] = manager.close()

  }

  val layer: ZLayer[UniDBConsole.Console & StoreSpaceManagerService, Throwable, Interpreter] =
    ZLayer(
      for {
        console <- ZIO.service[UniDBConsole.Console]
        manager <- ZIO.service[StoreSpaceManagerService]
      } yield new InterpreterLive(manager, console)
    )

}
