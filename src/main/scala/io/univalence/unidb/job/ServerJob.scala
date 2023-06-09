package io.univalence.unidb.job

import ujson.Value

import io.univalence.unidb.arg.ApplicationOption
import io.univalence.unidb.command.*
import io.univalence.unidb.db.network

import zio.*

import scala.annotation.tailrec

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.Path

case class ServerJob(defaultStoreDir: Path, defaultPort: Int) extends Job[Any, ApplicationOption.ServerOption] {

  override def run(option: ApplicationOption.ServerOption): RIO[Any, Unit] =
    ZIO.scoped {
      val storeDir = option.storeDir.getOrElse(defaultStoreDir)
      val port     = option.port.getOrElse(defaultPort)
      for {
        _             <- zio.Console.printLine(s"serving on port $port")
        nioSelector   <- ZIO.fromAutoCloseable(ZIO.attempt(Selector.open()))
        serverChannel <- ZIO.fromAutoCloseable(ZIO.attempt(openServerChannel(port)))
        _             <- ZIO.attempt(serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT))
        _             <- acceptLoop(nioSelector).forever.provide(StoreSpaceManagerService.layer(storeDir))
      } yield ()
    }

  def openServerChannel(port: Int): ServerSocketChannel = {
    val socketAddress = new InetSocketAddress(port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().bind(socketAddress)

    serverChannel
  }

  def registerKey(nioSelector: Selector, key: SelectionKey): Task[Unit] =
    ZIO.attempt {
      val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
      val socketChannel       = serverSocketChannel.accept()
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.register(nioSelector, SelectionKey.OP_READ)
    }

  def execute(command: StoreCommand | StoreSpaceCommand | ShowCommand): RIO[StoreSpaceManagerService, ujson.Value] =
    command match {
      case StoreCommand.Put(storeName, key, value) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          _          <- store.put(key, value)
        } yield ujson.Null

      case StoreCommand.Get(storeName, key) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          result     <- store.get(key)
        } yield result

      case StoreCommand.GetFrom(storeName, key, limit) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          iter       <- store.getFrom(key, limit)
        } yield ujson.Arr.from(
          iter.map(record =>
            ujson.Obj(
              "key"       -> ujson.Str(record.key),
              "value"     -> record.value,
              "timestamp" -> ujson.Num(record.timestamp.toDouble)
            )
          )
        )

      case StoreCommand.Delete(storeName, key) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          _          <- store.delete(key)
        } yield ujson.Null

      case StoreCommand.GetWithPrefix(storeName, prefix, limit) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          iter       <- store.getPrefix(prefix, limit)
        } yield ujson.Arr.from(
          iter.map(record =>
            ujson.Obj(
              "key"       -> ujson.Str(record.key),
              "value"     -> record.value,
              "timestamp" -> ujson.Num(record.timestamp.toDouble)
            )
          )
        )

      case StoreCommand.GetAll(storeName, limit) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          store      <- storeSpace.getStore(storeName.store)
          iter       <- store.scan(limit)
        } yield ujson.Arr.from(
          iter.map(record =>
            ujson.Obj(
              "key"       -> ujson.Str(record.key),
              "value"     -> record.value,
              "timestamp" -> ujson.Num(record.timestamp.toDouble)
            )
          )
        )

      case StoreSpaceCommand.CreateStore(storeName) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          _          <- storeSpace.createStore(storeName.store)
        } yield ujson.Null

      case StoreSpaceCommand.GetStore(storeName) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          _          <- storeSpace.getStore(storeName.store)
        } yield ujson.Null

      case StoreSpaceCommand.GetOrCreateStore(storeName) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          _          <- storeSpace.getOrCreateStore(storeName.store)
        } yield ujson.Null

      case StoreSpaceCommand.DropStore(storeName) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
          _          <- storeSpace.dropStore(storeName.store)
        } yield ujson.Null

      case StoreSpaceCommand.OpenStoreSpace(name, storeSpaceType) =>
        val result =
          storeSpaceType match {
            case StoreType.InMemory =>
              StoreSpaceManagerService.getOrCreateInMemory(name)
            case StoreType.Persistent =>
              StoreSpaceManagerService.getOrCreatePersistent(name)
            case StoreType.Remote(host, port) =>
              StoreSpaceManagerService.getOrOpenRemote(name, host, port)
          }

        result *> ZIO.succeed(ujson.Null)

      case StoreSpaceCommand.CloseStoreSpace(name) =>
        StoreSpaceManagerService.closeStoreSpace(name) *> ZIO.succeed(ujson.Null)

      case ShowCommand.StoreSpaces =>
        for {
          result <- StoreSpaceManagerService.getAllSpaces
          json   <- ZIO.attempt(ujson.Arr.from(result.map(r => ujson.Str(r))))
        } yield json

      case ShowCommand.Stores(storeSpaceName) =>
        for {
          storeSpace <- StoreSpaceManagerService.getPersistent(storeSpaceName)
          result     <- storeSpace.getAllStores
          json       <- ZIO.attempt(ujson.Arr.from(result.map(r => ujson.Str(r))))
        } yield json
    }

  def serve(data: String): RIO[StoreSpaceManagerService, ServerResponse] = {
    val serveStep: ZIO[StoreSpaceManagerService, CommandIssue, Value] =
      for {
        command  <- ZIO.fromEither(CommandParser.serverParse(data))
        response <- execute(command).mapError(e => CommandIssue.GenericError(e))
      } yield response

    serveStep
      .foldZIO(
        {
          case CommandIssue.SyntaxError(reason, line, offset) =>
            ZIO.succeed(ServerResponse.KO(s"syntax error: $reason ($offset): $line"))
          case CommandIssue.GenericError(e) =>
            ZIO.succeed(ServerResponse.KO(s"error: ${e.getMessage}"))
          case CommandIssue.Empty =>
            ZIO.fail(new Exception("unknown error"))
        },
        json => ZIO.succeed(ServerResponse.OK(json))
      )
  }

  def serveKey(key: SelectionKey): RIO[StoreSpaceManagerService, Unit] =
    ZIO.scoped {
      def serveLoop(client: SocketChannel): RIO[StoreSpaceManagerService, Boolean] = {
        def answer(request: String): RIO[StoreSpaceManagerService, Unit] =
          for {
            response <- serve(request)
            _ <- {
              if (response.toString.length > 100)
                Console.printLine(s"sending response: ${response.toString.take(100)}...")
              else
                Console.printLine(s"sending response: ${response.toString}")
            }
            _ <- ZIO.fromTry(network.sendAll(response.toString, client))
          } yield ()

        for {
          data <-
            ZIO
              .fromTry(network.receive(client))
              .map(_.trim)
              .repeat(
                Schedule.spaced(Duration.fromMillis(50))
                  *> Schedule.recurWhile[String](_.isEmpty)
              )
          _ <- Console.printLine(s"received data: $data")
          shouldCloseConnection = data.trim.toUpperCase == network.CloseConnectionMessage
          _ <- answer(data).when(!shouldCloseConnection && data.nonEmpty)
        } yield shouldCloseConnection
      }

      for {
        client <- ZIO.fromAutoCloseable(ZIO.succeed(key.channel().asInstanceOf[SocketChannel]))
        _      <- serveLoop(client).repeatWhileEquals(false)
      } yield ()
    }

  def acceptLoop(nioSelector: Selector): RIO[StoreSpaceManagerService, Unit] =
    for {
      ready <- ZIO.attempt(nioSelector.select(500))
      _ <-
        (for {
          keys <- ZIO.attempt(nioSelector.selectedKeys())
          _ <-
            ZIO.loop(keys.iterator)(_.hasNext, identity) { iter =>
              val key = iter.next()
              iter.remove()

              if (key.isAcceptable) {
                registerKey(nioSelector, key)
              } else if (key.isReadable) {
                serveKey(key)
              } else ZIO.unit
            }
        } yield ()).when(ready > 0)
    } yield ()

}

enum ServerResponse {
  case OK(value: ujson.Value)
  case KO(error: String)

  override def toString: String =
    this match {
      case OK(value) =>
        ujson
          .Obj(
            "status" -> ujson.Str("OK"),
            "value"  -> value
          )
          .toString
      case KO(error) =>
        ujson
          .Obj(
            "status" -> ujson.Str("KO"),
            "value"  -> ujson.Str(error)
          )
          .toString
    }
}
