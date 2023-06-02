package io.univalence.unidb.db

import ujson.Value

import io.univalence.unidb.command.{StoreCommand, StoreSpaceCommand, StoreType}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import java.net.{ConnectException, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel, UnresolvedAddressException}
import java.nio.charset.StandardCharsets

class RemoteStoreSpace private[db] (name: String, socket: SocketChannel) extends StoreSpace(name) { storeSpace =>
  val statusField            = "status"
  val valueField             = "value"
  val storeSpaceName: String = storeSpace.name

  val stores: mutable.Map[String, RemoteStore] = mutable.Map.empty

  override def createStore(name: String): Try[Store] =
    for {
      data     <- send(StoreSpaceCommand.CreateStore(StoreName(storeSpaceName, name)).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      store <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try {
            val store = new RemoteStore(name, this)
            stores.update(name, store)
            store
          }
    } yield store

  override def getStore(name: String): Try[Store] =
    for {
      data     <- send(StoreSpaceCommand.GetStore(StoreName(storeSpaceName, name)).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      store <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try {
            stores(name)
          }
    } yield store

  override def getOrCreateStore(name: String): Try[Store] =
    for {
      data     <- send(StoreSpaceCommand.GetOrCreateStore(StoreName(storeSpaceName, name)).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      store <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try {
            if (!stores.contains(name)) {
              val store = new RemoteStore(name, this)
              stores.update(name, store)
            }
            stores(name)
          }
    } yield store

  override def drop(name: String): Try[Unit] =
    for {
      data     <- send(StoreSpaceCommand.DropStore(StoreName(storeSpaceName, name)).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      _ <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Success(())
    } yield ()

  override def close(): Unit = sendClose().get

  private[db] def send(request: String): Try[ujson.Value] =
    for {
      byteWritten <- network.sendAll(request, socket)
      receivedData <-
        if (byteWritten < 0)
          Failure(new ConnectException("connection closed"))
        else
          network.receive(socket)
      json <- Try(ujson.read(receivedData))
    } yield json

  private[db] def sendClose(): Try[Unit] =
    for {
      byteWritten <- network.sendAll("CLOSE", socket)
      _ <-
        if (byteWritten < 0) {
          Failure(new ConnectException("connection closed"))
        } else {
          Success(())
        }
    } yield ()

}

object RemoteStoreSpace {

  def apply(name: String, host: String, port: Int): Try[RemoteStoreSpace] =
    for {
      socket <-
        Try(SocketChannel.open(new InetSocketAddress(host, port)))
          .transform(
            Try.apply(_),
            {
              case _: UnresolvedAddressException =>
                Failure(new ConnectException(s"Unresolvable remote $host:$port"))
              case e => Failure(e)
            }
          )
      remote   <- Try(new RemoteStoreSpace(name, socket))
      data     <- remote.send(StoreSpaceCommand.OpenStoreSpace(name, StoreType.Persistent).serialize)
      response <- Try(data.obj)
      status   <- Try(response("status").str)
      value    <- Try(response.value)
      _ <-
        if (status == "KO")
          Failure(new ConnectException(value.toString()))
        else
          Success(())
    } yield remote

}

class RemoteStore private[db] (name: String, storeSpace: RemoteStoreSpace) extends Store {
  val statusField            = "status"
  val valueField             = "value"
  val storeSpaceName: String = storeSpace.name

  override def put(key: String, value: Value): Try[Unit] =
    for {
      data     <- storeSpace.send(StoreCommand.Put(StoreName(storeSpaceName, name), key, value).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      _ <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Success(())
    } yield ()

  override def delete(key: String): Try[Unit] =
    for {
      data     <- storeSpace.send(StoreCommand.Delete(StoreName(storeSpaceName, name), key).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      _ <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Success(())
    } yield ()

  override def get(key: String): Try[Value] =
    for {
      data     <- storeSpace.send(StoreCommand.Get(StoreName(storeSpaceName, name), key).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      result <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try(value)
    } yield result

  override def getFrom(key: String): Try[Iterator[Record]] =
    for {
      data     <- storeSpace.send(StoreCommand.GetFrom(StoreName(storeSpaceName, name), key, None).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      result <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try(
            value.arr
              .map(obj =>
                Record(
                  key       = obj.obj("key").str,
                  value     = obj.obj("value"),
                  timestamp = obj.obj("timestamp").num.toLong,
                  deleted   = false
                )
              )
              .iterator
          )
    } yield result

  override def getPrefix(prefix: String): Try[Iterator[Record]] =
    for {
      data     <- storeSpace.send(StoreCommand.GetWithPrefix(StoreName(storeSpaceName, name), prefix, None).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      result <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try(
            value.arr
              .map(obj =>
                Record(
                  key       = obj.obj("key").str,
                  value     = obj.obj("value"),
                  timestamp = obj.obj("timestamp").num.toLong,
                  deleted   = false
                )
              )
              .iterator
          )
    } yield result

  override def scan(): Try[Iterator[Record]] =
    for {
      data     <- storeSpace.send(StoreCommand.GetAll(StoreName(storeSpaceName, name), None).serialize)
      response <- Try(data.obj)
      status   <- Try(response(statusField).str)
      value    <- Try(response(valueField))
      result <-
        if (status == "KO")
          Failure(new ConnectException(value.toString))
        else
          Try(
            value.arr
              .map(obj =>
                Record(
                  key       = obj.obj("key").str,
                  value     = obj.obj("value"),
                  timestamp = obj.obj("timestamp").num.toLong,
                  deleted   = false
                )
              )
              .iterator
          )
    } yield result

}
