package io.univalence.unidb.db

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import java.net.{ConnectException, InetSocketAddress}
import java.nio.channels.{SocketChannel, UnresolvedAddressException}
import java.nio.file.Path

class StoreSpaceManager(baseDir: Path) extends AutoCloseable {
  val storeSpaces: mutable.Map[String, StoreSpace]             = mutable.Map.empty
  val sockets: mutable.Map[(String, Int), SocketChannel]       = mutable.Map.empty
  val remotes: mutable.Map[(String, Int), mutable.Set[String]] = mutable.Map.empty
  val connections: mutable.Map[String, (String, Int)]          = mutable.Map.empty

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
    def getOrOpenSocket(host: String, port: Int) =
      if (!sockets.contains((host, port)))
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
        } yield {
          sockets.update((host, port), socket)
          socket
        }
      else
        Try(sockets((host, port)))

    if (storeSpaces.contains(name))
      Try(storeSpaces(name))
    else
      for {
        socket     <- getOrOpenSocket(host, port)
        storeSpace <- RemoteStoreSpace(name, socket)
      } yield {
        storeSpaces.update(name, storeSpace)
        remotes.getOrElseUpdate((host, port), mutable.Set.empty).add(name)
        connections.update(name, (host, port))
        storeSpace
      }

  def getAllSpaces: Try[Iterator[String]] = Try(storeSpaces.view.keys.iterator)

  def close(name: String): Try[Unit] =
    for {
      storeSpace <- Try(storeSpaces(name))
      _          <- Try(storeSpace.close())
      _          <- Try(storeSpaces.remove(name))
      _ <-
        if (storeSpace.isInstanceOf[RemoteStoreSpace])
          for {
            hostport <- Try(connections(name))
            _ <-
              Try {
                connections.remove(name)
                remotes(hostport).remove(name)
              }
            _ <-
              if (remotes(hostport).isEmpty)
                for {
                  socket <- Try(sockets(hostport))
                  _      <- sendClose(socket)
                  _      <- Try(socket.close())
                  _      <- Try(sockets.remove(hostport))
                } yield ()
              else Success(())
          } yield ()
        else
          Success(())
    } yield ()

  private[db] def sendClose(socket: SocketChannel): Try[Unit] =
    for {
      byteWritten <- network.sendAll(network.CloseConnectionMessage, socket)
      _ <-
        if (byteWritten < 0) {
          Failure(new ConnectException("connection closed"))
        } else {
          Success(())
        }
    } yield ()

  override def close(): Unit =
    for (storeSpace <- storeSpaces.values) storeSpace.close()
    storeSpaces.clear()
}
