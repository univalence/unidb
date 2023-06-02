package io.univalence.unidb.db

import scala.annotation.tailrec
import scala.util.Try

import java.net.ConnectException
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets

object network {

  val CloseConnectionMessage = "CLOSE CONNECTION"

  def sendAll(data: String, socket: SocketChannel): Try[Int] = {
    val writeBuffer = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8))
    Try(socket.write(writeBuffer))
  }

  def receive(socket: SocketChannel, bufferSize: Int = 4096): Try[String] = {
    @tailrec
    def receiveLoop(data: String): String =
      val readBuffer = ByteBuffer.allocate(bufferSize)
      val byteRead   = socket.read(readBuffer)
      if (byteRead < 0) throw new ConnectException("connection closed")
      else if (byteRead < bufferSize)
        readBuffer.rewind()
        data + new String(readBuffer.array(), readBuffer.arrayOffset(), byteRead)
      else
        readBuffer.rewind()
        receiveLoop(data + new String(readBuffer.array(), readBuffer.arrayOffset(), byteRead))

    Try(receiveLoop(data = ""))
  }

}
