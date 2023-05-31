package io.univalence.unidb.db

import io.univalence.unidb.db.bytearray.*

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Success, Try, Using}

import java.io.{FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

class CommitLog(file: Path) {
  locally {
    createIfNotExists().get
  }

  def add(record: CLRecord): Try[Unit] =
    Using(
      new RandomAccessFile(file.toFile, "rwd")
    ) { raf =>
      val f = raf.getChannel

      val prefix: String = {
        f.position(f.size())
        if (f.position() > 0) {
          f.position(f.position() - 1)
          val bufferLF = ByteBuffer.allocate(1)
          f.read(bufferLF)
          val previousChar = bufferLF.rewind().get().toChar
          if (previousChar != '\n') "\n" else ""
        } else ""
      }

      val content           = prefix + CLRecord.toJson(record).toString + "\n"
      val data: Array[Byte] = content.getBytes
      val buffer            = ByteBuffer.wrap(data)

      val lock = f.lock()
      try {
        f.write(buffer)
        raf.getFD.sync()
      } finally lock.release()
    }

  def createIfNotExists(): Try[Boolean] = Try(file.toFile.createNewFile())

  def readAll: Try[List[CLRecord]] =
    if (!Files.exists(file))
      Success(List.empty)
    else
      Using(Source.fromFile(file.toFile)) { f =>
        f.getLines()
          .map(CLRecord.from)
          .toList
      }

  def remove(): Try[Unit] = Try(Files.delete(file))

}

case class CLRecord(key: String, value: ujson.Value, timestamp: Long, deleted: Boolean)
object CLRecord {
  def toJson(record: CLRecord): ujson.Value =
    ujson.Obj(
      "key"       -> ujson.Str(record.key),
      "value"     -> record.value,
      "timestamp" -> ujson.Num(record.timestamp),
      "deleted"   -> ujson.Bool(record.deleted)
    )

  def from(data: String): CLRecord = {
    import ujson.Value.Selector.*

    val json      = ujson.read(data)
    val timestamp = json("timestamp").num.toLong
    val deleted   = json("deleted").bool
    val key       = json("key").str
    val value     = json("value")

    CLRecord(
      key       = key,
      value     = value,
      timestamp = timestamp,
      deleted   = deleted
    )
  }
}
