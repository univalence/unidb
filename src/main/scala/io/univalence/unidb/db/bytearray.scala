package io.univalence.unidb.db

import java.nio.ByteBuffer

object bytearray {
  opaque type ByteArray = Array[Byte]

  object ByteArray {
    def apply(bytes: Array[Byte]): ByteArray = bytes

    def fromInt(value: Int): ByteArray = {
      val buffer = ByteBuffer.allocate(4)
      buffer.putInt(value)
      buffer.rewind()

      val result = ByteArray(buffer.array())
      result
    }

    def fromLong(value: Long): ByteArray = {
      val buffer = ByteBuffer.allocate(8)
      buffer.putLong(value)
      buffer.rewind()

      ByteArray(buffer.array())
    }
  }

  extension (bytes: ByteArray) {
    def toArray: Array[Byte]                   = bytes
    def toBuffer: ByteBuffer                   = ByteBuffer.wrap(bytes)
    def toInt: Int                             = ByteBuffer.wrap(bytes).getInt(0)
    def toLong: Long                           = ByteBuffer.wrap(bytes).getLong(0)
    def append(other: ByteArray): ByteArray    = bytes ++ other
    def subPart(from: Int, to: Int): ByteArray = bytes.slice(from, to)
  }

}
