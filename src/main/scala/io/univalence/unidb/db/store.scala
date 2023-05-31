package io.univalence.unidb.db

import scala.util.Try

trait StoreSpace(val name: String) extends AutoCloseable {
  def createStore(name: String): Try[Store]
  def getStore(name: String): Try[Store]
  def getOrCreateStore(name: String): Try[Store]
  def drop(name: String): Try[Unit]
}

trait Store {
  def put(key: String, value: ujson.Value): Try[Unit]
  def get(key: String): Try[ujson.Value]
  def delete(key: String): Try[Unit]
  def getFrom(key: String): Try[Iterator[Record]]
  def getPrefix(prefix: String): Try[Iterator[Record]]
  def scan(): Try[Iterator[Record]]
}

case class Record(key: String, value: ujson.Value, timestamp: Long, deleted: Boolean)
