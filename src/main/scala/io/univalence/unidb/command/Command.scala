package io.univalence.unidb.command

import io.univalence.unidb.command.StoreCommand.{DropStore, GetOrCreateStore, GetWithPrefix, Put}
import io.univalence.unidb.command.StoreSpaceCommand.OpenStoreSpace
import io.univalence.unidb.command.StoreType.{InMemory, Persistent}
import io.univalence.unidb.db.StoreName

import java.nio.file.Path

enum StoreCommand(val name: StoreCommandName) {
  case Put(store: StoreName, key: String, value: ujson.Value) extends StoreCommand(StoreCommandName.PUT)
  case Get(store: StoreName, key: String) extends StoreCommand(StoreCommandName.GET)
  case Delete(store: StoreName, key: String) extends StoreCommand(StoreCommandName.DELETE)
  case GetWithPrefix(store: StoreName, prefix: String, limit: Option[Int])
      extends StoreCommand(StoreCommandName.GETPREFIX)
  case GetFrom(store: StoreName, key: String, limit: Option[Int]) extends StoreCommand(StoreCommandName.GETFROM)
  case GetAll(store: StoreName, limit: Option[Int]) extends StoreCommand(StoreCommandName.GETALL)
  case CreateStore(store: StoreName) extends StoreCommand(StoreCommandName.CREATESTORE)
  case GetStore(store: StoreName) extends StoreCommand(StoreCommandName.GETSTORE)
  case GetOrCreateStore(store: StoreName) extends StoreCommand(StoreCommandName.GETORCREATESTORE)
  case DropStore(store: StoreName) extends StoreCommand(StoreCommandName.DROPSTORE)

  def serialize: String =
    this match
      case Put(store, key, value) => s"${this.name} ${store.serialize} $key $value"
      case Get(store, key)        => s"${this.name} ${store.serialize} $key"
      case Delete(store, key)     => s"${this.name} ${store.serialize} $key"
      case GetWithPrefix(store, prefix, limit) =>
        s"${this.name} ${store.serialize} $prefix${limit.map(n => s" LIMIT $n").getOrElse("")}"
      case GetFrom(store, key, limit) =>
        s"${this.name} ${store.serialize} $key${limit.map(n => s" LIMIT $n").getOrElse("")}"
      case GetAll(store, limit) =>
        s"${this.name} ${store.serialize}${limit.map(n => s" LIMIT $n").getOrElse("")}"
      case CreateStore(store)      => s"${this.name} ${store.serialize}"
      case GetStore(store)         => s"${this.name} ${store.serialize}"
      case GetOrCreateStore(store) => s"${this.name} ${store.serialize}"
      case DropStore(store)        => s"${this.name} ${store.serialize}"
}
enum StoreSpaceCommand(val name: StoreSpaceCommandName) {
  case OpenStoreSpace(storeSpaceName: String, storeSpaceType: StoreType)
      extends StoreSpaceCommand(StoreSpaceCommandName.OPEN)

  def serialize: String =
    this match
      case OpenStoreSpace(storeSpaceName, storeSpaceType) => s"${this.name} $storeSpaceName ${storeSpaceType.serialize}"
}
enum CLICommand {
  case Quit
  case Help
}
enum StoreType(val name: StoreTypeName) {
  case InMemory extends StoreType(StoreTypeName.INMEMORY)
  case Persistent extends StoreType(StoreTypeName.PERSISTENT)
  case Remote(host: String, port: Int) extends StoreType(StoreTypeName.REMOTE)

  def serialize: String =
    this match
      case InMemory           => s"${this.name}"
      case Persistent         => s"${this.name}"
      case Remote(host, port) => s"${this.name} $host:$port"
}
enum StoreTypeName {
  case INMEMORY, PERSISTENT, REMOTE
}
enum RunningMode {
  case CLI, SERVER, WEB, LOAD, DUMP
}

enum StoreCommandName {
  case PUT
  case GET
  case DELETE
  case GETALL
  case GETFROM
  case GETPREFIX

  case CREATESTORE
  case GETSTORE
  case GETORCREATESTORE
  case DROPSTORE
}
enum StoreSpaceCommandName {
  case OPEN
}

enum CLICommandName {
  case QUIT
  case EXIT
  case HELP
}
