package io.univalence.unidb.command

import io.univalence.unidb.command.StoreCommand.*
import io.univalence.unidb.command.StoreSpaceCommand.*
import io.univalence.unidb.command.StoreType.*
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

  def serialize: String =
    this match {
      case Put(store, key, value) => s"${this.name} ${store.serialize} $key $value"
      case Get(store, key)        => s"${this.name} ${store.serialize} $key"
      case Delete(store, key)     => s"${this.name} ${store.serialize} $key"
      case GetWithPrefix(store, prefix, limit) =>
        s"${this.name} ${store.serialize} $prefix${limit.map(n => s" LIMIT $n").getOrElse("")}"
      case GetFrom(store, key, limit) =>
        s"${this.name} ${store.serialize} $key${limit.map(n => s" LIMIT $n").getOrElse("")}"
      case GetAll(store, limit) =>
        s"${this.name} ${store.serialize}${limit.map(n => s" LIMIT $n").getOrElse("")}"
    }
}
enum StoreSpaceCommand(val name: StoreSpaceCommandName) {
  case OpenStoreSpace(storeSpaceName: String, storeSpaceType: StoreType)
      extends StoreSpaceCommand(StoreSpaceCommandName.OPEN)
  case CloseStoreSpace(storeSpaceName: String) extends StoreSpaceCommand(StoreSpaceCommandName.CLOSE)
  case CreateStore(store: StoreName) extends StoreSpaceCommand(StoreSpaceCommandName.CREATESTORE)
  case GetStore(store: StoreName) extends StoreSpaceCommand(StoreSpaceCommandName.GETSTORE)
  case GetOrCreateStore(store: StoreName) extends StoreSpaceCommand(StoreSpaceCommandName.GETORCREATESTORE)
  case DropStore(store: StoreName) extends StoreSpaceCommand(StoreSpaceCommandName.DROPSTORE)

  def serialize: String =
    this match
      case OpenStoreSpace(storeSpaceName, storeSpaceType) => s"${this.name} $storeSpaceName ${storeSpaceType.serialize}"
      case CloseStoreSpace(storeSpaceName: String)        => s"${this.name} $storeSpaceName"
      case CreateStore(store)                             => s"${this.name} ${store.serialize}"
      case GetStore(store)                                => s"${this.name} ${store.serialize}"
      case GetOrCreateStore(store)                        => s"${this.name} ${store.serialize}"
      case DropStore(store)                               => s"${this.name} ${store.serialize}"
}
enum ShowCommand(val name: ShowCommandName) {
  case StoreSpaces extends ShowCommand(ShowCommandName.SPACES)
  case Stores(storeSpace: String) extends ShowCommand(ShowCommandName.STORES)

  def serialize: String =
    this match {
      case StoreSpaces        => s"SHOW ${this.name}"
      case Stores(storeSpace) => s"SHOW ${this.name} $storeSpace"
    }
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
}
enum StoreSpaceCommandName {
  case OPEN
  case CLOSE
  case SHOW

  case CREATESTORE
  case GETSTORE
  case GETORCREATESTORE
  case DROPSTORE
}

enum ShowCommandName {
  case SPACES
  case STORES
}

enum CLICommandName {
  case QUIT
  case EXIT
  case HELP
}
