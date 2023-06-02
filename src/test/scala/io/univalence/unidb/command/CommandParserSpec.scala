package io.univalence.unidb.command

import io.univalence.unidb.db.StoreName

import zio.test.*

object CommandParserSpec extends ZIOSpecDefault {
  def spec =
    suite("CommandParser CLI")(
      suite("Store space command")(
        test("should open space persistent by default") {
          val result = CommandParser.cliParse("open space")
          assertTrue(result == Right(StoreSpaceCommand.OpenStoreSpace("space", StoreType.Persistent)))
        },
        test("should open space in-memory") {
          val result = CommandParser.cliParse("open space inmemory")
          assertTrue(result == Right(StoreSpaceCommand.OpenStoreSpace("space", StoreType.InMemory)))
        },
        test("should open space remote with host") {
          val result = CommandParser.cliParse("open space remote host:19040")
          assertTrue(result == Right(StoreSpaceCommand.OpenStoreSpace("space", StoreType.Remote("host", 19040))))
        },
        test("should close space") {
          val result = CommandParser.cliParse("close space")
          assertTrue(result == Right(StoreSpaceCommand.CloseStoreSpace("space")))
        },
        test("should create store") {
          val result = CommandParser.cliParse("createstore space.store")
          assertTrue(result == Right(StoreSpaceCommand.CreateStore(StoreName("space", "store"))))
        },
        test("should drop store") {
          val result = CommandParser.cliParse("dropstore space.store")
          assertTrue(result == Right(StoreSpaceCommand.DropStore(StoreName("space", "store"))))
        },
        test("should get store") {
          val result = CommandParser.cliParse("getstore space.store")
          assertTrue(result == Right(StoreSpaceCommand.GetStore(StoreName("space", "store"))))
        },
        test("should get or create store") {
          val result = CommandParser.cliParse("getorcreatestore space.store")
          assertTrue(result == Right(StoreSpaceCommand.GetOrCreateStore(StoreName("space", "store"))))
        }
      ),
      suite("Show command")(
        test("should show spaces") {
          val result = CommandParser.cliParse("show spaces")
          assertTrue(result == Right(ShowCommand.StoreSpaces))
        },
        test("should show stores") {
          val result = CommandParser.cliParse("show stores space")
          assertTrue(result == Right(ShowCommand.Stores("space")))
        }
      ),
      suite("Store command")(
        test("should put") {
          val result = CommandParser.cliParse("put space.store 123 12")
          assertTrue(result == Right(StoreCommand.Put(StoreName("space", "store"), "123", ujson.Num(12))))
        },
        test("should get") {
          val result = CommandParser.cliParse("get space.store 123")
          assertTrue(result == Right(StoreCommand.Get(StoreName("space", "store"), "123")))
        },
        test("should getprefix with no limit") {
          val result = CommandParser.cliParse("getprefix space.store 123")
          assertTrue(result == Right(StoreCommand.GetWithPrefix(StoreName("space", "store"), "123", None)))
        },
        test("should getprefix with limit") {
          val result = CommandParser.cliParse("getprefix space.store 123 limit 100")
          assertTrue(result == Right(StoreCommand.GetWithPrefix(StoreName("space", "store"), "123", Some(100))))
        },
        test("should getfrom with no limit") {
          val result = CommandParser.cliParse("getfrom space.store 123")
          assertTrue(result == Right(StoreCommand.GetFrom(StoreName("space", "store"), "123", None)))
        },
        test("should getfrom with limit") {
          val result = CommandParser.cliParse("getfrom space.store 123 limit 100")
          assertTrue(result == Right(StoreCommand.GetFrom(StoreName("space", "store"), "123", Some(100))))
        },
        test("should getall with no limit") {
          val result = CommandParser.cliParse("getall space.store")
          assertTrue(result == Right(StoreCommand.GetAll(StoreName("space", "store"), None)))
        },
        test("should getall with limit") {
          val result = CommandParser.cliParse("getall space.store limit 100")
          assertTrue(result == Right(StoreCommand.GetAll(StoreName("space", "store"), Some(100))))
        },
        test("should delete") {
          val result = CommandParser.cliParse("delete space.store 123")
          assertTrue(result == Right(StoreCommand.Delete(StoreName("space", "store"), "123")))
        }
      )
    )
}
