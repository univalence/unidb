package io.univalence.unidb.command

import io.univalence.unidb.db.StoreName

import zio.test.*

object CommandParserSpec extends ZIOSpecDefault {
  def spec =
    suite("CommandParser CLI")(
      test("should from getfrom with limit") {
        val result = CommandParser.cliParse("getfrom space.store 123 limit 100")
        assertTrue(result == Right(StoreCommand.GetFrom(StoreName("space", "store"), "123", Some(100))))
      }
    )
}
