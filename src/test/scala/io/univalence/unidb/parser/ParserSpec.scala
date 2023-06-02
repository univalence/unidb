package io.univalence.unidb.parser

import zio.test.*

object ParserSpec extends ZIOSpecDefault {
  def spec =
    suite("String Parser")(
      test("should parse string LIMIT ignoring case") {
        val result = Parser.stringConstantIgnoreCase("LIMIT").parse(Input("limit"))
        assertTrue(result == ParseResult.Success("LIMIT", Input("limit", 5)))
      },
      test("should parse string literal") {
        val result = Parser.stringLiteral('"').parse(Input(""""hello world""""))
        assertTrue(result == ParseResult.Success("hello world", Input(""""hello world"""", 13)))
      },
      test("should parse uint") {
        val result = Parser.uint.parse(Input("42"))
        assertTrue(result == ParseResult.Success(42, Input("42", 2)))
      },
      test("should skip spaces") {
        val result = Parser.skipSpaces.parse(Input("  "))
        assertTrue(result == ParseResult.Success((), Input("  ", 2)))
      }
    )
}
