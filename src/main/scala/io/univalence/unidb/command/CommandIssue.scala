package io.univalence.unidb.command

enum CommandIssue {
  case SyntaxError(reason: String, line: String, offset: Int)
  case GenericError(e: Throwable)
  case Empty
}
