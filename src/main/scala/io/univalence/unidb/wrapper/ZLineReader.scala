package io.univalence.unidb.wrapper

import org.jline.reader.impl.history.DefaultHistory
import org.jline.reader.{EndOfFileException, LineReader, LineReaderBuilder, UserInterruptException}
import org.jline.terminal.{Terminal, TerminalBuilder}
import zio.*

import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

class ZLineReader(terminal: Terminal)
    extends ZWrapped(
      LineReaderBuilder
        .builder()
        .terminal(terminal)
        .history(new DefaultHistory())
        .build()
    ) {
  def read(prompt: String): Task[String] = execute(_.readLine(prompt))
}
