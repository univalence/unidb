package io.univalence.unidb

import org.jline.terminal.Terminal

import io.univalence.unidb.wrapper.ZLineReader

import zio.*

object UniDBConsole {

  object Color {
    val prompt: String    = scala.Console.MAGENTA
    val response: String  = scala.Console.YELLOW
    val error: String     = scala.Console.RED
    val important: String = scala.Console.MAGENTA
    val reset: String     = scala.Console.RESET
  }

  /** Kafka Sh console interface. */
  trait Console {
    def error(message: => Any): Task[Unit]

    def important(message: => Any): Task[Unit]

    def response(message: => Any): Task[Unit]

    def print(message: => Any): Task[Unit]

    def read(prompt: String): Task[String]
  }

  object Console {
    def error(message: => Any): ZIO[Console, Throwable, Unit] = ZIO.serviceWithZIO[Console](_.error(message))

    def important(message: => Any): ZIO[Console, Throwable, Unit] = ZIO.serviceWithZIO[Console](_.important(message))

    def response(message: => Any): ZIO[Console, Throwable, Unit] = ZIO.serviceWithZIO[Console](_.response(message))

    def print(message: => Any): ZIO[Console, Throwable, Unit] = ZIO.serviceWithZIO[Console](_.print(message))

    def read(prompt: String): ZIO[Console, Throwable, String] = ZIO.serviceWithZIO[Console](_.read(prompt))
  }

  /**
   * Kafka Sh console implementation.
   *
   * @param console
   *   ZIO console instance
   * @param terminal
   *   JLine terminal
   */
  case class ConsoleLive(console: zio.Console, terminal: Terminal) extends Console {

    val lineReader = new ZLineReader(terminal)

    override def error(message: => Any): Task[Unit] = console.printLine(s"${Color.error}$message${Color.reset}")

    override def important(message: => Any): Task[Unit] = console.printLine(s"${Color.important}$message${Color.reset}")

    override def response(message: => Any): Task[Unit] = console.printLine(s"${Color.response}$message${Color.reset}")

    override def print(message: => Any): Task[Unit] = console.printLine(s"$message")

    override def read(prompt: String): Task[String] =
      lineReader
        .read(s"${Color.prompt}$prompt${Color.reset} ")
        .map(_.trim)
  }

  val layer: URLayer[zio.Console with Terminal, Console] =
    ZLayer {
      for {
        zconsole <- ZIO.service[zio.Console]
        terminal <- ZIO.service[Terminal]
      } yield ConsoleLive(zconsole, terminal)
    }

}
