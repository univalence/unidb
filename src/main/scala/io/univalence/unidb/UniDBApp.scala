package io.univalence.unidb

import org.jline.reader.{EndOfFileException, UserInterruptException}
import org.jline.terminal.{Terminal, TerminalBuilder}

import io.univalence.unidb.arg.{ApplicationOption, ArgParser}
import io.univalence.unidb.command.*
import io.univalence.unidb.command.CommandIssue.Empty
import io.univalence.unidb.job.*

import zio.*
import zio.stream.*

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try, Using}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, NoSuchFileException, Paths}

object UniDBApp extends ZIOAppDefault {

  val defaultStoreDir     = Paths.get("/tmp", "unidb", "data")
  val defaultKeyDelimiter = "#"
  val defaultPort         = 19040
  val defaultWebPort      = 18040

  val helpDisplay: String =
    s"""UniDB <mode> [options...]
       |Usage:
       |  Mode
       |    cli                   Command line interface (interactive) mode
       |    server                Server mode
       |    web                   Web mode
       |    load                  Load data from file
       |    dump                  Dump table in output
       |    help                  Display help
       |  Common
       |  --store-dir <FILE>      Change default storage directory
       |  Server Mode
       |  --port <INT>            Change listening port in server mode
       |  Load Mode
       |  --to <STORE>            Store name to load data to
       |  --from <FILE>           Load JSON file
       |  --key <STRING>          Indicate which field to use as key (mandatory)
       |  --key-delim <CHAR>      Character to use as key delimiter (default: #)
       |""".stripMargin

  override def run = {
    val program =
      (
        for {
          option <- getArgs.flatMap(a => ZIO.fromTry(ArgParser.parse(a)))
          _      <- ZIO.attempt(Files.createDirectories(defaultStoreDir))
          _ <-
            option match
              case o: ApplicationOption.CliOption =>
                val ConsoleLayer: ZLayer[Any, Throwable, UniDBConsole.Console] =
                  (ZLayer.succeed(zio.Console.ConsoleLive)
                    ++ terminalLayer("unidb-cli"))
                    >>> UniDBConsole.layer

                CliJob(defaultStoreDir).run(o).provide(ConsoleLayer)

              case o: arg.ApplicationOption.ServerOption =>
                ServerJob(defaultStoreDir, defaultPort).run(o)

              case o: arg.ApplicationOption.WebOption =>
                WebJob(defaultStoreDir, defaultWebPort).run(o)

              case o: arg.ApplicationOption.LoadOption =>
                LoadJob(defaultStoreDir, defaultKeyDelimiter).run(o)

              case o: arg.ApplicationOption.DumpOption =>
                DumpJob(defaultStoreDir).run(o)
        } yield ()
      ).foldZIO(
        {
          case e: IllegalArgumentException =>
            Console.printLineError(e.getMessage)
          case e: NoSuchFileException =>
            Console.printLineError(e.getMessage)
          case e =>
            ZIO.succeed(e.printStackTrace())
        },
        ZIO.succeed(_)
      )

    program
  }

  def terminalLayer(terminalName: String): TaskLayer[Terminal] = {
    val terminal: Terminal =
      TerminalBuilder
        .builder()
        .name(terminalName)
        .system(true)
        .build()

    ZLayer.scoped {
      ZIO.fromAutoCloseable(ZIO.succeed(terminal))
    }
  }

  enum RunningMode {
    case CLI, SERVER, LOAD, DUMP
  }

}
