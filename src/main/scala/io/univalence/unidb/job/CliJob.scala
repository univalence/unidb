package io.univalence.unidb.job

import org.jline.reader.{EndOfFileException, UserInterruptException}

import io.univalence.unidb.*
import io.univalence.unidb.arg.ApplicationOption
import io.univalence.unidb.command.*

import zio.*

import java.net.ConnectException
import java.nio.channels.UnresolvedAddressException
import java.nio.file.Path

case class CliJob(defaultStoreDir: Path) extends Job[UniDBConsole.Console, ApplicationOption.CliOption] {
  val console: UniDBConsole.Console.type = UniDBConsole.Console

  override def run(option: ApplicationOption.CliOption): RIO[UniDBConsole.Console, Unit] =
    ZIO.scoped {
      val storeDir = option.storeDir.getOrElse(defaultStoreDir)

      replLoop.provideSome[UniDBConsole.Console](
        UniInterpreter.layer,
        StoreSpaceManagerService.layer(storeDir)
      )
    }

  def readCommand(
      prompt: String
  ): ZIO[UniDBConsole.Console, CommandIssue, StoreCommand | StoreSpaceCommand | ShowCommand | CLICommand] =
    console
      .read(prompt)
      .foldZIO(
        {
          case _: EndOfFileException     => ZIO.succeed(CLICommand.Quit)
          case _: UserInterruptException => ZIO.fail(CommandIssue.Empty)
          case e: Throwable =>
            e.printStackTrace()
            ZIO.fail(CommandIssue.GenericError(e))
        },
        line =>
          if (line.trim.isEmpty)
            ZIO.fail(CommandIssue.Empty)
          else
            ZIO.fromEither(CommandParser.cliParse(line))
      )

  def execute(
      command: StoreCommand | StoreSpaceCommand | ShowCommand | CLICommand
  ): ZIO[UniInterpreter.Interpreter, Throwable, RunningState] =
    command match
      case StoreCommand.Put(store, key, value)     => UniInterpreter.Interpreter.put(store, key, value)
      case StoreCommand.Get(store, key)            => UniInterpreter.Interpreter.get(store, key)
      case StoreCommand.Delete(store, key)         => UniInterpreter.Interpreter.delete(store, key)
      case StoreCommand.GetFrom(store, key, limit) => UniInterpreter.Interpreter.getFrom(store, key, limit)
      case StoreCommand.GetWithPrefix(store, prefix, limit) =>
        UniInterpreter.Interpreter.getWithPrefix(store, prefix, limit)
      case StoreCommand.GetAll(store, limit)         => UniInterpreter.Interpreter.getAll(store, limit)
      case StoreSpaceCommand.CreateStore(store)      => UniInterpreter.Interpreter.createStore(store)
      case StoreSpaceCommand.GetStore(store)         => UniInterpreter.Interpreter.createStore(store)
      case StoreSpaceCommand.GetOrCreateStore(store) => UniInterpreter.Interpreter.createStore(store)
      case StoreSpaceCommand.DropStore(store)        => UniInterpreter.Interpreter.dropStore(store)

      case StoreSpaceCommand.OpenStoreSpace(name, storeSpaceType) =>
        UniInterpreter.Interpreter.openStoreSpace(name, storeSpaceType)
      case ShowCommand.StoreSpaces =>
        UniInterpreter.Interpreter.showStoreSpaces()
      case ShowCommand.Stores(storeSpace) =>
        UniInterpreter.Interpreter.showStores(storeSpace)

      case CLICommand.Help => RunningState.ContinueM
      case CLICommand.Quit => RunningState.StopM

  val replStep: ZIO[UniInterpreter.Interpreter with UniDBConsole.Console, CommandIssue, RunningState] =
    for {
      command        <- readCommand(">")
      shouldContinue <- execute(command).mapError(e => CommandIssue.GenericError(e))
    } yield shouldContinue

  val replLoop: RIO[UniInterpreter.Interpreter with UniDBConsole.Console, Unit] =
    replStep
      .foldZIO(
        {
          case CommandIssue.SyntaxError(reason, line, offset) =>
            console.error(s"syntax error: $reason at ($offset): $line") *> RunningState.ContinueM
          case CommandIssue.GenericError(e) =>
            e match {
              case _: IllegalAccessException =>
                console.error(e.getMessage) *> RunningState.ContinueM
              case _: ConnectException =>
                console.error(e.getMessage) *> RunningState.ContinueM
              case _ =>
                ZIO.succeed(e.printStackTrace()) *> RunningState.StopM
            }
          case CommandIssue.Empty =>
            RunningState.ContinueM
        },
        ZIO.succeed _
      )
      .repeatUntil(_ == RunningState.Stop)
      .unit

}

enum RunningState {
  case Continue
  case Stop
}

object RunningState {
  val ContinueM: UIO[RunningState] = ZIO.succeed(RunningState.Continue)
  val StopM: UIO[RunningState]     = ZIO.succeed(RunningState.Stop)
}
