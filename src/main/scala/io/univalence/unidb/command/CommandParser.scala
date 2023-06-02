package io.univalence.unidb.command

import io.univalence.unidb.db.StoreName
import io.univalence.unidb.parser.{Input, Parser, ParseResult}
import io.univalence.unidb.parser.Input.StringInput
import io.univalence.unidb.parser.Parser.StringParser

import scala.util.{Failure, Success, Try}

import java.nio.file.{Path, Paths}

object CommandParser {
  import ujson.Readable.*

  def cliParse(input: String): Either[CommandIssue, StoreCommand | StoreSpaceCommand | ShowCommand | CLICommand] =
    cliParser.commit.parse(Input(input)) match {
      case ParseResult.Success(command, _) => Right(command)
      case ParseResult.Failure(reason, input) =>
        Left(CommandIssue.SyntaxError(reason, input.data.mkString, input.offset))
      case ParseResult.Error(reason, input) =>
        Left(CommandIssue.SyntaxError(reason, input.data.mkString, input.offset))
    }

  def serverParse(input: String): Either[CommandIssue, StoreCommand | StoreSpaceCommand | ShowCommand] =
    serverCommandParser.commit.parse(Input(input)) match {
      case ParseResult.Success(command, _) => Right(command)
      case ParseResult.Failure(reason, input) =>
        Left(CommandIssue.SyntaxError(reason, input.data.mkString, input.offset))
      case ParseResult.Error(reason, input) =>
        Left(CommandIssue.SyntaxError(reason, input.data.mkString, input.offset))
    }

  lazy val cliParser: StringParser[StoreCommand | StoreSpaceCommand | ShowCommand | CLICommand] =
    def getParserFrom(
        command: StoreCommandName | StoreSpaceCommandName | CLICommandName
    ): StringParser[StoreCommand | StoreSpaceCommand | ShowCommand | CLICommand] =
      command match {
        case StoreCommandName.GET       => getParser
        case StoreCommandName.PUT       => putParser
        case StoreCommandName.DELETE    => deleteParser
        case StoreCommandName.GETFROM   => getFromParser
        case StoreCommandName.GETPREFIX => getPrefixParser
        case StoreCommandName.GETALL    => getAllParser

        case StoreSpaceCommandName.CREATESTORE => commandStoreNameParser.map(StoreSpaceCommand.CreateStore.apply)
        case StoreSpaceCommandName.GETSTORE    => commandStoreNameParser.map(StoreSpaceCommand.GetStore.apply)
        case StoreSpaceCommandName.GETORCREATESTORE =>
          commandStoreNameParser.map(StoreSpaceCommand.GetOrCreateStore.apply)
        case StoreSpaceCommandName.DROPSTORE => commandStoreNameParser.map(StoreSpaceCommand.DropStore.apply)

        case StoreSpaceCommandName.OPEN  => openStoreSpaceParser
        case StoreSpaceCommandName.CLOSE => closeStoreSpaceParser
        case StoreSpaceCommandName.SHOW  => showParser

        case CLICommandName.QUIT => Parser(CLICommand.Quit)
        case CLICommandName.EXIT => Parser(CLICommand.Quit)
        case CLICommandName.HELP => Parser(CLICommand.Help)
      }

    for {
      _       <- Parser.skipSpaces
      command <- storeCommandParser.orElse(storeSpaceCommandParser).orElse(cliCommandParser)
      _       <- Parser.skipSpaces
      s       <- getParserFrom(command)
    } yield s

  lazy val serverCommandParser: StringParser[StoreCommand | StoreSpaceCommand | ShowCommand] =
    for {
      command <-
        for {
          _       <- Parser.skipSpaces
          command <- storeCommandParser.orElse(storeSpaceCommandParser)
          _       <- Parser.skipSpaces
        } yield command

      s <-
        command match {
          case StoreCommandName.GET       => getParser
          case StoreCommandName.PUT       => putParser
          case StoreCommandName.DELETE    => deleteParser
          case StoreCommandName.GETFROM   => getFromParser
          case StoreCommandName.GETPREFIX => getPrefixParser
          case StoreCommandName.GETALL    => getAllParser

          case StoreSpaceCommandName.CREATESTORE => commandStoreNameParser.map(StoreSpaceCommand.CreateStore.apply)
          case StoreSpaceCommandName.GETSTORE    => commandStoreNameParser.map(StoreSpaceCommand.GetStore.apply)
          case StoreSpaceCommandName.GETORCREATESTORE =>
            commandStoreNameParser.map(StoreSpaceCommand.GetOrCreateStore.apply)
          case StoreSpaceCommandName.DROPSTORE => commandStoreNameParser.map(StoreSpaceCommand.DropStore.apply)

          case StoreSpaceCommandName.OPEN  => openStoreSpaceParser
          case StoreSpaceCommandName.CLOSE => closeStoreSpaceParser
          case StoreSpaceCommandName.SHOW  => showParser
        }
    } yield s

  val identifier: StringParser[String] = { (input: StringInput) =>
    var i = input
    var w = ""
    if (i.hasNext && (i.current.isLetter || i.current == '_')) {
      w += i.current
      i = i.next
      while (i.hasNext && (i.current.isLetterOrDigit || i.current == '_' || i.current == '-')) {
        w += i.current
        i = i.next
      }
      if (w.nonEmpty)
        ParseResult.Success(w, i)
      else
        ParseResult.Failure("identifier expected", input)
    } else ParseResult.Failure("identifier expected", input)
  }

  val word: StringParser[String] = { (input: StringInput) =>
    var i = input
    var w = ""
    while (i.hasNext && i.current.isLetter) {
      w += i.current
      i = i.next
    }

    if (w.nonEmpty)
      ParseResult.Success(w, i)
    else
      ParseResult.Failure("word expected", input)
  }

  val keyParser: StringParser[String] = { (input: StringInput) =>
    var i = input
    var w = ""
    while (i.hasNext && !i.current.isSpaceChar) {
      w += i.current
      i = i.next
    }

    if (w.nonEmpty)
      ParseResult.Success(w, i)
    else
      ParseResult.Failure("key expected", input)
  }

  val fileNameParser: StringParser[String] = { (input: StringInput) =>
    var i = input
    var w = ""
    while (i.hasNext && i.current != '/') {
      w += i.current
      i = i.next
    }

    if (w.nonEmpty)
      ParseResult.Success(w, i)
    else
      ParseResult.Failure("key expected", input)
  }

  val pathParser: StringParser[Path] =
    val parser =
      for {
        root  <- Parser.char('/').optional.map(_.map(_.toString).getOrElse(""))
        first <- fileNameParser
        next  <- Parser.char('/').`then`(fileNameParser).repeat
      } yield Paths.get(root + first, next: _*)

    parser.or(Parser.char('/').map(_ => Paths.get("/")))

  val storeNameParser: StringParser[StoreName] =
    for {
      storeSpace <- identifier
      _          <- Parser.char('.')
      store      <- identifier
    } yield StoreName(storeSpace = storeSpace, store = store)

  val storeCommandParser: StringParser[StoreCommandName] =
    word
      .map(_.toUpperCase)
      .flatMap(name =>
        Try(StoreCommandName.valueOf(name)) match {
          case Success(value) => Parser(value)
          case Failure(_)     => Parser.fail(s"unknown command $name")
        }
      )

  val storeSpaceCommandParser: StringParser[StoreSpaceCommandName] =
    word
      .map(_.toUpperCase)
      .flatMap(name =>
        Try(StoreSpaceCommandName.valueOf(name)) match {
          case Success(value) => Parser(value)
          case Failure(_)     => Parser.fail(s"unknown command $name")
        }
      )

  val cliCommandParser: StringParser[CLICommandName] =
    word
      .map(_.toUpperCase)
      .flatMap(name =>
        Try(CLICommandName.valueOf(name)) match {
          case Success(value) => Parser(value)
          case Failure(_)     => Parser.fail(s"unknown command $name")
        }
      )

  val getParser: StringParser[StoreCommand.Get] =
    for {
      _     <- Parser.skipSpaces
      table <- storeNameParser
      _     <- Parser.skipSpaces
      key   <- keyParser
      _     <- Parser.skipSpaces
    } yield StoreCommand.Get(table, key)

  val getPrefixParser: StringParser[StoreCommand.GetWithPrefix] =
    for {
      _      <- Parser.skipSpaces
      table  <- storeNameParser
      _      <- Parser.skipSpaces
      prefix <- keyParser
      _      <- Parser.skipSpaces
      limit <-
        Parser
          .stringConstantIgnoreCase("LIMIT")
          .`then`(Parser.skipSpaces)
          .`then`(Parser.uint)
          .flatMap(l => Parser.skipSpaces.map(_ => l))
          .optional
    } yield StoreCommand.GetWithPrefix(table, prefix, limit)

  val getAllParser: StringParser[StoreCommand.GetAll] =
    for {
      _     <- Parser.skipSpaces
      table <- storeNameParser
      _     <- Parser.skipSpaces
      limit <-
        Parser
          .stringConstantIgnoreCase("LIMIT")
          .`then`(Parser.skipSpaces)
          .`then`(Parser.uint)
          .flatMap(l => Parser.skipSpaces.map(_ => l))
          .optional
    } yield StoreCommand.GetAll(table, limit)

  val getFromParser: StringParser[StoreCommand.GetFrom] =
    for {
      _     <- Parser.skipSpaces
      table <- storeNameParser
      _     <- Parser.skipSpaces
      key   <- keyParser
      _     <- Parser.skipSpaces
      limit <-
        Parser
          .stringConstantIgnoreCase("LIMIT")
          .`then`(Parser.skipSpaces)
          .`then`(Parser.uint)
          .flatMap(l => Parser.skipSpaces.map(_ => l))
          .optional
    } yield StoreCommand.GetFrom(table, key, limit)

  val deleteParser: StringParser[StoreCommand.Delete] =
    for {
      _     <- Parser.skipSpaces
      table <- storeNameParser
      _     <- Parser.skipSpaces
      key   <- keyParser
      _     <- Parser.skipSpaces
    } yield StoreCommand.Delete(table, key)

  val putParser: StringParser[StoreCommand.Put] =
    for {
      _        <- Parser.skipSpaces
      table    <- storeNameParser
      _        <- Parser.skipSpaces
      key      <- keyParser
      _        <- Parser.skipSpaces
      rawValue <- Parser.any
    } yield {
      val value = ujson.read(rawValue)
      StoreCommand.Put(table, key, value)
    }

  val commandStoreNameParser: StringParser[StoreName] =
    for {
      _     <- Parser.skipSpaces
      store <- storeNameParser
      _     <- Parser.skipSpaces
    } yield store

  val showParser: StringParser[ShowCommand] =
    for {
      _ <- Parser.skipSpaces
      command <-
        word
          .map(_.toUpperCase)
          .flatMap { optionName =>
            if (optionName == ShowCommandName.SPACES.toString)
              Parser(ShowCommand.StoreSpaces)
            else if (optionName == ShowCommandName.STORES.toString)
              for {
                _          <- Parser.skipSpaces
                storeSpace <- identifier
              } yield ShowCommand.Stores(storeSpace)
            else
              Parser.fail(s"unknown SHOW option $optionName")
          }
      _ <- Parser.skipSpaces
    } yield command

  val closeStoreSpaceParser: StringParser[StoreSpaceCommand.CloseStoreSpace] =
    for {
      _          <- Parser.skipSpaces
      storeSpace <- identifier
      _          <- Parser.skipSpaces
    } yield StoreSpaceCommand.CloseStoreSpace(storeSpace)

  val openStoreSpaceParser: StringParser[StoreSpaceCommand] =
    for {
      _          <- Parser.skipSpaces
      storeSpace <- identifier
      command <-
        Parser.skipSpaces
          .`then`(
            word
              .map(_.toUpperCase)
              .flatMap { optionName =>
                if (optionName == StoreTypeName.INMEMORY.toString)
                  Parser(StoreSpaceCommand.OpenStoreSpace(storeSpace, StoreType.InMemory))
                else if (optionName == StoreTypeName.PERSISTENT.toString)
                  Parser(StoreSpaceCommand.OpenStoreSpace(storeSpace, StoreType.Persistent))
                else if (optionName == StoreTypeName.REMOTE.toString)
                  for {
                    _    <- Parser.skipSpaces
                    host <- word
                    _    <- Parser.char(':')
                    port <- Parser.uint
                  } yield StoreSpaceCommand.OpenStoreSpace(storeSpace, StoreType.Remote(host, port))
                else
                  Parser.fail(s"unknown option $optionName")
              }
          )
          .optional
          .map(_.getOrElse(StoreSpaceCommand.OpenStoreSpace(storeSpace, StoreType.Persistent)))
      _ <- Parser.skipSpaces
    } yield command

}
