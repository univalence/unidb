package io.univalence.unidb.arg

import io.univalence.unidb.command.RunningMode
import io.univalence.unidb.command.RunningMode.*
import io.univalence.unidb.db.StoreName
import io.univalence.unidb.parser.{Input, Parser, ParseResult}

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

import java.net.InetSocketAddress
import java.nio.file.{Path, Paths}

object ArgParser {
  type ListStringInput     = Input[String]
  type ListStringParser[A] = Parser[String, A]

  def parse(input: Iterable[String]): Try[ApplicationOption] =
    argParser.commit.parse(Input[String](input.toSeq)) match {
      case ParseResult.Success(value, _) => Success(value)
      case ParseResult.Failure(reason, input) =>
        Failure(
          new IllegalArgumentException(s"$reason (parameter #${input.offset} in \"${input.data.mkString(" ")}\")")
        )
      case ParseResult.Error(reason, input) =>
        Failure(
          new IllegalArgumentException(s"$reason (parameter #${input.offset} in \"${input.data.mkString(" ")}\")")
        )
    }

  lazy val argParser: ListStringParser[ApplicationOption] =
    modeParser.flatMap {
      case CLI    => cliArgParser
      case SERVER => serverArgParser
      case WEB    => webArgParser
      case LOAD   => loadArgParser
      case DUMP   => dumpArgParser
    }

  lazy val cliArgParser: ListStringParser[ApplicationOption.CliOption] =
    storeDirParameterParser.optional
      .map(path =>
        ApplicationOption.CliOption(
          storeDir = path.map(_._2)
        )
      )

  lazy val serverArgParser: ListStringParser[ApplicationOption.ServerOption] =
    Parser
      .allOf(
        storeDirParameterParser,
        portParameterParser
      )
      .map(parameters =>
        ApplicationOption.ServerOption(
          storeDir = parameters.get(storeDirParameter).map(_.asInstanceOf[Path]),
          port     = parameters.get(portParameter).map(_.asInstanceOf[Int])
        )
      )

  lazy val webArgParser: ListStringParser[ApplicationOption.WebOption] =
    Parser
      .allOf(
        storeDirParameterParser,
        portParameterParser
      )
      .map(parameters =>
        ApplicationOption.WebOption(
          storeDir = parameters.get(storeDirParameter).map(_.asInstanceOf[Path]),
          port     = parameters.get(portParameter).map(_.asInstanceOf[Int])
        )
      )

  lazy val loadArgParser: ListStringParser[ApplicationOption.LoadOption] =
    Parser
      .allOf(
        storeDirParameterParser,
        pathParser.map(v => fileParameter -> v),
        keyFieldsParameterParser,
        keyDelimParameterParser,
        storeParameterParser
      )
      .flatMap { parameters =>
        val missingParameters = Set(fileParameter, keyFieldsParameter, storeParameter).diff(parameters.keySet)

        if (missingParameters.nonEmpty)
          Parser.fail(s"those parameters are missing: " + missingParameters.mkString(", "))
        else
          Parser(
            ApplicationOption.LoadOption(
              storeDir  = parameters.get(storeDirParameter).map(_.asInstanceOf[Path]),
              fromFile  = parameters(fileParameter).asInstanceOf[Path],
              keyFields = parameters(keyFieldsParameter).asInstanceOf[List[String]],
              keyDelim  = parameters.get(keyDelimParameter).map(_.asInstanceOf[String]),
              store     = parameters(storeParameter).asInstanceOf[StoreName]
            )
          )
      }

  lazy val dumpArgParser: ListStringParser[ApplicationOption.DumpOption] =
    storeDirParameterParser.optional.map(path =>
      ApplicationOption.DumpOption(
        storeDir = path.map(_._2)
      )
    )

  val storeDirParameter  = "store-dir"
  val portParameter      = "port"
  val hostParameter      = "server"
  val fileParameter      = "from"
  val storeParameter     = "to"
  val keyFieldsParameter = "keys"
  val keyDelimParameter  = "key-delim"

  lazy val storeDirParameterParser: Parser[String, (String, Path)] =
    parameterNameParser(storeDirParameter)
      .`then`(pathParser)
      .map(v => storeDirParameter -> v)

  lazy val portParameterParser: Parser[String, (String, Int)] =
    parameterNameParser(portParameter)
      .`then`(uintParser)
      .map(v => portParameter -> v)

  lazy val hostParameterParser: Parser[String, (String, InetSocketAddress)] =
    parameterNameParser(hostParameter)
      .`then`(hostParser)
      .map(v => hostParameter -> v)

  lazy val keyFieldsParameterParser: Parser[String, (String, List[String])] =
    parameterNameParser(keyFieldsParameter)
      .`then`(listParser)
      .map(v => keyFieldsParameter -> v)

  lazy val keyDelimParameterParser: Parser[String, (String, String)] =
    parameterNameParser(keyDelimParameter)
      .`then`(regexParser(".+".r))
      .map(v => keyDelimParameter -> v)

  lazy val storeParameterParser: Parser[String, (String, StoreName)] =
    parameterNameParser(storeParameter)
      .`then`(storeParser)
      .map(v => storeParameter -> v)

  val valueRe: Regex = "[\\d\\w-_.]+".r
  val pathRe: Regex  = "/?([\\d\\w-_.]+/)*[\\d\\w-_.]+".r
  val uintRe: Regex  = "\\d+".r
  val hostRe: Regex  = "[\\d\\w-_.]:\\d+".r
  val storeRe: Regex = "[\\d\\w-_.]\\.[\\d\\w-_.]".r
  val listRe: Regex  = "[^,]+(,[^,]+)*".r

  def regexParser(re: Regex): ListStringParser[String] = { (input: ListStringInput) =>
    if (input.hasNext) {
      val parameter = input.current
      if (re.matches(parameter))
        ParseResult.Success(parameter, input.next)
      else
        ParseResult.Failure(s"$parameter does not match $re", input)
    } else ParseResult.Failure(s"parameter missing", input)
  }

  val listParser: ListStringParser[List[String]] =
    regexParser(listRe)
      .map(_.split(",").toList)
      .mapFailure((_, _) => "list expected")

  val uintParser: ListStringParser[Int] =
    regexParser(uintRe)
      .map(_.toInt)
      .mapFailure((_, _) => "integer expected")

  val storeParser: ListStringParser[StoreName] =
    regexParser(hostRe)
      .map { hp =>
        val l = hp.split(".")
        StoreName(l(0), l(1))
      }
      .mapFailure((_, _) => "store expected")

  val hostParser: ListStringParser[InetSocketAddress] =
    regexParser(hostRe)
      .map { hp =>
        val l = hp.split(":")
        new InetSocketAddress(l(0), l(1).toInt)
      }
      .mapFailure((_, _) => "host expected")

  val pathParser: ListStringParser[Path] =
    regexParser(pathRe)
      .map(Paths.get(_))
      .mapFailure((_, _) => "path expected")

  val valueParser: ListStringParser[String] =
    regexParser(valueRe)
      .mapFailure((_, _) => "value expected")

  val modeParser: ListStringParser[RunningMode] =
    valueParser
      .mapFailure((_, _) =>
        "Expected one of the following mode: " + RunningMode.values.map(_.toString.toLowerCase).mkString(", ")
      )
      .map(_.toUpperCase)
      .flatMap(value =>
        if (RunningMode.values.map(_.toString).contains(value))
          Parser(RunningMode.valueOf(value))
        else
          Parser.error(s"unknown running mode: $value")
      )

  def parameterNameParser(name: String): ListStringParser[ParameterName] = { (input: ListStringInput) =>
    if (input.hasNext) {
      val parameter = input.current
      if (parameter == "--" + name)
        ParseResult.Success(ParameterName(name), input.next)
      else
        ParseResult.Failure(s"parameter name $name expected", input)
    } else ParseResult.Failure(s"parameter name $name expected", input)
  }

  case class ParameterName(name: String)
}
