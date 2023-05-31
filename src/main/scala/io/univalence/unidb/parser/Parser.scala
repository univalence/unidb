package io.univalence.unidb.parser

import io.univalence.unidb.parser.Input.StringInput

import scala.collection.mutable.ArrayBuffer

trait Parser[I, +A] {
  def parse(input: Input[I]): ParseResult[I, A]

  def flatMap[B](f: A => Parser[I, B]): Parser[I, B] =
    (input: Input[I]) => parse(input).flatMapS((a, input1) => f(a).parse(input1))

  def flatMapS[B](f: (A, Input[I]) => Parser[I, B]): Parser[I, B] =
    (input: Input[I]) => parse(input).flatMapS((a, input1) => f(a, input1).parse(input1))

  def map[B](f: A => B): Parser[I, B] = (input: Input[I]) => parse(input).map(f)

  def mapFailure(f: (String, Input[I]) => String): Parser[I, A] = (input: Input[I]) => parse(input).mapFailure(f)

  def mapError(f: (String, Input[I]) => String): Parser[I, A] = (input: Input[I]) => parse(input).mapError(f)

  def andThen[B](p: Parser[I, B]): Parser[I, (A, B)] =
    for {
      a <- this
      b <- p
    } yield (a, b)

  def `then`[B](p: Parser[I, B]): Parser[I, B] =
    for {
      _ <- this
      b <- p
    } yield b

  def `with`[B](p: Parser[I, B]): Parser[I, (A, B)] = this.andThen(p).or(p.andThen(this).map(_.swap))

  def commit: Parser[I, A] = (input: Input[I]) => parse(input).commit

  def optional: Parser[I, Option[A]] = { (input: Input[I]) =>
    parse(input) match {
      case ParseResult.Success(value, input1) => ParseResult.Success(Some(value), input1)
      case ParseResult.Failure(_, _)          => ParseResult.Success(None, input)
      case ParseResult.Error(reason, input1)  => ParseResult.Error(reason, input1)
    }
  }

  def repeat: Parser[I, List[A]] = { (input: Input[I]) =>
    val values = ArrayBuffer.empty[A]
    var result = parse(input)

    while (result.isSuccess) {
      result match {
        case ParseResult.Success(value, _) => values.append(value)
        case _                             => ()
      }

      result = parse(result.input)
    }

    ParseResult.Success(values.toList, result.input)
  }

  def or[A1 >: A](p: Parser[I, A1]): Parser[I, A1] = { (input: Input[I]) =>
    parse(input) match {
      case ParseResult.Failure(_, _)          => p.parse(input)
      case ParseResult.Success(value, input1) => ParseResult.Success(value, input1)
      case ParseResult.Error(reason, input1)  => ParseResult.Error(reason, input1)
    }
  }

  def orElse[B](p: Parser[I, B]): Parser[I, A | B] = { (input: Input[I]) =>
    parse(input) match {
      case ParseResult.Failure(_, _)          => p.parse(input)
      case ParseResult.Success(value, input1) => ParseResult.Success(value, input1)
      case ParseResult.Error(reason, input1)  => ParseResult.Error(reason, input1)
    }
  }
}
object Parser {
  type StringParser[+A] = Parser[Char, A]

  def apply[I, A](a: => A): Parser[I, A] = (input: Input[I]) => ParseResult.Success(a, input)

  val char: StringParser[Char] = { (input: StringInput) =>
    if (input.hasNext)
      ParseResult.Success(input.current, input.next)
    else
      ParseResult.Failure(s"char expected", input)
  }

  def char(c: Char): StringParser[Char] = { (input: StringInput) =>
    if (input.hasNext && input.current == c)
      ParseResult.Success(c, input.next)
    else if (input.hasNext)
      ParseResult.Failure(s"Found: ${input.current} - Expected: $c", input)
    else
      ParseResult.Failure(s"Found: (end of input) - Expected: $c", input)
  }

  val uint: StringParser[Int] = { (input: StringInput) =>
    var i     = input
    var value = ""
    while (i.hasNext && i.current.isDigit) {
      value += i.current
      i = i.next
    }

    if (value.nonEmpty)
      ParseResult.Success(value.toInt, i)
    else
      ParseResult.Failure("Integer expected", input)
  }

  val any: StringParser[String] = { (input: StringInput) =>
    ParseResult.Success(
      input.data.drop(input.offset).mkString,
      input.copy(offset = input.data.length)
    )
  }

  private def shouldContinue(input: StringInput, delimiter: Char): Boolean =
    (
      (input.hasNext && input.current != delimiter)
        || (input.hasNext && input.current == delimiter && input.next.hasNext && input.next.current == delimiter)
    )

  def stringConstantIgnoreCase(value: String): StringParser[String] = { (input: StringInput) =>
    if (!input.hasNext || input.current.toUpper != value(0).toUpper) ParseResult.Failure(s"$value expected", input)
    else {
      var i     = input.next
      var index = 1
      while (i.hasNext && index < value.length && i.current.toUpper == value(index).toUpper) {
        index += 1
        i = i.next
      }

      if (index < value.length)
        ParseResult.Failure(s"$value expected", input)
      else
        ParseResult.Success(value, i)
    }
  }

  def stringLiteral(delimiter: Char): StringParser[String] = { (input: StringInput) =>
    if (!input.hasNext) ParseResult.Failure("String expected", input)
    else if (input.current != delimiter) ParseResult.Failure(s"String should starts with \"$delimiter\"", input)
    else {
      var i = input.next
      var s = ""
      while (shouldContinue(i, delimiter)) {
        s += i.current
        if (i.current == delimiter && i.next.hasNext && i.next.current == delimiter)
          i = i.next.next
        else
          i = i.next
      }

      if (!i.hasNext || i.current != delimiter) {
        ParseResult.Failure(s"String should ends with \"$delimiter\"", input)
      } else
        ParseResult.Success(s, i.next)
    }
  }

  def skipSpaces: StringParser[Unit] = { (input: StringInput) =>
    var i = input

    while (i.hasNext && i.current.isSpaceChar)
      i = i.next

    ParseResult.Success((), i)
  }

  def fail[I, A](message: String): Parser[I, A] = (input: Input[I]) => ParseResult.Failure(message, input)

  def error[I, A](message: String): Parser[I, A] = (input: Input[I]) => ParseResult.Error(message, input)

  def oneOf[I](parser: Parser[I, (String, Any)], parsers: Parser[I, (String, Any)]*): Parser[I, (String, Any)] =
    parsers.prepended(parser).reduce(_ or _)

  def allOf[I](parser: Parser[I, (String, Any)], parsers: Parser[I, (String, Any)]*): Parser[I, Map[String, Any]] =
    oneOf(parser, parsers: _*).repeat.map(_.toMap)

}
