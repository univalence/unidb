package io.univalence.unidb.db

import scala.util.{Failure, Success, Try}

object tryext {

  extension [A](lt: List[Try[A]])
    def sequence: Try[List[A]] =
      lt.foldLeft(Try(List.empty[A])) {
        case (Success(l), Success(v)) => Success(l :+ v)
        case (Failure(e), _)          => Failure(e)
        case (_, Failure(e))          => Failure(e)
      }
      
  extension [A](lt: Iterator[Try[A]])
    def sequence: Try[List[A]] =
      lt.foldLeft(Try(List.empty[A])) {
        case (Success(l), Success(v)) => Success(l :+ v)
        case (Failure(e), _)          => Failure(e)
        case (_, Failure(e))          => Failure(e)
      }

}
