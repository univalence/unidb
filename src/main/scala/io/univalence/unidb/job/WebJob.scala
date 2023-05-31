package io.univalence.unidb.job

import io.univalence.unidb.StoreSpaceManagerService
import io.univalence.unidb.arg.ApplicationOption

import zio.*
import zio.http.*

import java.nio.file.{Path, Paths}

case class WebJob(defaultStoreDir: Path, defaultPort: Int) extends Job[Any, ApplicationOption.WebOption] {

  override def run(option: ApplicationOption.WebOption): RIO[Any, Unit] =
    ZIO.scoped {
      val storeDir = option.storeDir.getOrElse(defaultStoreDir)
      val port     = option.port.getOrElse(defaultPort)

      val program =
        for {
          _ <- Console.printLine(s"serving on port $port")
          _ <- Server.serve(service)
        } yield ()

      program.provide(Server.defaultWithPort(port), StoreSpaceManagerService.layer(storeDir))
    }

  val service: HttpApp[StoreSpaceManagerService, Nothing] =
    Http.collectZIO[Request] {
      case Method.GET -> !! =>
        ZIO.succeed(
          Response.json(
            ujson
              .Obj(
                "status"   -> ujson.Str("OK"),
                "database" -> ujson.Str("UniDB")
              )
              .toString
          )
        )

      case Method.GET -> !! / storeSpaceName / storeName =>
        val program =
          for {
            storeSpace <- StoreSpaceManagerService.getOrCreatePersistent(storeSpaceName)
            store      <- storeSpace.getOrCreateStore(storeName)
            response   <- store.scan()
          } yield Response.json(
            ujson.Arr
              .from(
                response
                  .map(r =>
                    ujson.Obj(
                      "key"       -> ujson.Str(r.key),
                      "value"     -> r.value,
                      "timestamp" -> ujson.Num(r.timestamp)
                    )
                  )
              )
              .toString
          )

        program
          .mapError(e =>
            Response
              .json(
                ujson.Arr
                  .from(
                    ujson.Str(e.getMessage) +: e.getStackTrace.map(s => ujson.Str(s.toString))
                  )
                  .toString
              )
              .withStatus(Status.NotFound)
          )
          .merge

      case request @ Method.POST -> !! / storeSpaceName / storeName =>
        val program =
          for {
            data       <- request.body.asString
            request    <- ZIO.attempt(ujson.read(data))
            key        <- ZIO.attempt(request.obj("key").str)
            value      <- ZIO.attempt(request.obj("value"))
            storeSpace <- StoreSpaceManagerService.getOrCreatePersistent(storeSpaceName)
            store      <- storeSpace.getOrCreateStore(storeName)
            _          <- store.put(key, value)
          } yield Response.json("null")

        program
          .mapError(e =>
            Response
              .json(
                ujson.Arr
                  .from(
                    ujson.Str(e.getMessage) +: e.getStackTrace.map(s => ujson.Str(s.toString))
                  )
                  .toString
              )
              .withStatus(Status.NotFound)
          )
          .merge

    }

}
