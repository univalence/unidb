package io.univalence.unidb.client

import ujson.Value

import io.univalence.unidb.db.StoreSpaceManager

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try, Using}

import java.net.{ConnectException, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.file.Paths

@main def client_run(): Unit =
  Using(new StoreSpaceManager(Paths.get("data"))) { manager =>
    val program =
      for {
        storeSpace <- manager.getOrOpenRemote("rdb", "localhost", 19040)
        store      <- storeSpace.getOrCreateStore("table")
        _          <- store.put("123", ujson.Num(12))
      } yield ()

    println(program)
  }.get
