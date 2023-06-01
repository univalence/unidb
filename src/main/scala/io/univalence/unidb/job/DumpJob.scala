package io.univalence.unidb.job

import io.univalence.unidb.StoreSpaceManagerService
import io.univalence.unidb.arg.ApplicationOption

import zio.*

import java.nio.file.Path

case class DumpJob(defaultStoreDir: Path) extends Job[Any, ApplicationOption.DumpOption] {
  override def run(option: ApplicationOption.DumpOption): RIO[Any, Unit] =
    val storeDir  = option.storeDir.getOrElse(defaultStoreDir)
    val storeName = option.fromStore

    ZIO.scoped {
      (for {
        storeSpace <- StoreSpaceManagerService.getPersistent(storeName.storeSpace)
        store      <- storeSpace.getStore(storeName.store)
        data       <- store.scan()
        _ <-
          ZStream
            .fromIterator(data)
            .run(ZSink.foreach(record => zio.Console.printLine(s"$record")))
      } yield ()).provide(StoreSpaceManagerService.layer(storeDir))
    }

}
