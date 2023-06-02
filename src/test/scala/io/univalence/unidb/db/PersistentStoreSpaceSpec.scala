package io.univalence.unidb.db

import zio.test.*

import scala.util.{Failure, Try}
import java.nio.file.{Files, NoSuchFileException}

object PersistentStoreSpaceSpec extends ZIOSpecDefault {

  def spec =
    suite("PersistentStoreSpace")(
      test("should not create store space") {
        val name    = "store-space"
        val baseDir = Files.createTempDirectory("unidb-storage").toAbsolutePath

        val Failure(e) = PersistentStoreSpace(name, baseDir, shouldCreate = false)

        val message = s"store space=$name: not found in base directory not found: $baseDir"
        assertTrue(e.getMessage == message)
      }
    )

}
