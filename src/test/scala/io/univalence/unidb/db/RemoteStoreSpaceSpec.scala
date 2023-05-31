package io.univalence.unidb.db

import zio.test.*

import scala.util.Failure

import java.net.ConnectException

class RemoteStoreSpaceSpec extends ZIOSpecDefault {
  def spec =
    suite("RemoteStoreSpace")(
      test("should fail when opening remote store space with bad ref") {
        val storeSpace = RemoteStoreSpace("db", "localhost", 10000)
        assertTrue(storeSpace.isFailure)
      }
    )
}
