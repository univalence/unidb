package io.univalence.unidb.job

import io.univalence.unidb.arg.ApplicationOption

import zio.RIO

trait Job[-R, O <: ApplicationOption] {
  def run(option: O): RIO[R, Unit]
}
