package io.univalence.unidb.arg

import io.univalence.unidb.db.StoreName

import java.nio.file.Path

enum ApplicationOption {
  case CliOption(
      storeDir: Option[Path]
  )
  case ServerOption(
      storeDir: Option[Path],
      port:     Option[Int]
  )
  case LoadOption(
      storeDir: Option[Path],
      fromFile: Path,
      keyFields: List[String],
      keyDelim: Option[String],
      store: StoreName
  )
  case DumpOption(
      storeDir: Option[Path]
  )
  case WebOption(
      storeDir: Option[Path],
      port:     Option[Int]
  )
}
