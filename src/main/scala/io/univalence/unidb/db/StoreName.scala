package io.univalence.unidb.db

case class StoreName(storeSpace: String, store: String) {
  val serialize = s"$storeSpace.$store"
}
