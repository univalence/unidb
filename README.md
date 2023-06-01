# UniDB

Toy key-value store.

## Features

* key-value store: UniDB manages key-value store. A key-value is like a
  hash table or a dictionary.
* in-memory or persistent: UniDB kv stores can be sored in memory only
  (they are volatile) or on a disk (they are persistent).
* records sorted by key
* find records by prefix
* find from a lower boundary
* local or remote access (TCP or WEB)
* load data from CSV
* a single application for everything

## Store

A store is a named set of records. A record is a key-value pair. A
store belongs to a store space.

Keys are string. Values are JSON document.

Operations:
* `PUT <store> <key> <value>`
* `GET <store> <key>`
* `DELETE <store> <key>`
* `GETPREFIX <store> <prefix> [LIMIT <int>]`
* `GETFROM <store> <key> [LIMIT <int>]`
* `GETALL <store> [LIMIT <int>]`

Where `<store>` is a pair `<store-space-name>.<store-name>`, `<prefix>`
is a key prefix.

## Store space

A store space is a context in which you manage a set of stores on the
same support (in-memory, persistent, or remote).
