# UniDB

Toy and polyvalent key-value store management system.

## Features

* key-value store: UniDB manages key-value store. A key-value is like a
  hash table or a dictionary.
* in-memory or persistent: UniDB kv stores can be sored in memory only
  (they are volatile) or on a disk (they are persistent).
* records sorted by key
* JSON value storage
* find records by prefix
* find from a lower boundary
* local or remote access (TCP or WEB)
* load data from CSV
* a single application for everything

### Roadmap

* Make it works in cluster (consensus management)
* Include TTL
* Load data from JSON

## Install

In the release section, download the UniDB ZIP file.
Then, unzip the file.

## Run

It is possible to use UniDB with or without a server.
With no server, UniDB uses directly the file system to store data, in
case of persistent store space.

In the UniDB root directory, to run the CLI client.

```shell
$ ./bin/unidb cli
> open myspace
> put myspace.mytable abc {"id":"abc","name":"jon","age":32}
> get myspace.mytable abc
abc -> {"id":"abc","name":"jon","age":32}
```

### Server mode

To run UniDB in server mode (optional).

```shell
$ ./bin/unidb server
```

The server default port is 19040.

```shell
$ ./bin/unidb cli
> open myspace remote localhost:19040
> put myspace.mytable abc {"id":"abc","name":"jon","age":32}
> get myspace.mytable abc
abc -> {"id":"abc","name":"jon","age":32}
> close myspace
```

### Bulk load

To load a CSV file into UniDB.

```shell
$ ./bin/unidb load --from mydata.csv --to myspace.mytable --keys id,name
```

THe CSV file should have a header line, and each line should represent
a whole record.

The default key delimiter used is "#".

### Dump

```shell
$ ./bin/unidb load --from myspace.mytable
```

### WEB mode

To run UniDB in WEB server mode (experimental).

```shell
$ ./bin/unidb web
```

The WEB API is available on port 18040.

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

Operations:
* `OPEN <store-space> [INMEMORY | PRESISTENT (default) | REMOTE &lt;host:port>]`
* `CLOSE <store-space>`
* `CREATESTORE <store>`
* `GETSTORE <store>`
* `GETORCREATESTORE <store>`
* `DROPSTORE <store>`
* `SHOW SPACES`
* `SHOW STORES <store-space>`
