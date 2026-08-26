# Databas

A SQLite-inspired relational database system built in Rust for educational
purposes. Still a work in progress.

Databas now runs as a strictly sequential TCP client/server system. A server
hosts one database file and serves one connection and one query at a time.
There is currently no authentication or transport encryption, so use it only
on a trusted network.

## Running

Start a server. It opens or creates the file and completes WAL recovery before
listening:

```sh
cargo run --bin server -- --address 127.0.0.1:5432 main ./main.db
```

Connect the interactive client using the logical database name:

```sh
cargo run --bin client -- --address 127.0.0.1:5432 main
```

Or execute one SQL item:

```sh
cargo run --bin client -- --address 127.0.0.1:5432 -c \
  "CREATE TABLE messages (id INT PRIMARY KEY, body TEXT)" main
```

The server hosts exactly one file. `main` above is an opaque logical name used
by the startup handshake; it is not a path.

See [`docs/wire-protocol.md`](docs/wire-protocol.md) for framing, query/response
sequencing, typed row encoding, error codes, and compatibility rules.

See also:
[mkdb](https://github.com/antoniosarosi/mkdb),
[simpledb](https://github.com/redixhumayun/simpledb), and
[Turso](https://github.com/tursodatabase/turso).
