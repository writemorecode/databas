# Databas

A relational database system built in Rust for educational
purposes. Still a work in progress.

See also:
[mkdb](https://github.com/antoniosarosi/mkdb),
[simpledb](https://github.com/redixhumayun/simpledb), and
[Turso](https://github.com/tursodatabase/turso).

The workspace contains the storage engine, relational layer, SQL parser, and
SQL/session facade. See [architecture and API contracts](docs/architecture.md).

```sh
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
```
