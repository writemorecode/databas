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

`DROP TABLE name;` removes a user table and all of its secondary indexes.
`DROP INDEX name;` removes a secondary index while preserving its table.
Both statements support explicit transactions: `ROLLBACK;` restores the catalog
entries and all tree pages, including overflow data. Committed drops make those
pages available for reuse. Missing names and attempts to drop a system table
return errors. Like other DDL, drops require exclusive database access.
