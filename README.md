# Databas

A relational database system built in Rust for educational
purposes. Still a work in progress.

See also:
[mkdb](https://github.com/antoniosarosi/mkdb),
[simpledb](https://github.com/redixhumayun/simpledb), and
[Turso](https://github.com/tursodatabase/turso).

The workspace is split into storage, relational, SQL parser, and query crates.
`crates/query/` owns planning, optimization, and execution; root `src/` owns
sessions, the TCP protocol, client, and server. See `AGENTS.md` for crate
boundaries and development commands.

```sh
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
```

### Database file format

Format version 14 reserves a common recovery trailer on each 4,096-byte
page. It contains the page incarnation and page LSN, followed by a
little-endian CRC32 over the physical page ID and the preceding 4,092 bytes. A required `.doublewrite` sidecar stages protected page
writes and is bound to the database file's filesystem identity (currently Unix
for real files). Do not delete or copy it as if it were a portable backup.
Older database files are not supported; there is no in-place migration.

Restart recovery analyzes WAL transaction chains, repeats history, and logs
compensation for incomplete transactions. `PageInit` derives a typed empty
page from its WAL record. The allocator reserves pages at initialization but
publishes frees only at a durable transaction outcome. Redo uses verified
page LSNs and incarnations to skip effects already reflected on disk. Data-page
updates log page-local operations instead of full images. Ordinary leaf edits
redo by slot; structural and overflow writes use byte runs.

On existing trees, completed row edits undo by tree incarnation and key. A
split can complete before a row is inserted, and a merge can complete after a
deletion has left a searchable tree. These completed structural actions can
survive transaction or savepoint rollback. An interrupted action is undone
physically; a tree created by an aborted transaction is reclaimed in full.

### Storage simulation

Run the seeded storage workload with engine assertions enabled.

```sh
CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo run --release -p databas-storage --features simulation --bin storage-sim -- --seed 42 --duration 15m
```

The simulator runs until the time limit or the first unexpected failure. Its
clients share the storage engine and keep operating across injected I/O errors
and power losses. Recovery checks the B+-trees against the client transaction
history. Focused crash sweeps also verify interrupted splits and restartable
undo across retained splits and merges. To replay a randomized failure, use
the same build and seed and replace
`--duration 15m` with the reported `--events N`.

`DROP TABLE name;` removes a user table and all of its secondary indexes.
`DROP INDEX name;` removes a secondary index while preserving its table.
Both statements support explicit transactions: `ROLLBACK;` restores the catalog
entries and all tree pages, including overflow data. Committed drops make those
pages available for reuse. Missing names and attempts to drop a system table
return errors. Like other DDL, drops require exclusive database access.
