# Architecture

Databas is a four-crate workspace. Dependencies flow from SQL orchestration to
relational operations to physical storage. The parser is an independent leaf.

| Crate | Owns | Workspace dependencies |
| --- | --- | --- |
| `databas-storage` | Raw byte B+-trees, pages, overflow, disk I/O, cache, WAL, recovery, physical transactions | None |
| `databas-relational` | Values, tuples, logical schemas, catalog persistence, table/index operations, table locks, statement atomicity | Storage |
| `databas-sql-parser` | Lexer, AST, parser, syntax errors | None |
| `databas` | Binding, logical optimization, physical planning, execution, SQL sessions, client/server protocol | Relational, parser; storage re-export and error classification |

Planner and executor stay together because their plan and expression types evolve
together. Networking stays in the root crate for now. There is no shared `core`
crate: `databas::core` only preserves existing imports with re-exports. New code
should import the owning crate or `databas::relational`, `databas::storage`, and
`databas::sql_parser`.

## Storage boundary

`Storage` is the entry point. It exposes file creation/opening, options, tree
creation and cursors, physical transactions, savepoints, rollback, and checkpoint
progress. Runtime, disk manager, WAL, page-cache, and page-format modules are
private. Records expose pinned bytes only through callbacks; owned records are
stable snapshots. Root page identifiers and cursor mutation epochs are explicit
low-level storage concepts.

Physical transactions provide WAL durability and undo. They do not provide
logical isolation: callers must serialize conflicting tree access and keep raw
cursors within the lifetime of their transaction. Relational provides that
coordination through table locks and scoped statement access. Nontransactional
raw tree writes are reserved for exclusive initialization, require explicit
flushing, and do not promise transaction rollback.

`PageId`, `TxnId`, `Lsn`, page sizes, and checkpoint outcomes belong to storage.
Storage errors never refer to SQL types, table schemas, catalog objects, or table
locks. Relational adds its own errors and translates common key/I/O failures for
compatibility, preserving the wrapped storage error for other physical failures.
The server still treats I/O, corruption, and internal failures as fatal.

## Relational boundary

`Database` owns catalog access and the table lock manager. `Database::begin`
returns an `ActiveTransaction` that owns transaction completion and cleanup.
`ActiveTransaction::statement` acquires the requested leases, creates a savepoint,
and supplies a scoped `Transaction` implementing `RelationalAccess` to a callback.
Propagate operation errors from the callback: an error restores the statement's
savepoint while earlier statements remain active. Lock-admission failure aborts
the transaction. DDL requires a DDL statement and its admission gate.

Use `commit`, `rollback`, or `close` to observe completion errors. Drop performs
best-effort rollback; failed explicit `close` is not retried by Drop. A cleanup
failure must be handled as a database failure, as the server does.

Scan associated types borrow the statement access handle. A page-backed scan
cannot escape the callback or survive transaction completion. Owned records may
escape. SQL execution materializes its results before returning, so consuming
SQL output does not retain storage pins or transaction locks.

`CatalogRead` is the planner's catalog interface; `RelationalAccess` is the
executor's operation interface. Neither requires a concrete storage implementation.
Logical schemas contain object identity, names, and columns. Physical root
locations remain in private catalog rows, and operations resolve them internally.
The concrete gateway validates supplied schemas against the catalog before using
their object identities for lock authorization.
The SQL binder translates AST types and literals into relational definitions.

Plans carry logical `IndexValueRange` bounds. Relational owns their comparison in
persisted index order and their encoding into byte keys, including primary-key
suffixes. Planning does not construct or inspect encoded index keys.

## Format and initialization contracts

This refactor does not change the database header, page layout, WAL format,
tuple encoding, index encoding, or system-catalog row encodings. Storage owns
physical format validation; relational owns catalog and tuple interpretation.

The existing relational format reserves pages 1, 2, and 3 for `sys_tables`,
`sys_indexes`, and `sys_columns`; storage's header occupies page 0. Relational
bootstrap uses exclusive access to a header-only file, allocates these roots,
then seeds their rows. Opening validates roots and catalog records. Partial
bootstrap is an error, not an invitation to overwrite an existing database.
Bootstrap remains nontransactional in this format; adding a durable completion
marker or a restartable initialization protocol is separate format/recovery work.

## Tests and features

Run `cargo test --workspace`, `cargo fmt --all --check`, and
`cargo clippy --workspace --all-targets --all-features`. All crates are default
workspace members, so plain `cargo test` also exercises the workspace.

- Storage tests include raw public-API recovery tests without relational or SQL.
- Relational tests exercise direct transactions without SQL, plus catalog, tuple,
  index, and lock behavior.
- Parser round-trip/property tests belong to the parser crate.
- Root tests exercise planning, execution, sessions, protocol, and TCP behavior.

Each concurrent crate owns its small synchronization adapter and bounded Loom
harness. `cfg(all(test, loom))` selects Loom only for that crate's unit tests;
it must not be assumed to propagate through dependencies. Run:

```sh
RUSTFLAGS='--cfg loom' cargo test --workspace --lib loom_tests -- --test-threads=1
```

## API migration

Existing `databas::core`, `databas::sql_parser`, and common database/value imports
remain available. Deliberate API changes are:

- `TableSchema` and `IndexSchema` no longer contain physical root page IDs.
- AST-to-schema and AST-to-value conversion is binder-owned rather than an
  inherent relational method or cross-crate `From` implementation.
- Secondary-index plans and relational scan requests use logical value ranges.
- `RelationalAccess` scan types are generic over the access lifetime.
- Physical storage corruption is wrapped by `RelationalError::Storage`; relational
  corruption has its own diagnostic types. `core::StorageError` remains an alias
  for the relational facade error, while `storage::error::StorageError` is the
  physical error.
