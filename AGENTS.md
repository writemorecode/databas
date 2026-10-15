# Repository Guidelines

## Project Structure & Module Organization
`databas` is a Rust 2024 workspace for a SQLite-inspired embedded relational database.
- `crates/storage/` owns byte-oriented B+-trees, pages, disk I/O, page caching, WAL, recovery, and physical transactions. It has no SQL or relational dependencies.
- `crates/relational/` builds SQL-independent schemas, tuples, catalog, tables, indexes, locking, and scoped transactions on storage.
- `crates/sql-parser/` owns the lexer, parser, AST, and parser property tests, with no engine dependencies.
- The root `src/` owns planning, optimization, execution, sessions, protocol, client, and server. `src/core/` is a compatibility re-export facade, not an implementation layer. Binaries live in `src/bin/`.
Unit tests are colocated with their modules; crate-level integration tests live in each crate's `tests/`, and SQL integration tests live in the root `tests/`. See `docs/architecture.md` for API and format contracts.

## Build, Test, and Development Commands
- `cargo test --workspace`: runs unit, integration, and doc tests. Slow fsync-heavy tests are ignored by default.
- `cargo test --workspace -- --ignored`: runs ignored stress tests.
- `cargo fmt --all --check`: verifies formatting using `rustfmt.toml`.
- `cargo clippy --workspace --all-targets --all-features`: runs lint checks across library, binary, tests, and examples.
- `cargo run --bin client`: starts the TCP database client; `cargo run --bin server` starts the server.

## Coding Style & Naming Conventions
Use standard Rust formatting with the repository rustfmt settings. Keep modules small and domain-oriented. Use `snake_case` for functions, modules, and variables; `CamelCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants. Prefer explicit error types and `Result` propagation over panics outside tests.

## Testing Guidelines
Add unit tests near the behavior they cover and integration tests under `tests/` for cross-module behavior. Name tests after the expected behavior, for example `insert_values_rejects_null_for_non_nullable_columns`. Use `tempfile` for database files and keep deterministic tests in the default `cargo test` set; mark expensive stress tests as ignored.

## Commit & Pull Request Guidelines
Recent commits use imperative, concise subjects such as `Split executor module` and `Route table scans through database`. Follow that style. Pull requests should describe the behavioral change, mention relevant tests run, and call out storage-format, recovery, or public API impacts when applicable.
