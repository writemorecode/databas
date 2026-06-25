# Repository Guidelines

## Project Structure & Module Organization
`databas` is a Rust 2024 crate for a SQLite-inspired embedded relational database. Source lives in `src/`: `core/` contains storage, paging, B-tree, WAL, recovery, and catalog code; `sql_parser/`, `planner/`, `executor/`, and `session.rs` cover query flow; `main.rs` starts the interactive client. Integration tests live in `tests/`, while focused unit tests are colocated in module files such as `src/core/btree/tests.rs`. Examples live in `examples/`.

## Build, Test, and Development Commands
- `cargo test`: runs unit, integration, and doc tests. Slow fsync-heavy tests are ignored by default.
- `cargo test -- --ignored`: runs ignored stress tests.
- `cargo fmt --check`: verifies formatting using `rustfmt.toml`.
- `cargo clippy --all-targets --all-features`: runs lint checks across library, binary, tests, and examples.
- `cargo run`: starts the local database client.
- `cargo run --example table_index_query`: runs the table/index query example.

## Coding Style & Naming Conventions
Use standard Rust formatting with the repository rustfmt settings. Keep modules small and domain-oriented. Use `snake_case` for functions, modules, and variables; `CamelCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants. Prefer explicit error types and `Result` propagation over panics outside tests.

## Testing Guidelines
Add unit tests near the behavior they cover and integration tests under `tests/` for cross-module behavior. Name tests after the expected behavior, for example `insert_values_rejects_null_for_non_nullable_columns`. Use `tempfile` for database files and keep deterministic tests in the default `cargo test` set; mark expensive stress tests as ignored.

## Commit & Pull Request Guidelines
Recent commits use imperative, concise subjects such as `Split executor module` and `Route table scans through database`. Follow that style. Pull requests should describe the behavioral change, mention relevant tests run, and call out storage-format, recovery, or public API impacts when applicable.
