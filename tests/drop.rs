#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use std::{fs, path::Path, process::Command};

use databas::{
    core::{Database, Value},
    executor::ExecutionOutput,
    session::Session,
};
use databas_relational::{
    IndexValueRange, InvalidArgumentError, LockMode, RelationalError, RelationalResult,
    StatementTransactionMode as Mode,
    access::{CatalogRead, RelationalAccess},
};

const ROWS: i32 = 64;
const PAGE_SIZE: usize = 4096;

fn payload(id: i32) -> String {
    format!("{id:04}{}", "x".repeat(9000))
}

fn query(session: &mut Session<'_>, sql: &str) -> Vec<Vec<Value>> {
    let ExecutionOutput::Rows { rows, .. } = session.execute_sql(sql).unwrap() else {
        panic!("expected rows: {sql}");
    };
    rows.into_iter().map(|row| row.unwrap().values().to_vec()).collect()
}

fn seed(database: &Database) {
    let mut session = Session::new(database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, payload TEXT);").unwrap();
    let values = (0..ROWS).map(|id| format!("({id}, '{}')", payload(id))).collect::<Vec<_>>();
    session
        .execute_sql(&format!("INSERT INTO items (id, payload) VALUES {};", values.join(",")))
        .unwrap();
    session.execute_sql("CREATE INDEX items_payload ON items (payload);").unwrap();
    session.execute_sql("CREATE INDEX items_id ON items (id);").unwrap();
    session.close().unwrap();
    database.checkpoint().unwrap();
}

fn verify_rows(database: &Database) {
    let rows = query(&mut Session::new(database), "SELECT id, payload FROM items;");
    let expected = (0..ROWS)
        .map(|id| vec![Value::Integer(id), Value::String(payload(id))])
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}

fn verify_indexes(database: &Database) {
    let table = database.table_schema_by_name("items").unwrap();
    let indexes = database.index_schemas_for_table(&table).unwrap();
    let names = indexes.iter().map(|index| index.name.as_str()).collect::<Vec<_>>();
    assert_eq!(names, ["items_payload", "items_id"]);
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    for index in &indexes {
        let keys: RelationalResult<Vec<_>> = transaction.statement(
            Mode::Ordinary,
            &[(table.table_id.into(), LockMode::Shared)],
            |access| {
                access
                    .scan_index(&table, index, IndexValueRange::default())?
                    .map(|row| row.map(|row| row.table_key))
                    .collect()
            },
        );
        assert_eq!(keys.unwrap(), (0..ROWS).collect::<Vec<_>>());
    }
    transaction.close().unwrap();
    let mut session = Session::new(database);
    // Exercise SQL lookups through the restored overflow keys as well.
    for id in 0..ROWS {
        let sql = format!("SELECT id, payload FROM items WHERE payload == '{}';", payload(id));
        let ExecutionOutput::Explain(plan) =
            session.execute_sql(&format!("EXPLAIN {sql}")).unwrap()
        else {
            panic!("expected plan");
        };
        assert!(plan.contains("SecondaryIndexScan"), "{plan}");
        assert!(plan.contains("items_payload"), "{plan}");
        assert_eq!(
            query(&mut session, &sql),
            vec![vec![Value::Integer(id), Value::String(payload(id))]]
        );
    }
}

// Inspect the allocation bitmap to prove DROP reclaims complete trees, rather
// than merely hiding their catalog entries. This mirrors the storage format tests.
fn allocated_count(path: &Path) -> usize {
    let bytes = fs::read(path).unwrap();
    let extent_pages = (PAGE_SIZE - 16) * 8;
    bytes
        .chunks(PAGE_SIZE * extent_pages)
        .map(|extent| {
            extent[16..PAGE_SIZE].iter().map(|byte| byte.count_ones() as usize).sum::<usize>()
        })
        .sum()
}

#[test]
fn rollback_undrops_multi_page_tables_indexes_and_overflow_after_name_reuse() {
    for drop_table in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let database = Database::create(&path).unwrap();
        seed(&database);
        let before = allocated_count(&path);
        assert!(before > 100, "fixture must span many pages");
        let mut session = Session::new(&database);
        session.execute_sql("BEGIN;").unwrap();
        if drop_table {
            session.execute_sql("DROP TABLE items;").unwrap();
            assert!(session.execute_sql("SELECT id FROM items;").is_err());
            assert!(session.execute_sql("DROP INDEX items_id;").is_err());
            session
                .execute_sql("CREATE TABLE items (id INT PRIMARY KEY, replacement TEXT);")
                .unwrap();
            session
                .execute_sql(&format!(
                    "INSERT INTO items (id, replacement) VALUES (999, '{}');",
                    "z".repeat(30000)
                ))
                .unwrap();
        } else {
            session.execute_sql("DROP INDEX items_payload;").unwrap();
            assert_eq!(
                query(&mut session, "SELECT id FROM items;").len(),
                usize::try_from(ROWS).unwrap()
            );
            session.execute_sql("CREATE INDEX items_payload ON items (payload);").unwrap();
            session
                .execute_sql(&format!(
                    "UPDATE items SET payload = '{}' WHERE id == 0;",
                    "z".repeat(30000)
                ))
                .unwrap();
        }
        // Exercise rollback after STEAL/checkpoint has persisted uncommitted data.
        database.checkpoint().unwrap();
        session.execute_sql("ROLLBACK;").unwrap();
        session.close().unwrap();
        verify_rows(&database);
        verify_indexes(&database);
        database.checkpoint().unwrap();
        assert_eq!(allocated_count(&path), before);
        drop(database);
        let database = Database::open(&path).unwrap();
        verify_rows(&database);
        verify_indexes(&database);
    }
}

#[test]
fn committed_drops_reclaim_all_pages_and_reuse_space_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let database = Database::create(&path).unwrap();
    // Keep another object alive to catch accidental reclamation of unrelated trees.
    Session::new(&database)
        .execute_sql("CREATE TABLE keep (id INT PRIMARY KEY); INSERT INTO keep (id) VALUES (42);")
        .unwrap();
    database.checkpoint().unwrap();
    let baseline = allocated_count(&path);
    seed(&database);
    let before = allocated_count(&path);
    let size = fs::metadata(&path).unwrap().len();
    Session::new(&database).execute_sql("DROP INDEX items_payload;").unwrap();
    database.checkpoint().unwrap();
    assert!(allocated_count(&path) + 100 < before);
    verify_rows(&database);
    drop(database);
    let database = Database::open(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE INDEX items_payload ON items (payload);").unwrap();
    database.checkpoint().unwrap();
    assert_eq!(allocated_count(&path), before);
    // Rebuilding with a different free-page order may need one extra scratch
    // page while replacing overflow separators, but must reuse the tree's space.
    assert!(fs::metadata(&path).unwrap().len() <= size + PAGE_SIZE as u64);
    session.execute_sql("DROP TABLE items;").unwrap();
    session.close().unwrap();
    database.checkpoint().unwrap();
    assert_eq!(allocated_count(&path), baseline);
    drop(database);
    let database = Database::open(&path).unwrap();
    let mut session = Session::new(&database);
    assert!(session.execute_sql("SELECT id FROM items;").is_err());
    assert!(session.execute_sql("DROP INDEX items_payload;").is_err());
    assert!(session.execute_sql("DROP INDEX items_id;").is_err());
    assert_eq!(query(&mut session, "SELECT id FROM keep;"), vec![vec![Value::Integer(42)]]);
    seed(&database);
    assert_eq!(allocated_count(&path), before);
    assert!(fs::metadata(&path).unwrap().len() <= size + PAGE_SIZE as u64);
    verify_rows(&database);
    verify_indexes(&database);
}

#[test]
fn failed_statement_undrops_trees_and_preserves_earlier_transaction_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let database = Database::create(&path).unwrap();
    seed(&database);
    let mut transaction = database.begin(Mode::Ddl).unwrap();
    let schema = database.table_schema_by_name("items").unwrap().row;
    transaction.statement(Mode::Ddl, &[], |access| access.create_table("earlier", schema)).unwrap();
    let result: RelationalResult<()> = transaction.statement(Mode::Ddl, &[], |access| {
        access.drop_index("items_payload")?;
        access.drop_table("items")?;
        access.drop_table("missing")
    });
    assert!(matches!(
        result,
        Err(RelationalError::InvalidArgument(InvalidArgumentError::TableNotFound { .. }))
    ));
    transaction.commit().unwrap();
    assert!(database.table_schema_by_name("earlier").is_ok());
    verify_rows(&database);
    verify_indexes(&database);
    drop(transaction);
    drop(database);
    let database = Database::open(&path).unwrap();
    assert!(database.table_schema_by_name("earlier").is_ok());
    verify_rows(&database);
    verify_indexes(&database);
}

#[test]
fn drops_require_ddl_admission_and_protect_system_catalogs() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("db")).unwrap();
    let mut session = Session::new(&database);
    for name in ["sys_tables", "sys_indexes", "sys_columns"] {
        assert!(session.execute_sql(&format!("DROP TABLE {name};")).is_err());
        assert!(
            !query(&mut session, &format!("SELECT * FROM {name};")).is_empty()
                || name == "sys_indexes"
        );
    }
    session
        .execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY); CREATE INDEX items_id ON items (id);",
        )
        .unwrap();
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    for drop_table in [true, false] {
        let result: RelationalResult<()> = transaction.statement(Mode::Ordinary, &[], |access| {
            if drop_table { access.drop_table("items") } else { access.drop_index("items_id") }
        });
        assert!(matches!(
            result,
            Err(RelationalError::InvalidArgument(InvalidArgumentError::DdlRequired))
        ));
    }
    assert!(session.execute_sql("DROP TABLE items;").is_err());
    assert!(session.execute_sql("DROP INDEX items_id;").is_err());
    transaction.close().unwrap();
    assert!(session.execute_sql("DROP TABLE missing;").is_err());
    assert!(session.execute_sql("DROP INDEX missing;").is_err());
    session.execute_sql("DROP INDEX items_id; DROP TABLE items;").unwrap();
}

#[test]
fn empty_and_newly_created_objects_can_be_dropped_in_the_same_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let database = Database::create(&path).unwrap();
    database.checkpoint().unwrap();
    let baseline = allocated_count(&path);
    let mut session = Session::new(&database);
    for outcome in ["COMMIT", "ROLLBACK"] {
        session
            .execute_sql(&format!(
                "BEGIN; CREATE TABLE empty (id INT PRIMARY KEY); \
             CREATE INDEX empty_id ON empty (id); DROP INDEX empty_id; \
             CREATE INDEX empty_id ON empty (id); DROP TABLE empty; {outcome};"
            ))
            .unwrap();
        assert!(database.table_schema_by_name("empty").is_err());
        database.checkpoint().unwrap();
        assert_eq!(allocated_count(&path), baseline);
    }
}

#[test]
fn drop_crash_probe() {
    let Some(path) = std::env::var_os("DATABAS_DROP_CRASH_PATH") else {
        return;
    };
    let mode = std::env::var("DATABAS_DROP_CRASH_MODE").unwrap();
    let database = Database::open(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("BEGIN;").unwrap();
    session
        .execute_sql(if mode.starts_with("table") {
            "DROP TABLE items;"
        } else {
            "DROP INDEX items_payload;"
        })
        .unwrap();
    database.checkpoint().unwrap();
    if mode.ends_with("commit") {
        session.execute_sql("COMMIT;").unwrap();
    }
    // Skip destructors to leave both committed and uncommitted DDL in the WAL.
    std::process::exit(0);
}

#[test]
fn crash_recovery_restores_uncommitted_drops_and_replays_committed_drops() {
    for mode in ["table-loser", "table-commit", "index-loser", "index-commit"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let database = Database::create(&path).unwrap();
        database.checkpoint().unwrap();
        let baseline = allocated_count(&path);
        seed(&database);
        let before = allocated_count(&path);
        drop(database);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "drop_crash_probe", "--nocapture"])
            .env("DATABAS_DROP_CRASH_PATH", &path)
            .env("DATABAS_DROP_CRASH_MODE", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let database = Database::open(&path).unwrap();
        if mode.ends_with("loser") {
            verify_rows(&database);
            verify_indexes(&database);
            assert_eq!(allocated_count(&path), before);
        } else if mode.starts_with("table") {
            assert!(database.table_schema_by_name("items").is_err());
            assert_eq!(allocated_count(&path), baseline);
        } else {
            verify_rows(&database);
            let table = database.table_schema_by_name("items").unwrap();
            let indexes = database.index_schemas_for_table(&table).unwrap();
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].name, "items_id");
            assert!(allocated_count(&path) + 100 < before);
        }
    }
}
