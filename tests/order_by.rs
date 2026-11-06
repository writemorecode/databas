#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use databas::{core::Database, executor::ExecutionOutput, session::Session};
use databas_relational::Value;

fn query(session: &mut Session<'_>, sql: &str) -> Vec<Value> {
    let ExecutionOutput::Rows { rows, .. } = session.execute_sql(sql).unwrap() else {
        panic!("expected rows for {sql}");
    };
    rows.into_iter().map(|row| row.unwrap().values()[0].clone()).collect()
}

fn explain(session: &mut Session<'_>, sql: &str) -> String {
    let sql = format!("EXPLAIN {sql}");
    let ExecutionOutput::Explain(plan) = session.execute_sql(&sql).unwrap() else {
        panic!("expected a plan for {sql}");
    };
    plan
}

#[test]
fn ordered_scans_use_primary_and_text_index_ranges_in_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("db")).unwrap();
    let mut session = Session::new(&database);
    session
        .execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);
             INSERT INTO items (id, name) VALUES
                 (1, 'z'), (6, 'aa'), (0, 'a'), (2, ''), (3, 'a\0'), (4, 'é');
             CREATE INDEX items_name ON items (name);",
        )
        .unwrap();

    let primary = "SELECT name FROM items WHERE id > 0 AND id <= 3
                   ORDER BY id DESC LIMIT 2 OFFSET 1;";
    let plan = explain(&mut session, primary);
    assert!(
        plan.contains("PrimaryKeyRangeScan")
            && plan.contains("direction=DESC")
            && !plan.contains("Sort"),
        "{plan}"
    );
    assert_eq!(query(&mut session, primary), [Value::String("".into()), Value::String("z".into())]);

    let secondary = "SELECT id FROM items WHERE name > 'a' AND name <= 'z'
                     ORDER BY name DESC LIMIT 2 OFFSET 1;";
    let plan = explain(&mut session, secondary);
    assert!(
        plan.contains("SecondaryIndexScan")
            && plan.contains("direction=DESC")
            && !plan.contains("Sort"),
        "{plan}"
    );
    assert_eq!(query(&mut session, secondary), [Value::Integer(6), Value::Integer(3)]);

    let full = "SELECT id FROM items ORDER BY name;";
    let plan = explain(&mut session, full);
    assert!(plan.contains("SecondaryIndexScan") && !plan.contains("Sort"), "{plan}");
    assert_eq!(query(&mut session, full), [2, 0, 3, 6, 1, 4].map(Value::Integer));
}

#[test]
fn in_memory_ordering_preserves_duplicates_and_applies_null_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("db")).unwrap();
    let mut session = Session::new(&database);
    session
        .execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NULLABLE);
             INSERT INTO items (id, name) VALUES (1, 'z'), (2, 'aa'), (3, 'aa'), (4, 'a');
             INSERT INTO items (id) VALUES (5);
             CREATE INDEX items_name ON items (name);",
        )
        .unwrap();

    let asc = "SELECT name FROM items ORDER BY name ASC LIMIT 4;";
    let plan = explain(&mut session, asc);
    assert!(plan.contains("Sort"), "{plan}");
    assert_eq!(
        query(&mut session, asc),
        [
            Value::Null,
            Value::String("a".into()),
            Value::String("aa".into()),
            Value::String("aa".into())
        ]
    );
    assert_eq!(
        query(&mut session, "SELECT name FROM items ORDER BY name DESC LIMIT 2 OFFSET 3;"),
        [Value::String("a".into()), Value::Null]
    );
}
