#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use databas::{core::Database, executor::ExecutionOutput, session::Session};
use databas_relational::{
    IndexValueBound, IndexValueRange, LockMode, RelationalResult, StatementTransactionMode as Mode,
    TableKey, Value,
    access::{CatalogRead, RelationalAccess},
};

#[test]
fn text_indexes_scan_lexicographically_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session
        .execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, rank INT);
             INSERT INTO items (id, name, rank) VALUES
                 (1, 'z', 0), (2, 'aa', 2), (3, 'a', 0), (4, '', 0),
                 (5, 'a\0', 0), (6, 'aa', 1), (7, 'é', 0);
             CREATE INDEX items_name ON items (name);
             CREATE INDEX items_name_rank ON items (name, rank);
             INSERT INTO items (id, name, rank) VALUES (8, 'aa', 1);",
        )
        .unwrap();
    session.close().unwrap();
    database.checkpoint().unwrap();
    drop(database);

    let database = Database::open(&path).unwrap();
    let table = database.table_schema_by_name("items").unwrap();
    let indexes = database.index_schemas_for_table(&table).unwrap();
    let name_index = indexes.iter().find(|index| index.name == "items_name").unwrap();
    let composite_index = indexes.iter().find(|index| index.name == "items_name_rank").unwrap();
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    transaction
        .statement(
            Mode::Ordinary,
            &[(table.table_id.into(), LockMode::Shared)],
            |access| -> RelationalResult<()> {
                let scan = |index, range| -> RelationalResult<Vec<TableKey>> {
                    access
                        .scan_index(&table, index, range)?
                        .map(|row| row.map(|row| row.table_key))
                        .collect()
                };
                assert_eq!(scan(name_index, IndexValueRange::default())?, [4, 3, 5, 2, 6, 8, 1, 7]);
                assert_eq!(
                    scan(composite_index, IndexValueRange::default())?,
                    [4, 3, 5, 6, 8, 2, 1, 7]
                );
                assert_eq!(
                    scan(
                        name_index,
                        IndexValueRange {
                            lower: Some(IndexValueBound::Exclusive(Value::String("aa".into()))),
                            upper: None,
                        }
                    )?,
                    [1, 7]
                );
                Ok(())
            },
        )
        .unwrap();
    transaction.close().unwrap();

    let mut session = Session::new(&database);
    for (predicate, expected) in [
        ("name > 'aa'", &[1, 7][..]),
        ("name >= 'z' AND name > 'aa' AND name <= 'z'", &[1][..]),
        ("'a' < name AND name <= 'aa' AND name < 'z'", &[5, 2, 6, 8][..]),
        ("name == 'aa'", &[2, 6, 8][..]),
    ] {
        let sql = format!("SELECT id FROM items WHERE {predicate};");
        let ExecutionOutput::Explain(plan) =
            session.execute_sql(&format!("EXPLAIN {sql}")).unwrap()
        else {
            panic!("expected a query plan");
        };
        assert!(
            plan.contains("SecondaryIndexScan") && plan.contains("index=items_name "),
            "{plan}"
        );

        let ExecutionOutput::Rows { rows, .. } = session.execute_sql(&sql).unwrap() else {
            panic!("expected rows for {sql}");
        };
        let rows: Vec<_> = rows.into_iter().map(|row| row.unwrap().values()[0].clone()).collect();
        let expected: Vec<_> = expected.iter().copied().map(Value::Integer).collect();
        assert_eq!(rows, expected, "{sql}");
    }
}

#[test]
fn nullable_text_range_uses_a_table_scan() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("db")).unwrap();
    let mut session = Session::new(&database);
    session
        .execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NULLABLE);
             CREATE INDEX items_name ON items (name);",
        )
        .unwrap();
    let ExecutionOutput::Explain(plan) =
        session.execute_sql("EXPLAIN SELECT id FROM items WHERE name < 'z';").unwrap()
    else {
        panic!("expected a query plan");
    };
    assert!(plan.contains("FullTableScan"), "{plan}");
}
