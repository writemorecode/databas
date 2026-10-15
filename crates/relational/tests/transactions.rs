use databas_relational::{
    ColumnSchema, DataType, Database, LockMode, StatementTransactionMode as Mode, StorageResult,
    TableSchema, TupleSchema, Value,
    access::{CatalogRead, RelationalAccess},
};

#[allow(clippy::unwrap_used)]
fn create_table(database: &Database) -> TableSchema {
    let mut transaction = database.begin(Mode::Ddl).unwrap();
    let table = transaction
        .statement(Mode::Ddl, &[], |access| {
            access.create_table(
                "items",
                TupleSchema {
                    columns: vec![ColumnSchema {
                        name: "id".into(),
                        data_type: DataType::Integer,
                        nullable: false,
                        primary_key: true,
                    }],
                },
            )
        })
        .unwrap();
    transaction.commit().unwrap();
    table
}

#[test]
fn direct_relational_statements_restore_failed_writes_and_preserve_prior_work() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let table = create_table(&database);
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    let locks = [(table.table_id.into(), LockMode::Exclusive)];
    transaction
        .statement(Mode::Ordinary, &locks, |access| {
            access.insert_table_row(&table, vec![Value::Integer(7)])
        })
        .unwrap();
    let failed: StorageResult<()> = transaction.statement(Mode::Ordinary, &locks, |access| {
        access.insert_table_row(&table, vec![Value::Integer(8)])?;
        access.insert_table_row(&table, vec![Value::Integer(8)])?;
        Ok(())
    });
    assert!(failed.is_err());
    let keys: StorageResult<Vec<_>> = transaction.statement(Mode::Ordinary, &locks, |access| {
        access.scan_table(&table)?.map(|row| row.map(|row| row.table_key)).collect()
    });
    assert_eq!(keys.unwrap(), vec![7]);
    transaction.commit().unwrap();
}

#[test]
fn dropping_direct_transaction_rolls_back_table_and_index_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    let table = create_table(&database);
    {
        let mut transaction = database.begin(Mode::Ddl).unwrap();
        transaction
            .statement(Mode::Ddl, &[(table.table_id.into(), LockMode::Exclusive)], |access| {
                access.insert_table_row(&table, vec![Value::Integer(9)])?;
                access.create_index("items_id", "items", &["id"])
            })
            .unwrap();
    }
    assert!(database.index_schemas_for_table(&table).unwrap().is_empty());
    drop(database);
    let database = Database::open(&path).unwrap();
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    let count: StorageResult<usize> = transaction.statement(
        Mode::Ordinary,
        &[(table.table_id.into(), LockMode::Shared)],
        |access| Ok(access.scan_table(&table)?.collect::<StorageResult<Vec<_>>>()?.len()),
    );
    assert_eq!(count.unwrap(), 0);
    transaction.close().unwrap();
}

#[test]
fn ordinary_statement_cannot_bypass_ddl_admission() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    assert!(
        transaction
            .statement(Mode::Ordinary, &[], |access| {
                access.create_table("illegal", TupleSchema { columns: vec![] })
            })
            .is_err()
    );
    assert!(database.table_schema_by_name("illegal").is_err());
    transaction.close().unwrap();
}
