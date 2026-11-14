use databas_relational::{
    ColumnSchema, DataType, Database, InvalidArgumentError, LockMode, RelationalError,
    RelationalResult, StatementTransactionMode as Mode, StorageResult, TableSchema, Tuple,
    TupleSchema, Value,
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
#[cfg(panic = "unwind")]
#[allow(clippy::panic)]
fn a_panicking_statement_restores_its_savepoint_and_preserves_prior_work() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    let table = create_table(&database);
    let mut ddl = database.begin(Mode::Ddl).unwrap();
    let index = ddl
        .statement(Mode::Ddl, &[], |access| access.create_index("items_id", "items", &["id"]))
        .unwrap();
    ddl.commit().unwrap();
    drop(ddl);

    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    let locks = [(table.table_id.into(), LockMode::Exclusive)];
    transaction
        .statement(Mode::Ordinary, &locks, |access| {
            access.insert_table_row(&table, vec![Value::Integer(7)])
        })
        .unwrap();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        transaction.statement::<(), RelationalError>(Mode::Ordinary, &locks, |access| {
            access.insert_table_row(&table, vec![Value::Integer(8)])?;
            panic!("callback failed");
        })
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"callback failed"));
    transaction
        .statement(Mode::Ordinary, &locks, |access| {
            access.insert_table_row(&table, vec![Value::Integer(9)])
        })
        .unwrap();
    transaction.commit().unwrap();
    drop(transaction);
    drop(database);

    let database = Database::open(&path).unwrap();
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    transaction
        .statement(Mode::Ordinary, &locks, |access| {
            let keys = access
                .scan_table(&table)?
                .map(|row| row.map(|row| row.table_key))
                .collect::<RelationalResult<Vec<_>>>()?;
            let index_keys = access
                .scan_index(&table, &index, Default::default())?
                .map(|row| row.map(|row| row.table_key))
                .collect::<RelationalResult<Vec<_>>>()?;
            assert_eq!(keys, [7, 9]);
            assert_eq!(index_keys, keys);
            Ok::<_, RelationalError>(())
        })
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn system_catalogs_allow_reads_and_reject_caller_writes_and_new_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let user_table = create_table(&database);
    let table = database.table_schema_by_name("sys_tables").unwrap();
    let locks = [(table.table_id.into(), LockMode::Exclusive)];
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    transaction.statement(Mode::Ordinary, &locks, |access| {
        let row = access.scan_table(&table)?
            .find_map(|row| match row {
                Ok(row) if row.table_key != user_table.table_id => None,
                result => Some(result),
            })
            .unwrap()?;
        let mut values = Tuple::from_bytes(&row.record)?.into_values();
        values[1] = Value::String("tampered".into());
        let update = access.update_table_row(&table, &row, values.clone()).map(|_| ());
        let delete = access.delete_table_row(&table, &row);
        values[0] = Value::Integer(42);
        let insert = access.insert_table_row(&table, values).map(|_| ());
        for result in [update, delete, insert] {
            assert!(matches!(
                result,
                Err(RelationalError::InvalidArgument(InvalidArgumentError::SystemTableWrite { name }))
                    if name == table.name
            ));
        }
        Ok::<_, RelationalError>(())
    }).unwrap();
    transaction.commit().unwrap();
    let mut ddl = database.begin(Mode::Ddl).unwrap();
    let result = ddl.statement(Mode::Ddl, &[], |access| {
        access.create_index("catalog_names", &table.name, &["name"])
    });
    assert!(matches!(
        result,
        Err(RelationalError::InvalidArgument(InvalidArgumentError::SystemTableIndex { name }))
            if name == table.name
    ));
    ddl.commit().unwrap();
    assert_eq!(database.table_schema_by_name(&user_table.name).unwrap(), user_table);
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

#[test]
fn a_forged_schema_cannot_authorize_access_to_another_table() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let first = create_table(&database);
    let mut ddl = database.begin(Mode::Ddl).unwrap();
    let second = ddl
        .statement(Mode::Ddl, &[], |access| access.create_table("other", first.row.clone()))
        .unwrap();
    ddl.commit().unwrap();

    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    let mut forged = first.clone();
    forged.name = second.name.clone();
    let result = transaction.statement(
        Mode::Ordinary,
        &[(first.table_id.into(), LockMode::Exclusive)],
        |access| access.insert_table_row(&forged, vec![Value::Integer(1)]),
    );
    assert!(matches!(
        result,
        Err(databas_relational::RelationalError::InvalidArgument(
            databas_relational::InvalidArgumentError::SchemaMismatch { .. }
        ))
    ));
    transaction.close().unwrap();
    let mut reader = database.begin(Mode::Ordinary).unwrap();
    let count: StorageResult<usize> =
        reader.statement(Mode::Ordinary, &[(second.table_id.into(), LockMode::Shared)], |access| {
            Ok(access.scan_table(&second)?.collect::<StorageResult<Vec<_>>>()?.len())
        });
    assert_eq!(count.unwrap(), 0);
    reader.close().unwrap();
}

#[test]
fn reverse_ranges_use_inclusive_and_exclusive_bounds_across_duplicate_index_entries() {
    use databas_relational::{
        IndexValueBound, IndexValueRange, ScanDirection, TableKey, TableKeyBound, TableKeyRange,
    };

    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut ddl = database.begin(Mode::Ddl).unwrap();
    let (table, index, composite_index) = ddl
        .statement(Mode::Ddl, &[], |access| {
            let table = access.create_table(
                "items",
                TupleSchema {
                    columns: vec![
                        ColumnSchema {
                            name: "id".into(),
                            data_type: DataType::Integer,
                            nullable: false,
                            primary_key: true,
                        },
                        ColumnSchema {
                            name: "name".into(),
                            data_type: DataType::Text,
                            nullable: false,
                            primary_key: false,
                        },
                    ],
                },
            )?;
            let index = access.create_index("items_name", "items", &["name"])?;
            let composite_index = access.create_index("items_name_id", "items", &["name", "id"])?;
            Ok::<_, databas_relational::RelationalError>((table, index, composite_index))
        })
        .unwrap();
    ddl.commit().unwrap();

    let low = Value::String("a".into());
    let middle = Value::String("m".repeat(240));
    let high = Value::String("z".into());
    let mut transaction = database.begin(Mode::Ordinary).unwrap();
    transaction
        .statement(Mode::Ordinary, &[(table.table_id.into(), LockMode::Exclusive)], |access| {
            for key in (0..64).chain([TableKey::MIN, TableKey::MAX]) {
                let value = match key {
                    0 => &low,
                    63 => &high,
                    _ => &middle,
                };
                access.insert_table_row(&table, vec![Value::Integer(key), value.clone()])?;
            }

            let index_range = IndexValueRange {
                lower: Some(IndexValueBound::Exclusive(low.clone())),
                upper: Some(IndexValueBound::Inclusive(middle.clone())),
            };
            for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
                assert!(matches!(
                    access.scan_index_in_direction(&table, &composite_index, index_range.clone(), direction),
                    Err(RelationalError::InvalidArgument(InvalidArgumentError::BoundedCompositeIndexScan { name }))
                        if name == composite_index.name
                ));
                assert_eq!(
                    access.scan_index_in_direction(&table, &composite_index, IndexValueRange::default(), direction)?
                        .collect::<StorageResult<Vec<_>>>()?.len(),
                    66
                );
            }
            let rows = access
                .scan_index_in_direction(&table, &index, index_range, ScanDirection::Reverse)?
                .map(|record| record.map(|record| record.table_key))
                .collect::<StorageResult<Vec<_>>>()?;
            assert_eq!(
                rows,
                [TableKey::MAX]
                    .into_iter()
                    .chain((1..63).rev())
                    .chain([TableKey::MIN])
                    .collect::<Vec<_>>()
            );

            let index_range = IndexValueRange {
                lower: None,
                upper: Some(IndexValueBound::Exclusive(middle.clone())),
            };
            let rows = access
                .scan_index_in_direction(&table, &index, index_range, ScanDirection::Reverse)?
                .map(|record| record.map(|record| record.table_key))
                .collect::<StorageResult<Vec<_>>>()?;
            assert_eq!(rows, [0]);

            let table_range = TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(59)),
                upper: Some(TableKeyBound::Exclusive(63)),
            };
            let rows = access
                .scan_table_range_in_direction(&table, table_range, ScanDirection::Reverse)?
                .map(|record| record.map(|record| record.table_key))
                .collect::<StorageResult<Vec<_>>>()?;
            assert_eq!(rows, [62, 61, 60, 59]);

            let empty_range =
                TableKeyRange { lower: None, upper: Some(TableKeyBound::Exclusive(TableKey::MIN)) };
            assert!(
                access
                    .scan_table_range_in_direction(&table, empty_range, ScanDirection::Reverse)?
                    .next()
                    .is_none()
            );
            Ok::<_, databas_relational::RelationalError>(())
        })
        .unwrap();
    transaction.commit().unwrap();
}
