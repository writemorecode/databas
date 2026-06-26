use crate::core::{
    DataType, OwnedTableRecord, TableSchema, Tuple, Value,
    catalog_manager::CatalogManager,
    cursor::TableCursor,
    error::{ConstraintError, InvalidArgumentError, StorageError, StorageResult},
    index_manager::IndexManager,
};

/// Internal manager for table record access and mutation.
#[derive(Clone)]
pub(crate) struct RecordManager {
    catalog: CatalogManager,
    indexes: IndexManager,
}

/// Iterator over owned records in one table.
pub(crate) struct TableScan {
    cursor: TableCursor,
    done: bool,
}

impl RecordManager {
    pub(crate) fn new(catalog: CatalogManager, indexes: IndexManager) -> Self {
        Self { catalog, indexes }
    }

    pub(crate) fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        Ok(TableScan { cursor: self.catalog.table_cursor_by_name(&table.name)?, done: false })
    }

    pub(crate) fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;

        let record = Tuple::new(values).to_bytes()?;
        let row_id = self.catalog.allocate_table_row_id(table)?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.insert(row_id, &record)?;

        let record = OwnedTableRecord { row_id, record: record.into_boxed_slice() };
        self.indexes.insert_index_entries(table, &record)?;
        Ok(record)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.indexes.delete_index_entries(table, record)?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.delete(record.row_id)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;

        let updated = Tuple::new(values).to_bytes()?;
        let updated =
            OwnedTableRecord { row_id: record.row_id, record: updated.into_boxed_slice() };

        self.indexes.delete_index_entries(table, record)?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.update(record.row_id, &updated.record)?;
        self.indexes.insert_index_entries(table, &updated)?;

        Ok(updated)
    }
}

impl Iterator for TableScan {
    type Item = StorageResult<OwnedTableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.cursor.next_owned_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

fn validate_table_row(table: &TableSchema, values: &[Value]) -> StorageResult<()> {
    if values.len() != table.row.columns.len() {
        return Err(StorageError::InvalidArgument(InvalidArgumentError::TableRowValueCount {
            table: table.name.clone(),
            columns: table.row.columns.len(),
            values: values.len(),
        }));
    }

    for (column, value) in table.row.columns.iter().zip(values.iter()) {
        if matches!(value, Value::Null) {
            if !column.nullable || column.primary_key {
                return Err(StorageError::Constraint(ConstraintError::NullValue {
                    column: column.name.clone(),
                }));
            }
            continue;
        }

        if !value_matches_data_type(value, column.data_type) {
            return Err(StorageError::Constraint(ConstraintError::ColumnTypeMismatch {
                column: column.name.clone(),
                expected: column.data_type,
                actual: value_type_name(value),
            }));
        }
    }

    Ok(())
}

fn value_matches_data_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::String(_), DataType::Text)
            | (Value::Boolean(_), DataType::Boolean)
            | (Value::Integer(_), DataType::Integer)
            | (Value::Float(_), DataType::Float)
            | (Value::UnsignedInteger(_), DataType::UnsignedInteger)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::String(_) => "text",
        Value::Boolean(_) => "boolean",
        Value::Integer(_) => "integer",
        Value::Float(_) => "float",
        Value::UnsignedInteger(_) => "unsigned integer",
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        ColumnSchema, TupleSchema, catalog_manager::CatalogManager, cursor::IndexCursor,
        index_manager::IndexManager, pager::Pager,
    };

    fn open(path: impl AsRef<std::path::Path>) -> StorageResult<(CatalogManager, RecordManager)> {
        let catalog = CatalogManager::from_pager(Pager::open_or_create(path)?)?;
        let indexes = IndexManager::new(catalog.clone());
        let records = RecordManager::new(catalog.clone(), indexes);
        Ok((catalog, records))
    }

    fn users_schema() -> TupleSchema {
        TupleSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "active".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    primary_key: false,
                },
            ],
        }
    }

    fn values(record: &OwnedTableRecord) -> Vec<Value> {
        Tuple::from_bytes(&record.record).unwrap().into_values()
    }

    fn name_key(name: &str) -> Vec<u8> {
        Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap()
    }

    #[test]
    fn insert_table_row_persists_row_and_updates_secondary_indexes() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let inserted = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();

        let mut users = catalog.table_cursor_by_name("users").unwrap();
        let stored = users.get(1).unwrap().expect("inserted row should be stored");
        let mut index: IndexCursor = catalog.index_cursor_by_name("idx_users_name").unwrap();
        let entry = index.get(&name_key("Ada")).unwrap().expect("index should track inserted row");

        assert_eq!(inserted.row_id, 1);
        assert_eq!(
            values(&stored),
            vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)]
        );
        assert_eq!(entry.row_id, 1);
    }

    #[test]
    fn insert_table_row_rejects_null_before_allocating_row_id() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();

        let error = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Null],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::Constraint(ConstraintError::NullValue { column }) if column == "active"
        ));
        assert_eq!(catalog.table_schema_by_name("users").unwrap().last_row_id, 0);
        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }

    #[test]
    fn insert_table_row_rejects_wrong_type_before_allocating_row_id() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();

        let error = records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(1),
                    Value::String("Ada".to_owned()),
                    Value::String("yes".to_owned()),
                ],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::Constraint(ConstraintError::ColumnTypeMismatch {
                column,
                expected: DataType::Boolean,
                actual: "text",
            }) if column == "active"
        ));
        assert_eq!(catalog.table_schema_by_name("users").unwrap().last_row_id, 0);
        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }
}
