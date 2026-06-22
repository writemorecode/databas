use std::path::Path;

use crate::core::{
    DataType, IndexSchema, OwnedTableRecord, RowId, TableCursor, TableSchema, Tuple, TupleSchema,
    TupleView, Value,
    catalog_manager::CatalogManager,
    cursor::IndexCursor,
    error::{
        ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind,
        InvalidArgumentError, StorageError, StorageResult,
    },
    log_manager::TxnId,
    pager::Pager,
    transaction_manager::TransactionSavepoint,
    transaction_runtime::TransactionRuntime,
};

/// Public database handle for one database file.
pub struct Database {
    catalog: CatalogManager,
    transactions: TransactionRuntime,
}

/// Iterator over owned records in one table.
pub struct TableScan {
    cursor: TableCursor,
    done: bool,
}

impl Database {
    /// Creates a new database file.
    pub fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::create(path)?;
        Self::from_pager(pager)
    }

    /// Opens an existing database file.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::open(path)?;
        Self::from_pager(pager)
    }

    /// Opens a database file, creating and initializing it if needed.
    pub fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::open_or_create(path)?;
        Self::from_pager(pager)
    }

    fn from_pager(pager: Pager) -> StorageResult<Self> {
        let transactions = pager.transaction_runtime();
        let catalog = CatalogManager::from_pager(pager)?;
        Ok(Self { catalog, transactions })
    }

    /// Returns the database-file path associated with this database.
    pub fn path(&self) -> &Path {
        self.catalog.path()
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.catalog.flush()
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.transactions.begin_transaction()
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.transactions.commit_transaction(txn_id)
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.transactions.statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        self.transactions.rollback_to_savepoint(savepoint)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.transactions.rollback_transaction(txn_id)
    }

    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.transactions.active_transaction_id()
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.transactions.transaction_is_poisoned(txn_id)
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&self) {
        self.transactions.force_next_lsn_exhausted_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.transactions.fail_next_wal_flush_for_test();
    }

    /// Creates a table and records its schema in the system catalog.
    pub fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.catalog.create_table(name, row)
    }

    /// Creates a secondary index, records its schema, and backfills existing rows.
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        let table = self.catalog.table_schema_by_name(table_name)?;
        let index = self.catalog.create_index(name, table_name, columns)?;
        self.backfill_index(&table, &index)?;
        Ok(index)
    }

    /// Returns the schema for the table named `name`.
    pub fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        self.catalog.table_schema_by_name(name)
    }

    /// Returns the schemas for secondary indexes defined on `table`.
    pub fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>> {
        self.catalog.index_schemas_for_table(table)
    }

    /// Returns a typed cursor for the table named `name`.
    pub fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        self.catalog.table_cursor_by_name(name)
    }

    /// Returns a lazy full-table scan over owned table records.
    pub fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        Ok(TableScan { cursor: self.catalog.table_cursor_by_name(&table.name)?, done: false })
    }

    /// Allocates and persists the next row id for a table.
    pub fn allocate_table_row_id(&self, table: &TableSchema) -> StorageResult<RowId> {
        self.catalog.allocate_table_row_id(table)
    }

    /// Inserts a fully-shaped table row and mirrors it into secondary indexes.
    pub fn insert_table_row(
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
        self.insert_index_entries(table, &record)?;
        Ok(record)
    }

    /// Returns a typed cursor for the index named `name`.
    pub fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog.index_cursor_by_name(name)
    }

    fn insert_index_entries(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        for index in self.catalog.index_schemas_for_table(table)? {
            let key = index_key_from_record(table, &index, record)?;
            let mut index_cursor = self.catalog.index_cursor_by_name(&index.name)?;
            index_cursor.insert(&key, record.row_id)?;
        }

        Ok(())
    }

    fn backfill_index(&self, table: &TableSchema, index: &IndexSchema) -> StorageResult<()> {
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        let mut index_cursor = self.catalog.index_cursor_by_name(&index.name)?;

        while let Some(record) = table_cursor.next_owned_record()? {
            let key = index_key_from_record(table, index, &record)?;
            index_cursor.insert(&key, record.row_id)?;
        }

        Ok(())
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

fn index_key_from_record(
    table: &TableSchema,
    index: &IndexSchema,
    record: &OwnedTableRecord,
) -> StorageResult<Vec<u8>> {
    let tuple = TupleView::parse(&record.record).map_err(|error| {
        invalid_table_record(table, record.row_id, format!("invalid tuple bytes: {error}"))
    })?;
    let mut values = Vec::with_capacity(index.columns.len());

    for column in &index.columns {
        let ordinal = column.source_column_ordinal as usize;
        let value = tuple.values().nth(ordinal).ok_or_else(|| {
            invalid_table_record(
                table,
                record.row_id,
                format!(
                    "index {} references source column ordinal {ordinal}, but row has {} values",
                    index.name,
                    tuple.len()
                ),
            )
        })?;
        values.push(Value::from(value));
    }

    Tuple::new(values).to_bytes().map_err(StorageError::from)
}

fn invalid_table_record(table: &TableSchema, row_id: RowId, reason: String) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: None,
        kind: CorruptionKind::InvalidTableRecord { table: table.name.clone(), row_id, reason },
    })
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
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::core::{
        ColumnSchema, DataType,
        error::{ConstraintError, CorruptionError, CorruptionKind, StorageError},
    };

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
    fn create_initializes_database_that_can_be_opened() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let database = Database::create(&path).unwrap();
        database.flush().unwrap();

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.path(), path);
    }

    #[test]
    fn create_initializes_write_ahead_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let _database = Database::create(&path).unwrap();

        assert!(path.with_added_extension("wal").exists());
    }

    #[test]
    fn create_rejects_existing_file() {
        let file = NamedTempFile::new().unwrap();

        assert!(Database::create(file.path()).is_err());
    }

    #[test]
    fn open_rejects_empty_file_without_header() {
        let file = NamedTempFile::new().unwrap();

        assert!(matches!(
            Database::open(file.path()),
            Err(StorageError::Corruption(CorruptionError {
                kind: CorruptionKind::MissingDatabaseHeader,
                ..
            }))
        ));
    }

    #[test]
    fn insert_table_row_persists_row_and_updates_secondary_indexes() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let table = database.create_table("users", users_schema()).unwrap();
        database.create_index("idx_users_name", "users", &["name"]).unwrap();

        let inserted = database
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();

        let mut users = database.table_cursor_by_name("users").unwrap();
        let stored = users.get(1).unwrap().expect("inserted row should be stored");
        let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
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
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let table = database.create_table("users", users_schema()).unwrap();

        let error = database
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Null],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::Constraint(ConstraintError::NullValue { column }) if column == "active"
        ));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
        let mut users = database.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }

    #[test]
    fn insert_table_row_rejects_wrong_type_before_allocating_row_id() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let table = database.create_table("users", users_schema()).unwrap();

        let error = database
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
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
        let mut users = database.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }

    #[test]
    fn create_index_backfills_existing_table_rows_without_executor() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let table = database.create_table("users", users_schema()).unwrap();
        database
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();
        database
            .insert_table_row(
                &table,
                vec![Value::Integer(2), Value::String("Grace".to_owned()), Value::Boolean(false)],
            )
            .unwrap();

        database.create_index("idx_users_name", "users", &["name"]).unwrap();

        let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
        assert_eq!(index.get(&name_key("Ada")).unwrap().unwrap().row_id, 1);
        assert_eq!(index.get(&name_key("Grace")).unwrap().unwrap().row_id, 2);
    }
}
