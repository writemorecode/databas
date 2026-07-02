use std::path::Path;

use crate::core::{
    IndexSchema, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
    access::{DdlAccess, RecordAccess, SchemaAccess},
    catalog_manager::CatalogManager,
    cursor::{IndexCursor, TableCursor},
    error::StorageResult,
    index_manager::IndexManager,
    log_manager::TxnId,
    pager::Pager,
    record_manager::{IndexScan, RecordManager, TableScan},
    transaction_manager::TransactionSavepoint,
    transaction_runtime::TransactionRuntime,
};

/// Public database handle for one database file.
pub struct Database {
    catalog: CatalogManager,
    indexes: IndexManager,
    records: RecordManager,
    transactions: TransactionRuntime,
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
        let indexes = IndexManager::new(catalog.clone());
        let records = RecordManager::new(catalog.clone(), indexes.clone());
        Ok(Self { catalog, indexes, records, transactions })
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

    pub(crate) fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.catalog.create_table(name, row)
    }

    pub(crate) fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        self.indexes.create_index(name, table_name, columns)
    }

    pub(crate) fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        self.catalog.table_schema_by_name(name)
    }

    pub(crate) fn index_schemas_for_table(
        &self,
        table: &TableSchema,
    ) -> StorageResult<Vec<IndexSchema>> {
        self.catalog.index_schemas_for_table(table)
    }

    pub(crate) fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        self.catalog.table_cursor_by_name(name)
    }

    pub(crate) fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        self.records.scan_table(table)
    }

    pub(crate) fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        self.records.scan_table_range(table, range)
    }

    pub(crate) fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.records.insert_table_row(table, values)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.records.delete_table_row(table, record)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.records.update_table_row(table, record, values)
    }

    pub(crate) fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog.index_cursor_by_name(name)
    }
}

impl SchemaAccess for Database {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        Database::table_schema_by_name(self, name)
    }

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>> {
        Database::index_schemas_for_table(self, table)
    }
}

impl DdlAccess for Database {
    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        Database::create_table(self, name, row)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        Database::create_index(self, name, table_name, columns)
    }
}

impl RecordAccess for Database {
    fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        Database::scan_table(self, table)
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        Database::scan_table_range(self, table, range)
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_prefix: Vec<u8>,
    ) -> StorageResult<IndexScan> {
        self.records.scan_index(table, index, key_prefix)
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        Database::insert_table_row(self, table, values)
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        Database::delete_table_row(self, table, record)
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        Database::update_table_row(self, table, record, values)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::core::error::{CorruptionError, CorruptionKind, StorageError};

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
}
