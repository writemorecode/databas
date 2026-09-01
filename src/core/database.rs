use std::path::Path;

use crate::core::{
    IndexKeyRange, IndexSchema, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
    access::CatalogRead, error::StorageResult,
};
#[cfg(test)]
use crate::relational::cursor::{IndexCursor, TableCursor};
use crate::relational::{
    catalog_manager::CatalogManager,
    index_manager,
    record_manager::{self, IndexScan, TableScan},
};
use crate::storage::{
    log_manager::TxnId, pager::Pager, transaction_manager::TransactionSavepoint,
    transaction_runtime::TransactionRuntime,
};

/// Public database handle for one database file.
pub struct Database {
    catalog: CatalogManager,
    transactions: TransactionRuntime,
}

impl Database {
    /// Creates a new database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file already exists, cannot be initialized,
    /// or its initial catalog and write-ahead log cannot be written.
    pub fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::create(path)?;
        Self::from_pager(pager)
    }

    /// Opens an existing database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, its format is invalid,
    /// or recovery cannot restore it to a consistent state.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::open(path)?;
        Self::from_pager(pager)
    }

    /// Opens a database file, creating and initializing it if needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or initialized, its
    /// format is invalid, or recovery cannot restore it to a consistent state.
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
    ///
    /// # Errors
    ///
    /// Returns an error when a dirty page cannot be written or when an active
    /// page pin prevents a consistent flush.
    pub fn flush(&self) -> StorageResult<()> {
        self.catalog.flush()
    }

    #[cfg(test)]
    pub(crate) fn unlock_for_crash_for_test(&self) {
        self.transactions.unlock_for_crash_for_test().unwrap();
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.transactions.begin_transaction()
    }

    /// Returns the concrete relational gateway for an active transaction.
    pub(crate) fn transaction(&self, txn_id: TxnId) -> crate::core::transaction::Transaction<'_> {
        crate::core::transaction::Transaction::new(self, txn_id)
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
    pub(crate) fn fail_next_savepoint_rollback_for_test(&self) {
        self.transactions.fail_next_savepoint_rollback_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.transactions.fail_next_wal_flush_for_test();
    }

    #[cfg(test)]
    pub(crate) fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        self.catalog.table_cursor_by_name(name)
    }

    #[cfg(test)]
    pub(crate) fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog.index_cursor_by_name(name)
    }
}

impl CatalogRead for Database {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        self.catalog.table_schema_by_name(name)
    }

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>> {
        self.catalog.index_schemas_for_table(table)
    }
}

impl Database {
    pub(crate) fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.catalog.create_table(name, row)
    }

    pub(crate) fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        index_manager::create_index(&self.catalog, name, table_name, columns)
    }
}

impl Database {
    pub(crate) fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        record_manager::scan_table(&self.catalog, table)
    }

    pub(crate) fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        record_manager::scan_table_range(&self.catalog, table, range)
    }

    pub(crate) fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan> {
        record_manager::scan_index(&self.catalog, table, index, key_range)
    }

    pub(crate) fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        record_manager::insert_table_row(&self.catalog, table, values)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        record_manager::delete_table_row(&self.catalog, table, record)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        record_manager::update_table_row(&self.catalog, table, record, values)
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
        drop(database);

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
