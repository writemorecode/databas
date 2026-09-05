use std::path::Path;

use crate::core::{
    IndexSchema, TableId, TableSchema,
    access::CatalogRead,
    error::StorageResult,
    lock_manager::{LockManager, TableLease},
};
use crate::relational::catalog_manager::CatalogManager;
#[cfg(test)]
use crate::relational::cursor::{IndexCursor, TableCursor};
use crate::storage::{
    engine::Storage, log_manager::TxnId, transaction_manager::TransactionSavepoint,
};

/// Public database handle for one database file.
pub struct Database {
    catalog: CatalogManager,
    storage: Storage,
    locks: LockManager,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementTransactionMode {
    Ordinary,
    Ddl,
}

impl Database {
    /// Creates a new database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file already exists, cannot be initialized,
    /// or its initial catalog and write-ahead log cannot be written.
    pub fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let storage = Storage::create(path)?;
        Self::from_storage(storage)
    }

    /// Opens an existing database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, its format is invalid,
    /// or recovery cannot restore it to a consistent state.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let storage = Storage::open(path)?;
        Self::from_storage(storage)
    }

    /// Opens a database file, creating and initializing it if needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or initialized, its
    /// format is invalid, or recovery cannot restore it to a consistent state.
    pub fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let storage = Storage::open_or_create(path)?;
        Self::from_storage(storage)
    }

    fn from_storage(storage: Storage) -> StorageResult<Self> {
        let catalog = CatalogManager::from_storage(storage.clone())?;
        Ok(Self { catalog, storage, locks: LockManager::default() })
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
        self.storage.unlock_for_crash_for_test().unwrap();
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.begin_statement_transaction(StatementTransactionMode::Ordinary)
    }

    pub(crate) fn acquire_ddl_gate(&self, txn_id: TxnId) -> StorageResult<()> {
        self.locks.acquire_ddl_gate(txn_id).map_err(Into::into)
    }

    pub(crate) fn begin_statement_transaction(
        &self,
        mode: StatementTransactionMode,
    ) -> StorageResult<TxnId> {
        let txn_id = self.storage.begin_transaction()?;
        let admission = match mode {
            StatementTransactionMode::Ordinary => self.locks.begin_transaction(txn_id),
            StatementTransactionMode::Ddl => self.locks.begin_ddl_transaction(txn_id),
        };
        if let Err(error) = admission {
            self.storage.rollback_transaction(txn_id)?;
            return Err(error.into());
        }
        Ok(txn_id)
    }

    pub(crate) fn acquire_table_leases(
        &self,
        txn_id: TxnId,
        table_ids: &[TableId],
    ) -> StorageResult<Vec<TableLease>> {
        table_ids
            .iter()
            .map(|table_id| self.locks.acquire(txn_id, *table_id).map_err(Into::into))
            .collect()
    }

    /// Returns the concrete relational gateway for an active transaction.
    pub(crate) fn transaction(
        &self,
        txn_id: TxnId,
        leases: Vec<TableLease>,
    ) -> crate::core::transaction::Transaction<'_> {
        crate::core::transaction::Transaction::new(self, txn_id, leases)
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let result = self.storage.commit_transaction(txn_id);
        if !self.storage.transaction_is_active(txn_id)? {
            self.locks.begin_commit(txn_id)?;
            self.locks.finish_transaction(txn_id)?;
        }
        result
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.storage.statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        self.storage.rollback_to_savepoint(savepoint)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.locks.begin_rollback(txn_id)?;
        self.storage.rollback_transaction(txn_id)?;
        self.locks.finish_transaction(txn_id)?;
        Ok(())
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.storage.transaction_is_active(txn_id)
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.storage.transaction_is_poisoned(txn_id)
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&self) {
        self.storage.force_next_lsn_exhausted_for_test().unwrap();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_savepoint_rollback_for_test(&self) {
        self.storage.fail_next_savepoint_rollback_for_test().unwrap();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.storage.fail_next_wal_flush_for_test().unwrap();
    }

    #[cfg(test)]
    pub(crate) fn transaction_is_waiting_for_test(
        &self,
        txn_id: TxnId,
        table_id: TableId,
    ) -> StorageResult<bool> {
        Ok(self.locks.transaction_is_waiting_for(txn_id, table_id)?)
    }

    #[cfg(test)]
    pub(crate) fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        self.catalog.table_cursor_by_name(name)
    }

    #[cfg(test)]
    pub(crate) fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog.index_cursor_by_name(name)
    }

    pub(super) fn catalog(&self) -> &CatalogManager {
        &self.catalog
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

#[cfg(test)]
mod tests {
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::core::{
        LockError,
        error::{CorruptionError, CorruptionKind, StorageError},
    };

    #[test]
    fn database_handle_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
    }

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
    fn failed_lock_admission_rolls_back_the_new_storage_transaction() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let ddl_txn = database.begin_statement_transaction(StatementTransactionMode::Ddl).unwrap();
        let rejected_txn = ddl_txn + 1;

        assert!(matches!(
            database.begin_statement_transaction(StatementTransactionMode::Ordinary),
            Err(StorageError::Lock(LockError::DdlBusy { txn_id }))
                if txn_id == rejected_txn
        ));
        assert!(!database.storage.transaction_is_active(rejected_txn).unwrap());
        assert_eq!(
            database.locks.transaction_phase(rejected_txn),
            Err(LockError::TransactionNotActive { txn_id: rejected_txn })
        );

        database.rollback_transaction(ddl_txn).unwrap();
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
