use std::path::Path;

use crate::access::CatalogRead;
use crate::catalog_manager::CatalogManager;
use crate::error::RelationalResult;
use crate::lock_manager::{LockManager, LockMode, TableLease};
use crate::storage::{Storage, TransactionSavepoint, TxnId};
use crate::{IndexSchema, TableId, TableSchema};

/// Public database handle for one database file.
pub struct Database {
    catalog: CatalogManager,
    locks: LockManager,
}

pub use crate::storage::CheckpointOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementTransactionMode {
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
    pub fn create(path: impl AsRef<Path>) -> RelationalResult<Self> {
        let storage = Storage::create(path)?;
        Self::from_storage(storage)
    }

    /// Opens an existing database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened, its format is invalid,
    /// or recovery cannot restore it to a consistent state.
    pub fn open(path: impl AsRef<Path>) -> RelationalResult<Self> {
        let storage = Storage::open(path)?;
        Self::from_storage(storage)
    }

    /// Opens a database file, creating and initializing it if needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or initialized, its
    /// format is invalid, or recovery cannot restore it to a consistent state.
    pub fn open_or_create(path: impl AsRef<Path>) -> RelationalResult<Self> {
        let storage = Storage::open_or_create(path)?;
        Self::from_storage(storage)
    }

    fn from_storage(storage: Storage) -> RelationalResult<Self> {
        let catalog = CatalogManager::from_storage(storage)?;
        Ok(Self { catalog, locks: LockManager::default() })
    }

    /// Returns the database-file path associated with this database.
    #[cfg(test)]
    fn path(&self) -> &Path {
        self.catalog.path()
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    ///
    /// # Errors
    ///
    /// Returns an error when a dirty page cannot be written or when an active
    /// page pin prevents a consistent flush.
    pub fn flush(&self) -> RelationalResult<()> {
        self.catalog.flush()
    }

    /// Flushes pages incrementally and reclaims a safe WAL prefix while
    /// transactions continue. Busy pages are deferred, not errors. Active
    /// transactions retain their undo history but do not prevent the pass.
    ///
    /// # Errors
    /// Returns an error if flushing, replay, or WAL replacement fails. A failure
    /// during replacement disables further WAL writes; reopen the database.
    pub fn checkpoint(&self) -> RelationalResult<CheckpointOutcome> {
        self.catalog.storage().checkpoint().map_err(Into::into)
    }

    /// Disables new lock requests and cancels pending waits after a fatal error.
    pub fn stop(&self) -> RelationalResult<()> {
        self.locks.stop().map_err(Into::into)
    }

    /// Begins a transaction whose locks and rollback lifecycle are owned by the handle.
    pub fn begin(
        &self,
        mode: StatementTransactionMode,
    ) -> RelationalResult<super::ActiveTransaction<'_>> {
        let id = self.begin_statement_transaction(mode)?;
        Ok(super::ActiveTransaction::new(self, id))
    }

    pub(crate) fn acquire_ddl_gate(&self, txn_id: TxnId) -> RelationalResult<()> {
        self.locks.acquire_ddl_gate(txn_id).map_err(Into::into)
    }

    fn begin_statement_transaction(
        &self,
        mode: StatementTransactionMode,
    ) -> RelationalResult<TxnId> {
        let txn_id = self.catalog.storage().begin_transaction()?;
        let admission = match mode {
            StatementTransactionMode::Ordinary => self.locks.begin_transaction(txn_id),
            StatementTransactionMode::Ddl => self.locks.begin_ddl_transaction(txn_id),
        };
        if let Err(error) = admission {
            self.catalog.storage().rollback_transaction(txn_id)?;
            return Err(error.into());
        }
        Ok(txn_id)
    }

    pub(crate) fn acquire_table_leases(
        &self,
        txn_id: TxnId,
        table_locks: &[(TableId, LockMode)],
    ) -> RelationalResult<Vec<TableLease>> {
        // The relational API, rather than its SQL caller, owns lock ordering.
        let mut ordered = std::collections::BTreeMap::new();
        for &(table, mode) in table_locks {
            ordered
                .entry(table)
                .and_modify(|held| {
                    if mode == LockMode::Exclusive {
                        *held = mode;
                    }
                })
                .or_insert(mode);
        }
        ordered
            .into_iter()
            .map(|(table, mode)| self.locks.acquire_mode(txn_id, table, mode).map_err(Into::into))
            .collect()
    }

    /// Returns the concrete relational gateway for an active transaction.
    pub(crate) fn transaction(
        &self,
        txn_id: TxnId,
        leases: Vec<TableLease>,
        ddl: bool,
    ) -> crate::transaction::Transaction<'_> {
        crate::transaction::Transaction::new(self, txn_id, leases, ddl)
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> RelationalResult<()> {
        let result = self.catalog.storage().commit_transaction(txn_id);
        if !self.catalog.storage().transaction_is_active(txn_id)? {
            self.locks.begin_commit(txn_id)?;
            self.locks.finish_transaction(txn_id)?;
        }
        result.map_err(Into::into)
    }

    pub(crate) fn statement_savepoint(
        &self,
        txn_id: TxnId,
    ) -> RelationalResult<TransactionSavepoint> {
        self.catalog.storage().statement_savepoint(txn_id).map_err(Into::into)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> RelationalResult<()> {
        self.catalog.storage().rollback_to_savepoint(savepoint).map_err(Into::into)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> RelationalResult<()> {
        self.locks.begin_rollback(txn_id)?;
        self.catalog.storage().rollback_transaction(txn_id)?;
        self.locks.finish_transaction(txn_id)?;
        Ok(())
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> RelationalResult<bool> {
        self.catalog.storage().transaction_is_active(txn_id).map_err(Into::into)
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> RelationalResult<bool> {
        self.catalog.storage().transaction_is_poisoned(txn_id).map_err(Into::into)
    }

    pub(super) fn catalog(&self) -> &CatalogManager {
        &self.catalog
    }
}

impl CatalogRead for Database {
    fn table_schema_by_name(&self, name: &str) -> RelationalResult<TableSchema> {
        self.catalog.table_schema_by_name(name)
    }

    fn index_schemas_for_table(&self, table: &TableSchema) -> RelationalResult<Vec<IndexSchema>> {
        self.catalog.index_schemas_for_table(table)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::LockError;
    use crate::error::RelationalError;

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
            Err(RelationalError::Lock(LockError::DdlBusy { txn_id }))
                if txn_id == rejected_txn
        ));
        assert!(!database.catalog.storage().transaction_is_active(rejected_txn).unwrap());
        assert_eq!(
            database.locks.transaction_phase(rejected_txn),
            Err(LockError::TransactionNotActive { txn_id: rejected_txn })
        );

        database.rollback_transaction(ddl_txn).unwrap();
    }

    #[test]
    fn open_rejects_empty_file_without_header() {
        use crate::storage::error::{CorruptionError, CorruptionKind, StorageError as EngineError};
        let file = NamedTempFile::new().unwrap();

        assert!(matches!(
            Database::open(file.path()),
            Err(RelationalError::Storage(EngineError::Corruption(CorruptionError {
                kind: CorruptionKind::MissingDatabaseHeader,
                ..
            })))
        ));
    }
}
