use std::rc::Rc;

use crate::core::error::StorageResult;
use crate::storage::{
    log_manager::TxnId, page_cache::PageCache, storage_runtime::StorageRuntime,
    transaction_manager::TransactionSavepoint,
};

/// Transaction-facing runtime for a database file.
///
/// `TransactionRuntime` owns the transaction lifecycle surface used by higher
/// layers. It keeps rollback orchestration close to the page cache and storage
/// runtime without routing transaction calls through catalog code.
#[derive(Clone)]
pub(crate) struct TransactionRuntime {
    runtime: Rc<StorageRuntime>,
    page_cache: PageCache,
}

impl TransactionRuntime {
    pub(crate) fn new(runtime: Rc<StorageRuntime>, page_cache: PageCache) -> Self {
        Self { runtime, page_cache }
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.runtime.begin_transaction()
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.runtime.commit_transaction(txn_id)
    }

    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.runtime.active_transaction_id()
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.runtime.transaction_is_poisoned(txn_id)
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.runtime.statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        let undo_pages = self.runtime.rollback_to_savepoint(savepoint)?;
        if let Err(err) = self.page_cache.restore_rollback_pages(undo_pages) {
            self.runtime.record_transaction_failure();
            return Err(err.into());
        }
        Ok(())
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let rollback = self.runtime.prepare_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(rollback.pages)?;
        self.page_cache.flush_all()?;
        self.runtime.sync_database_file()?;
        self.runtime.finish_rollback(txn_id)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&self) {
        self.runtime.force_next_lsn_exhausted_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.runtime.fail_next_wal_flush_for_test();
    }
}
