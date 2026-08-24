use std::rc::Rc;

use crate::core::error::StorageResult;
#[cfg(test)]
use crate::storage::disk_manager::DiskManagerError;
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

    #[cfg(test)]
    pub(crate) fn unlock_for_crash_for_test(&self) -> Result<(), DiskManagerError> {
        self.runtime.unlock_for_crash_for_test()
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
        self.runtime.complete_savepoint_rollback(savepoint)
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
    pub(crate) fn fail_next_savepoint_rollback_for_test(&self) {
        self.runtime.fail_next_savepoint_rollback_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.runtime.fail_next_wal_flush_for_test();
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::PAGE_SIZE;
    use crate::storage::{
        disk_manager::DiskManager, page_cache::PageCache, storage_runtime::StorageRuntime,
    };

    // Issue: `rollback_to_savepoint` truncates the manager's undo log before the
    // page cache installs the returned images. If installation then fails (here a
    // pinned one-frame cache cannot fetch the evicted page), full rollback has no
    // undo image left and can make the failed statement's page change permanent.
    #[test]
    fn failed_savepoint_restore_remains_undoable_by_full_rollback() {
        let file = NamedTempFile::new().unwrap();
        let before = [1; PAGE_SIZE];
        let other = [2; PAGE_SIZE];
        let mut disk = DiskManager::new(file.path()).unwrap();
        disk.ensure_page_exists(1).unwrap();
        disk.write_page(0, &before).unwrap();
        disk.write_page(1, &other).unwrap();
        let runtime = Rc::new(StorageRuntime::new(file.path().to_path_buf(), disk).unwrap());
        let cache = PageCache::new(Rc::clone(&runtime), 1).unwrap();
        let transactions = TransactionRuntime::new(Rc::clone(&runtime), cache.clone());

        let txn_id = transactions.begin_transaction().unwrap();
        let savepoint = transactions.statement_savepoint(txn_id).unwrap();
        {
            let page = cache.fetch_page(0).unwrap();
            page.write().unwrap().page_mut()[PAGE_SIZE - 1] = 99;
        }

        // Evict the changed page and keep the only frame pinned so restoring it fails.
        let pinned_other_page = cache.fetch_page(1).unwrap();
        assert!(transactions.rollback_to_savepoint(savepoint).is_err());
        drop(pinned_other_page);

        transactions.rollback_transaction(txn_id).unwrap();

        let mut actual = [0; PAGE_SIZE];
        runtime.read_page(0, &mut actual).unwrap();
        assert_eq!(actual[PAGE_SIZE - 1], before[PAGE_SIZE - 1]);
    }
}
