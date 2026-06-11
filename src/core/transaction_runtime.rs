use std::rc::Rc;

use crate::core::{
    error::StorageResult, log_manager::TxnId, page_cache::PageCache,
    storage_runtime::StorageRuntime,
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

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let undo_pages = self.runtime.take_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(undo_pages)?;
        self.page_cache.flush_all()?;
        self.runtime.sync_database_file()?;
        self.runtime.finish_rollback(txn_id)
    }
}
