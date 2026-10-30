use std::path::PathBuf;

use crate::disk_manager::DiskManager;
use crate::error::{InternalError, StorageError, StorageResult};
use crate::log_manager::{LogManager, Lsn, TxnId};
use crate::page_allocator::PageAllocator;
use crate::recovery::recover_from_wal;
use crate::transaction_manager::{
    LoggedPageUpdate, PageRestore, TransactionManager, TransactionRollback, TransactionSavepoint,
};
use crate::{CheckpointOutcome, PAGE_SIZE, PageId};
use std::sync::{Mutex, MutexGuard};

type ActiveTransactionManager = TransactionManager;

fn make_transaction_manager(max_txn_id: TxnId) -> ActiveTransactionManager {
    TransactionManager::new(max_txn_id)
}

/// Shared concrete storage runtime for database pages and the write-ahead log.
///
/// The runtime keeps raw database-file I/O and WAL I/O adjacent without making
/// either manager own the other. Page cache code uses it for WAL-protected page
/// writes, and future transaction code can share the same log manager.
/// Operations that need both transaction and log state always lock transactions
/// before the log.
pub struct StorageRuntime {
    path: PathBuf,
    checkpoint: Mutex<Checkpointer>,
    disk: Mutex<DiskManager>,
    log: Mutex<LogManager>,
    transactions: Mutex<ActiveTransactionManager>,
    allocator: Mutex<PageAllocator>,
}

impl StorageRuntime {
    pub(crate) fn new(path: PathBuf, mut disk: DiskManager) -> StorageResult<Self> {
        disk.lock_exclusive()?;
        let _ = PageAllocator::load(&mut disk)?;
        let recovery = recover_from_wal(&path, &mut disk)?;
        let log = LogManager::new(&path)?;
        let allocator = PageAllocator::load(&mut disk)?;
        let max_txn_id = recovery.max_txn_id.max(log.highest_txn_id());
        Ok(Self {
            checkpoint: Mutex::new(Checkpointer { path: path.clone() }),
            path,
            allocator: Mutex::new(allocator),
            disk: Mutex::new(disk),
            log: Mutex::new(log),
            transactions: Mutex::new(make_transaction_manager(max_txn_id)),
        })
    }

    fn lock<'a, T>(mutex: &'a Mutex<T>, lock: &'static str) -> StorageResult<MutexGuard<'a, T>> {
        mutex.lock().map_err(|_poisoned| {
            StorageError::Internal(InternalError::SynchronizationPoisoned { lock })
        })
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(crate) fn new_page(&self, txn_id: Option<TxnId>) -> StorageResult<PageId> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        if let Some(id) = txn_id
            && !transactions.transaction_is_active(id)
        {
            return Err(StorageError::Internal(InternalError::InvariantViolation(
                crate::error::InvariantViolation::NoActiveTransaction,
            )));
        }
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let mut disk = Self::lock(&self.disk, "disk manager")?;
        let page_id = allocator.candidate(&mut disk)?;
        if let Some(id) = txn_id {
            transactions.record_page_alloc(&mut log, id, page_id)?;
        }
        allocator.set_allocated(page_id, true);
        Ok(page_id)
    }

    pub(crate) fn dealloc_page(&self, txn_id: Option<TxnId>, page_id: PageId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        if PageAllocator::is_metadata(page_id)
            || !allocator.is_allocated(page_id)
            || transactions.page_pending_dealloc(page_id)
        {
            return Err(StorageError::Internal(InternalError::InvariantViolation(
                crate::error::InvariantViolation::InvalidPageDeallocation { page_id },
            )));
        }
        if let Some(id) = txn_id {
            let mut log = Self::lock(&self.log, "log manager")?;
            transactions.record_page_dealloc(&mut log, id, page_id)?;
        } else {
            allocator.set_allocated(page_id, false);
        }
        Ok(())
    }

    pub(crate) fn page_is_live(&self, page_id: PageId) -> StorageResult<bool> {
        let transactions = Self::lock(&self.transactions, "transaction manager")?;
        let allocator = Self::lock(&self.allocator, "page allocator")?;
        Ok(allocator.is_allocated(page_id) && !transactions.page_pending_dealloc(page_id))
    }

    /// Lock order: cache metadata, transactions, allocator, WAL, disk. Never
    /// enter the cache while holding runtime latches. WAL protects bitmap writes
    /// even when a checkpoint persists reservations belonging to active writers.
    pub(crate) fn flush_allocations(&self) -> StorageResult<()> {
        let _transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        log.flush_all()?;
        let mut disk = Self::lock(&self.disk, "disk manager")?;
        allocator.flush(&mut disk)
    }

    pub(crate) fn read_page(
        &self,
        page_id: PageId,
        buf: &mut [u8; PAGE_SIZE],
    ) -> StorageResult<()> {
        Ok(Self::lock(&self.disk, "disk manager")?.read_page(page_id, buf)?)
    }

    pub(crate) fn write_page(&self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> StorageResult<()> {
        Ok(Self::lock(&self.disk, "disk manager")?.write_page(page_id, buf)?)
    }

    pub(crate) fn sync_database_file(&self) -> StorageResult<()> {
        Ok(Self::lock(&self.disk, "disk manager")?.sync()?)
    }

    pub(crate) fn flush_wal_through(&self, lsn: Lsn) -> StorageResult<()> {
        Self::lock(&self.log, "log manager")?.flush_through(lsn)?;
        Ok(())
    }

    /// Conservative dependency for a write guard that has not logged yet.
    pub(crate) fn next_wal_lsn(&self) -> StorageResult<Lsn> {
        // Exhaustion is reported by the mutation's normal logging path, which
        // restores bytes and poisons its transaction. Until then retain all WAL.
        Ok(Self::lock(&self.log, "log manager")?.next_lsn().unwrap_or(0))
    }

    pub(crate) fn checkpoint<F>(&self, flush_cache: F) -> StorageResult<CheckpointOutcome>
    where
        F: FnOnce() -> StorageResult<Option<Lsn>>,
    {
        Self::lock(&self.checkpoint, "checkpoint serialization")?.run(self, flush_cache)
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.begin(&mut log)
    }

    pub(crate) fn record_page_update(
        &self,
        txn_id: Option<TxnId>,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        let Some(txn_id) = txn_id else {
            return Ok(None);
        };
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let result = transactions.record_page_update(&mut log, txn_id, page_id, before, after);
        if result.is_err() {
            transactions.record_failure(txn_id);
        }
        result
    }

    pub(crate) fn record_transaction_failure(&self, txn_id: TxnId) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?.record_failure(txn_id);
        Ok(())
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> StorageResult<bool> {
        Ok(Self::lock(&self.transactions, "transaction manager")?.transaction_is_active(txn_id))
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        Self::lock(&self.transactions, "transaction manager")?.transaction_is_poisoned(txn_id)
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let changes = transactions.allocation_changes(txn_id)?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.commit(&mut log, txn_id)?;
        for (id, allocated) in changes {
            allocator.set_allocated(id, allocated);
        }
        Ok(())
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        Self::lock(&self.transactions, "transaction manager")?.statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.rollback_to_savepoint(&mut log, savepoint)
    }

    pub(crate) fn complete_savepoint_rollback(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?
            .complete_savepoint_rollback(savepoint)
    }

    pub(crate) fn prepare_rollback_pages(
        &self,
        txn_id: TxnId,
    ) -> StorageResult<TransactionRollback> {
        Self::lock(&self.transactions, "transaction manager")?.prepare_rollback_pages(txn_id)
    }

    pub(crate) fn finish_rollback(&self, txn_id: TxnId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let changes = transactions.allocation_changes(txn_id)?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.finish_rollback(&mut log, txn_id)?;
        // The inverse of the first transition is each page's original state.
        for (id, allocated) in changes.into_iter().rev() {
            allocator.set_allocated(id, !allocated);
        }
        Ok(())
    }
}

/// Owns checkpoint execution and the path used for WAL reclamation.
///
/// `run` requires mutable access, so the runtime must hold its checkpoint mutex
/// throughout horizon capture, cache flushing, and WAL publication.
struct Checkpointer {
    path: PathBuf,
}

impl Checkpointer {
    fn run<F>(&self, runtime: &StorageRuntime, flush_cache: F) -> StorageResult<CheckpointOutcome>
    where
        F: FnOnce() -> StorageResult<Option<Lsn>>,
    {
        // Capture a fixed horizon and preserve all undo for transactions alive
        // now. New transactions begin after this horizon and are retained too.
        let mut retain_from = {
            let transactions = StorageRuntime::lock(&runtime.transactions, "transaction manager")?;
            let log = StorageRuntime::lock(&runtime.log, "log manager")?;
            let horizon = log.next_lsn()?;
            transactions.oldest_begin_lsn().map_or(horizon, |oldest| oldest.min(horizon))
        };
        // No transaction/log latch is held while entering the page cache.
        let deferred = flush_cache()?;
        if let Some(oldest) = deferred {
            retain_from = retain_from.min(oldest);
        }
        runtime.flush_allocations()?;
        runtime.sync_database_file()?;
        // Publication serializes with WAL appends, not
        // transaction lifetimes. Never acquire cache latches in this section.
        let _transactions = StorageRuntime::lock(&runtime.transactions, "transaction manager")?;
        let mut log = StorageRuntime::lock(&runtime.log, "log manager")?;
        let progress = log.reclaim_prefix(&self.path, retain_from)?;
        if progress.remaining_records == 0 && deferred.is_none() {
            Ok(CheckpointOutcome::Completed)
        } else {
            Ok(CheckpointOutcome::Partial)
        }
    }
}
