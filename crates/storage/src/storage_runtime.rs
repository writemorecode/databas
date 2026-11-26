use std::path::PathBuf;

use crate::disk_manager::DiskManager;
use crate::error::{InternalError, StorageError, StorageResult};
use crate::filesystem::FileSystem;
use crate::log_manager::{LogManager, Lsn, TxnId};
use crate::logical_undo::LogicalUndo;
use crate::page_allocator::PageAllocator;
use crate::page_operation::LeafOperation;
use crate::page_recovery::PageInitKind;
use crate::recovery::{Restart, repeat_history};
use crate::transaction_manager::{
    ActionBoundary, TransactionManager, TransactionSavepoint, UndoStep,
};
use crate::{CheckpointOutcome, PAGE_SIZE, PageId};
use std::sync::{Mutex, MutexGuard};

/// Shared concrete storage runtime for database pages and the write-ahead log.
///
/// The runtime keeps raw database-file I/O and WAL I/O adjacent without making
/// either manager own the other. Page cache code uses it for WAL-protected page
/// writes, and future transaction code can share the same log manager.
/// Operations that need both transaction and log state always lock transactions
/// before the log.
pub struct StorageRuntime {
    path: PathBuf,
    checkpoint: Mutex<()>,
    disk: Mutex<DiskManager>,
    log: Mutex<LogManager>,
    transactions: Mutex<TransactionManager>,
    allocator: Mutex<PageAllocator>,
}

impl StorageRuntime {
    pub(crate) fn new(
        fs: FileSystem,
        path: PathBuf,
        mut disk: DiskManager,
    ) -> StorageResult<(Self, bool)> {
        disk.lock_exclusive()?;
        let Restart { log, allocator, transactions, needs_checkpoint } =
            repeat_history(fs, &path, &mut disk)?;
        Ok((
            Self {
                checkpoint: Mutex::new(()),
                path,
                allocator: Mutex::new(allocator),
                disk: Mutex::new(disk),
                log: Mutex::new(log),
                transactions: Mutex::new(transactions),
            },
            needs_checkpoint,
        ))
    }

    pub(crate) fn validate_allocation_bounds(&self) -> StorageResult<()> {
        let allocator = Self::lock(&self.allocator, "page allocator")?;
        let disk = Self::lock(&self.disk, "disk manager")?;
        allocator.validate_bounds(disk.page_count())
    }

    fn lock<'a, T>(mutex: &'a Mutex<T>, lock: &'static str) -> StorageResult<MutexGuard<'a, T>> {
        mutex.lock().map_err(|_poisoned| {
            StorageError::Internal(InternalError::SynchronizationPoisoned { lock })
        })
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(crate) fn new_page(
        &self,
        txn_id: TxnId,
        kind: PageInitKind,
    ) -> StorageResult<(PageId, Lsn)> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        transactions.validate_transaction_write(txn_id)?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let mut disk = Self::lock(&self.disk, "disk manager")?;
        let page_id = allocator.candidate(&mut disk)?;
        let lsn = transactions.record_page_init(&mut log, txn_id, page_id, kind)?;
        allocator.set_allocated(page_id, true, lsn);
        Ok((page_id, lsn))
    }

    pub(crate) fn dealloc_page(&self, txn_id: TxnId, page_id: PageId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let allocator = Self::lock(&self.allocator, "page allocator")?;
        if PageAllocator::is_metadata(page_id)
            || !allocator.is_allocated(page_id)
            || transactions.page_pending_dealloc(page_id)
        {
            return Err(StorageError::Internal(InternalError::InvariantViolation(
                crate::error::InvariantViolation::InvalidPageDeallocation { page_id },
            )));
        }
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.record_page_dealloc(&mut log, txn_id, page_id)
    }

    pub(crate) fn page_is_live(&self, page_id: PageId) -> StorageResult<bool> {
        let transactions = Self::lock(&self.transactions, "transaction manager")?;
        let allocator = Self::lock(&self.allocator, "page allocator")?;
        Ok(!PageAllocator::is_metadata(page_id)
            && allocator.is_allocated(page_id)
            && !transactions.page_pending_dealloc(page_id))
    }

    /// Lock order: cache metadata, transactions, allocator, WAL, disk. Never
    /// enter the cache while holding runtime latches. WAL protects bitmap writes
    /// even when a checkpoint persists reservations belonging to active writers.
    pub(crate) fn flush_allocations(&self) -> StorageResult<()> {
        let transactions = Self::lock(&self.transactions, "transaction manager")?;
        transactions.validate_can_append()?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let mut disk = Self::lock(&self.disk, "disk manager")?;
        allocator.flush(&mut disk, &mut log)
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
        // Serialize the whole checkpoint, including cache flushing and WAL publication.
        let _checkpoint = Self::lock(&self.checkpoint, "checkpoint serialization")?;
        // Keep undo for transactions already alive, including any CLR whose
        // page effect is not yet in the cache. New writes start at the horizon.
        let mut retain_from = {
            let transactions = Self::lock(&self.transactions, "transaction manager")?;
            let log = Self::lock(&self.log, "log manager")?;
            let horizon = log.next_lsn()?;
            transactions
                .oldest_begin_lsn()
                .unwrap_or(horizon)
                .min(transactions.oldest_pending_restore_lsn().unwrap_or(horizon))
        };
        // No transaction/log latch is held while entering the page cache.
        let deferred = flush_cache()?;
        if let Some(oldest) = deferred {
            retain_from = retain_from.min(oldest);
        }
        self.flush_allocations()?;
        self.sync_database_file()?;
        // Publication serializes with WAL appends, not
        // transaction lifetimes. Never acquire cache latches in this section.
        let _transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        if log.reclaim_prefix(retain_from)? == 0 && deferred.is_none() {
            Ok(CheckpointOutcome::Completed)
        } else {
            Ok(CheckpointOutcome::Partial)
        }
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.begin(&mut log)
    }

    pub(crate) fn record_page_update(
        &self,
        txn_id: TxnId,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
        leaf_redo: Option<LeafOperation>,
    ) -> StorageResult<Lsn> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let result =
            transactions.record_page_update(&mut log, txn_id, page_id, before, after, leaf_redo);
        if result.is_err() {
            transactions.record_failure(txn_id);
        }
        result
    }

    pub(crate) fn record_transaction_failure(&self, txn_id: TxnId) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?.record_failure(txn_id);
        Ok(())
    }

    pub(crate) fn validate_transaction_write(&self, txn_id: TxnId) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?.validate_transaction_write(txn_id)
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> StorageResult<bool> {
        Ok(Self::lock(&self.transactions, "transaction manager")?.transaction_is_active(txn_id))
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        Self::lock(&self.transactions, "transaction manager")?.transaction_is_poisoned(txn_id)
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let changes = transactions.allocation_states(txn_id)?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let lsn = transactions.commit(&mut log, txn_id)?;
        for (id, allocated) in changes {
            allocator.set_allocated(id, allocated, lsn);
        }
        Ok(())
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        Self::lock(&self.transactions, "transaction manager")?.statement_savepoint(txn_id)
    }

    pub(crate) fn has_surviving_root(
        &self,
        txn_id: TxnId,
        incarnation: Lsn,
    ) -> StorageResult<bool> {
        Self::lock(&self.transactions, "transaction manager")?
            .has_surviving_root(txn_id, incarnation)
    }

    pub(crate) fn begin_action(&self, txn_id: TxnId) -> StorageResult<ActionBoundary> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let log = Self::lock(&self.log, "log manager")?;
        transactions.begin_action(&log, txn_id)
    }

    pub(crate) fn complete_action(
        &self,
        txn_id: TxnId,
        boundary: ActionBoundary,
        inverse: Option<LogicalUndo>,
    ) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.complete_action(&mut log, txn_id, boundary, inverse)
    }

    pub(crate) fn start_abort(&self, txn_id: TxnId) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?.start_abort(txn_id)
    }

    pub(crate) fn loser_heads(&self) -> StorageResult<std::collections::BinaryHeap<(Lsn, TxnId)>> {
        Ok(Self::lock(&self.transactions, "transaction manager")?.loser_heads())
    }

    pub(crate) fn undo_head(&self, txn_id: TxnId) -> StorageResult<Lsn> {
        Self::lock(&self.transactions, "transaction manager")?.undo_head(txn_id)
    }

    pub(crate) fn prepare_undo(
        &self,
        txn_id: TxnId,
        savepoint: Option<TransactionSavepoint>,
    ) -> StorageResult<UndoStep> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.prepare_undo(&mut log, txn_id, savepoint)
    }

    pub(crate) fn finish_page_undo(&self, txn_id: TxnId) -> StorageResult<()> {
        Self::lock(&self.transactions, "transaction manager")?.finish_page_undo(txn_id)
    }

    pub(crate) fn finish_logical_undo(
        &self,
        txn_id: TxnId,
        boundary: ActionBoundary,
        completed: bool,
    ) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        transactions.finish_logical_undo(&mut log, txn_id, boundary, completed)
    }

    pub(crate) fn flush_transaction_wal(&self, txn_id: TxnId) -> StorageResult<()> {
        let mut log = Self::lock(&self.log, "log manager")?;
        let lsn = log.transaction_head(txn_id);
        Ok(log.flush_through(lsn)?)
    }

    pub(crate) fn finish_rollback(&self, txn_id: TxnId) -> StorageResult<()> {
        let mut transactions = Self::lock(&self.transactions, "transaction manager")?;
        let changes = transactions.allocation_states(txn_id)?;
        let mut allocator = Self::lock(&self.allocator, "page allocator")?;
        let mut log = Self::lock(&self.log, "log manager")?;
        let lsn = transactions.finish_rollback(&mut log, txn_id)?;
        for (id, allocated) in changes {
            allocator.set_allocated(id, allocated, lsn);
        }
        Ok(())
    }
}
