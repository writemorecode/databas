//! WAL-backed transactions with restartable physical and logical undo.
//!
//! An open action uses ordinary physical undo. Its completion record either
//! replaces that work with one logical inverse or skips it altogether. A
//! logical inverse runs as another physical action until a CLR completes it.
//! Live rollback and restart walk the same undo stack and log before installing
//! page compensation. Pending installation is a checkpoint dependency.

use std::collections::{BTreeMap, HashMap};

use crate::error::{InternalError, InvariantViolation, StorageError, StorageResult};
use crate::log_manager::{
    LogManager, LogRecordKind, Lsn, RecoveryLogRecord, RecoveryLogRecordKind as Kind, TxnId,
};
use crate::logical_undo::LogicalUndo;
use crate::page_operation::{LeafOperation, PageOperation};
use crate::page_patch::PagePatch;
use crate::page_recovery::{PageInitKind, PageRecovery};
use crate::{PAGE_SIZE, PageId};

#[derive(Debug, Clone)]
struct UndoEntry {
    lsn: Lsn,
    prev_lsn: Lsn,
    kind: UndoKind,
}

#[derive(Debug, Clone)]
enum UndoKind {
    Page { page_id: PageId, incarnation: Lsn, undo: PagePatch },
    Allocation { page_id: PageId, allocated: bool },
    Logical(LogicalUndo),
}

/// A logged compensation to install in the page cache.
#[derive(Debug, Clone)]
pub struct PageRestore {
    pub(crate) page_id: PageId,
    pub(crate) incarnation: Lsn,
    pub(crate) patch: PagePatch,
    pub(crate) wal_flush_lsn: Lsn,
}

/// Work that must run without holding the transaction/WAL latches.
pub enum UndoStep {
    Done,
    Allocation,
    Page(PageRestore),
    Logical { inverse: LogicalUndo, boundary: ActionBoundary },
}

/// Captures both the undo stack and the actual WAL predecessor. The latter
/// may be a CLR, which is not itself on the stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionBoundary {
    undo_len: usize,
    predecessor: Lsn,
}

/// Transaction-scoped boundary for statement rollback.
#[derive(Debug, Clone, Copy)]
pub struct TransactionSavepoint {
    pub(crate) txn_id: TxnId,
    undo_len: usize,
}

#[derive(Debug)]
pub struct TransactionManager {
    max_txn_id: TxnId,
    transactions: HashMap<TxnId, ActiveTransaction>,
    // A failed outcome flush must be retried in place or resolved on reopen
    // before a newer allocation change can advance the same bitmap's LSN.
    finishing: Option<TxnId>,
}

#[derive(Debug)]
struct ActiveTransaction {
    begin_lsn: Lsn,
    undo: Vec<UndoEntry>,
    // The final state to publish at Commit or Rollback. The physical bitmap
    // keeps all reservations until the outcome is durable.
    allocation_states: BTreeMap<PageId, bool>,
    rollback_lsn: Option<Lsn>,
    commit_lsn: Option<Lsn>,
    poisoned: bool,
    inverse_writable: bool,
    inverse_failed: bool,
    pending_restore: Option<PageRestore>,
    open_action: Option<ActionBoundary>,
}

impl ActiveTransaction {
    fn new(begin_lsn: Lsn, allocation_states: BTreeMap<PageId, bool>) -> Self {
        Self {
            begin_lsn,
            undo: Vec::new(),
            allocation_states,
            rollback_lsn: None,
            commit_lsn: None,
            poisoned: false,
            inverse_writable: false,
            inverse_failed: false,
            pending_restore: None,
            open_action: None,
        }
    }
}

impl TransactionManager {
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, transactions: HashMap::new(), finishing: None }
    }

    pub(crate) fn oldest_begin_lsn(&self) -> Option<Lsn> {
        self.transactions.values().map(|transaction| transaction.begin_lsn).min()
    }

    pub(crate) fn oldest_pending_restore_lsn(&self) -> Option<Lsn> {
        self.transactions
            .values()
            .filter_map(|txn| txn.pending_restore.as_ref().map(|restore| restore.wal_flush_lsn))
            .min()
    }

    pub(crate) fn restore_loser(
        &mut self,
        txn_id: TxnId,
        begin_lsn: Lsn,
        pending: BTreeMap<PageId, bool>,
        chain: Vec<(&RecoveryLogRecord, Lsn)>,
    ) {
        let mut active = ActiveTransaction::new(begin_lsn, pending);
        active.poisoned = true;
        for (record, prev_lsn) in chain.into_iter().rev() {
            let kind = match &record.kind {
                Kind::PageInit { page_id, .. } => {
                    UndoKind::Allocation { page_id: *page_id, allocated: true }
                }
                Kind::PageDealloc { page_id } => {
                    UndoKind::Allocation { page_id: *page_id, allocated: false }
                }
                Kind::PageUpdate { page_id, incarnation, undo, .. } => UndoKind::Page {
                    page_id: *page_id,
                    incarnation: *incarnation,
                    undo: undo.clone(),
                },
                Kind::LogicalEdit { undo, .. } => UndoKind::Logical(undo.clone()),
                _ => continue,
            };
            active.undo.push(UndoEntry { lsn: record.lsn, prev_lsn, kind });
        }
        self.transactions.insert(txn_id, active);
    }

    pub(crate) fn loser_heads(&self) -> std::collections::BinaryHeap<(Lsn, TxnId)> {
        self.transactions
            .iter()
            .map(|(&id, active)| {
                (active.undo.last().map_or(active.begin_lsn, |entry| entry.lsn), id)
            })
            .collect()
    }

    pub(crate) fn undo_head(&self, txn_id: TxnId) -> StorageResult<Lsn> {
        let active = self.transaction(txn_id)?;
        Ok(active.undo.last().map_or(active.begin_lsn, |entry| entry.lsn))
    }

    /// No newer change may pass a transaction with uncertain completion.
    pub(crate) fn validate_can_append(&self) -> StorageResult<()> {
        if let Some(txn_id) = self.finishing {
            return Err(invariant(InvariantViolation::UncertainTransactionOutcome { txn_id }));
        }
        Ok(())
    }

    pub(crate) fn begin(&mut self, log: &mut LogManager) -> StorageResult<TxnId> {
        self.validate_can_append()?;
        let txn_id = self
            .max_txn_id
            .checked_add(1)
            .ok_or_else(|| invariant(InvariantViolation::TransactionIdExhausted))?;
        let begin_lsn = log.append_record(txn_id, LogRecordKind::Begin)?;
        self.max_txn_id = txn_id;
        self.transactions.insert(txn_id, ActiveTransaction::new(begin_lsn, BTreeMap::new()));
        Ok(txn_id)
    }

    pub(crate) fn allocation_states(&self, txn_id: TxnId) -> StorageResult<BTreeMap<PageId, bool>> {
        Ok(self.transaction(txn_id)?.allocation_states.clone())
    }

    pub(crate) fn page_pending_dealloc(&self, page_id: PageId) -> bool {
        self.transactions
            .values()
            .any(|active| active.allocation_states.get(&page_id) == Some(&false))
    }

    pub(crate) fn record_page_init(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
        kind: PageInitKind,
    ) -> StorageResult<Lsn> {
        self.record_allocation_change(log, txn_id, page_id, Some(kind))
    }

    pub(crate) fn record_page_dealloc(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
    ) -> StorageResult<()> {
        self.record_allocation_change(log, txn_id, page_id, None).map(|_| ())
    }

    fn record_allocation_change(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
        init: Option<PageInitKind>,
    ) -> StorageResult<Lsn> {
        self.validate_transaction_write(txn_id)?;
        let prev_lsn = log.transaction_head(txn_id);
        let kind = match init {
            Some(kind) => LogRecordKind::PageInit { page_id, kind },
            None => LogRecordKind::PageDealloc { page_id },
        };
        let allocated = init.is_some();
        let lsn = log.append_record(txn_id, kind).inspect_err(|_| self.record_failure(txn_id))?;
        let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
        active.undo.push(UndoEntry {
            lsn,
            prev_lsn,
            kind: UndoKind::Allocation { page_id, allocated },
        });
        active.allocation_states.insert(page_id, allocated);
        Ok(lsn)
    }

    pub(crate) fn record_page_update(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
        leaf_redo: Option<LeafOperation>,
    ) -> StorageResult<Lsn> {
        self.validate_transaction_write(txn_id)?;
        let result = (|| {
            let prev_lsn = log.transaction_head(txn_id);
            let PageRecovery { incarnation, lsn: before_lsn } = PageRecovery::read(before);
            debug_assert!(incarnation != 0 && incarnation <= before_lsn);
            let (patch, undo) = PagePatch::between(before, after);
            let redo = leaf_redo.map_or(PageOperation::Patch(patch), PageOperation::Leaf);
            #[cfg(debug_assertions)]
            {
                // Physical undo depends on redo reproducing the exact layout,
                // including slot offsets and freeblocks, not just the same rows.
                let mut replayed = *before;
                debug_assert!(redo.apply(&mut replayed).is_ok());
                debug_assert!(replayed == *after);
                debug_assert!(undo.apply(&mut replayed).is_ok());
                debug_assert!(replayed == *before);
            }
            let lsn = log.append_record(
                txn_id,
                LogRecordKind::PageUpdate {
                    page_id,
                    incarnation,
                    before_lsn,
                    redo: &redo,
                    undo: &undo,
                },
            )?;
            let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
            active.undo.push(UndoEntry {
                lsn,
                prev_lsn,
                kind: UndoKind::Page { page_id, incarnation, undo },
            });
            Ok(lsn)
        })();
        result.inspect_err(|_| self.record_failure(txn_id))
    }

    pub(crate) fn record_failure(&mut self, txn_id: TxnId) {
        if let Some(active) = self.transactions.get_mut(&txn_id) {
            active.poisoned = true;
            if active.inverse_writable {
                active.inverse_failed = true;
            }
        }
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> bool {
        self.transactions.contains_key(&txn_id)
    }

    pub(crate) fn validate_transaction_write(&self, txn_id: TxnId) -> StorageResult<()> {
        self.validate_can_append()?;
        let active = self.transaction(txn_id)?;
        if active.poisoned && (!active.inverse_writable || active.inverse_failed) {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }
        Ok(())
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        Ok(self.transaction(txn_id)?.poisoned)
    }

    /// Retain an uncertain Commit and its reservations for a flush retry. Never
    /// permit explicit abort after Commit may already have reached stable storage.
    pub(crate) fn commit(&mut self, log: &mut LogManager, txn_id: TxnId) -> StorageResult<Lsn> {
        let active = self.transaction(txn_id)?;
        if active.open_action.is_some() || active.inverse_writable {
            return Err(invariant(InvariantViolation::UnfinishedTreeAction { txn_id }));
        }
        let commit_lsn = if let Some(lsn) = active.commit_lsn {
            lsn
        } else {
            self.validate_transaction_write(txn_id)?;
            let lsn = log
                .append_record(txn_id, LogRecordKind::Commit)
                .inspect_err(|_| self.record_failure(txn_id))?;
            let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
            active.commit_lsn = Some(lsn);
            active.poisoned = true;
            self.finishing = Some(txn_id);
            lsn
        };
        log.flush_through(commit_lsn)?;
        self.transactions.remove(&txn_id);
        self.finishing = None;
        Ok(commit_lsn)
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        Ok(TransactionSavepoint { txn_id, undo_len: self.transaction(txn_id)?.undo.len() })
    }

    /// Trees created by this transaction remain entirely physically undoable.
    /// There is no surviving owner for pages from a retained split on such a tree.
    pub(crate) fn has_surviving_root(
        &self,
        txn_id: TxnId,
        incarnation: Lsn,
    ) -> StorageResult<bool> {
        Ok(incarnation < self.transaction(txn_id)?.begin_lsn)
    }

    pub(crate) fn begin_action(
        &mut self,
        log: &LogManager,
        txn_id: TxnId,
    ) -> StorageResult<ActionBoundary> {
        self.validate_transaction_write(txn_id)?;
        let active = self.active_mut(txn_id)?;
        if active.open_action.is_some() {
            return Err(invariant(InvariantViolation::UnfinishedTreeAction { txn_id }));
        }
        let boundary = ActionBoundary {
            undo_len: active.undo.len(),
            predecessor: log.transaction_head(txn_id),
        };
        active.open_action = Some(boundary);
        Ok(boundary)
    }

    pub(crate) fn complete_action(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        boundary: ActionBoundary,
        inverse: Option<LogicalUndo>,
    ) -> StorageResult<()> {
        self.validate_transaction_write(txn_id)?;
        let active = self.active_mut(txn_id)?;
        if active.open_action != Some(boundary) {
            return Err(invariant(InvariantViolation::UnfinishedTreeAction { txn_id }));
        }
        if log.transaction_head(txn_id) == boundary.predecessor {
            active.open_action = None;
            return Ok(());
        }
        let kind = match &inverse {
            Some(undo) => LogRecordKind::LogicalEdit { undo, undo_next_lsn: boundary.predecessor },
            None => LogRecordKind::SkipUndo { undo_next_lsn: boundary.predecessor },
        };
        let lsn = log.append_record(txn_id, kind).inspect_err(|_| self.record_failure(txn_id))?;
        let active = self.active_mut(txn_id)?;
        active.undo.truncate(boundary.undo_len);
        active.open_action = None;
        if let Some(undo) = inverse {
            active.undo.push(UndoEntry {
                lsn,
                prev_lsn: boundary.predecessor,
                kind: UndoKind::Logical(undo),
            });
        }
        Ok(())
    }

    pub(crate) fn start_abort(&mut self, txn_id: TxnId) -> StorageResult<()> {
        self.active_mut(txn_id)?.poisoned = true;
        Ok(())
    }

    /// Append one CLR in undo order. A page CLR remains a pending redo
    /// dependency and stays on the undo stack until its cache install succeeds.
    pub(crate) fn prepare_undo(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        savepoint: Option<TransactionSavepoint>,
    ) -> StorageResult<UndoStep> {
        let active = self.transaction(txn_id)?;
        if active.commit_lsn.is_some() {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }
        if active.rollback_lsn.is_some() && savepoint.is_none() {
            return Ok(UndoStep::Done);
        }
        self.validate_can_append()?;
        let undo_len = savepoint.map_or(0, |savepoint| savepoint.undo_len);
        let active = self.active_mut(txn_id)?;
        if undo_len > active.undo.len() {
            return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                txn_id,
                undo_len,
                active_undo_len: active.undo.len(),
            }));
        }
        if active.open_action.is_some_and(|boundary| active.undo.len() <= boundary.undo_len) {
            active.open_action = None;
        }
        if undo_len == active.undo.len() {
            return Ok(UndoStep::Done);
        }
        if let Some(restore) = &active.pending_restore {
            return Ok(UndoStep::Page(restore.clone()));
        }
        let Some(entry) = active.undo.last() else { return Ok(UndoStep::Done) };
        let next = entry.prev_lsn;
        match &entry.kind {
            UndoKind::Page { page_id, incarnation, undo } => {
                let lsn = log.append_record(
                    txn_id,
                    LogRecordKind::PageCompensation {
                        page_id: *page_id,
                        incarnation: *incarnation,
                        redo: undo,
                        undo_next_lsn: next,
                    },
                )?;
                let restore = PageRestore {
                    page_id: *page_id,
                    incarnation: *incarnation,
                    patch: undo.clone(),
                    wal_flush_lsn: lsn,
                };
                active.pending_restore = Some(restore.clone());
                Ok(UndoStep::Page(restore))
            }
            UndoKind::Allocation { page_id, allocated } => {
                let (page_id, allocated) = (*page_id, !allocated);
                log.append_record(
                    txn_id,
                    LogRecordKind::AllocationCompensation {
                        page_id,
                        allocated,
                        undo_next_lsn: next,
                    },
                )?;
                active.allocation_states.insert(page_id, allocated);
                active.undo.pop();
                Ok(UndoStep::Allocation)
            }
            UndoKind::Logical(inverse) => {
                active.inverse_writable = true;
                active.inverse_failed = false;
                Ok(UndoStep::Logical {
                    inverse: inverse.clone(),
                    boundary: ActionBoundary {
                        undo_len: active.undo.len(),
                        predecessor: log.transaction_head(txn_id),
                    },
                })
            }
        }
    }

    pub(crate) fn finish_page_undo(&mut self, txn_id: TxnId) -> StorageResult<()> {
        let active = self.active_mut(txn_id)?;
        debug_assert!(active.pending_restore.is_some());
        active.pending_restore = None;
        active.undo.pop();
        Ok(())
    }

    pub(crate) fn finish_logical_undo(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        boundary: ActionBoundary,
        completed: bool,
    ) -> StorageResult<()> {
        let active = self.active_mut(txn_id)?;
        active.inverse_writable = false;
        if !completed || active.inverse_failed {
            active.poisoned = true;
            return Ok(()); // The inverse's physical prefix is still undoable.
        }
        let index = boundary.undo_len - 1;
        let entry = &active.undo[index];
        debug_assert!(matches!(entry.kind, UndoKind::Logical(_)));
        log.append_record(txn_id, LogRecordKind::SkipUndo { undo_next_lsn: entry.prev_lsn })
            .inspect_err(|_| self.record_failure(txn_id))?;
        self.active_mut(txn_id)?.undo.truncate(index);
        Ok(())
    }

    /// Force the outcome and its CLR prefix, not restored home pages. Recovery
    /// replays CLRs even for completed aborts; they never enter the undo chain.
    pub(crate) fn finish_rollback(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
    ) -> StorageResult<Lsn> {
        let active = self.transaction(txn_id)?;
        if active.commit_lsn.is_some() {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }
        let rollback_lsn = if let Some(lsn) = active.rollback_lsn {
            lsn
        } else {
            self.validate_can_append()?;
            let lsn = log.append_record(txn_id, LogRecordKind::Rollback)?;
            let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
            active.poisoned = true;
            active.rollback_lsn = Some(lsn);
            self.finishing = Some(txn_id);
            lsn
        };
        log.flush_through(rollback_lsn)?;
        self.transactions.remove(&txn_id);
        self.finishing = None;
        Ok(rollback_lsn)
    }

    fn active_mut(&mut self, txn_id: TxnId) -> StorageResult<&mut ActiveTransaction> {
        self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)
    }

    fn transaction(&self, txn_id: TxnId) -> StorageResult<&ActiveTransaction> {
        if let Some(active) = self.transactions.get(&txn_id) {
            return Ok(active);
        }
        if self.transactions.len() == 1
            && let Some(&expected) = self.transactions.keys().next()
        {
            return Err(invariant(InvariantViolation::TransactionMismatch {
                expected,
                actual: txn_id,
            }));
        }
        Err(no_active_transaction())
    }
}

fn no_active_transaction() -> StorageError {
    invariant(InvariantViolation::NoActiveTransaction)
}

fn invariant(kind: InvariantViolation) -> StorageError {
    StorageError::Internal(InternalError::InvariantViolation(kind))
}
