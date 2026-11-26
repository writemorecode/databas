//! WAL-backed transactions with restartable physical undo.
//!
//! Each original page/bitmap mutation remembers its transaction predecessor.
//! Undo emits redo-only CLRs in reverse *combined* mutation order, including
//! allocation changes. Logging all compensations before returning cache work is
//! safe: recovery repeats their effects even if the cache never installed them.
//! Keep undo state until installation succeeds, so a failed cache restore can
//! still fall back to full abort. A durable rollback outcome forces CLRs, not
//! compensated home pages (recovery_protocol T1/T2).
//!
//! Data-page undo restores byte runs within one incarnation. Retained B+-tree
//! structural actions must not be enabled until user undo follows logical
//! identity rather than these physical offsets (recovery_protocol T3-T5).

use std::collections::{BTreeMap, HashMap};

use crate::error::{InternalError, InvariantViolation, StorageError, StorageResult};
use crate::log_manager::{LogManager, LogRecordKind, Lsn, TxnId};
use crate::page_operation::{LeafOperation, PageOperation};
use crate::page_patch::PagePatch;
use crate::page_recovery::{PageInitKind, PageRecovery};
use crate::{PAGE_SIZE, PageId};

#[derive(Debug, Clone)]
struct UndoEntry {
    prev_lsn: Lsn,
    kind: UndoKind,
}

#[derive(Debug, Clone)]
enum UndoKind {
    Page { page_id: PageId, incarnation: Lsn, undo: PagePatch },
    Allocation { page_id: PageId, allocated: bool },
}

/// A logged compensation to install in the page cache.
#[derive(Debug, Clone)]
pub struct PageRestore {
    pub(crate) page_id: PageId,
    pub(crate) incarnation: Lsn,
    pub(crate) patch: PagePatch,
    pub(crate) wal_flush_lsn: Lsn,
}

/// Work to install before making abort completion durable.
#[derive(Debug, Clone)]
pub struct TransactionRollback {
    pub(crate) pages: Vec<PageRestore>,
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
}

impl TransactionManager {
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, transactions: HashMap::new(), finishing: None }
    }

    pub(crate) fn oldest_begin_lsn(&self) -> Option<Lsn> {
        self.transactions.values().map(|transaction| transaction.begin_lsn).min()
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
        self.transactions.insert(
            txn_id,
            ActiveTransaction {
                begin_lsn,
                undo: Vec::new(),
                allocation_states: BTreeMap::new(),
                rollback_lsn: None,
                commit_lsn: None,
                poisoned: false,
            },
        );
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
        active.undo.push(UndoEntry { prev_lsn, kind: UndoKind::Allocation { page_id, allocated } });
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
            active
                .undo
                .push(UndoEntry { prev_lsn, kind: UndoKind::Page { page_id, incarnation, undo } });
            Ok(lsn)
        })();
        result.inspect_err(|_| self.record_failure(txn_id))
    }

    pub(crate) fn record_failure(&mut self, txn_id: TxnId) {
        if let Some(active) = self.transactions.get_mut(&txn_id) {
            active.poisoned = true;
        }
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> bool {
        self.transactions.contains_key(&txn_id)
    }

    pub(crate) fn validate_transaction_write(&self, txn_id: TxnId) -> StorageResult<()> {
        self.validate_can_append()?;
        if self.transaction(txn_id)?.poisoned {
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
        let commit_lsn = if let Some(lsn) = self.transaction(txn_id)?.commit_lsn {
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

    pub(crate) fn rollback_to_savepoint(
        &mut self,
        log: &mut LogManager,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        self.compensate_from(log, savepoint.txn_id, savepoint.undo_len)
    }

    /// Emit CLRs in combined page/allocation order, never one class at a time:
    /// every undo_next_lsn must skip only effects already compensated. A retry
    /// of failed installation repeats the whole inverse sequence in that order.
    fn compensate_from(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        undo_len: usize,
    ) -> StorageResult<Vec<PageRestore>> {
        self.validate_can_append()?;
        let result = (|| {
            let active = self.transaction(txn_id)?;
            if active.commit_lsn.is_some() {
                return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
            }
            if undo_len > active.undo.len() {
                return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                    txn_id,
                    undo_len,
                    active_undo_len: active.undo.len(),
                }));
            }
            let entries = active.undo[undo_len..].to_vec();
            let mut pages = Vec::new();
            for entry in entries.into_iter().rev() {
                match entry.kind {
                    UndoKind::Page { page_id, incarnation, undo } => {
                        let lsn = log.append_record(
                            txn_id,
                            LogRecordKind::PageCompensation {
                                page_id,
                                incarnation,
                                redo: &undo,
                                undo_next_lsn: entry.prev_lsn,
                            },
                        )?;
                        pages.push(PageRestore {
                            page_id,
                            incarnation,
                            patch: undo,
                            wal_flush_lsn: lsn,
                        });
                    }
                    UndoKind::Allocation { page_id, allocated } => {
                        let allocated = !allocated;
                        log.append_record(
                            txn_id,
                            LogRecordKind::AllocationCompensation {
                                page_id,
                                allocated,
                                undo_next_lsn: entry.prev_lsn,
                            },
                        )?;
                        let active =
                            self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
                        active.allocation_states.insert(page_id, allocated);
                    }
                }
            }
            Ok(pages)
        })();
        result.inspect_err(|_| self.record_failure(txn_id))
    }

    pub(crate) fn complete_savepoint_rollback(
        &mut self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        let active =
            self.transactions.get_mut(&savepoint.txn_id).ok_or_else(no_active_transaction)?;
        if savepoint.undo_len > active.undo.len() {
            return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                txn_id: savepoint.txn_id,
                undo_len: savepoint.undo_len,
                active_undo_len: active.undo.len(),
            }));
        }
        active.undo.truncate(savepoint.undo_len);
        Ok(())
    }

    pub(crate) fn prepare_rollback_pages(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
    ) -> StorageResult<TransactionRollback> {
        let active = self.transaction(txn_id)?;
        if active.commit_lsn.is_some() {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }
        if active.rollback_lsn.is_some() {
            return Ok(TransactionRollback { pages: Vec::new() });
        }
        self.validate_can_append()?;
        self.record_failure(txn_id); // Abort cannot be followed by a commit.
        Ok(TransactionRollback { pages: self.compensate_from(log, txn_id, 0)? })
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
