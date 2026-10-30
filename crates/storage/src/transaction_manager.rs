//! Transaction coordinator for WAL-backed page changes.
//!
//! While a transaction is active, page allocations and full-page updates are
//! appended to the WAL immediately, while durability is deferred until the
//! write-ahead rule or transaction outcome requires a flush. Rollback uses the
//! in-memory undo images accumulated here; crash recovery uses the durable WAL
//! records written by [`LogManager`].

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::{InternalError, InvariantViolation, StorageError, StorageResult};
use crate::log_manager::{LogManager, LogRecordKind, Lsn, TxnId};
use crate::page;
use crate::{PAGE_SIZE, PageId};

#[derive(Debug, Clone)]
struct PageUndo {
    /// Page to restore.
    page_id: PageId,
    /// Full page image captured before the logged update.
    before: [u8; PAGE_SIZE],
    /// Full page image installed by the logged update.
    after: [u8; PAGE_SIZE],
    /// LSN assigned to the update that this image undoes.
    lsn: Lsn,
}

/// Page image to install while rolling back in memory.
#[derive(Debug, Clone)]
pub struct PageRestore {
    /// Page to restore.
    pub(crate) page_id: PageId,
    /// Full page image to install.
    pub(crate) image: [u8; PAGE_SIZE],
    /// WAL dependency that must be durable before this restored image is written.
    pub(crate) wal_flush_lsn: Lsn,
}

/// Physical rollback work for the active transaction.
#[derive(Debug, Clone)]
pub struct TransactionRollback {
    /// Restore operations in reverse mutation order.
    pub(crate) pages: Vec<PageRestore>,
    /// Pages restored by this rollback or an earlier savepoint rollback that
    /// must reach disk before the rollback record is durable.
    pub(crate) flush_page_ids: Vec<PageId>,
}

/// In-memory checkpoint for rolling back one statement inside a transaction.
#[derive(Debug, Clone, Copy)]
pub struct TransactionSavepoint {
    /// Transaction that created this savepoint, used to reject stale handles.
    pub(crate) txn_id: TxnId,
    /// Undo-log boundary before the statement made any page changes.
    undo_len: usize,
    allocation_len: usize,
}

/// WAL metadata returned after a transactional page update is logged.
///
/// The returned redo image is the caller's `after` image with the assigned LSN
/// stamped into the page header when the page format supports an LSN field.
#[derive(Debug)]
pub struct LoggedPageUpdate {
    /// LSN assigned to the WAL update record.
    pub(crate) lsn: Lsn,
    /// Full page image that should be installed into the cache after logging.
    pub(crate) redo: [u8; PAGE_SIZE],
}

/// Tracks active transactions and their rollback state.
///
/// `TransactionManager` is deliberately small: it assigns monotonically
/// increasing transaction ids, remembers in-memory undo images for explicit
/// rollback, and marks a transaction as poisoned after an error that may have
/// left its effects only partially logged.
#[derive(Debug)]
pub struct TransactionManager {
    /// Greatest transaction ID issued by this manager or observed during open.
    max_txn_id: TxnId,
    /// Transactions currently accepted by the storage runtime, keyed by id.
    transactions: HashMap<TxnId, ActiveTransaction>,
}

/// In-memory state for one transaction owned by the storage runtime.
#[derive(Debug)]
struct ActiveTransaction {
    /// Preserve undo history for transactions alive at checkpoint capture.
    begin_lsn: Lsn,
    /// Logical allocation transitions, including savepoint compensation.
    allocation_changes: Vec<(PageId, bool)>,
    allocation_states: HashMap<PageId, bool>,
    /// One before-image per logical page mutation, in mutation order.
    ///
    /// Explicit rollback walks every entry in reverse so repeated writes restore
    /// intermediate images before finally restoring the transaction's original
    /// image.
    undo_pages: Vec<PageUndo>,
    /// Savepoint restores whose undo entries were discarded after installation
    /// in the page cache. A later full rollback must still flush these pages.
    savepoint_restored_pages: HashSet<PageId>,
    /// LSN of an appended rollback outcome awaiting or having completed flush.
    ///
    /// `Some` means physical page restoration has finished and rollback
    /// finalization has begun. If flushing fails, retrying uses this same LSN
    /// instead of appending a duplicate outcome.
    rollback_lsn: Option<Lsn>,
    /// Whether commit is unsafe for this transaction.
    ///
    /// Logging or mutation failures set this flag conservatively. Rollback
    /// finalization also sets it so a transaction retained after a flush error
    /// cannot subsequently commit; rollback and rollback-flush retry remain
    /// allowed.
    poisoned: bool,
}

impl TransactionManager {
    pub(crate) fn oldest_begin_lsn(&self) -> Option<Lsn> {
        self.transactions.values().map(|transaction| transaction.begin_lsn).min()
    }

    /// Creates a manager whose next transaction id will be greater than `max_txn_id`.
    ///
    /// Callers seed this with the largest transaction id observed during
    /// recovery and WAL reopening so transaction ids remain monotonic across
    /// process restarts.
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, transactions: HashMap::new() }
    }

    /// Begins a transaction and appends its `Begin` WAL record.
    ///
    /// Returns an invariant violation if the transaction-id counter is exhausted.
    pub(crate) fn begin(&mut self, log: &mut LogManager) -> StorageResult<TxnId> {
        let txn_id = self
            .max_txn_id
            .checked_add(1)
            .ok_or_else(|| invariant(InvariantViolation::TransactionIdExhausted))?;
        let begin_lsn = log.append_record(txn_id, LogRecordKind::Begin)?;
        self.max_txn_id = txn_id;
        self.transactions.insert(
            txn_id,
            ActiveTransaction {
                undo_pages: Vec::new(),
                allocation_changes: Vec::new(),
                allocation_states: HashMap::new(),
                savepoint_restored_pages: HashSet::new(),
                begin_lsn,
                rollback_lsn: None,
                poisoned: false,
            },
        );
        Ok(txn_id)
    }

    pub(crate) fn allocation_changes(&self, txn_id: TxnId) -> StorageResult<Vec<(PageId, bool)>> {
        Ok(self.transaction(txn_id)?.allocation_changes.clone())
    }

    pub(crate) fn page_pending_dealloc(&self, page_id: PageId) -> bool {
        self.transactions
            .values()
            .any(|active| active.allocation_states.get(&page_id) == Some(&false))
    }

    pub(crate) fn record_page_alloc(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
    ) -> StorageResult<Lsn> {
        self.record_allocation_change(log, txn_id, page_id, true)
    }

    pub(crate) fn record_page_dealloc(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
    ) -> StorageResult<()> {
        self.record_allocation_change(log, txn_id, page_id, false).map(|_| ())
    }

    fn record_allocation_change(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
        allocated: bool,
    ) -> StorageResult<Lsn> {
        if self.transaction(txn_id)?.poisoned {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }
        let kind = if allocated {
            LogRecordKind::PageAlloc { page_id }
        } else {
            LogRecordKind::PageDealloc { page_id }
        };
        let lsn = log.append_record(txn_id, kind).inspect_err(|_| self.record_failure(txn_id))?;
        let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
        active.allocation_changes.push((page_id, allocated));
        active.allocation_states.insert(page_id, allocated);
        Ok(lsn)
    }

    /// Appends a full-page update record for a transaction, if any.
    ///
    /// When the transaction is not active, the update is not logged and `Ok(None)`
    /// is returned. With an active transaction, this method obtains the next WAL
    /// position, stamps it into the redo image for current B+-tree pages, appends
    /// the update containing both redo and undo full-page images, and remembers
    /// the undo image for explicit rollback.
    ///
    /// If WAL insertion fails, the active transaction is marked poisoned. A
    /// poisoned transaction cannot commit because the caller can no longer prove
    /// that all page effects were logged.
    pub(crate) fn record_page_update(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        if !self.transactions.contains_key(&txn_id) {
            return Ok(None);
        }

        let expected_lsn = match log.next_lsn() {
            Ok(lsn) => lsn,
            Err(err) => {
                self.record_failure(txn_id);
                return Err(err.into());
            }
        };
        let mut redo = *after;
        stamp_page_lsn(&mut redo, expected_lsn);
        let lsn = match log.append_record(
            txn_id,
            LogRecordKind::PageUpdate { page_id, redo_data: &redo, undo_data: before },
        ) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.record_failure(txn_id);
                return Err(err.into());
            }
        };
        if lsn != expected_lsn {
            self.record_failure(txn_id);
            return Err(invariant(InvariantViolation::WalLog {
                message: format!(
                    "page-update WAL append assigned LSN {lsn}, expected {expected_lsn}"
                ),
            }));
        }

        let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
        active.undo_pages.push(PageUndo { page_id, before: *before, after: redo, lsn });
        Ok(Some(LoggedPageUpdate { lsn, redo }))
    }

    /// Marks a transaction as unsafe to commit.
    ///
    /// Storage layers call this after an error outside direct WAL append paths
    /// when the transaction may have observed a partial mutation.
    pub(crate) fn record_failure(&mut self, txn_id: TxnId) {
        if let Some(active) = self.transactions.get_mut(&txn_id) {
            active.poisoned = true;
        }
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> bool {
        self.transactions.contains_key(&txn_id)
    }
    /// Returns whether a transaction has observed an unrecoverable error.
    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        Ok(self.transaction(txn_id)?.poisoned)
    }

    /// Commits a transaction and flushes its commit record to durable storage.
    ///
    /// The active transaction is cleared after the commit record is appended.
    /// If the subsequent WAL flush fails, callers receive the flush error but
    /// the transaction is no longer available for explicit rollback; recovery
    /// will decide the outcome from the WAL contents on the next open.
    pub(crate) fn commit(&mut self, log: &mut LogManager, txn_id: TxnId) -> StorageResult<()> {
        if self.transaction(txn_id)?.poisoned {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }

        let commit_lsn = match log.append_record(txn_id, LogRecordKind::Commit) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.record_failure(txn_id);
                return Err(err.into());
            }
        };
        self.transactions.remove(&txn_id);
        log.flush_through(commit_lsn)?;
        Ok(())
    }

    /// Creates a checkpoint at the current end of a transaction's undo log.
    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        let active = self.transaction(txn_id)?;

        Ok(TransactionSavepoint {
            txn_id,
            undo_len: active.undo_pages.len(),
            allocation_len: active.allocation_changes.len(),
        })
    }

    /// Logs compensation records and returns page images that restore a savepoint.
    ///
    /// The active transaction remains open. Compensation records are ordinary
    /// page updates in the same transaction, so if the transaction later commits
    /// crash recovery replays both the failed statement's physical updates and
    /// these compensating updates in LSN order.
    ///
    /// This is the first half of a two-phase in-memory rollback. Undo entries are
    /// deliberately retained until [`Self::complete_savepoint_rollback`] confirms
    /// that the page cache installed every returned image. If installation fails,
    /// those entries remain available to a full transaction rollback.
    pub(crate) fn rollback_to_savepoint(
        &mut self,
        log: &mut LogManager,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        let rollback = {
            let active = self.transaction(savepoint.txn_id)?;
            if savepoint.undo_len > active.undo_pages.len() {
                return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                    txn_id: savepoint.txn_id,
                    undo_len: savepoint.undo_len,
                    active_undo_len: active.undo_pages.len(),
                }));
            }

            if savepoint.allocation_len > active.allocation_changes.len() {
                return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                    txn_id: savepoint.txn_id,
                    undo_len: savepoint.allocation_len,
                    active_undo_len: active.allocation_changes.len(),
                }));
            }

            let undo_pages = &active.undo_pages[savepoint.undo_len..];
            let mut restore_pages = Vec::with_capacity(undo_pages.len());
            for undo in undo_pages.iter().rev() {
                let expected_lsn = log.next_lsn()?;
                let mut redo = undo.before;
                stamp_page_lsn(&mut redo, expected_lsn);
                let lsn = log.append_record(
                    savepoint.txn_id,
                    LogRecordKind::PageUpdate {
                        page_id: undo.page_id,
                        redo_data: &redo,
                        undo_data: &undo.after,
                    },
                )?;
                if lsn != expected_lsn {
                    return Err(invariant(InvariantViolation::WalLog {
                        message: format!(
                            "compensation WAL append assigned LSN {lsn}, expected {expected_lsn}"
                        ),
                    }));
                }
                restore_pages.push(PageRestore {
                    page_id: undo.page_id,
                    image: redo,
                    wal_flush_lsn: lsn,
                });
            }

            // Do not release any reservation until the transaction completes.
            // Log inverses so a later commit replays the savepoint outcome too.
            let mut originals = BTreeMap::new();
            for &(id, allocated) in &active.allocation_changes[savepoint.allocation_len..] {
                originals.entry(id).or_insert(!allocated);
            }
            let changes: Vec<_> = originals
                .into_iter()
                .filter(|(id, allocated)| active.allocation_states.get(id) != Some(allocated))
                .collect();
            for &(page_id, allocated) in &changes {
                let kind = if allocated {
                    LogRecordKind::PageAlloc { page_id }
                } else {
                    LogRecordKind::PageDealloc { page_id }
                };
                log.append_record(savepoint.txn_id, kind)?;
            }
            let active =
                self.transactions.get_mut(&savepoint.txn_id).ok_or_else(no_active_transaction)?;
            for &(id, allocated) in &changes {
                active.allocation_states.insert(id, allocated);
            }
            active.allocation_changes.extend(changes);
            Ok(restore_pages)
        };

        rollback.inspect_err(|_| self.record_failure(savepoint.txn_id))
    }

    /// Discards undo entries after their savepoint restore images are installed.
    ///
    /// Call this only after every [`PageRestore`] returned by
    /// [`Self::rollback_to_savepoint`] has been installed successfully. Keeping
    /// this step separate prevents a partial page-cache restore from destroying
    /// the fallback undo state needed by a full transaction rollback.
    pub(crate) fn complete_savepoint_rollback(
        &mut self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        let active =
            self.transactions.get_mut(&savepoint.txn_id).ok_or_else(no_active_transaction)?;
        if savepoint.undo_len > active.undo_pages.len() {
            return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                txn_id: savepoint.txn_id,
                undo_len: savepoint.undo_len,
                active_undo_len: active.undo_pages.len(),
            }));
        }

        active
            .savepoint_restored_pages
            .extend(active.undo_pages[savepoint.undo_len..].iter().map(|undo| undo.page_id));
        active.undo_pages.truncate(savepoint.undo_len);
        Ok(())
    }

    /// Returns a transaction's undo images for rollback.
    ///
    /// The returned vector is ordered from newest update to oldest update. The
    /// active transaction stays available while callers restore pages so a
    /// failed restoration can still be retried.
    pub(crate) fn prepare_rollback_pages(
        &self,
        txn_id: TxnId,
    ) -> StorageResult<TransactionRollback> {
        let active = self.transaction(txn_id)?;

        let mut pages = active
            .undo_pages
            .iter()
            .rev()
            .map(|undo| PageRestore {
                page_id: undo.page_id,
                image: undo.before,
                wal_flush_lsn: undo.lsn,
            })
            .collect::<Vec<_>>();
        pages.shrink_to_fit();
        let mut flush_page_ids =
            active.savepoint_restored_pages.iter().copied().collect::<Vec<_>>();
        flush_page_ids.extend(pages.iter().map(|page| page.page_id));
        flush_page_ids.sort_unstable();
        flush_page_ids.dedup();
        Ok(TransactionRollback { pages, flush_page_ids })
    }

    /// Writes and flushes the `Rollback` record after undo pages reach disk.
    ///
    /// Callers perform the physical page restoration first, then use this
    /// method to append and make the completed rollback durable in the WAL.
    ///
    /// If the outcome is appended but flushing fails, the active transaction is
    /// retained with `rollback_lsn` set. Calling this method again flushes through
    /// the same LSN and does not append another rollback outcome.
    pub(crate) fn finish_rollback(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
    ) -> StorageResult<()> {
        let active = self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?;
        active.poisoned = true;
        if let Some(rollback_lsn) = active.rollback_lsn {
            log.flush_through(rollback_lsn)?;
            self.transactions.remove(&txn_id);
            return Ok(());
        }

        let rollback_lsn = log.append_record(txn_id, LogRecordKind::Rollback)?;
        self.transactions.get_mut(&txn_id).ok_or_else(no_active_transaction)?.rollback_lsn =
            Some(rollback_lsn);
        log.flush_through(rollback_lsn)?;
        self.transactions.remove(&txn_id);
        Ok(())
    }

    fn transaction(&self, txn_id: TxnId) -> StorageResult<&ActiveTransaction> {
        if let Some(active) = self.transactions.get(&txn_id) {
            return Ok(active);
        }
        if self.transactions.len() == 1
            && let Some(&expected) = self.transactions.keys().next()
        {
            return Err(transaction_mismatch(expected, txn_id));
        }
        Err(no_active_transaction())
    }
}

/// Stamps the assigned page LSN into page formats that carry one.
///
/// Overflow pages and unknown page formats are left unchanged; recovery uses
/// their authoritative full-page WAL images without consulting an on-page LSN.
fn stamp_page_lsn(page_bytes: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    if page::is_overflow_page(page_bytes) {
        return;
    }

    if page::is_current_btree_page(page_bytes) {
        page::format::write_u64(page_bytes, page::format::LSN_OFFSET, lsn);
    }
}

fn no_active_transaction() -> StorageError {
    invariant(InvariantViolation::NoActiveTransaction)
}

fn transaction_mismatch(expected: TxnId, actual: TxnId) -> StorageError {
    invariant(InvariantViolation::TransactionMismatch { expected, actual })
}

fn invariant(kind: InvariantViolation) -> StorageError {
    StorageError::Internal(InternalError::InvariantViolation(kind))
}
