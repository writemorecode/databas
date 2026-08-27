//! Transaction coordinator for WAL-backed page changes.
//!
//! While a transaction is active, page allocations and full-page updates are
//! assigned LSNs immediately, but WAL bytes are only appended when the
//! write-ahead rule requires them or when the transaction commits. Rollback
//! uses the in-memory undo images accumulated here; crash recovery uses the
//! durable WAL records written by [`LogManager`].

use std::collections::HashMap;

use crate::core::{
    PAGE_SIZE, PageId,
    error::{InternalError, InvariantViolation, StorageError, StorageResult},
};
use crate::storage::{
    log_manager::{LogManager, LogManagerError, LogRecord, LogRecordKind, Lsn, TxnId, ZERO_LSN},
    page,
};

#[cfg(test)]
mod fault_injection;

#[cfg(test)]
pub(crate) use fault_injection::FaultInjectingTransactionManager;

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
    /// Index of the matching pending WAL record.
    pending_record_index: usize,
}

/// Page image to install while rolling back in memory.
#[derive(Debug, Clone)]
pub(crate) struct PageRestore {
    /// Page to restore.
    pub(crate) page_id: PageId,
    /// Full page image to install.
    pub(crate) image: [u8; PAGE_SIZE],
    /// WAL dependency that must be durable before this restored image is written.
    pub(crate) wal_flush_lsn: Lsn,
}

/// Physical rollback work for the active transaction.
#[derive(Debug, Clone)]
pub(crate) struct TransactionRollback {
    /// Restore operations in reverse mutation order.
    pub(crate) pages: Vec<PageRestore>,
}

/// In-memory checkpoint for rolling back one statement inside a transaction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransactionSavepoint {
    /// Transaction that created this savepoint, used to reject stale handles.
    txn_id: TxnId,
    /// Undo-log boundary before the statement made any page changes.
    undo_len: usize,
}

/// WAL metadata returned after a transactional page update is logged.
///
/// The returned redo image is the caller's `after` image with the assigned LSN
/// stamped into the page header when the page format supports an LSN field.
#[derive(Debug)]
pub(crate) struct LoggedPageUpdate {
    /// LSN assigned to the WAL update record.
    pub(crate) lsn: Lsn,
    /// Full page image that should be installed into the cache after logging.
    pub(crate) redo: [u8; PAGE_SIZE],
}

/// Tracks active transactions and their rollback state.
///
/// `TransactionManager` is deliberately small: it assigns monotonically
/// increasing transaction ids, buffers or appends transaction-control records,
/// remembers in-memory undo images for explicit rollback, and marks a
/// transaction as poisoned after an error that may have left its effects only
/// partially logged.
#[derive(Debug)]
pub(crate) struct TransactionManager {
    /// Greatest transaction ID issued by this manager or observed during open.
    max_txn_id: TxnId,
    /// Transactions currently accepted by the storage runtime, keyed by id.
    transactions: HashMap<TxnId, ActiveTransaction>,
}

/// In-memory state for one transaction owned by the storage runtime.
///
/// WAL records can be assigned logical LSNs before they are physically appended.
/// Consequently, `last_lsn` and the LSNs in `pending_records` describe the
/// transaction's intended WAL order, while each record's `appended` flag records
/// how much of that order has actually reached the WAL writer.
#[derive(Debug)]
struct ActiveTransaction {
    /// ID assigned when the transaction began.
    txn_id: TxnId,
    /// Greatest logical LSN reserved for a buffered record in this transaction.
    ///
    /// This can be ahead of the log manager's highest appended LSN. Repeated
    /// updates coalesced into one pending page record reuse that record's LSN and
    /// therefore do not advance this value.
    last_lsn: Lsn,
    /// Transaction records in reserved LSN order.
    ///
    /// Appended entries remain in the vector because [`PageUndo`] values refer
    /// to records by stable index when deciding whether rollback needs a WAL
    /// flush before writing a before-image.
    pending_records: Vec<PendingLogRecord>,
    /// Unappended page-update record currently eligible for redo coalescing.
    ///
    /// Each value indexes `pending_records`. The entry is removed when its WAL
    /// record is appended or when savepoint compensation starts for the page,
    /// after which another update must reserve a new record and LSN.
    pending_page_updates: HashMap<PageId, usize>,
    /// One before-image per logical page mutation, in mutation order.
    ///
    /// Unlike `pending_records`, this log is not coalesced: explicit rollback
    /// walks every entry in reverse so repeated writes restore intermediate
    /// images before finally restoring the transaction's original image.
    undo_pages: Vec<PageUndo>,
    /// LSN of an appended rollback outcome awaiting or having completed flush.
    ///
    /// `Some` means physical page restoration has finished and rollback
    /// finalization has begun. If flushing fails, retrying uses this same LSN
    /// instead of appending a duplicate outcome. Pending transaction records
    /// must not be appended once this is set because the rollback record already
    /// occupies the next physical WAL position.
    rollback_lsn: Option<Lsn>,
    /// Whether commit is unsafe for this transaction.
    ///
    /// Logging or mutation failures set this flag conservatively. Rollback
    /// finalization also sets it so a transaction retained after a flush error
    /// cannot subsequently commit; rollback and rollback-flush retry remain
    /// allowed.
    poisoned: bool,
}

/// WAL record reserved by the active transaction.
#[derive(Debug)]
struct PendingLogRecord {
    /// Logical LSN this record must receive if it is appended.
    lsn: Lsn,
    /// Payload retained until commit, rollback, or write-ahead flushing decides its fate.
    kind: PendingLogRecordKind,
    /// Whether a complete frame containing this record was appended.
    ///
    /// This does not imply durability; [`LogManager::flush_through`] establishes
    /// that separately.
    appended: bool,
}

/// Owned payload for a WAL record buffered by the transaction manager.
#[derive(Debug)]
enum PendingLogRecordKind {
    /// Start marker reserved when the transaction is created.
    Begin,
    /// Full-page physical update used for both redo and undo during recovery.
    PageUpdate {
        /// Page changed by this record.
        page_id: PageId,
        /// Latest page image to install when redoing the transaction.
        redo_data: Box<[u8; PAGE_SIZE]>,
        /// Earliest page image covered by this record, restored when undoing it.
        undo_data: Box<[u8; PAGE_SIZE]>,
    },
    /// Page whose allocation becomes visible if the transaction commits.
    PageAlloc {
        /// Allocated database page.
        page_id: PageId,
    },
}

impl TransactionManager {
    /// Creates a manager whose next transaction id will be greater than `max_txn_id`.
    ///
    /// Callers seed this with the largest transaction id observed during
    /// recovery and WAL reopening so transaction ids remain monotonic across
    /// process restarts.
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, transactions: HashMap::new() }
    }

    /// Begins a transaction and buffers its `Begin` WAL record.
    ///
    /// Returns an invariant violation if the transaction-id counter is exhausted.
    pub(crate) fn begin(&mut self, log: &mut LogManager) -> StorageResult<TxnId> {
        let txn_id = self
            .max_txn_id
            .checked_add(1)
            .ok_or_else(|| invariant(InvariantViolation::TransactionIdExhausted))?;
        let lsn = self.next_lsn(log)?;
        self.max_txn_id = txn_id;
        self.transactions.insert(
            txn_id,
            ActiveTransaction {
                txn_id,
                last_lsn: lsn,
                pending_records: vec![PendingLogRecord {
                    lsn,
                    kind: PendingLogRecordKind::Begin,
                    appended: false,
                }],
                pending_page_updates: HashMap::new(),
                undo_pages: Vec::new(),
                rollback_lsn: None,
                poisoned: false,
            },
        );
        Ok(txn_id)
    }

    /// Reserves an LSN after both the log and all active transactions.
    fn next_lsn(&self, log: &LogManager) -> StorageResult<Lsn> {
        self.transactions
            .values()
            .map(|active| active.last_lsn)
            .max()
            .map_or_else(|| log.next_lsn().map_err(Into::into), next_lsn)
    }

    /// Records a page allocation for a transaction, if it exists.
    ///
    /// Page allocations outside a transaction are allowed and do not write WAL.
    /// Allocated page ids are currently not reclaimed during rollback; the WAL
    /// record exists so crash recovery can make committed allocations visible
    /// before replaying their updates.
    pub(crate) fn record_page_alloc(
        &mut self,
        txn_id: TxnId,
        page_id: PageId,
    ) -> StorageResult<Option<Lsn>> {
        // Allocated page ids are not reclaimed on rollback until a freelist exists.
        let Some(active) = self.transactions.get_mut(&txn_id) else {
            return Ok(None);
        };
        let lsn = match next_lsn(active.last_lsn) {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err);
            }
        };
        active.pending_records.push(PendingLogRecord {
            lsn,
            kind: PendingLogRecordKind::PageAlloc { page_id },
            appended: false,
        });
        active.last_lsn = lsn;
        Ok(Some(lsn))
    }

    /// Buffers a full-page update record for a transaction, if any.
    ///
    /// When the transaction is not active, the update is not logged and `Ok(None)`
    /// is returned. With an active transaction, this method reserves the next
    /// LSN, stamps it into the redo image for current B+-tree pages, buffers a
    /// `PageUpdate` WAL record containing both redo and undo full-page images,
    /// and remembers the undo image for explicit rollback.
    ///
    /// If LSN reservation or WAL append later fails, the active transaction is marked
    /// poisoned. A poisoned transaction cannot commit because the caller can no
    /// longer prove that all page effects were logged.
    pub(crate) fn record_page_update(
        &mut self,
        txn_id: TxnId,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        let Some(active) = self.transactions.get_mut(&txn_id) else {
            return Ok(None);
        };

        if let Some(&pending_record_index) = active.pending_page_updates.get(&page_id) {
            let record = &mut active.pending_records[pending_record_index];
            if let PendingLogRecordKind::PageUpdate { redo_data, .. } = &mut record.kind {
                let mut redo = *after;
                stamp_page_lsn(&mut redo, record.lsn);
                **redo_data = redo;
                active.undo_pages.push(PageUndo {
                    page_id,
                    before: *before,
                    after: redo,
                    lsn: record.lsn,
                    pending_record_index,
                });
                return Ok(Some(LoggedPageUpdate { lsn: record.lsn, redo }));
            }

            active.poisoned = true;
            return Err(invariant(InvariantViolation::WalLog {
                message: format!(
                    "pending page-update index {pending_record_index} for page {page_id} did not point to a PageUpdate record"
                ),
            }));
        }

        let lsn = match next_lsn(active.last_lsn) {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err);
            }
        };
        let mut redo = *after;
        stamp_page_lsn(&mut redo, lsn);

        let pending_record_index = active.pending_records.len();
        active.pending_records.push(PendingLogRecord {
            lsn,
            kind: PendingLogRecordKind::PageUpdate {
                page_id,
                redo_data: Box::new(redo),
                undo_data: Box::new(*before),
            },
            appended: false,
        });
        active.last_lsn = lsn;
        active.undo_pages.push(PageUndo {
            page_id,
            before: *before,
            after: redo,
            lsn,
            pending_record_index,
        });
        active.pending_page_updates.insert(page_id, pending_record_index);
        Ok(Some(LoggedPageUpdate { lsn, redo }))
    }

    /// Appends a transaction's buffered records up to `requested_lsn`, preserving record order.
    ///
    /// This is a no-op after a rollback outcome has been appended. At that point
    /// the outcome occupies the next physical WAL position and only its durability
    /// flush may be retried; appending older reserved records would assign them
    /// different LSNs from those stamped into their page images.
    pub(crate) fn append_pending_through(
        &mut self,
        txn_id: TxnId,
        log: &mut LogManager,
        requested_lsn: Lsn,
    ) -> StorageResult<()> {
        if requested_lsn == ZERO_LSN {
            return Ok(());
        }

        let Some(active) = self.transactions.get_mut(&txn_id) else {
            return Ok(());
        };
        if active.rollback_lsn.is_some() {
            return Ok(());
        }

        let Some(start) = active.pending_records.iter().position(|record| !record.appended) else {
            return Ok(());
        };
        if active.pending_records[start].lsn > requested_lsn {
            return Ok(());
        }

        let mut end = start;
        while end < active.pending_records.len()
            && !active.pending_records[end].appended
            && active.pending_records[end].lsn <= requested_lsn
        {
            end += 1;
        }

        let records = active.pending_records[start..end]
            .iter()
            .map(|record| pending_log_record(active.txn_id, record))
            .collect::<Vec<_>>();
        let expected_lsn = active.pending_records[end - 1].lsn;
        let appended_lsn = match log.append_transaction(active.txn_id, &records) {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err.into());
            }
        };
        if appended_lsn != expected_lsn {
            active.poisoned = true;
            return Err(invariant(InvariantViolation::WalLog {
                message: format!(
                    "pending WAL append assigned LSN {appended_lsn}, expected {expected_lsn}"
                ),
            }));
        }

        for record in &mut active.pending_records[start..end] {
            record.appended = true;
        }
        for pending_record_index in start..end {
            if let PendingLogRecordKind::PageUpdate { page_id, .. } =
                &active.pending_records[pending_record_index].kind
                && active
                    .pending_page_updates
                    .get(page_id)
                    .is_some_and(|index| *index == pending_record_index)
            {
                active.pending_page_updates.remove(page_id);
            }
        }
        Ok(())
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

    /// Returns one active transaction id, if a transaction is open.
    ///
    /// The storage runtime is currently strictly sequential, so callers only
    /// use this when there is at most one transaction making page changes.
    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.transactions.keys().next().copied()
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
        let active = self.transaction(txn_id)?;
        if active.poisoned {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }

        let commit_lsn = match next_lsn(active.last_lsn) {
            Ok(lsn) => lsn,
            Err(err) => {
                if let Some(active) = self.transactions.get_mut(&txn_id) {
                    active.poisoned = true;
                }
                return Err(err);
            }
        };
        let mut records = active
            .pending_records
            .iter()
            .filter(|record| !record.appended)
            .map(|record| pending_log_record(txn_id, record))
            .collect::<Vec<_>>();
        records.push(LogRecord { txn_id, kind: LogRecordKind::Commit });
        let appended_lsn = match log.append_transaction(txn_id, &records) {
            Ok(lsn) => lsn,
            Err(err) => {
                if let Some(active) = self.transactions.get_mut(&txn_id) {
                    active.poisoned = true;
                }
                return Err(err.into());
            }
        };
        if appended_lsn != commit_lsn {
            if let Some(active) = self.transactions.get_mut(&txn_id) {
                active.poisoned = true;
            }
            return Err(invariant(InvariantViolation::WalLog {
                message: format!(
                    "commit WAL append assigned LSN {appended_lsn}, expected {commit_lsn}"
                ),
            }));
        }

        self.transactions.remove(&txn_id);
        log.flush_through(commit_lsn)?;
        Ok(())
    }

    /// Creates a checkpoint at the current end of a transaction's undo log.
    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        let active = self.transaction(txn_id)?;

        Ok(TransactionSavepoint { txn_id, undo_len: active.undo_pages.len() })
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
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        let active =
            self.transactions.get_mut(&savepoint.txn_id).ok_or_else(no_active_transaction)?;
        if savepoint.undo_len > active.undo_pages.len() {
            return Err(invariant(InvariantViolation::InvalidTransactionSavepoint {
                txn_id: savepoint.txn_id,
                undo_len: savepoint.undo_len,
                active_undo_len: active.undo_pages.len(),
            }));
        }

        let rollback_pages = active.undo_pages[savepoint.undo_len..].to_vec();
        let mut restore_pages = Vec::with_capacity(rollback_pages.len());
        for undo in rollback_pages.into_iter().rev() {
            let lsn = match next_lsn(active.last_lsn) {
                Ok(lsn) => lsn,
                Err(err) => {
                    active.poisoned = true;
                    return Err(err);
                }
            };
            let mut redo = undo.before;
            stamp_page_lsn(&mut redo, lsn);
            active.pending_page_updates.remove(&undo.page_id);
            active.pending_records.push(PendingLogRecord {
                lsn,
                kind: PendingLogRecordKind::PageUpdate {
                    page_id: undo.page_id,
                    redo_data: Box::new(redo),
                    undo_data: Box::new(undo.after),
                },
                appended: false,
            });
            active.last_lsn = lsn;
            restore_pages.push(PageRestore {
                page_id: undo.page_id,
                image: redo,
                wal_flush_lsn: lsn,
            });
        }

        Ok(restore_pages)
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

        active.undo_pages.truncate(savepoint.undo_len);
        Ok(())
    }

    /// Returns a transaction's undo images for rollback.
    ///
    /// The returned vector is ordered from newest update to oldest update. The
    /// active transaction stays available while callers restore pages because
    /// cache eviction during restore may still need to append buffered WAL
    /// records for dirty transaction pages.
    pub(crate) fn prepare_rollback_pages(
        &mut self,
        txn_id: TxnId,
    ) -> StorageResult<TransactionRollback> {
        let active = self.transaction(txn_id)?;

        let mut pages = active
            .undo_pages
            .iter()
            .rev()
            .map(|undo| {
                let appended = active
                    .pending_records
                    .get(undo.pending_record_index)
                    .is_some_and(|record| record.appended);
                PageRestore {
                    page_id: undo.page_id,
                    image: undo.before,
                    wal_flush_lsn: if appended { undo.lsn } else { ZERO_LSN },
                }
            })
            .collect::<Vec<_>>();
        pages.shrink_to_fit();
        Ok(TransactionRollback { pages })
    }

    /// Writes and flushes the `Rollback` record after undo pages reach disk.
    ///
    /// Callers perform the physical page restoration first, then use this
    /// method to make the completed rollback durable in the WAL. When no prior
    /// record was appended, a compact `Begin`/`Rollback` frame is written so the
    /// assigned transaction ID remains observable after restart.
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
        let rollback_lsn = match active.rollback_lsn {
            Some(lsn) => lsn,
            None => {
                let lsn = if active.pending_records.iter().any(|record| record.appended) {
                    log.append_record(txn_id, LogRecordKind::Rollback)?
                } else {
                    log.append_transaction(
                        txn_id,
                        &[
                            LogRecord { txn_id, kind: LogRecordKind::Begin },
                            LogRecord { txn_id, kind: LogRecordKind::Rollback },
                        ],
                    )?
                };
                active.rollback_lsn = Some(lsn);
                lsn
            }
        };
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

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&mut self) -> bool {
        let Some(active) = self.transactions.values_mut().next() else {
            return false;
        };
        active.last_lsn = Lsn::MAX;
        true
    }
}

fn next_lsn(current_lsn: Lsn) -> StorageResult<Lsn> {
    current_lsn.checked_add(1).ok_or_else(|| LogManagerError::LsnExhausted.into())
}

fn pending_log_record(txn_id: TxnId, record: &PendingLogRecord) -> LogRecord<'_> {
    LogRecord {
        txn_id,
        kind: match &record.kind {
            PendingLogRecordKind::Begin => LogRecordKind::Begin,
            PendingLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                LogRecordKind::PageUpdate {
                    page_id: *page_id,
                    redo_data: redo_data.as_ref(),
                    undo_data: undo_data.as_ref(),
                }
            }
            PendingLogRecordKind::PageAlloc { page_id } => {
                LogRecordKind::PageAlloc { page_id: *page_id }
            }
        },
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

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::error::{InternalError, InvariantViolation};
    use crate::storage::log_manager::{
        LogManager, OwnedLogRecordKind, RecoveryLogRecordKind, read_log_record_kinds_for_test,
        read_recovery_log,
    };

    #[test]
    fn page_alloc_without_active_transaction_does_not_write_wal() {
        let file = NamedTempFile::new().unwrap();
        let _log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let lsn = transactions.record_page_alloc(0, 7).unwrap();

        assert_eq!(lsn, None);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn page_alloc_with_active_transaction_buffers_wal_until_commit() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let alloc_lsn = transactions.record_page_alloc(txn_id, 7).unwrap();

        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(txn_id, 1);
        assert_eq!(alloc_lsn, Some(2));
        assert_eq!(log.highest_appended_lsn(), Some(3));
        assert_eq!(log.highest_durable_lsn(), Some(3));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (1, OwnedLogRecordKind::Begin),
                (1, OwnedLogRecordKind::PageAlloc { page_id: 7 }),
                (1, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn repeated_page_updates_commit_as_one_page_update_record() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        let first_update =
            transactions.record_page_update(txn_id, 7, &before, &after_first).unwrap();
        let second_update =
            transactions.record_page_update(txn_id, 7, &after_first, &after_second).unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(first_update.as_ref().map(|update| update.lsn), Some(2));
        assert_eq!(second_update.as_ref().map(|update| update.lsn), Some(2));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn coalesced_page_update_keeps_first_undo_and_latest_redo_image() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after_first).unwrap();
        transactions.record_page_update(txn_id, 7, &after_first, &after_second).unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        let scan = read_recovery_log(file.path()).unwrap();
        match &scan.records[1].kind {
            RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                assert_eq!(*page_id, 7);
                assert_eq!(undo_data.as_ref(), &before);
                assert_eq!(redo_data.as_ref(), &after_second);
            }
            kind => panic!("unexpected record kind: {kind:?}"),
        }
    }

    #[test]
    fn mixed_page_updates_coalesce_per_page_without_reordering() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before_a = [0; PAGE_SIZE];
        let after_a_first = [1; PAGE_SIZE];
        let before_b = [10; PAGE_SIZE];
        let after_b = [11; PAGE_SIZE];
        let after_a_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before_a, &after_a_first).unwrap();
        transactions.record_page_update(txn_id, 8, &before_b, &after_b).unwrap();
        transactions.record_page_update(txn_id, 7, &after_a_first, &after_a_second).unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 8 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn page_update_after_append_creates_new_record_for_same_page() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after_first).unwrap();
        transactions.append_pending_through(txn_id, &mut log, 2).unwrap();
        transactions.record_page_update(txn_id, 7, &after_first, &after_second).unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn savepoint_rollback_buffers_compensation_record_until_commit() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after_first).unwrap();
        let savepoint = transactions.statement_savepoint(txn_id).unwrap();
        transactions.record_page_update(txn_id, 7, &after_first, &after_second).unwrap();

        let restore_pages = transactions.rollback_to_savepoint(savepoint).unwrap();
        transactions.complete_savepoint_rollback(savepoint).unwrap();

        assert_eq!(restore_pages.len(), 1);
        assert_eq!(restore_pages[0].page_id, 7);
        assert_eq!(restore_pages[0].wal_flush_lsn, 3);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);

        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn rollback_to_invalid_savepoint_returns_error_without_panicking() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let savepoint = TransactionSavepoint { txn_id, undo_len: 1 };
        let result = transactions.rollback_to_savepoint(savepoint);

        assert!(matches!(
            result,
            Err(StorageError::Internal(InternalError::InvariantViolation(
                InvariantViolation::InvalidTransactionSavepoint {
                    txn_id: actual_txn_id,
                    undo_len: 1,
                    active_undo_len: 0,
                }
            ))) if actual_txn_id == txn_id
        ));
    }

    // Issue: A rollback-complete WAL flush failure removes the active transaction.
    // The one-shot failure is retryable, but `finish_rollback` took the transaction
    // before flushing, so the caller could neither retry nor inspect its rollback state.
    #[test]
    fn rollback_flush_failure_keeps_transaction_active_for_retry() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after = [1; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after).unwrap();
        transactions.append_pending_through(txn_id, &mut log, 2).unwrap();
        log.fail_next_flush_for_test();

        assert!(transactions.finish_rollback(&mut log, txn_id).is_err());

        assert_eq!(transactions.active_transaction_id(), Some(txn_id));
        transactions.finish_rollback(&mut log, txn_id).unwrap();
        assert_eq!(transactions.active_transaction_id(), None);
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    // Issue: Retaining a transaction after its rollback flush failed left lower-LSN
    // pending records appendable after the rollback marker. A later page flush could
    // append one at a new LSN and poison the transaction with an LSN mismatch.
    #[test]
    fn pending_records_are_not_appended_while_rollback_flush_is_pending() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after_first).unwrap();
        transactions.append_pending_through(txn_id, &mut log, 2).unwrap();
        transactions.record_page_update(txn_id, 8, &before, &after_second).unwrap();
        log.fail_next_flush_for_test();
        assert!(transactions.finish_rollback(&mut log, txn_id).is_err());

        transactions.append_pending_through(txn_id, &mut log, 3).unwrap();
        transactions.finish_rollback(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    // Issue: A transaction that rolled back before any WAL record was appended left
    // no durable trace of its assigned ID. Reopening seeded the manager from the WAL,
    // so the next transaction reused that ID instead of remaining monotonic.
    #[test]
    fn clean_rollback_preserves_transaction_id_across_reopen() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut log = LogManager::new(file.path()).unwrap();
            let mut transactions = TransactionManager::new(0);
            let txn_id = transactions.begin(&mut log).unwrap();
            transactions.finish_rollback(&mut log, txn_id).unwrap();
            assert_eq!(txn_id, 1);
            assert_eq!(
                read_log_record_kinds_for_test(file.path()),
                [(txn_id, OwnedLogRecordKind::Begin), (txn_id, OwnedLogRecordKind::Rollback),]
            );
        }

        let mut reopened_log = LogManager::new(file.path()).unwrap();
        let mut reopened_transactions = TransactionManager::new(reopened_log.highest_txn_id());

        assert_eq!(reopened_transactions.begin(&mut reopened_log).unwrap(), 2);
    }

    #[test]
    fn commit_flush_failure_ends_transaction_without_rollback_record() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after = [1; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(txn_id, 7, &before, &after).unwrap();
        log.fail_next_flush_for_test();

        let result = transactions.commit(&mut log, txn_id);

        assert!(result.is_err());
        assert_eq!(transactions.active_transaction_id(), None);
        assert!(matches!(
            transactions.prepare_rollback_pages(txn_id),
            Err(StorageError::Internal(InternalError::InvariantViolation(
                InvariantViolation::NoActiveTransaction
            )))
        ));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }
}
