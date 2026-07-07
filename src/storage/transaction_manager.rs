//! Single-transaction coordinator for WAL-backed page changes.
//!
//! The storage layer currently allows at most one active transaction per
//! [`TransactionManager`]. While that transaction is active, page allocations
//! and full-page updates are assigned LSNs immediately, but WAL bytes are only
//! appended when the write-ahead rule requires them or when the transaction
//! commits. Rollback uses the in-memory undo images accumulated here; crash
//! recovery uses the durable WAL records written by [`LogManager`].

use std::collections::HashMap;

use crate::core::{
    PAGE_SIZE, PageId,
    error::{InternalError, InvariantViolation, StorageError, StorageResult},
};
use crate::storage::{
    log_manager::{LogManager, LogManagerError, LogRecord, LogRecordKind, Lsn, TxnId, ZERO_LSN},
    page,
};

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
    pub(crate) pages: Vec<PageRestore>,
}

/// In-memory checkpoint for rolling back one statement inside a transaction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransactionSavepoint {
    txn_id: TxnId,
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

/// Tracks the single active transaction and its rollback state.
///
/// `TransactionManager` is deliberately small: it assigns monotonically
/// increasing transaction ids, buffers or appends transaction-control records,
/// remembers in-memory undo images for explicit rollback, and marks the active
/// transaction as poisoned after an error that may have left its effects only
/// partially logged.
#[derive(Debug)]
pub(crate) struct TransactionManager {
    max_txn_id: TxnId,
    active: Option<ActiveTransaction>,
}

#[derive(Debug)]
struct ActiveTransaction {
    txn_id: TxnId,
    last_lsn: Lsn,
    pending_records: Vec<PendingLogRecord>,
    pending_page_updates: HashMap<PageId, usize>,
    undo_pages: Vec<PageUndo>,
    poisoned: bool,
}

#[derive(Debug)]
struct PendingLogRecord {
    lsn: Lsn,
    kind: PendingLogRecordKind,
    appended: bool,
}

#[derive(Debug)]
enum PendingLogRecordKind {
    Begin,
    PageUpdate { page_id: PageId, redo_data: Box<[u8; PAGE_SIZE]>, undo_data: Box<[u8; PAGE_SIZE]> },
    PageAlloc { page_id: PageId },
}

impl TransactionManager {
    /// Creates a manager whose next transaction id will be greater than `max_txn_id`.
    ///
    /// Callers seed this with the largest transaction id observed during
    /// recovery and WAL reopening so transaction ids remain monotonic across
    /// process restarts.
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, active: None }
    }

    /// Begins the only active transaction and buffers its `Begin` WAL record.
    ///
    /// Returns an invariant violation if another transaction is already active
    /// or if the transaction-id counter is exhausted.
    pub(crate) fn begin(&mut self, log: &mut LogManager) -> StorageResult<TxnId> {
        if let Some(active) = &self.active {
            return Err(invariant(InvariantViolation::ActiveTransaction { txn_id: active.txn_id }));
        }

        let txn_id = self
            .max_txn_id
            .checked_add(1)
            .ok_or_else(|| invariant(InvariantViolation::TransactionIdExhausted))?;
        let lsn = log.next_lsn()?;
        self.max_txn_id = txn_id;
        self.active = Some(ActiveTransaction {
            txn_id,
            last_lsn: lsn,
            pending_records: vec![PendingLogRecord {
                lsn,
                kind: PendingLogRecordKind::Begin,
                appended: false,
            }],
            pending_page_updates: HashMap::new(),
            undo_pages: Vec::new(),
            poisoned: false,
        });
        Ok(txn_id)
    }

    /// Records a page allocation for the active transaction, if any.
    ///
    /// Page allocations outside a transaction are allowed and do not write WAL.
    /// Allocated page ids are currently not reclaimed during rollback; the WAL
    /// record exists so crash recovery can make committed allocations visible
    /// before replaying their updates.
    pub(crate) fn record_page_alloc(&mut self, page_id: PageId) -> StorageResult<Option<Lsn>> {
        // Allocated page ids are not reclaimed on rollback until a freelist exists.
        let Some(active) = self.active.as_mut() else {
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

    /// Buffers a full-page update record for the active transaction, if any.
    ///
    /// When no transaction is active, the update is not logged and `Ok(None)`
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
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        let Some(active) = self.active.as_mut() else {
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

    /// Appends buffered records up to `requested_lsn`, preserving record order.
    pub(crate) fn append_pending_through(
        &mut self,
        log: &mut LogManager,
        requested_lsn: Lsn,
    ) -> StorageResult<()> {
        if requested_lsn == ZERO_LSN {
            return Ok(());
        }

        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };

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

    /// Marks the active transaction as unsafe to commit.
    ///
    /// Storage layers call this after an error outside direct WAL append paths
    /// when the active transaction may have observed a partial mutation.
    pub(crate) fn record_failure(&mut self) {
        if let Some(active) = self.active.as_mut() {
            active.poisoned = true;
        }
    }

    /// Returns the active transaction id, if a transaction is open.
    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.active.as_ref().map(|active| active.txn_id)
    }

    /// Returns whether the active transaction has observed an unrecoverable error.
    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        let active = self.active.as_ref().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            return Err(transaction_mismatch(active.txn_id, txn_id));
        }

        Ok(active.poisoned)
    }

    /// Commits the active transaction and flushes its commit record to durable storage.
    ///
    /// The active transaction is cleared after the commit record is appended.
    /// If the subsequent WAL flush fails, callers receive the flush error but
    /// the transaction is no longer available for explicit rollback; recovery
    /// will decide the outcome from the WAL contents on the next open.
    pub(crate) fn commit(&mut self, log: &mut LogManager, txn_id: TxnId) -> StorageResult<()> {
        let active = self.active.as_ref().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            return Err(transaction_mismatch(active.txn_id, txn_id));
        }
        if active.poisoned {
            return Err(invariant(InvariantViolation::TransactionPoisoned { txn_id }));
        }

        let commit_lsn = match next_lsn(active.last_lsn) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.active.as_mut().expect("active transaction exists").poisoned = true;
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
                self.active.as_mut().expect("active transaction exists").poisoned = true;
                return Err(err.into());
            }
        };
        if appended_lsn != commit_lsn {
            self.active.as_mut().expect("active transaction exists").poisoned = true;
            return Err(invariant(InvariantViolation::WalLog {
                message: format!(
                    "commit WAL append assigned LSN {appended_lsn}, expected {commit_lsn}"
                ),
            }));
        }

        self.active = None;
        log.flush_through(commit_lsn)?;
        Ok(())
    }

    /// Creates a checkpoint at the current end of the active transaction's undo log.
    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        let active = self.active.as_ref().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            return Err(transaction_mismatch(active.txn_id, txn_id));
        }

        Ok(TransactionSavepoint { txn_id, undo_len: active.undo_pages.len() })
    }

    /// Logs compensation records and returns page images that restore a savepoint.
    ///
    /// The active transaction remains open. Compensation records are ordinary
    /// page updates in the same transaction, so if the transaction later commits
    /// crash recovery replays both the failed statement's physical updates and
    /// these compensating updates in LSN order.
    pub(crate) fn rollback_to_savepoint(
        &mut self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        let active = self.active.as_mut().ok_or_else(no_active_transaction)?;
        if active.txn_id != savepoint.txn_id {
            return Err(transaction_mismatch(active.txn_id, savepoint.txn_id));
        }
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

        active.undo_pages.truncate(savepoint.undo_len);
        Ok(restore_pages)
    }

    /// Returns the active transaction's undo images for rollback.
    ///
    /// The returned vector is ordered from newest update to oldest update. The
    /// active transaction stays available while callers restore pages because
    /// cache eviction during restore may still need to append buffered WAL
    /// records for dirty transaction pages.
    pub(crate) fn prepare_rollback_pages(
        &mut self,
        txn_id: TxnId,
    ) -> StorageResult<TransactionRollback> {
        let active = self.active.as_ref().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            return Err(transaction_mismatch(active.txn_id, txn_id));
        }

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
    /// method to make the completed rollback durable in the WAL.
    pub(crate) fn finish_rollback(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
    ) -> StorageResult<()> {
        let active = self.active.take().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            let expected = active.txn_id;
            self.active = Some(active);
            return Err(transaction_mismatch(expected, txn_id));
        }

        if active.pending_records.iter().any(|record| record.appended) {
            let lsn = log.append_record(txn_id, LogRecordKind::Rollback)?;
            log.flush_through(lsn)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&mut self) -> bool {
        let Some(active) = self.active.as_mut() else {
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
/// Overflow pages and unknown page formats are left unchanged. Their effective
/// page LSN is treated as [`ZERO_LSN`] by [`page_lsn`].
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

/// Reads the recovery LSN stored in a page image.
///
/// Current B+-tree pages store an LSN in their page header. Overflow pages and
/// unrecognized or zeroed pages return [`ZERO_LSN`], which makes recovery treat
/// them as not yet reflecting any logged update.
pub(crate) fn page_lsn(page_bytes: &[u8; PAGE_SIZE]) -> Lsn {
    if page::is_overflow_page(page_bytes) {
        return ZERO_LSN;
    }

    if page::is_current_btree_page(page_bytes) {
        page::format::read_u64(page_bytes, page::format::LSN_OFFSET)
    } else {
        ZERO_LSN
    }
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

        let lsn = transactions.record_page_alloc(7).unwrap();

        assert_eq!(lsn, None);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn page_alloc_with_active_transaction_buffers_wal_until_commit() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let alloc_lsn = transactions.record_page_alloc(7).unwrap();

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
        let first_update = transactions.record_page_update(7, &before, &after_first).unwrap();
        let second_update =
            transactions.record_page_update(7, &after_first, &after_second).unwrap();
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
        transactions.record_page_update(7, &before, &after_first).unwrap();
        transactions.record_page_update(7, &after_first, &after_second).unwrap();
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
        transactions.record_page_update(7, &before_a, &after_a_first).unwrap();
        transactions.record_page_update(8, &before_b, &after_b).unwrap();
        transactions.record_page_update(7, &after_a_first, &after_a_second).unwrap();
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
        transactions.record_page_update(7, &before, &after_first).unwrap();
        transactions.append_pending_through(&mut log, 2).unwrap();
        transactions.record_page_update(7, &after_first, &after_second).unwrap();
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
        transactions.record_page_update(7, &before, &after_first).unwrap();
        let savepoint = transactions.statement_savepoint(txn_id).unwrap();
        transactions.record_page_update(7, &after_first, &after_second).unwrap();

        let restore_pages = transactions.rollback_to_savepoint(savepoint).unwrap();

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

    #[test]
    fn commit_flush_failure_ends_transaction_without_rollback_record() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after = [1; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(7, &before, &after).unwrap();
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
