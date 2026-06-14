//! Single-transaction coordinator for WAL-backed page changes.
//!
//! The storage layer currently allows at most one active transaction per
//! [`TransactionManager`]. While that transaction is active, page allocations
//! and full-page updates are recorded in the write-ahead log before the dirty
//! page is allowed to reach the database file. Rollback uses the in-memory undo
//! images accumulated here; crash recovery uses the durable WAL records written
//! by [`LogManager`].

use crate::core::{
    PAGE_SIZE, PageId,
    error::{InternalError, InvariantViolation, StorageError, StorageResult},
    log_manager::{LogManager, LogRecordKind, Lsn, TxnId, ZERO_LSN},
    page,
};

/// Full-page image needed to undo one page update during an explicit rollback.
///
/// Entries are collected in update order and later returned in reverse LSN
/// order so a rollback restores the newest version of each page first.
#[derive(Debug, Clone)]
pub(crate) struct PageUndo {
    /// Page to restore.
    pub(crate) page_id: PageId,
    /// Full page image captured before the logged update.
    pub(crate) before: [u8; PAGE_SIZE],
    /// Full page image installed by the logged update.
    pub(crate) after: [u8; PAGE_SIZE],
    /// LSN assigned to the update that this image undoes.
    pub(crate) lsn: Lsn,
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
/// increasing transaction ids, appends transaction-control records, remembers
/// in-memory undo images for explicit rollback, and marks the active
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
    undo_pages: Vec<PageUndo>,
    poisoned: bool,
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

    /// Begins the only active transaction and writes its `Begin` WAL record.
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
        let lsn = log.append_record(txn_id, LogRecordKind::Begin)?;
        self.max_txn_id = txn_id;
        self.active = Some(ActiveTransaction {
            txn_id,
            last_lsn: lsn,
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
    pub(crate) fn record_page_alloc(
        &mut self,
        log: &mut LogManager,
        page_id: PageId,
    ) -> StorageResult<Option<Lsn>> {
        // Allocated page ids are not reclaimed on rollback until a freelist exists.
        let Some(active) = self.active.as_mut() else {
            return Ok(None);
        };
        let lsn = match log.append_record(active.txn_id, LogRecordKind::PageAlloc { page_id }) {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err.into());
            }
        };
        active.last_lsn = lsn;
        Ok(Some(lsn))
    }

    /// Writes a full-page update record for the active transaction, if any.
    ///
    /// When no transaction is active, the update is not logged and `Ok(None)`
    /// is returned. With an active transaction, this method reserves the next
    /// LSN, stamps it into the redo image for current B+-tree pages, appends a
    /// `PageUpdate` WAL record containing both redo and undo full-page images,
    /// and remembers the undo image for explicit rollback.
    ///
    /// If LSN reservation or WAL append fails, the active transaction is marked
    /// poisoned. A poisoned transaction cannot commit because the caller can no
    /// longer prove that all page effects were logged.
    pub(crate) fn record_page_update(
        &mut self,
        log: &mut LogManager,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        let Some(active) = self.active.as_mut() else {
            return Ok(None);
        };

        let lsn = match log.next_lsn() {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err.into());
            }
        };
        let mut redo = *after;
        stamp_page_lsn(&mut redo, lsn);

        let appended_lsn = match log.append_record(
            active.txn_id,
            LogRecordKind::PageUpdate { page_id, redo_data: &redo, undo_data: before },
        ) {
            Ok(lsn) => lsn,
            Err(err) => {
                active.poisoned = true;
                return Err(err.into());
            }
        };
        debug_assert_eq!(appended_lsn, lsn);
        active.last_lsn = lsn;
        active.undo_pages.push(PageUndo { page_id, before: *before, after: redo, lsn });
        Ok(Some(LoggedPageUpdate { lsn, redo }))
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

        let lsn = log.append_record(txn_id, LogRecordKind::Commit)?;
        self.active = None;
        log.flush_through(lsn)?;
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
        log: &mut LogManager,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageUndo>> {
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
            let lsn = match log.next_lsn() {
                Ok(lsn) => lsn,
                Err(err) => {
                    active.poisoned = true;
                    return Err(err.into());
                }
            };
            let mut redo = undo.before;
            stamp_page_lsn(&mut redo, lsn);
            let appended_lsn = match log.append_record(
                active.txn_id,
                LogRecordKind::PageUpdate {
                    page_id: undo.page_id,
                    redo_data: &redo,
                    undo_data: &undo.after,
                },
            ) {
                Ok(lsn) => lsn,
                Err(err) => {
                    active.poisoned = true;
                    return Err(err.into());
                }
            };
            debug_assert_eq!(appended_lsn, lsn);
            active.last_lsn = lsn;
            restore_pages.push(PageUndo {
                page_id: undo.page_id,
                before: redo,
                after: undo.after,
                lsn,
            });
        }

        active.undo_pages.truncate(savepoint.undo_len);
        Ok(restore_pages)
    }

    /// Removes the active transaction and returns its undo images for rollback.
    ///
    /// The returned vector is ordered from newest update to oldest update. If
    /// `txn_id` does not match the active transaction, the transaction is put
    /// back before returning the mismatch error.
    pub(crate) fn take_rollback_pages(&mut self, txn_id: TxnId) -> StorageResult<Vec<PageUndo>> {
        let active = self.active.take().ok_or_else(no_active_transaction)?;
        if active.txn_id != txn_id {
            let expected = active.txn_id;
            self.active = Some(active);
            return Err(transaction_mismatch(expected, txn_id));
        }

        let mut undo_pages = active.undo_pages;
        undo_pages.reverse();
        Ok(undo_pages)
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
        let lsn = log.append_record(txn_id, LogRecordKind::Rollback)?;
        log.flush_through(lsn)?;
        Ok(())
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
    use crate::core::{
        error::{InternalError, InvariantViolation},
        log_manager::{LogManager, OwnedLogRecordKind, read_log_record_kinds_for_test},
    };

    #[test]
    fn page_alloc_without_active_transaction_does_not_write_wal() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let lsn = transactions.record_page_alloc(&mut log, 7).unwrap();

        assert_eq!(lsn, None);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn page_alloc_with_active_transaction_writes_wal_record() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let alloc_lsn = transactions.record_page_alloc(&mut log, 7).unwrap();
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
    fn rollback_to_invalid_savepoint_returns_error_without_panicking() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let savepoint = TransactionSavepoint { txn_id, undo_len: 1 };
        let result = transactions.rollback_to_savepoint(&mut log, savepoint);

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
        transactions.record_page_update(&mut log, 7, &before, &after).unwrap();
        log.fail_next_flush_for_test();

        let result = transactions.commit(&mut log, txn_id);

        assert!(result.is_err());
        assert!(matches!(
            transactions.take_rollback_pages(txn_id),
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
