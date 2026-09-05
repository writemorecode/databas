//! Transaction coordinator for WAL-backed page changes.
//!
//! While a transaction is active, page allocations and full-page updates are
//! appended to the WAL immediately, while durability is deferred until the
//! write-ahead rule or transaction outcome requires a flush. Rollback uses the
//! in-memory undo images accumulated here; crash recovery uses the durable WAL
//! records written by [`LogManager`].

use std::collections::HashMap;

use crate::core::{
    PAGE_SIZE, PageId,
    error::{InternalError, InvariantViolation, StorageError, StorageResult},
};
use crate::storage::{
    log_manager::{LogManager, LogRecordKind, Lsn, TxnId},
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
    pub(crate) txn_id: TxnId,
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
/// increasing transaction ids, remembers in-memory undo images for explicit
/// rollback, and marks a transaction as poisoned after an error that may have
/// left its effects only partially logged.
#[derive(Debug)]
pub(crate) struct TransactionManager {
    /// Greatest transaction ID issued by this manager or observed during open.
    max_txn_id: TxnId,
    /// Transactions currently accepted by the storage runtime, keyed by id.
    transactions: HashMap<TxnId, ActiveTransaction>,
}

/// In-memory state for one transaction owned by the storage runtime.
#[derive(Debug)]
struct ActiveTransaction {
    /// One before-image per logical page mutation, in mutation order.
    ///
    /// Explicit rollback walks every entry in reverse so repeated writes restore
    /// intermediate images before finally restoring the transaction's original
    /// image.
    undo_pages: Vec<PageUndo>,
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
        log.append_record(txn_id, LogRecordKind::Begin)?;
        self.max_txn_id = txn_id;
        self.transactions.insert(
            txn_id,
            ActiveTransaction { undo_pages: Vec::new(), rollback_lsn: None, poisoned: false },
        );
        Ok(txn_id)
    }

    /// Records a page allocation for a transaction, if it exists.
    ///
    /// Page allocations outside a transaction are allowed and do not write WAL.
    /// Allocated page ids are currently not reclaimed during rollback; the WAL
    /// record exists so crash recovery can make committed allocations visible
    /// before replaying their updates.
    pub(crate) fn record_page_alloc(
        &mut self,
        log: &mut LogManager,
        txn_id: TxnId,
        page_id: PageId,
    ) -> StorageResult<Option<Lsn>> {
        // Allocated page ids are not reclaimed on rollback until a freelist exists.
        if !self.transactions.contains_key(&txn_id) {
            return Ok(None);
        }
        match log.append_record(txn_id, LogRecordKind::PageAlloc { page_id }) {
            Ok(lsn) => Ok(Some(lsn)),
            Err(err) => {
                self.record_failure(txn_id);
                Err(err.into())
            }
        }
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

    #[cfg(test)]
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
        log: &mut LogManager,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        let active = self.transaction(savepoint.txn_id)?;
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
            let expected_lsn = match log.next_lsn() {
                Ok(lsn) => lsn,
                Err(err) => {
                    self.record_failure(savepoint.txn_id);
                    return Err(err.into());
                }
            };
            let mut redo = undo.before;
            stamp_page_lsn(&mut redo, expected_lsn);
            let lsn = match log.append_record(
                savepoint.txn_id,
                LogRecordKind::PageUpdate {
                    page_id: undo.page_id,
                    redo_data: &redo,
                    undo_data: &undo.after,
                },
            ) {
                Ok(lsn) => lsn,
                Err(err) => {
                    self.record_failure(savepoint.txn_id);
                    return Err(err.into());
                }
            };
            if lsn != expected_lsn {
                self.record_failure(savepoint.txn_id);
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
    /// active transaction stays available while callers restore pages so a
    /// failed restoration can still be retried.
    pub(crate) fn prepare_rollback_pages(
        &mut self,
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
        Ok(TransactionRollback { pages })
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
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let lsn = transactions.record_page_alloc(&mut log, 0, 7).unwrap();

        assert_eq!(lsn, None);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn page_alloc_with_active_transaction_appends_wal_immediately() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);

        let txn_id = transactions.begin(&mut log).unwrap();
        let alloc_lsn = transactions.record_page_alloc(&mut log, txn_id, 7).unwrap();

        assert_eq!(log.highest_appended_lsn(), Some(2));
        assert_eq!(log.highest_durable_lsn(), None);
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
    fn repeated_page_updates_append_separate_wal_records() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        let first_update =
            transactions.record_page_update(&mut log, txn_id, 7, &before, &after_first).unwrap();
        let second_update = transactions
            .record_page_update(&mut log, txn_id, 7, &after_first, &after_second)
            .unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(first_update.as_ref().map(|update| update.lsn), Some(2));
        assert_eq!(second_update.as_ref().map(|update| update.lsn), Some(3));
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
    fn eager_page_updates_keep_each_undo_and_redo_image() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after_first).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &after_first, &after_second).unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        let scan = read_recovery_log(file.path()).unwrap();
        match (&scan.records[1].kind, &scan.records[2].kind) {
            (
                RecoveryLogRecordKind::PageUpdate {
                    page_id: first_page_id,
                    redo_data: first_redo,
                    undo_data: first_undo,
                },
                RecoveryLogRecordKind::PageUpdate {
                    page_id: second_page_id,
                    redo_data: second_redo,
                    undo_data: second_undo,
                },
            ) => {
                assert_eq!((*first_page_id, *second_page_id), (7, 7));
                assert_eq!(first_undo.as_ref(), &before);
                assert_eq!(first_redo.as_ref(), &after_first);
                assert_eq!(second_undo.as_ref(), &after_first);
                assert_eq!(second_redo.as_ref(), &after_second);
            }
            kinds => panic!("unexpected record kinds: {kinds:?}"),
        }
    }

    #[test]
    fn mixed_page_updates_append_in_mutation_order() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before_a = [0; PAGE_SIZE];
        let after_a_first = [1; PAGE_SIZE];
        let before_b = [10; PAGE_SIZE];
        let after_b = [11; PAGE_SIZE];
        let after_a_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &before_a, &after_a_first).unwrap();
        transactions.record_page_update(&mut log, txn_id, 8, &before_b, &after_b).unwrap();
        transactions
            .record_page_update(&mut log, txn_id, 7, &after_a_first, &after_a_second)
            .unwrap();
        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 8 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn page_updates_advance_highest_appended_lsn_before_commit() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after_first).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &after_first, &after_second).unwrap();

        assert_eq!(log.highest_appended_lsn(), Some(3));
        assert_eq!(log.highest_durable_lsn(), None);
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
    fn savepoint_rollback_appends_compensation_record_immediately() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after_first).unwrap();
        let savepoint = transactions.statement_savepoint(txn_id).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &after_first, &after_second).unwrap();

        let restore_pages = transactions.rollback_to_savepoint(&mut log, savepoint).unwrap();
        transactions.complete_savepoint_rollback(savepoint).unwrap();

        assert_eq!(restore_pages.len(), 1);
        assert_eq!(restore_pages[0].page_id, 7);
        assert_eq!(restore_pages[0].wal_flush_lsn, 4);
        assert_eq!(log.highest_appended_lsn(), Some(4));

        transactions.commit(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
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
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after).unwrap();
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

    // After an outcome append succeeds, a flush retry must not append it again.
    #[test]
    fn rollback_flush_retry_does_not_append_another_outcome() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_first = [1; PAGE_SIZE];
        let after_second = [2; PAGE_SIZE];

        let txn_id = transactions.begin(&mut log).unwrap();
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after_first).unwrap();
        transactions.record_page_update(&mut log, txn_id, 8, &before, &after_second).unwrap();
        log.fail_next_flush_for_test();
        assert!(transactions.finish_rollback(&mut log, txn_id).is_err());

        transactions.finish_rollback(&mut log, txn_id).unwrap();

        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 8 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    #[test]
    fn interleaved_transactions_receive_lsns_in_append_order() {
        let file = NamedTempFile::new().unwrap();
        let mut log = LogManager::new(file.path()).unwrap();
        let mut transactions = TransactionManager::new(0);
        let before = [0; PAGE_SIZE];
        let after_a = [1; PAGE_SIZE];
        let after_b = [2; PAGE_SIZE];

        let txn_a = transactions.begin(&mut log).unwrap();
        let txn_b = transactions.begin(&mut log).unwrap();
        let update_a =
            transactions.record_page_update(&mut log, txn_a, 7, &before, &after_a).unwrap();
        let update_b =
            transactions.record_page_update(&mut log, txn_b, 8, &before, &after_b).unwrap();

        transactions.commit(&mut log, txn_b).unwrap();
        transactions.commit(&mut log, txn_a).unwrap();

        assert_eq!(update_a.map(|update| update.lsn), Some(3));
        assert_eq!(update_b.map(|update| update.lsn), Some(4));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_a, OwnedLogRecordKind::Begin),
                (txn_b, OwnedLogRecordKind::Begin),
                (txn_a, OwnedLogRecordKind::PageUpdate { page_id: 7 }),
                (txn_b, OwnedLogRecordKind::PageUpdate { page_id: 8 }),
                (txn_b, OwnedLogRecordKind::Commit),
                (txn_a, OwnedLogRecordKind::Commit),
            ]
        );
    }

    // A rolled-back transaction remains observable in the WAL so reopening can
    // keep transaction IDs monotonic.
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
        transactions.record_page_update(&mut log, txn_id, 7, &before, &after).unwrap();
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
