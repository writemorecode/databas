use crate::core::{
    PAGE_SIZE, PageId,
    error::{InternalError, InvariantViolation, StorageError, StorageResult},
    log_manager::{LogManager, LogRecordKind, Lsn, TxnId, ZERO_LSN},
    page,
};

#[derive(Debug, Clone)]
pub(crate) struct PageUndo {
    pub(crate) page_id: PageId,
    pub(crate) before: [u8; PAGE_SIZE],
    pub(crate) lsn: Lsn,
}

#[derive(Debug)]
pub(crate) struct LoggedPageUpdate {
    pub(crate) lsn: Lsn,
    pub(crate) redo: [u8; PAGE_SIZE],
}

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
    pub(crate) fn new(max_txn_id: TxnId) -> Self {
        Self { max_txn_id, active: None }
    }

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
        active.undo_pages.push(PageUndo { page_id, before: *before, lsn });
        Ok(Some(LoggedPageUpdate { lsn, redo }))
    }

    pub(crate) fn record_failure(&mut self) {
        if let Some(active) = self.active.as_mut() {
            active.poisoned = true;
        }
    }

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
