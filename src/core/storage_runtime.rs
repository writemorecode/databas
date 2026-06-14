use std::{cell::RefCell, path::PathBuf};

use crate::core::{
    PAGE_SIZE, PageId,
    disk_manager::DiskManager,
    error::{DiskManagerError, StorageResult},
    log_manager::{LogManager, LogManagerError, LogManagerFlushError, LogRecord, Lsn, TxnId},
    recovery::recover_from_wal,
    transaction_manager::{LoggedPageUpdate, PageUndo, TransactionManager, TransactionSavepoint},
};

/// Shared concrete storage runtime for database pages and the write-ahead log.
///
/// The runtime keeps raw database-file I/O and WAL I/O adjacent without making
/// either manager own the other. Page cache code uses it for WAL-protected page
/// writes, and future transaction code can share the same log manager.
pub(crate) struct StorageRuntime {
    path: PathBuf,
    disk: RefCell<DiskManager>,
    log: RefCell<LogManager>,
    transactions: RefCell<TransactionManager>,
}

impl StorageRuntime {
    pub(crate) fn new(path: PathBuf, mut disk: DiskManager) -> StorageResult<Self> {
        let recovery = recover_from_wal(&path, &mut disk)?;
        let log = LogManager::new(&path)?;
        let max_txn_id = recovery.max_txn_id.max(log.highest_txn_id());
        Ok(Self {
            path,
            disk: RefCell::new(disk),
            log: RefCell::new(log),
            transactions: RefCell::new(TransactionManager::new(max_txn_id)),
        })
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub(crate) fn page_count(&self) -> u64 {
        self.disk.borrow().page_count()
    }

    pub(crate) fn new_page(&self) -> Result<PageId, DiskManagerError> {
        self.disk.borrow_mut().new_page()
    }

    pub(crate) fn record_page_alloc(&self, page_id: PageId) -> StorageResult<Option<Lsn>> {
        self.transactions.borrow_mut().record_page_alloc(&mut self.log.borrow_mut(), page_id)
    }

    pub(crate) fn read_page(
        &self,
        page_id: PageId,
        buf: &mut [u8; PAGE_SIZE],
    ) -> Result<(), DiskManagerError> {
        self.disk.borrow_mut().read_page(page_id, buf)
    }

    pub(crate) fn write_page(
        &self,
        page_id: PageId,
        buf: &[u8; PAGE_SIZE],
    ) -> Result<(), DiskManagerError> {
        self.disk.borrow_mut().write_page(page_id, buf)
    }

    pub(crate) fn sync_database_file(&self) -> Result<(), DiskManagerError> {
        self.disk.borrow().sync()
    }

    pub(crate) fn flush_wal_through(&self, lsn: Lsn) -> Result<(), LogManagerFlushError> {
        self.log.borrow_mut().flush_through(lsn)
    }

    pub(crate) fn append_log_transaction<'a>(
        &self,
        txn_id: TxnId,
        records: &[LogRecord<'a>],
    ) -> Result<Lsn, LogManagerError> {
        self.log.borrow_mut().append_transaction(txn_id, records)
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&self) {
        self.log.borrow_mut().force_next_lsn_exhausted_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.log.borrow_mut().fail_next_flush_for_test();
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.transactions.borrow_mut().begin(&mut self.log.borrow_mut())
    }

    pub(crate) fn record_page_update(
        &self,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> StorageResult<Option<LoggedPageUpdate>> {
        let result = self.transactions.borrow_mut().record_page_update(
            &mut self.log.borrow_mut(),
            page_id,
            before,
            after,
        );
        if result.is_err() {
            self.transactions.borrow_mut().record_failure();
        }
        result
    }

    pub(crate) fn record_transaction_failure(&self) {
        self.transactions.borrow_mut().record_failure();
    }

    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.transactions.borrow().active_transaction_id()
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.transactions.borrow().transaction_is_poisoned(txn_id)
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.transactions.borrow_mut().commit(&mut self.log.borrow_mut(), txn_id)
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.transactions.borrow().statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageUndo>> {
        self.transactions.borrow_mut().rollback_to_savepoint(&mut self.log.borrow_mut(), savepoint)
    }

    pub(crate) fn take_rollback_pages(&self, txn_id: TxnId) -> StorageResult<Vec<PageUndo>> {
        self.transactions.borrow_mut().take_rollback_pages(txn_id)
    }

    pub(crate) fn finish_rollback(&self, txn_id: TxnId) -> StorageResult<()> {
        self.transactions.borrow_mut().finish_rollback(&mut self.log.borrow_mut(), txn_id)
    }
}
