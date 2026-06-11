use std::{cell::RefCell, path::PathBuf};

use crate::core::{
    PAGE_SIZE, PageId,
    disk_manager::DiskManager,
    error::DiskManagerError,
    log_manager::{LogManager, LogManagerError, LogManagerFlushError, LogRecord, Lsn, TxnId},
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
}

impl StorageRuntime {
    pub(crate) fn new(path: PathBuf, disk: DiskManager) -> std::io::Result<Self> {
        let log = LogManager::new(&path)?;
        Ok(Self { path, disk: RefCell::new(disk), log: RefCell::new(log) })
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

    pub(crate) fn flush_wal_through(&self, lsn: Lsn) -> Result<(), LogManagerFlushError> {
        self.log.borrow_mut().flush_through(lsn)
    }

    pub(crate) fn append_log_transaction<'a>(
        &self,
        txn_id: TxnId,
        records: &[LogRecord<'a>],
    ) -> Result<Lsn, LogManagerError<'a>> {
        self.log.borrow_mut().append_transaction(txn_id, records)
    }
}
