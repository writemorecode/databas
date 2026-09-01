use std::{path::Path, rc::Rc};

use crate::core::{PageId, error::StorageResult};
#[cfg(test)]
use crate::storage::disk_manager::DiskManagerError;
use crate::storage::{
    btree::{TreeCursor, initialize_empty_root, validate_tree_page_formats},
    database_header::{DATABASE_HEADER_PAGE_ID, DatabaseHeader, missing_header},
    disk_manager::DiskManager,
    log_manager::TxnId,
    page_cache::PageCache,
    storage_runtime::StorageRuntime,
    transaction_manager::TransactionSavepoint,
};

const DEFAULT_PAGE_CACHE_SIZE: usize = 16384;

/// Configuration for [`crate::core::Database`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageOptions {
    /// Number of frames to preallocate in the page cache.
    pub cache_frames: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self { cache_frames: DEFAULT_PAGE_CACHE_SIZE }
    }
}

/// Storage-engine handle for one database file.
///
/// `Storage` owns the disk manager and page cache indirectly, and is responsible
/// only for producing raw B+-tree cursors rooted at specific page ids.
#[derive(Clone)]
pub(crate) struct Storage {
    runtime: Rc<StorageRuntime>,
    page_cache: PageCache,
    opened_page_count: u64,
}

impl Storage {
    /// Creates a new database file and initializes its database header.
    pub(crate) fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::create_with_options(path, StorageOptions::default())
    }

    /// Creates a new database file with explicit cache settings.
    pub(crate) fn create_with_options(
        path: impl AsRef<Path>,
        options: StorageOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::create_new(&path)?;
        disk_manager.lock_exclusive()?;
        initialize_header_page(&mut disk_manager)?;
        Self::from_disk_manager(path, disk_manager, options)
    }

    /// Opens an existing storage with default options.
    pub(crate) fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, StorageOptions::default())
    }

    /// Opens an existing storage with explicit cache settings.
    pub(crate) fn open_with_options(
        path: impl AsRef<Path>,
        options: StorageOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::open_existing(&path)?;
        disk_manager.lock_exclusive()?;
        validate_header_page(&mut disk_manager)?;
        Self::from_disk_manager(path, disk_manager, options)
    }

    /// Opens a storage, creating and initializing an empty file if needed.
    pub(crate) fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_or_create_with_options(path, StorageOptions::default())
    }

    /// Opens a storage with explicit cache settings, creating an empty file if needed.
    pub(crate) fn open_or_create_with_options(
        path: impl AsRef<Path>,
        options: StorageOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::new(&path)?;
        disk_manager.lock_exclusive()?;
        if disk_manager.page_count() == 0 {
            initialize_header_page(&mut disk_manager)?;
        } else {
            validate_header_page(&mut disk_manager)?;
        }
        Self::from_disk_manager(path, disk_manager, options)
    }

    fn from_disk_manager(
        path: std::path::PathBuf,
        disk_manager: DiskManager,
        options: StorageOptions,
    ) -> StorageResult<Self> {
        let opened_page_count = disk_manager.page_count();
        let runtime = Rc::new(StorageRuntime::new(path, disk_manager)?);
        let page_cache = PageCache::new(Rc::clone(&runtime), options.cache_frames)?;
        Ok(Self { runtime, page_cache, opened_page_count })
    }

    /// Returns the database-file path associated with this storage.
    pub(crate) fn path(&self) -> &Path {
        self.runtime.path()
    }

    /// Returns the page count observed when this storage was opened.
    pub(crate) fn opened_page_count(&self) -> u64 {
        self.opened_page_count
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub(crate) fn flush(&self) -> StorageResult<()> {
        self.page_cache.flush_all()?;
        self.runtime.sync_database_file()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn unlock_for_crash_for_test(&self) -> Result<(), DiskManagerError> {
        self.runtime.unlock_for_crash_for_test()
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.runtime.begin_transaction()
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.runtime.commit_transaction(txn_id)
    }

    pub(crate) fn active_transaction_id(&self) -> Option<TxnId> {
        self.runtime.active_transaction_id()
    }

    pub(crate) fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.runtime.transaction_is_poisoned(txn_id)
    }

    pub(crate) fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.runtime.statement_savepoint(txn_id)
    }

    pub(crate) fn rollback_to_savepoint(
        &self,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<()> {
        let undo_pages = self.runtime.rollback_to_savepoint(savepoint)?;
        if let Err(err) = self.page_cache.restore_rollback_pages(undo_pages) {
            self.runtime.record_transaction_failure();
            return Err(err.into());
        }
        self.runtime.complete_savepoint_rollback(savepoint)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let rollback = self.runtime.prepare_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(rollback.pages)?;
        self.page_cache.flush_all()?;
        self.runtime.sync_database_file()?;
        self.runtime.finish_rollback(txn_id)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&self) {
        self.runtime.force_next_lsn_exhausted_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_savepoint_rollback_for_test(&self) {
        self.runtime.fail_next_savepoint_rollback_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) {
        self.runtime.fail_next_wal_flush_for_test();
    }

    /// Creates a new empty raw tree and returns a cursor rooted at it.
    pub(crate) fn create_tree(&self) -> StorageResult<TreeCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache)?;
        Ok(TreeCursor::new(self.page_cache.clone(), root_page_id))
    }

    /// Returns a raw cursor rooted at an existing tree.
    pub(crate) fn tree_cursor(&self, root_page_id: PageId) -> TreeCursor {
        TreeCursor::new(self.page_cache.clone(), root_page_id)
    }

    /// Validates every B+-tree page reachable from `root_page_id`.
    pub(crate) fn validate_tree_page_formats(&self, root_page_id: PageId) -> StorageResult<()> {
        validate_tree_page_formats(&self.page_cache, root_page_id)
    }
}

fn initialize_header_page(disk_manager: &mut DiskManager) -> StorageResult<()> {
    let page_id = disk_manager.new_page()?;
    debug_assert_eq!(page_id, DATABASE_HEADER_PAGE_ID);
    disk_manager.write_page(DATABASE_HEADER_PAGE_ID, &DatabaseHeader::encode_page())?;
    disk_manager.sync()?;
    Ok(())
}

fn validate_header_page(disk_manager: &mut DiskManager) -> StorageResult<()> {
    if disk_manager.page_count() == 0 {
        return Err(missing_header());
    }

    let mut page = [0u8; crate::core::PAGE_SIZE];
    disk_manager.read_page(DATABASE_HEADER_PAGE_ID, &mut page)?;
    DatabaseHeader::validate_page(&page)
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn opens_database_and_manages_raw_trees() {
        let file = NamedTempFile::new().unwrap();
        let storage = Storage::open_or_create(file.path()).unwrap();

        assert_eq!(storage.opened_page_count(), 1);
        assert_eq!(storage.create_tree().unwrap().root_page_id(), 1);
        assert_eq!(storage.create_tree().unwrap().root_page_id(), 2);
        storage.flush().unwrap();
        drop(storage);

        let storage = Storage::open(file.path()).unwrap();
        assert_eq!(storage.opened_page_count(), 3);
        assert_eq!(storage.tree_cursor(1).root_page_id(), 1);
        assert_eq!(storage.tree_cursor(2).root_page_id(), 2);
    }
}
