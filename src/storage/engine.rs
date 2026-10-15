use std::path::Path;

use crate::storage::PageId;
use crate::storage::btree::TreeCursor;
use crate::storage::btree::initialize_empty_root;
use crate::storage::btree::validate_tree_page_formats;
use crate::storage::database_header::DATABASE_HEADER_PAGE_ID;
use crate::storage::database_header::DatabaseHeader;
use crate::storage::database_header::missing_header;
use crate::storage::disk_manager::DiskManager;
use crate::storage::error::StorageResult;
use crate::storage::log_manager::TxnId;
use crate::storage::page_cache::PageCache;
use crate::storage::storage_runtime::StorageRuntime;
use crate::storage::transaction_manager::TransactionSavepoint;
use crate::sync::Arc;

const DEFAULT_PAGE_CACHE_SIZE: usize = 16384;

/// Configuration for [`crate::storage::Database`].
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
pub struct Storage {
    runtime: Arc<StorageRuntime>,
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
        let runtime = Arc::new(StorageRuntime::new(path, disk_manager)?);
        let page_cache = PageCache::new(Arc::clone(&runtime), options.cache_frames)?;
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

    pub(crate) fn checkpoint(&self) -> StorageResult<crate::storage::CheckpointOutcome> {
        self.runtime.checkpoint(|| Ok(self.page_cache.flush_checkpoint()?))
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.runtime.begin_transaction()
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.runtime.commit_transaction(txn_id)
    }

    pub(crate) fn transaction_is_active(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.runtime.transaction_is_active(txn_id)
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
            self.runtime.record_transaction_failure(savepoint.txn_id)?;
            return Err(err.into());
        }
        self.runtime.complete_savepoint_rollback(savepoint)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let rollback = self.runtime.prepare_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(rollback.pages)?;
        for page_id in rollback.flush_page_ids {
            self.page_cache.flush_page(page_id)?;
        }
        self.runtime.sync_database_file()?;
        self.runtime.finish_rollback(txn_id)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_wal_flush_for_test(&self) -> StorageResult<()> {
        self.runtime.fail_next_wal_flush_for_test()
    }

    /// Creates a new empty raw tree and returns a cursor rooted at it.
    pub(crate) fn create_tree(&self) -> StorageResult<TreeCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache, None)?;
        TreeCursor::new(self.page_cache.clone(), root_page_id)
    }

    pub(crate) fn transaction_create_tree(&self, txn_id: TxnId) -> StorageResult<TreeCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache, Some(txn_id))?;
        Ok(TreeCursor::new(self.page_cache.clone(), root_page_id)?.for_transaction(txn_id))
    }

    /// Returns a raw cursor rooted at an existing tree.
    pub(crate) fn tree_cursor(&self, root_page_id: PageId) -> StorageResult<TreeCursor> {
        TreeCursor::new(self.page_cache.clone(), root_page_id)
    }

    pub(crate) fn transaction_tree_cursor(
        &self,
        txn_id: TxnId,
        root_page_id: PageId,
    ) -> StorageResult<TreeCursor> {
        Ok(self.tree_cursor(root_page_id)?.for_transaction(txn_id))
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

    let mut page = [0u8; crate::storage::PAGE_SIZE];
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
        assert_eq!(storage.tree_cursor(1).unwrap().root_page_id(), 1);
        assert_eq!(storage.tree_cursor(2).unwrap().root_page_id(), 2);
    }

    #[test]
    fn full_rollback_flushes_savepoint_restores_before_reopen() {
        use std::io::{Seek, SeekFrom, Write};

        use crate::storage::PAGE_SIZE;
        use crate::storage::log_manager::OwnedLogRecordKind;
        use crate::storage::log_manager::read_log_record_kinds_for_test;
        use crate::storage::page::format;
        use crate::storage::page::validate_btree_page;

        let file = NamedTempFile::new().unwrap();
        let storage = Storage::open_or_create(file.path()).unwrap();
        // Keep the baseline out of the WAL so replaying an earlier commit cannot
        // hide a torn page left behind by rollback.
        let mut tree = storage.create_tree().unwrap();
        let root_page_id = tree.root_page_id();
        tree.insert(b"original", b"preserved value").unwrap();
        drop(tree);
        storage.flush().unwrap();

        let txn_id = storage.begin_transaction().unwrap();
        let savepoint = storage.statement_savepoint(txn_id).unwrap();
        let mut tree = storage.transaction_tree_cursor(txn_id, root_page_id).unwrap();
        tree.insert(b"rolled-back", b"discarded value").unwrap();
        drop(tree);

        let updated_page = {
            let pin = storage.page_cache.fetch_page(root_page_id).unwrap();
            *pin.read().unwrap().page()
        };
        let update_lsn = format::read_u64(&updated_page, format::LSN_OFFSET);
        storage.runtime.flush_wal_through(update_lsn).unwrap();

        // Simulate a failed write: the new header and slots reach disk, but the
        // cell contents in the second half of the page do not (issue #134).
        let mut database_file = file.reopen().unwrap();
        database_file.seek(SeekFrom::Start(root_page_id * PAGE_SIZE as u64)).unwrap();
        database_file.write_all(&updated_page[..PAGE_SIZE / 2]).unwrap();
        database_file.sync_all().unwrap();
        drop(database_file);

        let mut torn_page = [0; PAGE_SIZE];
        storage.runtime.read_page(root_page_id, &mut torn_page).unwrap();
        assert!(validate_btree_page(&torn_page).is_err());

        storage.rollback_to_savepoint(savepoint).unwrap();
        storage.validate_tree_page_formats(root_page_id).unwrap();

        let mut disk_page_after_savepoint = [0; PAGE_SIZE];
        storage.runtime.read_page(root_page_id, &mut disk_page_after_savepoint).unwrap();
        assert_eq!(disk_page_after_savepoint, torn_page);

        // The savepoint restored the cache and discarded all undo entries.
        // Full rollback must still flush that page before recording completion.
        assert!(storage.runtime.prepare_rollback_pages(txn_id).unwrap().pages.is_empty());
        storage.rollback_transaction(txn_id).unwrap();
        assert!(!storage.transaction_is_active(txn_id).unwrap());
        assert_eq!(
            read_log_record_kinds_for_test(file.path()).last(),
            Some(&(txn_id, OwnedLogRecordKind::Rollback))
        );
        drop(storage);

        let reopened = Storage::open(file.path()).unwrap();
        reopened.validate_tree_page_formats(root_page_id).unwrap();
        let mut tree = reopened.tree_cursor(root_page_id).unwrap();
        assert!(tree.seek_to_first().unwrap());
        let original = tree.current().unwrap().unwrap();
        original
            .with_view(|record| {
                assert_eq!(record.key(), b"original");
                assert_eq!(record.value(), b"preserved value");
            })
            .unwrap();
        drop(original);
        assert!(tree.next_record().unwrap().is_none());
        assert!(tree.get(b"rolled-back").unwrap().is_none());
    }

    #[test]
    fn rollback_does_not_flush_pages_pinned_by_other_work() {
        let file = NamedTempFile::new().unwrap();
        let storage = Storage::open_or_create(file.path()).unwrap();
        let first_root = storage.create_tree().unwrap().root_page_id();
        let second_root = storage.create_tree().unwrap().root_page_id();
        storage.flush().unwrap();

        let txn_id = storage.begin_transaction().unwrap();
        storage
            .transaction_tree_cursor(txn_id, first_root)
            .unwrap()
            .insert(b"key", b"value")
            .unwrap();
        let _unrelated_pin = storage.page_cache.fetch_page(second_root).unwrap();

        storage.rollback_transaction(txn_id).unwrap();
    }
}
