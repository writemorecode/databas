use std::path::Path;

use crate::PageId;
use crate::btree::{TreeCursor, initialize_empty_root, validate_tree_page_formats};
use crate::database_header::{DATABASE_HEADER_PAGE_ID, DatabaseHeader, missing_header};
use crate::disk_manager::{DiskManager, missing_doublewrite};
use crate::error::StorageResult;
use crate::filesystem::{FileSystem, OpenMode};
use crate::log_manager::TxnId;
use crate::page_cache::PageCache;
use crate::storage_runtime::StorageRuntime;
use crate::transaction_manager::TransactionSavepoint;
use std::sync::Arc;

const DEFAULT_PAGE_CACHE_SIZE: usize = 16384;

/// Configuration for [`Storage`].
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
/// Owns the shared storage runtime and page cache, and coordinates raw trees,
/// transaction durability, rollback, and checkpoints.
#[derive(Clone)]
pub struct Storage {
    runtime: Arc<StorageRuntime>,
    page_cache: PageCache,
    opened_page_count: u64,
}

impl Storage {
    /// Creates a new database file and initializes its database header.
    pub fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_in(FileSystem::Os, path.as_ref(), StorageOptions::default(), OpenMode::CreateNew)
    }

    /// Opens an existing storage with default options.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_in(FileSystem::Os, path.as_ref(), StorageOptions::default(), OpenMode::Existing)
    }

    /// Opens a storage, creating and initializing an empty file if needed.
    pub fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_or_create_with_options(path, StorageOptions::default())
    }

    /// Opens a storage with explicit cache settings, creating an empty file if needed.
    pub fn open_or_create_with_options(
        path: impl AsRef<Path>,
        options: StorageOptions,
    ) -> StorageResult<Self> {
        Self::open_in(FileSystem::Os, path.as_ref(), options, OpenMode::Create)
    }

    pub(crate) fn open_in(
        fs: FileSystem,
        path: &Path,
        options: StorageOptions,
        mode: OpenMode,
    ) -> StorageResult<Self> {
        let mut disk_manager = DiskManager::open(&fs, path, mode)?;
        disk_manager.lock_exclusive()?;
        if matches!(mode, OpenMode::CreateNew)
            || matches!(mode, OpenMode::Create) && disk_manager.page_count() == 0
        {
            disk_manager.create_doublewrite(&fs, path)?;
            initialize_header_page(&mut disk_manager)?;
        } else if disk_manager.repair_doublewrite(&fs, path)? {
            validate_header_page(&mut disk_manager)?;
        } else {
            if disk_manager.page_count() == 0 {
                return Err(missing_header());
            }
            // Detect older formats without creating or modifying their sidecars.
            let mut page = [0; crate::PAGE_SIZE];
            disk_manager.read_page_unchecked(DATABASE_HEADER_PAGE_ID, &mut page)?;
            DatabaseHeader::validate_page(&page)?;
            return Err(missing_doublewrite().into());
        }
        let opened_page_count = disk_manager.page_count();
        let runtime = Arc::new(StorageRuntime::new(fs, path.to_path_buf(), disk_manager)?);
        let page_cache = PageCache::new(Arc::clone(&runtime), options.cache_frames)?;
        Ok(Self { runtime, page_cache, opened_page_count })
    }

    /// Returns the database-file path associated with this storage.
    pub fn path(&self) -> &Path {
        self.runtime.path()
    }

    /// Returns the page count observed when this storage was opened.
    pub fn opened_page_count(&self) -> u64 {
        self.opened_page_count
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.page_cache.flush_all()?;
        self.runtime.flush_allocations()?;
        self.runtime.sync_database_file()?;
        Ok(())
    }

    pub fn checkpoint(&self) -> StorageResult<crate::CheckpointOutcome> {
        self.runtime.checkpoint(|| Ok(self.page_cache.flush_checkpoint()?))
    }

    pub fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.runtime.begin_transaction()
    }

    pub fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.runtime.commit_transaction(txn_id)
    }

    pub fn transaction_is_active(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.runtime.transaction_is_active(txn_id)
    }

    pub fn transaction_is_poisoned(&self, txn_id: TxnId) -> StorageResult<bool> {
        self.runtime.transaction_is_poisoned(txn_id)
    }

    pub fn statement_savepoint(&self, txn_id: TxnId) -> StorageResult<TransactionSavepoint> {
        self.runtime.statement_savepoint(txn_id)
    }

    pub fn rollback_to_savepoint(&self, savepoint: TransactionSavepoint) -> StorageResult<()> {
        let undo_pages = self.runtime.rollback_to_savepoint(savepoint)?;
        if let Err(err) = self.page_cache.restore_rollback_pages(undo_pages) {
            self.runtime.record_transaction_failure(savepoint.txn_id)?;
            return Err(err.into());
        }
        self.runtime.complete_savepoint_rollback(savepoint)
    }

    pub fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let rollback = self.runtime.prepare_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(rollback.pages)?;
        // Completion forces WAL through the CLRs, not the compensated pages.
        // Restart will redo them if their cache images never reached home.
        self.runtime.finish_rollback(txn_id)?;
        Ok(())
    }

    /// Creates a new empty raw tree and returns a cursor rooted at it.
    pub fn create_tree(&self) -> StorageResult<TreeCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache, None)?;
        TreeCursor::new(self.page_cache.clone(), root_page_id)
    }

    pub fn transaction_create_tree(&self, txn_id: TxnId) -> StorageResult<TreeCursor> {
        self.runtime.validate_transaction_write(txn_id)?;
        let root_page_id = initialize_empty_root(&self.page_cache, Some(txn_id))?;
        self.runtime.validate_transaction_write(txn_id)?;
        Ok(TreeCursor::new(self.page_cache.clone(), root_page_id)?.for_transaction(txn_id))
    }

    /// Reclaims a whole tree, including its root and overflow chains.
    ///
    /// The caller must hold exclusive logical access and remove the catalog
    /// reference in this same transaction. On error, roll back the transaction
    /// or its statement savepoint. Cursors may not outlive that access scope.
    pub fn transaction_destroy_tree(
        &self,
        txn_id: TxnId,
        root_page_id: PageId,
    ) -> StorageResult<()> {
        self.runtime.validate_transaction_write(txn_id)?;
        crate::btree::destroy_tree(&self.page_cache, txn_id, root_page_id)?;
        self.runtime.validate_transaction_write(txn_id)
    }

    /// Returns a raw cursor rooted at an existing tree.
    pub fn tree_cursor(&self, root_page_id: PageId) -> StorageResult<TreeCursor> {
        TreeCursor::new(self.page_cache.clone(), root_page_id)
    }

    /// Opens a cursor for an active, writable transaction.
    ///
    /// Serialize transaction completion with all mutations using its cursors.
    /// A retained cursor rejects mutations after the transaction ends.
    pub fn transaction_tree_cursor(
        &self,
        txn_id: TxnId,
        root_page_id: PageId,
    ) -> StorageResult<TreeCursor> {
        self.runtime.validate_transaction_write(txn_id)?;
        Ok(self.tree_cursor(root_page_id)?.for_transaction(txn_id))
    }

    /// Validates every B+-tree page reachable from `root_page_id`.
    pub fn validate_tree_page_formats(&self, root_page_id: PageId) -> StorageResult<()> {
        validate_tree_page_formats(&self.page_cache, root_page_id)
    }
}

fn initialize_header_page(disk_manager: &mut DiskManager) -> StorageResult<()> {
    let page_id = disk_manager.new_page(&DatabaseHeader::encode_page())?;
    debug_assert_eq!(page_id, DATABASE_HEADER_PAGE_ID);
    disk_manager.sync()?;
    Ok(())
}

fn validate_header_page(disk_manager: &mut DiskManager) -> StorageResult<()> {
    if disk_manager.page_count() == 0 {
        return Err(missing_header());
    }

    let mut page = [0u8; crate::PAGE_SIZE];
    disk_manager.read_page(DATABASE_HEADER_PAGE_ID, &mut page)?;
    DatabaseHeader::validate_page(&page)
}
