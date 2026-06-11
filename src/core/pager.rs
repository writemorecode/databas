use std::{path::Path, rc::Rc};

use crate::core::{
    PageId,
    btree::{TreeCursor, initialize_empty_root, validate_root_page},
    cursor::{IndexCursor, TableCursor},
    database_header::{DATABASE_HEADER_PAGE_ID, DatabaseHeader, missing_header},
    disk_manager::DiskManager,
    error::StorageResult,
    log_manager::TxnId,
    page_cache::PageCache,
    storage_runtime::StorageRuntime,
};

const DEFAULT_PAGE_CACHE_SIZE: usize = 64;

/// Configuration for [`crate::core::Database`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PagerOptions {
    /// Number of frames to preallocate in the page cache.
    pub cache_frames: usize,
}

impl Default for PagerOptions {
    fn default() -> Self {
        Self { cache_frames: DEFAULT_PAGE_CACHE_SIZE }
    }
}

/// Storage-engine handle for one database file.
///
/// `Pager` owns the disk manager and page cache indirectly, and is responsible
/// only for producing typed B+-tree cursors rooted at specific page ids.
#[derive(Clone)]
pub(crate) struct Pager {
    runtime: Rc<StorageRuntime>,
    page_cache: PageCache,
    opened_page_count: u64,
}

impl Pager {
    /// Creates a new database file and initializes its database header.
    pub(crate) fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::create_with_options(path, PagerOptions::default())
    }

    /// Creates a new database file with explicit cache settings.
    pub(crate) fn create_with_options(
        path: impl AsRef<Path>,
        options: PagerOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::create_new(&path)?;
        initialize_header_page(&mut disk_manager)?;
        Self::from_disk_manager(path, disk_manager, options)
    }

    /// Opens an existing pager with default options.
    pub(crate) fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, PagerOptions::default())
    }

    /// Opens an existing pager with explicit cache settings.
    pub(crate) fn open_with_options(
        path: impl AsRef<Path>,
        options: PagerOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::open_existing(&path)?;
        validate_header_page(&mut disk_manager)?;
        Self::from_disk_manager(path, disk_manager, options)
    }

    /// Opens a pager, creating and initializing an empty file if needed.
    pub(crate) fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_or_create_with_options(path, PagerOptions::default())
    }

    /// Opens a pager with explicit cache settings, creating an empty file if needed.
    pub(crate) fn open_or_create_with_options(
        path: impl AsRef<Path>,
        options: PagerOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut disk_manager = DiskManager::new(&path)?;
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
        options: PagerOptions,
    ) -> StorageResult<Self> {
        let opened_page_count = disk_manager.page_count();
        let runtime = Rc::new(StorageRuntime::new(path, disk_manager)?);
        let page_cache = PageCache::new(Rc::clone(&runtime), options.cache_frames)?;
        Ok(Self { runtime, page_cache, opened_page_count })
    }

    /// Returns the database-file path associated with this pager.
    pub(crate) fn path(&self) -> &Path {
        self.runtime.path()
    }

    /// Returns the page count observed when this pager was opened.
    pub(crate) fn opened_page_count(&self) -> u64 {
        self.opened_page_count
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub(crate) fn flush(&self) -> StorageResult<()> {
        self.page_cache.flush_all()?;
        Ok(())
    }

    pub(crate) fn begin_transaction(&self) -> StorageResult<TxnId> {
        self.runtime.begin_transaction()
    }

    pub(crate) fn commit_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        self.runtime.commit_transaction(txn_id)
    }

    pub(crate) fn rollback_transaction(&self, txn_id: TxnId) -> StorageResult<()> {
        let undo_pages = self.runtime.take_rollback_pages(txn_id)?;
        self.page_cache.restore_rollback_pages(undo_pages)?;
        self.page_cache.flush_all()?;
        self.runtime.finish_rollback(txn_id)
    }

    /// Creates a new empty table tree and returns a cursor rooted at it.
    pub(crate) fn create_table_tree(&self) -> StorageResult<TableCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache)?;
        Ok(TableCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Creates a new empty secondary-index tree and returns a cursor rooted at it.
    pub(crate) fn create_index_tree(&self) -> StorageResult<IndexCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache)?;
        Ok(IndexCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Returns a typed cursor rooted at an existing table tree.
    pub(crate) fn table_cursor(&self, root_page_id: PageId) -> StorageResult<TableCursor> {
        validate_root_page(&self.page_cache, root_page_id)?;
        Ok(TableCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Returns a typed cursor rooted at an existing secondary-index tree.
    pub(crate) fn index_cursor(&self, root_page_id: PageId) -> StorageResult<IndexCursor> {
        validate_root_page(&self.page_cache, root_page_id)?;
        Ok(IndexCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }
}

fn initialize_header_page(disk_manager: &mut DiskManager) -> StorageResult<()> {
    let page_id = disk_manager.new_page()?;
    debug_assert_eq!(page_id, DATABASE_HEADER_PAGE_ID);
    disk_manager.write_page(DATABASE_HEADER_PAGE_ID, &DatabaseHeader::encode_page())?;
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
    fn opens_database_and_manages_table_and_index_trees() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open_or_create(file.path()).unwrap();

        assert_eq!(pager.opened_page_count(), 1);
        assert_eq!(pager.create_table_tree().unwrap().root_page_id(), 1);
        assert_eq!(pager.create_index_tree().unwrap().root_page_id(), 2);
        pager.flush().unwrap();

        let pager = Pager::open(file.path()).unwrap();
        assert_eq!(pager.opened_page_count(), 3);
        assert_eq!(pager.table_cursor(1).unwrap().root_page_id(), 1);
        assert_eq!(pager.index_cursor(2).unwrap().root_page_id(), 2);
    }
}
