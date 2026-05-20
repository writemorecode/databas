use std::path::{Path, PathBuf};

use crate::core::{
    PageId,
    btree::{TreeCursor, initialize_empty_root, validate_root_page},
    cursor::{IndexCursor, TableCursor},
    disk_manager::DiskManager,
    error::StorageResult,
    page_cache::PageCache,
};

const DEFAULT_PAGE_CACHE_SIZE: usize = 64;

/// Configuration for [`crate::core::CatalogManager`].
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
    path: PathBuf,
    page_cache: PageCache,
    options: PagerOptions,
    opened_page_count: u64,
}

impl Pager {
    /// Opens a pager with default options.
    pub(crate) fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, PagerOptions::default())
    }

    /// Opens a pager with explicit cache settings.
    pub(crate) fn open_with_options(
        path: impl AsRef<Path>,
        options: PagerOptions,
    ) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let disk_manager = DiskManager::new(&path)?;
        let opened_page_count = disk_manager.page_count();
        let page_cache = PageCache::new(disk_manager, options.cache_frames)?;
        Ok(Self { path, page_cache, options, opened_page_count })
    }

    /// Returns the database-file path associated with this pager.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the options used when this pager was opened.
    pub(crate) fn options(&self) -> PagerOptions {
        self.options
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

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn opens_database_and_manages_table_and_index_trees() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open(file.path()).unwrap();

        assert_eq!(pager.opened_page_count(), 0);
        assert_eq!(pager.create_table_tree().unwrap().root_page_id(), 0);
        assert_eq!(pager.create_index_tree().unwrap().root_page_id(), 1);
        pager.flush().unwrap();

        let pager = Pager::open(file.path()).unwrap();
        assert_eq!(pager.opened_page_count(), 2);
        assert_eq!(pager.table_cursor(0).unwrap().root_page_id(), 0);
        assert_eq!(pager.index_cursor(1).unwrap().root_page_id(), 1);
    }
}
