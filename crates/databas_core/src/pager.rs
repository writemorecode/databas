//! Public pager API for the storage engine.
//!
//! [`Pager`] owns the single-threaded page cache and acts as the top-level
//! entry point for opening an existing database file or creating new table and
//! index trees. It returns typed tree cursors anchored at the root page of the
//! requested tree, while page-kind validation remains inside the pager so
//! callers do not need to reason about low-level page formats.
//!
//! At this stage the caller is still responsible for persisting tree root page
//! ids somewhere higher in the database catalog or file header. That metadata
//! layer is intentionally left outside this initial API skeleton.

use std::path::{Path, PathBuf};

use crate::{
    PageId,
    btree::{
        Index, IndexCursor, Table, TableCursor, TreeCursor, TreeKindExt, initialize_empty_root,
        validate_root_page,
    },
    disk_manager::DiskManager,
    error::StorageResult,
    page_cache::PageCache,
};

/// Configuration for [`Pager`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PagerOptions {
    /// Number of frames to preallocate in the page cache.
    pub cache_frames: usize,
}

impl Default for PagerOptions {
    fn default() -> Self {
        Self { cache_frames: 64 }
    }
}

/// Storage-engine entry point for one database file.
///
/// `Pager` owns the disk manager and page cache indirectly, and is responsible
/// for producing typed cursors rooted at specific page ids.
#[derive(Clone)]
pub struct Pager {
    path: PathBuf,
    page_cache: PageCache,
    options: PagerOptions,
}

impl Pager {
    /// Opens a pager with default options.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, PagerOptions::default())
    }

    /// Opens a pager with explicit cache settings.
    pub fn open_with_options(path: impl AsRef<Path>, options: PagerOptions) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let disk_manager = DiskManager::new(&path)?;
        let page_cache = PageCache::new(disk_manager, options.cache_frames)?;
        Ok(Self { path, page_cache, options })
    }

    /// Returns the database-file path associated with this pager.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the options used when this pager was opened.
    pub fn options(&self) -> PagerOptions {
        self.options
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.page_cache.flush_all()?;
        Ok(())
    }

    /// Opens an existing table tree rooted at `root_page_id`.
    ///
    /// The returned cursor starts anchored at the root page.
    pub fn open_table(&self, root_page_id: PageId) -> StorageResult<TableCursor> {
        self.open_tree::<Table>(root_page_id)
    }

    /// Opens an existing index tree rooted at `root_page_id`.
    ///
    /// The returned cursor starts anchored at the root page.
    pub fn open_index(&self, root_page_id: PageId) -> StorageResult<IndexCursor> {
        self.open_tree::<Index>(root_page_id)
    }

    /// Allocates and initializes a new empty table tree.
    ///
    /// The returned tree starts as a single leaf root page.
    pub fn create_table(&self) -> StorageResult<TableCursor> {
        self.create_tree::<Table>()
    }

    /// Allocates and initializes a new empty index tree.
    ///
    /// The returned tree starts as a single leaf root page.
    pub fn create_index(&self) -> StorageResult<IndexCursor> {
        self.create_tree::<Index>()
    }

    fn open_tree<K>(&self, root_page_id: PageId) -> StorageResult<TreeCursor<K>>
    where
        K: TreeKindExt,
    {
        validate_root_page::<K>(&self.page_cache, root_page_id)?;
        Ok(TreeCursor::new(self.page_cache.clone(), root_page_id))
    }

    fn create_tree<K>(&self) -> StorageResult<TreeCursor<K>>
    where
        K: TreeKindExt,
    {
        let root_page_id = initialize_empty_root::<K>(&self.page_cache)?;
        Ok(TreeCursor::new(self.page_cache.clone(), root_page_id))
    }
}
