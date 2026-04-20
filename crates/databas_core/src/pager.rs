use std::path::{Path, PathBuf};

use crate::{disk_manager::DiskManager, error::StorageResult, page_cache::PageCache};

const DEFAULT_PAGE_CACHE_SIZE: usize = 64;

/// Configuration for [`Pager`].
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
}
