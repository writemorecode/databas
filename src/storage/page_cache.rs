//! Shared page cache with explicit pin and page-access guards.
//!
//! [`PageCache`] is a cheap-to-clone handle that shares cache state through
//! [`Arc`]. The cache uses synchronized interior
//! mutability to allow multiple concurrent pins without requiring a mutable
//! borrow of the cache handle itself.
//!
//! The cache distinguishes between two kinds of ownership:
//!
//! - [`PinGuard`] keeps a frame resident in the cache and prevents eviction.
//! - [`PageReadGuard`] and [`PageWriteGuard`] provide temporary access to the
//!   page bytes stored in a pinned frame.
//!
//! This split makes the ownership model explicit: pinning controls residency,
//! while read and write guards control access to the page contents. Dropping a
//! [`PinGuard`] decrements the frame pin count. Dirty pages are written only by
//! explicit flushes or eviction.

use std::{
    collections::{HashMap, TryReserveError},
    sync::TryLockError,
};

use crate::sync::{
    Arc, AtomicBool, AtomicU32, AtomicU64, Mutex, MutexGuard, Ordering, RwLock, RwLockReadGuard,
    RwLockWriteGuard,
};

use thiserror::Error;

use crate::core::{PAGE_SIZE, PageId, TxnId, error::StorageError};
use crate::storage::{
    log_manager::ZERO_LSN,
    page::{NodeMarker, Page, PageResult, Read, Write},
    page_replacement::ClockPolicy,
    storage_runtime::StorageRuntime,
    transaction_manager::PageRestore,
};

#[derive(Debug, Error)]
pub(crate) enum PageCacheError {
    #[error("storage runtime error: {0}")]
    Storage(Box<StorageError>),
    #[error("no evictable frame available")]
    NoEvictableFrame,
    #[error("page {page_id} is pinned")]
    PinnedPage { page_id: PageId },
    #[error("page {page_id} cannot be borrowed immutably while a mutable borrow is active")]
    PageImmutableBorrowConflict { page_id: PageId },
    #[error("page {page_id} cannot be borrowed mutably while another borrow is active")]
    PageMutableBorrowConflict { page_id: PageId },
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error("failed to allocate {frame_count} page cache frames: {source}")]
    FrameAllocationFailed { frame_count: usize, source: TryReserveError },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
    #[error("page {page_id} pin count overflowed")]
    PinCountOverflow { page_id: PageId },
    #[error("synchronization lock poisoned: {lock}")]
    Poisoned { lock: &'static str },
}

pub(crate) type PageCacheResult<T> = Result<T, PageCacheError>;

fn runtime_error(error: StorageError) -> PageCacheError {
    PageCacheError::Storage(Box::new(error))
}

fn try_read_page_data(
    data: &RwLock<[u8; PAGE_SIZE]>,
    page_id: PageId,
) -> PageCacheResult<RwLockReadGuard<'_, [u8; PAGE_SIZE]>> {
    match data.try_read() {
        Ok(page) => Ok(page),
        Err(TryLockError::WouldBlock) => {
            Err(PageCacheError::PageImmutableBorrowConflict { page_id })
        }
        Err(TryLockError::Poisoned(_poisoned)) => {
            Err(PageCacheError::Poisoned { lock: "page data" })
        }
    }
}

fn try_write_page_data(
    data: &RwLock<[u8; PAGE_SIZE]>,
    page_id: PageId,
) -> PageCacheResult<RwLockWriteGuard<'_, [u8; PAGE_SIZE]>> {
    match data.try_write() {
        Ok(page) => Ok(page),
        Err(TryLockError::WouldBlock) => Err(PageCacheError::PageMutableBorrowConflict { page_id }),
        Err(TryLockError::Poisoned(_poisoned)) => {
            Err(PageCacheError::Poisoned { lock: "page data" })
        }
    }
}

pub(crate) type FrameId = usize;

#[derive(Debug)]
struct Frame {
    page_id: AtomicU64,
    data: RwLock<[u8; PAGE_SIZE]>,
    dirty: AtomicBool,
    lsn: AtomicU64,
    pin_count: AtomicU32,
}

impl Frame {
    fn page_id(&self) -> Option<PageId> {
        match self.page_id.load(Ordering::Acquire) {
            u64::MAX => None,
            page_id => Some(page_id),
        }
    }

    fn set_page_id(&self, page_id: Option<PageId>) {
        self.page_id.store(page_id.unwrap_or(u64::MAX), Ordering::Release);
    }

    /// Creates an empty frame with zeroed page data and cleared metadata bits.
    fn empty() -> Self {
        Self {
            page_id: AtomicU64::new(u64::MAX),
            data: RwLock::new([0u8; PAGE_SIZE]),
            dirty: AtomicBool::new(false),
            lsn: AtomicU64::new(ZERO_LSN),
            pin_count: AtomicU32::new(0),
        }
    }
}

struct CacheMeta {
    page_table: HashMap<PageId, FrameId>,
    replacement: ClockPolicy,
    tree_mutation_epochs: HashMap<PageId, Arc<AtomicU64>>,
}

struct PageCacheInner {
    runtime: Arc<StorageRuntime>,
    meta: Mutex<CacheMeta>,
    frames: Vec<Frame>,
}

impl PageCacheInner {
    fn lock_meta(&self) -> PageCacheResult<MutexGuard<'_, CacheMeta>> {
        self.meta
            .lock()
            .map_err(|_poisoned| PageCacheError::Poisoned { lock: "page cache metadata" })
    }
}

/// Thread-safe shared handle to the page cache.
///
/// Cloning the handle shares the same cache state through [`Arc`]. The handle
/// itself does not represent a pin or a page borrow; it only provides access to
/// cache operations. Use [`PinGuard`] to keep pages resident and use
/// [`PageReadGuard`] or [`PageWriteGuard`] for temporary access to the page
/// bytes.
pub(crate) struct PageCache {
    inner: Arc<PageCacheInner>,
}

impl Clone for PageCache {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl PageCache {
    /// Creates a new page cache with a fixed number of preallocated frames.
    ///
    /// Returns an error when `frame_count` is zero.
    pub(crate) fn new(runtime: Arc<StorageRuntime>, frame_count: usize) -> PageCacheResult<Self> {
        if frame_count == 0 {
            return Err(PageCacheError::InvalidFrameCount { frame_count });
        }

        let mut frames = Vec::new();
        frames
            .try_reserve_exact(frame_count)
            .map_err(|source| PageCacheError::FrameAllocationFailed { frame_count, source })?;
        frames.extend((0..frame_count).map(|_| Frame::empty()));

        Ok(Self {
            inner: Arc::new(PageCacheInner {
                runtime,
                meta: Mutex::new(CacheMeta {
                    page_table: HashMap::new(),
                    replacement: ClockPolicy::new(frame_count),
                    tree_mutation_epochs: HashMap::new(),
                }),
                frames,
            }),
        })
    }

    /// Returns the mutation epoch shared by cursors over one B+-tree root.
    pub(crate) fn tree_mutation_epoch(
        &self,
        root_page_id: PageId,
    ) -> PageCacheResult<Arc<AtomicU64>> {
        let mut meta = self.inner.lock_meta()?;
        Ok(Arc::clone(
            meta.tree_mutation_epochs
                .entry(root_page_id)
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        ))
    }

    /// Fetches an existing page into the cache and returns a pin guard.
    ///
    /// Cache hits update replacement state and increment pin count.
    /// Cache misses use CLOCK replacement and may evict a dirty page.
    pub(crate) fn fetch_page(&self, page_id: PageId) -> PageCacheResult<PinGuard> {
        let mut meta = self.inner.lock_meta()?;
        if let Some(frame_id) = self.resident_frame_id(&meta, page_id)? {
            let frame = &self.inner.frames[frame_id];
            let pin_count = frame
                .pin_count
                .load(Ordering::Acquire)
                .checked_add(1)
                .ok_or(PageCacheError::PinCountOverflow { page_id })?;
            frame.pin_count.store(pin_count, Ordering::Release);
            meta.replacement.record_access(frame_id);
            return Ok(PinGuard::new(Arc::clone(&self.inner), frame_id, page_id));
        }

        let frame_id =
            self.select_victim_frame(&mut meta).ok_or(PageCacheError::NoEvictableFrame)?;
        self.replace_frame(&mut meta, frame_id, page_id)?;
        Ok(PinGuard::new(Arc::clone(&self.inner), frame_id, page_id))
    }

    /// Allocates a new on-disk page and returns it pinned in the cache.
    ///
    /// A victim frame is selected before allocation so a full pinned cache
    /// returns `NoEvictableFrame` without growing the file.
    pub(crate) fn new_page(&self, txn_id: Option<TxnId>) -> PageCacheResult<(PageId, PinGuard)> {
        let mut meta = self.inner.lock_meta()?;
        let frame_id =
            self.select_victim_frame(&mut meta).ok_or(PageCacheError::NoEvictableFrame)?;
        let page_id = self.inner.runtime.new_page().map_err(runtime_error)?;
        if let Err(err) = self.inner.runtime.record_page_alloc(txn_id, page_id) {
            return Err(PageCacheError::Storage(Box::new(err)));
        }
        self.replace_frame(&mut meta, frame_id, page_id)?;
        Ok((page_id, PinGuard::new(Arc::clone(&self.inner), frame_id, page_id)))
    }

    /// Flushes one resident page if dirty.
    ///
    /// Non-resident pages are a no-op. Pinned pages return `PinnedPage`.
    pub(crate) fn flush_page(&self, page_id: PageId) -> PageCacheResult<()> {
        let meta = self.inner.lock_meta()?;
        let Some(frame_id) = self.resident_frame_id(&meta, page_id)? else {
            return Ok(());
        };

        let frame = &self.inner.frames[frame_id];
        if frame.pin_count.load(Ordering::Acquire) > 0 {
            return Err(PageCacheError::PinnedPage { page_id });
        }

        self.flush_frame_if_dirty(frame_id)
    }

    /// Flushes all dirty pages that are currently unpinned.
    ///
    /// Returns `PinnedPage` if a dirty page is pinned.
    pub(crate) fn flush_all(&self) -> PageCacheResult<()> {
        let _meta = self.inner.lock_meta()?;
        for (frame_id, frame) in self.inner.frames.iter().enumerate() {
            let page_id = frame.page_id();
            let pin_count = frame.pin_count.load(Ordering::Acquire);
            let dirty = frame.dirty.load(Ordering::Acquire);

            if !dirty {
                continue;
            }

            let Some(page_id) = page_id else {
                continue;
            };

            if pin_count > 0 {
                return Err(PageCacheError::PinnedPage { page_id });
            }

            self.flush_frame_if_dirty(frame_id)?;
        }

        Ok(())
    }

    fn resident_frame_id(
        &self,
        meta: &CacheMeta,
        page_id: PageId,
    ) -> PageCacheResult<Option<FrameId>> {
        let Some(&frame_id) = meta.page_table.get(&page_id) else {
            return Ok(None);
        };
        self.validate_frame_id(page_id, frame_id)?;
        Ok(Some(frame_id))
    }

    fn validate_frame_id(&self, page_id: PageId, frame_id: FrameId) -> PageCacheResult<()> {
        if frame_id >= self.inner.frames.len() {
            return Err(PageCacheError::CorruptPageTableEntry {
                page_id,
                frame_id,
                frame_count: self.inner.frames.len(),
            });
        }
        Ok(())
    }

    fn select_victim_frame(&self, meta: &mut CacheMeta) -> Option<FrameId> {
        let frames = &self.inner.frames;
        meta.replacement
            .select_victim(|frame_id| frames[frame_id].pin_count.load(Ordering::Acquire) > 0)
    }

    /// Replaces frame contents with `new_page_id`, flushing old dirty data first.
    fn replace_frame(
        &self,
        meta: &mut CacheMeta,
        frame_id: FrameId,
        new_page_id: PageId,
    ) -> PageCacheResult<()> {
        self.flush_frame_if_dirty(frame_id)?;

        let frame = &self.inner.frames[frame_id];
        let old_page_id = frame.page_id();

        let mut data = [0u8; PAGE_SIZE];
        self.inner.runtime.read_page(new_page_id, &mut data).map_err(runtime_error)?;

        {
            let mut frame_data =
                try_write_page_data(&frame.data, old_page_id.unwrap_or(new_page_id))?;
            *frame_data = data;
        }

        frame.set_page_id(Some(new_page_id));
        frame.dirty.store(false, Ordering::Release);
        frame.lsn.store(ZERO_LSN, Ordering::Release);
        frame.pin_count.store(1, Ordering::Release);

        if let Some(old_page_id) = old_page_id {
            meta.page_table.remove(&old_page_id);
        }
        meta.replacement.record_insert(frame_id);
        meta.page_table.insert(new_page_id, frame_id);
        Ok(())
    }

    /// Writes a dirty resident frame to disk and clears its dirty bit.
    fn flush_frame_if_dirty(&self, frame_id: FrameId) -> PageCacheResult<()> {
        let frame = &self.inner.frames[frame_id];
        if !frame.dirty.load(Ordering::Acquire) {
            return Ok(());
        }

        let Some(page_id) = frame.page_id() else {
            return Ok(());
        };

        let page = try_read_page_data(&frame.data, page_id)?;
        self.inner
            .runtime
            .flush_wal_through(frame.lsn.load(Ordering::Acquire))
            .map_err(|err| PageCacheError::Storage(Box::new(err)))?;
        self.inner.runtime.write_page(page_id, &page).map_err(runtime_error)?;
        frame.dirty.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn restore_rollback_pages(
        &self,
        restore_pages: Vec<PageRestore>,
    ) -> PageCacheResult<()> {
        for restore in restore_pages {
            let pin = self.fetch_page(restore.page_id)?;
            let frame = &self.inner.frames[pin.frame_id];
            {
                let mut data = try_write_page_data(&frame.data, restore.page_id)?;
                *data = restore.image;
            }
            frame.dirty.store(true, Ordering::Release);
            frame.lsn.store(restore.wal_flush_lsn, Ordering::Release);
        }
        Ok(())
    }
}

/// Residency guard for a pinned page.
///
/// Holding a `PinGuard` increments the frame pin count and guarantees that the
/// underlying frame cannot be selected for eviction. A pin does not itself
/// expose the page bytes. Call [`PinGuard::read`] or [`PinGuard::write`] to
/// borrow the page contents temporarily.
///
/// Dropping the guard decrements the frame pin count.
pub(crate) struct PinGuard {
    page_cache: Arc<PageCacheInner>,
    frame_id: FrameId,
    page_id: PageId,
}

impl PinGuard {
    /// Creates a new pin guard for a specific frame.
    fn new(page_cache: Arc<PageCacheInner>, frame_id: FrameId, page_id: PageId) -> Self {
        Self { page_cache, frame_id, page_id }
    }

    /// Returns the page ID associated with this pin.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Borrows the pinned page immutably.
    ///
    /// Multiple read guards may coexist for the same page, but immutable access
    /// fails while a write guard is active.
    pub(crate) fn read(&self) -> PageCacheResult<PageReadGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = try_read_page_data(&frame.data, self.page_id)?;
        Ok(PageReadGuard { page })
    }

    /// Borrows the pinned page mutably and marks it dirty immediately.
    ///
    /// Mutable access fails while any read or write guard is active for the
    /// same frame. Acquiring a write guard marks the frame dirty even if the
    /// caller later decides not to mutate the page bytes.
    pub(crate) fn write(&self, txn_id: Option<TxnId>) -> PageCacheResult<PageWriteGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = try_write_page_data(&frame.data, self.page_id)?;
        let before = *page;
        let was_dirty = frame.dirty.load(Ordering::Acquire);
        frame.dirty.store(true, Ordering::Release);
        Ok(PageWriteGuard {
            page,
            before,
            was_dirty,
            runtime: Arc::clone(&self.page_cache.runtime),
            frame,
            page_id: self.page_id,
            txn_id,
        })
    }
}

impl Drop for PinGuard {
    /// Decrements the frame pin count when the guard leaves scope.
    fn drop(&mut self) {
        // A poisoned cache is permanently fail-closed. Retaining this pin is
        // safer than mutating cache state whose invariants may no longer hold.
        let Ok(_meta) = self.page_cache.lock_meta() else {
            return;
        };
        let frame = &self.page_cache.frames[self.frame_id];
        let pin_count = frame.pin_count.load(Ordering::Acquire);
        debug_assert!(pin_count > 0, "pin count underflow");
        if pin_count > 0 {
            frame.pin_count.store(pin_count - 1, Ordering::Release);
        }
    }
}

/// Immutable page-byte borrow for a pinned frame.
///
/// `PageReadGuard` owns the active immutable borrow of the page bytes. It does
/// not affect eviction on its own; the associated [`PinGuard`] must remain alive
/// for the page to stay resident. Use this guard for raw byte inspection or to
/// construct typed read-only page views.
pub(crate) struct PageReadGuard<'a> {
    page: RwLockReadGuard<'a, [u8; PAGE_SIZE]>,
}

impl PageReadGuard<'_> {
    /// Returns the pinned page bytes.
    pub(crate) fn page(&self) -> &[u8; PAGE_SIZE] {
        &self.page
    }

    /// Opens a typed immutable view over the page bytes.
    pub(crate) fn open<N>(&self) -> PageResult<Page<Read<'_>, N>>
    where
        N: NodeMarker,
    {
        Page::<Read<'_>, N>::open(self.page())
    }
}

/// Mutable page-byte borrow for a pinned frame.
///
/// `PageWriteGuard` owns the active mutable borrow of the page bytes. Only one
/// write guard may exist for a frame at a time, and no read guards may coexist
/// with it. Creating a write guard marks the frame dirty immediately.
pub(crate) struct PageWriteGuard<'a> {
    page: RwLockWriteGuard<'a, [u8; PAGE_SIZE]>,
    before: [u8; PAGE_SIZE],
    was_dirty: bool,
    runtime: Arc<StorageRuntime>,
    frame: &'a Frame,
    page_id: PageId,
    txn_id: Option<TxnId>,
}

impl PageWriteGuard<'_> {
    /// Returns the pinned page bytes immutably.
    pub(crate) fn page(&self) -> &[u8; PAGE_SIZE] {
        &self.page
    }

    /// Returns the pinned page bytes mutably.
    pub(crate) fn page_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.page
    }

    /// Opens a typed mutable view over the page bytes.
    pub(crate) fn open_mut<N>(&mut self) -> PageResult<Page<Write<'_>, N>>
    where
        N: NodeMarker,
    {
        Page::<Write<'_>, N>::open(self.page_mut())
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        if *self.page == self.before {
            self.frame.dirty.store(self.was_dirty, Ordering::Release);
            return;
        }

        match self.runtime.record_page_update(self.txn_id, self.page_id, &self.before, &self.page) {
            Ok(Some(update)) => {
                *self.page = update.redo;
                self.frame.lsn.store(update.lsn, Ordering::Release);
            }
            Ok(None) => {
                self.frame.lsn.store(ZERO_LSN, Ordering::Release);
            }
            Err(_) => {
                *self.page = self.before;
                self.frame.dirty.store(self.was_dirty, Ordering::Release);
                if let Some(txn_id) = self.txn_id {
                    let _ = self.runtime.record_transaction_failure(txn_id);
                }
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{path::Path, sync::Arc, thread};

    use tempfile::NamedTempFile;

    use super::*;
    use crate::storage::disk_manager::{DiskManager, DiskManagerError};
    use crate::storage::log_manager::{Lsn, OwnedLogRecordKind, read_log_record_kinds_for_test};
    use crate::storage::page;
    use crate::storage::page::format::PageKind;
    use crate::storage::page::{Leaf, Page, Write};
    use crate::storage::storage_runtime::StorageRuntime;

    /// Generates a deterministic page payload from a seed byte.
    fn page_with_pattern(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        for (index, byte) in (0u8..=u8::MAX).cycle().zip(&mut page) {
            *byte = seed.wrapping_add(index);
        }
        page::format::write_u64(&mut page, page::format::LSN_OFFSET, ZERO_LSN);
        page
    }

    fn formatted_page_with_lsn(seed: u8, lsn: Lsn) -> [u8; PAGE_SIZE] {
        let mut page = page_with_pattern(seed);
        page[page::format::KIND_OFFSET] = PageKind::RawLeaf as u8;
        page[page::format::VERSION_OFFSET] = page::format::FORMAT_VERSION;
        page::format::write_u64(&mut page, page::format::LSN_OFFSET, lsn);
        page
    }

    /// Creates a temporary database file and writes the provided pages to it.
    fn runtime_for_disk(path: &Path, disk_manager: DiskManager) -> Arc<StorageRuntime> {
        Arc::new(StorageRuntime::new(path.to_path_buf(), disk_manager).unwrap())
    }

    fn runtime_for_path(path: &Path) -> Arc<StorageRuntime> {
        let disk_manager = DiskManager::new(path).unwrap();
        runtime_for_disk(path, disk_manager)
    }

    /// Creates a temporary database file and writes the provided pages to it.
    fn create_disk_with_pages(pages: &[[u8; PAGE_SIZE]]) -> (NamedTempFile, Arc<StorageRuntime>) {
        let file = NamedTempFile::new().unwrap();
        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        for page in pages {
            let page_id = disk_manager.new_page().unwrap();
            disk_manager.write_page(page_id, page).unwrap();
        }
        let runtime = runtime_for_disk(file.path(), disk_manager);
        (file, runtime)
    }

    /// Reads one page from disk for assertions in tests.
    fn read_disk_page(path: &Path, page_id: PageId) -> [u8; PAGE_SIZE] {
        let mut disk_manager = DiskManager::new(path).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        disk_manager.read_page(page_id, &mut page).unwrap();
        page
    }

    #[test]
    fn constructor_rejects_zero_frame_count() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let result = PageCache::new(disk_manager, 0);
        assert!(matches!(result, Err(PageCacheError::InvalidFrameCount { frame_count: 0 })));
    }

    #[test]
    fn frames_are_preallocated_and_empty() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 3).unwrap();

        assert_eq!(cache.inner.frames.len(), 3);
        for frame in &cache.inner.frames {
            assert_eq!(frame.page_id(), None);
            assert!(!frame.dirty.load(Ordering::Acquire));
            assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
            assert_eq!(*frame.data.read().unwrap(), [0u8; PAGE_SIZE]);
        }
    }

    #[test]
    fn fetch_page_loads_page_and_pins() {
        let page = page_with_pattern(7);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        assert_eq!(guard.read().unwrap().page(), &page);
        drop(guard);

        assert_eq!(cache.inner.frames[0].page_id(), Some(0));
        assert_eq!(cache.inner.frames[0].pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn pin_guard_drop_decrements_pin_count() {
        let page = page_with_pattern(11);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }

        assert_eq!(cache.inner.frames[0].pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn multiple_pin_guards_can_exist_at_the_same_time() {
        let pages = [page_with_pattern(1), page_with_pattern(2)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let left = cache.fetch_page(0).unwrap();
        let right = cache.fetch_page(1).unwrap();

        assert_eq!(left.page_id(), 0);
        assert_eq!(right.page_id(), 1);
        assert_eq!(cache.inner.frames[0].pin_count.load(Ordering::Acquire), 1);
        assert_eq!(cache.inner.frames[1].pin_count.load(Ordering::Acquire), 1);
    }

    #[test]
    fn multiple_read_guards_can_borrow_same_frame() {
        let page = page_with_pattern(13);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let read_a = guard.read().unwrap();
        let read_b = guard.read().unwrap();

        assert_eq!(read_a.page()[0], page[0]);
        assert_eq!(read_b.page()[PAGE_SIZE - 1], page[PAGE_SIZE - 1]);
    }

    #[test]
    fn read_guards_can_borrow_two_different_frames() {
        let page0 = page_with_pattern(3);
        let page1 = page_with_pattern(4);
        let pages = [page0, page1];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let guard0 = cache.fetch_page(0).unwrap();
        let guard1 = cache.fetch_page(1).unwrap();

        let read0 = guard0.read().unwrap();
        let read1 = guard1.read().unwrap();

        assert_eq!(read0.page()[0], page0[0]);
        assert_eq!(read1.page()[0], page1[0]);
    }

    #[test]
    fn write_guards_can_borrow_two_different_frames() {
        let page0 = page_with_pattern(5);
        let page1 = page_with_pattern(6);
        let pages = [page0, page1];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let guard0 = cache.fetch_page(0).unwrap();
        let guard1 = cache.fetch_page(1).unwrap();

        let mut write0 = guard0.write(None).unwrap();
        let mut write1 = guard1.write(None).unwrap();

        write0.page_mut()[0] = 42;
        write1.page_mut()[0] = 84;

        assert_eq!(write0.page()[0], 42);
        assert_eq!(write1.page()[0], 84);
    }

    #[test]
    fn page_guards_support_typed_page_views() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (_page_id, guard) = cache.new_page(None).unwrap();

        {
            let mut write = guard.write(None).unwrap();
            let _ = Page::<Write<'_>, Leaf>::init(write.page_mut());

            assert_eq!(
                Page::<Read<'_>, Leaf>::open(write.page()).unwrap().kind(),
                PageKind::RawLeaf
            );
            assert_eq!(write.open_mut::<Leaf>().unwrap().kind(), PageKind::RawLeaf);
        }

        let read = guard.read().unwrap();
        assert_eq!(read.open::<Leaf>().unwrap().kind(), PageKind::RawLeaf);
    }

    #[test]
    fn page_read_does_not_mark_dirty_but_write_does() {
        let page = page_with_pattern(13);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            assert_eq!(guard.read().unwrap().page()[0], page[0]);
        }
        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));

        {
            let guard = cache.fetch_page(0).unwrap();
            let mut page = guard.write(None).unwrap();
            page.page_mut()[0] = 99;
        }

        assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
    }

    #[test]
    fn unchanged_page_write_restores_previous_dirty_state() {
        let page = page_with_pattern(13);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();
        let guard = cache.fetch_page(0).unwrap();

        {
            let _write = guard.write(None).unwrap();
            assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
        }

        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));

        cache.inner.frames[0].dirty.store(true, Ordering::Release);
        {
            let _write = guard.write(None).unwrap();
            assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
        }

        assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
    }

    #[test]
    fn read_returns_error_while_write_guard_is_active() {
        let page = page_with_pattern(14);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let _write = guard.write(None).unwrap();

        let result = guard.read();
        assert!(matches!(result, Err(PageCacheError::PageImmutableBorrowConflict { page_id: 0 })));
    }

    #[test]
    fn write_returns_error_while_read_guard_is_active() {
        let page = page_with_pattern(15);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let _read = guard.read().unwrap();

        let result = guard.write(None);
        assert!(matches!(result, Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })));
    }

    #[test]
    fn write_returns_error_while_write_guard_is_active() {
        let page = page_with_pattern(16);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let _first_write = guard.write(None).unwrap();

        let result = guard.write(None);
        assert!(matches!(result, Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })));
    }

    #[test]
    #[allow(clippy::panic)]
    fn poisoned_page_data_is_not_reported_as_a_borrow_conflict() {
        let page = page_with_pattern(17);
        let (_file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(runtime, 1).unwrap();
        let guard = cache.fetch_page(0).unwrap();

        let inner = Arc::clone(&cache.inner);
        let panicked = thread::spawn(move || {
            let _page = inner.frames[0].data.write().unwrap();
            panic!("poison page data");
        })
        .join();
        assert!(panicked.is_err());

        assert!(matches!(guard.read(), Err(PageCacheError::Poisoned { lock: "page data" })));
    }

    #[test]
    #[allow(clippy::panic)]
    fn poisoned_metadata_prevents_further_cache_operations() {
        let page = page_with_pattern(18);
        let (_file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(runtime, 1).unwrap();

        let inner = Arc::clone(&cache.inner);
        let panicked = thread::spawn(move || {
            let _meta = inner.meta.lock().unwrap();
            panic!("poison page cache metadata");
        })
        .join();
        assert!(panicked.is_err());

        assert!(matches!(
            cache.fetch_page(0),
            Err(PageCacheError::Poisoned { lock: "page cache metadata" })
        ));
    }

    #[test]
    fn rollback_restoration_installs_its_wal_dependency() {
        let page = page_with_pattern(21);
        let (_file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(runtime, 1).unwrap();

        cache
            .restore_rollback_pages(vec![PageRestore {
                page_id: 0,
                image: page_with_pattern(22),
                wal_flush_lsn: 9,
            }])
            .unwrap();

        let frame = &cache.inner.frames[0];
        assert_eq!(frame.lsn.load(Ordering::Acquire), 9);
        assert!(frame.dirty.load(Ordering::Acquire));
    }

    #[test]
    fn dirty_page_is_written_during_eviction() {
        let page0 = page_with_pattern(1);
        let page1 = page_with_pattern(2);
        let pages = [page0, page1];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[0] = 222;
        }

        {
            let _guard = cache.fetch_page(1).unwrap();
        }

        let flushed_page0 = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page0[0], 222);
    }

    #[test]
    fn clock_gives_second_chance_before_eviction() {
        let pages = [page_with_pattern(10), page_with_pattern(20), page_with_pattern(30)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }
        {
            let _guard = cache.fetch_page(1).unwrap();
        }
        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        let page_table = &cache.inner.lock_meta().unwrap().page_table;
        assert!(!page_table.contains_key(&0));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn eviction_skips_pinned_frames() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let pinned = cache.fetch_page(0).unwrap();
        {
            let _unpinned = cache.fetch_page(1).unwrap();
        }

        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        assert_eq!(pinned.page_id(), 0);
        assert_eq!(cache.inner.frames[0].page_id(), Some(0));
        let page_table = &cache.inner.lock_meta().unwrap().page_table;
        assert!(page_table.contains_key(&0));
        assert!(!page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn fetch_returns_error_when_all_frames_are_pinned() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let _first = cache.fetch_page(0).unwrap();
        let _second = cache.fetch_page(1).unwrap();

        let result = cache.fetch_page(2);
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));
    }

    #[test]
    fn flush_page_writes_dirty_data_and_clears_dirty_bit() {
        let page = page_with_pattern(15);
        let pages = [page];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[0] = 177;
        }
        assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));

        cache.flush_page(0).unwrap();

        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[0], 177);
    }

    #[test]
    fn flush_page_ignores_lsn_loaded_from_disk() {
        let page = formatted_page_with_lsn(15, 7);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(runtime, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }

        cache.flush_page(0).unwrap();

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 177);
        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
    }

    #[test]
    fn dirty_page_eviction_ignores_lsn_loaded_from_disk() {
        let page0 = formatted_page_with_lsn(1, 13);
        let page1 = page_with_pattern(2);
        let (file, runtime) = create_disk_with_pages(&[page0, page1]);
        let cache = PageCache::new(runtime, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[PAGE_SIZE - 1] = 222;
        }

        {
            let _guard = cache.fetch_page(1).unwrap();
        }

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 222);
    }

    #[test]
    fn flush_all_ignores_lsn_loaded_from_disk() {
        let page0 = formatted_page_with_lsn(4, 21);
        let page1 = formatted_page_with_lsn(5, 34);
        let (file, runtime) = create_disk_with_pages(&[page0, page1]);
        let cache = PageCache::new(runtime, 2).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[PAGE_SIZE - 1] = 10;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.write(None).unwrap().page_mut()[PAGE_SIZE - 1] = 20;
        }

        cache.flush_all().unwrap();

        let flushed_page0 = read_disk_page(file.path(), 0);
        let flushed_page1 = read_disk_page(file.path(), 1);
        assert_eq!(flushed_page0[PAGE_SIZE - 1], 10);
        assert_eq!(flushed_page1[PAGE_SIZE - 1], 20);
        for frame in &cache.inner.frames {
            assert!(!frame.dirty.load(Ordering::Acquire));
        }
    }

    #[test]
    fn transactional_page_flush_makes_eager_wal_durable_before_write() {
        let page = formatted_page_with_lsn(15, ZERO_LSN);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }

        cache.flush_page(0).unwrap();

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 177);
        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
            ]
        );
    }

    #[test]
    fn transactional_page_flush_preserves_each_eager_page_update() {
        let page = formatted_page_with_lsn(15, ZERO_LSN);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }
        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 222;
        }

        cache.flush_page(0).unwrap();

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 222);
        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
            ]
        );
    }

    #[test]
    fn wal_flush_failure_prevents_transactional_page_write_and_leaves_frame_dirty() {
        let page = formatted_page_with_lsn(15, ZERO_LSN);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }
        runtime.fail_next_wal_flush_for_test().unwrap();

        let result = cache.flush_page(0);

        assert!(matches!(result, Err(PageCacheError::Storage(_))));
        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[PAGE_SIZE - 1], page[PAGE_SIZE - 1]);
        assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
    }

    #[test]
    fn rollback_without_forced_wal_flush_restores_page_and_logs_transaction_outcome() {
        let page = formatted_page_with_lsn(15, ZERO_LSN);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }

        let rollback = runtime.prepare_rollback_pages(txn_id).unwrap();
        cache.restore_rollback_pages(rollback.pages).unwrap();
        cache.flush_all().unwrap();
        runtime.sync_database_file().unwrap();
        runtime.finish_rollback(txn_id).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), page);
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    #[test]
    fn rollback_after_steal_flush_restores_page_and_writes_rollback_record() {
        let page = formatted_page_with_lsn(15, ZERO_LSN);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }
        cache.flush_page(0).unwrap();

        let rollback = runtime.prepare_rollback_pages(txn_id).unwrap();
        cache.restore_rollback_pages(rollback.pages).unwrap();
        cache.flush_all().unwrap();
        runtime.sync_database_file().unwrap();
        runtime.finish_rollback(txn_id).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), page);
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    #[test]
    fn prepared_rollback_keeps_eager_wal_available_for_cache_eviction() {
        let pages = [
            formatted_page_with_lsn(10, ZERO_LSN),
            formatted_page_with_lsn(20, ZERO_LSN),
            formatted_page_with_lsn(30, ZERO_LSN),
        ];
        let (file, runtime) = create_disk_with_pages(&pages);
        let cache = PageCache::new(Arc::clone(&runtime), 2).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 100;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.write(Some(txn_id)).unwrap().page_mut()[PAGE_SIZE - 1] = 110;
        }

        let rollback = runtime.prepare_rollback_pages(txn_id).unwrap();
        {
            let _guard = cache.fetch_page(2).unwrap();
        }
        cache.restore_rollback_pages(rollback.pages).unwrap();
        cache.flush_all().unwrap();
        runtime.sync_database_file().unwrap();
        runtime.finish_rollback(txn_id).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), pages[0]);
        assert_eq!(read_disk_page(file.path(), 1), pages[1]);
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 0 }),
                (txn_id, OwnedLogRecordKind::PageUpdate { page_id: 1 }),
                (txn_id, OwnedLogRecordKind::Rollback),
            ]
        );
    }

    #[test]
    fn wal_logging_failure_restores_page_bytes_and_dirty_state() {
        let page = formatted_page_with_lsn(17, ZERO_LSN);
        let (_file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        runtime.force_next_lsn_exhausted_for_test().unwrap();
        let guard = cache.fetch_page(0).unwrap();

        {
            let mut write = guard.write(Some(txn_id)).unwrap();
            write.page_mut()[PAGE_SIZE - 1] = 88;
        }

        assert_eq!(guard.read().unwrap().page(), &page);
        assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
        assert!(runtime.commit_transaction(txn_id).is_err());
    }

    #[test]
    fn page_write_failure_after_wal_flush_leaves_frame_dirty() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.frames[0].set_page_id(Some(99));
        *cache.inner.frames[0].data.write().unwrap() = page_with_pattern(15);
        cache.inner.frames[0].dirty.store(true, Ordering::Release);
        cache.inner.frames[0].pin_count.store(0, Ordering::Release);
        cache.inner.lock_meta().unwrap().page_table.insert(99, 0);

        let result = cache.flush_page(99);

        assert!(matches!(
            result,
            Err(PageCacheError::Storage(error))
                if matches!(
                    *error,
                    StorageError::InvalidArgument(
                        crate::core::error::InvalidArgumentError::InvalidPageId { page_id: 99 }
                    )
                )
        ));
        assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
    }

    #[test]
    fn flush_page_returns_error_if_page_is_pinned() {
        let pages = [page_with_pattern(8)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        guard.write(None).unwrap().page_mut()[0] = 99;

        let result = cache.flush_page(0);
        assert!(matches!(result, Err(PageCacheError::PinnedPage { page_id: 0 })));
    }

    #[test]
    fn flush_page_is_noop_for_nonresident_page() {
        let pages = [page_with_pattern(1), page_with_pattern(2)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }

        assert!(cache.flush_page(1).is_ok());
    }

    #[test]
    fn flush_all_writes_all_dirty_unpinned_pages() {
        let pages = [page_with_pattern(4), page_with_pattern(5)];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write(None).unwrap().page_mut()[0] = 10;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.write(None).unwrap().page_mut()[0] = 20;
        }

        cache.flush_all().unwrap();

        for frame in &cache.inner.frames {
            assert!(!frame.dirty.load(Ordering::Acquire));
        }

        let page0 = read_disk_page(file.path(), 0);
        let page1 = read_disk_page(file.path(), 1);
        assert_eq!(page0[0], 10);
        assert_eq!(page1[0], 20);
    }

    #[test]
    fn flush_all_returns_error_if_dirty_page_is_pinned() {
        let pages = [page_with_pattern(19)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        guard.write(None).unwrap().page_mut()[0] = 99;

        let result = cache.flush_all();
        assert!(matches!(result, Err(PageCacheError::PinnedPage { page_id: 0 })));
    }

    #[test]
    fn drop_does_not_flush_dirty_unpinned_pages() {
        let page = page_with_pattern(33);
        let pages = [page];
        let (file, disk_manager) = create_disk_with_pages(&pages);

        {
            let cache = PageCache::new(disk_manager, 1).unwrap();
            {
                let guard = cache.fetch_page(0).unwrap();
                guard.write(None).unwrap().page_mut()[0] = 144;
            }
            assert!(cache.inner.frames[0].dirty.load(Ordering::Acquire));
        }

        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[0], page[0]);
    }

    #[test]
    fn new_page_returns_pinned_zero_initialized_page() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (page_id, guard) = cache.new_page(None).unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(guard.read().unwrap().page(), &[0u8; PAGE_SIZE]);
    }

    #[test]
    fn new_page_allocates_sequential_ids() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (first_page_id, first_guard) = cache.new_page(None).unwrap();
        assert_eq!(first_page_id, 0);
        drop(first_guard);

        let (second_page_id, second_guard) = cache.new_page(None).unwrap();
        assert_eq!(second_page_id, 1);
        drop(second_guard);
    }

    #[test]
    fn new_page_without_active_transaction_does_not_write_wal() {
        let file = NamedTempFile::new().unwrap();
        let runtime = runtime_for_path(file.path());
        let cache = PageCache::new(runtime, 1).unwrap();

        let (page_id, guard) = cache.new_page(None).unwrap();
        drop(guard);

        assert_eq!(page_id, 0);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn new_page_with_active_transaction_writes_page_alloc_wal_record() {
        let file = NamedTempFile::new().unwrap();
        let runtime = runtime_for_path(file.path());
        let cache = PageCache::new(Arc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        let (page_id, guard) = cache.new_page(Some(txn_id)).unwrap();
        drop(guard);
        runtime.commit_transaction(txn_id).unwrap();

        assert_eq!(page_id, 0);
        assert_eq!(
            read_log_record_kinds_for_test(file.path()),
            [
                (txn_id, OwnedLogRecordKind::Begin),
                (txn_id, OwnedLogRecordKind::PageAlloc { page_id }),
                (txn_id, OwnedLogRecordKind::Commit),
            ]
        );
    }

    #[test]
    fn new_page_returns_error_when_all_frames_are_pinned() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.frames[0].pin_count.store(1, Ordering::Release);

        let result = cache.new_page(None);
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));

        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        let read_result = disk_manager.read_page(0, &mut page);
        assert!(matches!(read_result, Err(DiskManagerError::InvalidPageId { page_id: 0 })));
    }

    #[test]
    fn new_page_changes_are_durable_after_flush_and_reopen() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());

        let page_id = {
            let cache = PageCache::new(disk_manager, 1).unwrap();
            let (page_id, guard) = cache.new_page(None).unwrap();
            let mut page = guard.write(None).unwrap();
            page.page_mut()[0] = 61;
            page.page_mut()[PAGE_SIZE - 1] = 142;
            drop(page);
            drop(guard);
            cache.flush_page(page_id).unwrap();
            page_id
        };

        let mut reopened_disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        reopened_disk_manager.read_page(page_id, &mut page).unwrap();

        assert_eq!(page[0], 61);
        assert_eq!(page[PAGE_SIZE - 1], 142);
    }

    #[test]
    fn fetch_page_returns_error_for_corrupt_page_table_entry() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.lock_meta().unwrap().page_table.insert(7, 99);

        let result = cache.fetch_page(7);
        assert!(matches!(
            result,
            Err(PageCacheError::CorruptPageTableEntry { page_id: 7, frame_id: 99, frame_count: 1 })
        ));
    }

    #[test]
    fn flush_page_returns_error_for_corrupt_page_table_entry() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.lock_meta().unwrap().page_table.insert(8, 100);

        let result = cache.flush_page(8);
        assert!(matches!(
            result,
            Err(PageCacheError::CorruptPageTableEntry {
                page_id: 8,
                frame_id: 100,
                frame_count: 1
            })
        ));
    }
}

#[cfg(all(test, loom))]
#[allow(clippy::panic, clippy::unwrap_used)]
mod loom_tests {
    use loom::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as LoomOrdering},
    };
    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        loom_support::{check_model, thread},
        storage::{disk_manager::DiskManager, storage_runtime::StorageRuntime},
    };

    fn cache_with_pages(patterns: &[u8], frame_count: usize) -> (NamedTempFile, PageCache) {
        let file = NamedTempFile::new().unwrap();
        let mut disk = DiskManager::new(file.path()).unwrap();
        for (page_id, pattern) in patterns.iter().copied().enumerate() {
            assert_eq!(disk.new_page().unwrap(), page_id as PageId);
            disk.write_page(page_id as PageId, &[pattern; PAGE_SIZE]).unwrap();
        }
        let runtime = Arc::new(StorageRuntime::new(file.path().to_path_buf(), disk).unwrap());
        (file, PageCache::new(runtime, frame_count).unwrap())
    }

    fn assert_guard_matches_frame(guard: &PinGuard, pattern: u8) {
        let _meta = guard.page_cache.lock_meta().unwrap();
        let frame = &guard.page_cache.frames[guard.frame_id];
        assert_eq!(frame.page_id(), Some(guard.page_id));
        assert!(frame.pin_count.load(Ordering::Acquire) > 0);
        assert_eq!(guard.read().unwrap().page()[0], pattern);
    }

    fn assert_cache_consistent(cache: &PageCache) {
        let meta = cache.inner.lock_meta().unwrap();
        for (page_id, frame_id) in &meta.page_table {
            assert!(*frame_id < cache.inner.frames.len());
            assert_eq!(cache.inner.frames[*frame_id].page_id(), Some(*page_id));
        }
        for (frame_id, frame) in cache.inner.frames.iter().enumerate() {
            assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
            if let Some(page_id) = frame.page_id() {
                assert_eq!(meta.page_table.get(&page_id), Some(&frame_id));
            }
        }
    }

    #[test]
    fn concurrent_hits_keep_each_live_pin_counted() {
        check_model(|| {
            let (_file, cache) = cache_with_pages(&[17], 1);
            drop(cache.fetch_page(0).unwrap());

            let first_cache = cache.clone();
            let first = thread::spawn(move || {
                let guard = first_cache.fetch_page(0).unwrap();
                thread::yield_now();
                assert_guard_matches_frame(&guard, 17);
                drop(guard);
            });
            let second_cache = cache.clone();
            let second = thread::spawn(move || {
                let guard = second_cache.fetch_page(0).unwrap();
                thread::yield_now();
                assert_guard_matches_frame(&guard, 17);
                drop(guard);
            });

            first.join().unwrap();
            second.join().unwrap();
            assert_cache_consistent(&cache);
        });
    }

    #[test]
    fn concurrent_misses_for_one_page_install_one_resident_frame() {
        check_model(|| {
            let (_file, cache) = cache_with_pages(&[29], 2);

            let first_cache = cache.clone();
            let first = thread::spawn(move || {
                let guard = first_cache.fetch_page(0).unwrap();
                thread::yield_now();
                assert_guard_matches_frame(&guard, 29);
            });
            let second_cache = cache.clone();
            let second = thread::spawn(move || {
                let guard = second_cache.fetch_page(0).unwrap();
                thread::yield_now();
                assert_guard_matches_frame(&guard, 29);
            });

            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(
                cache.inner.frames.iter().filter(|frame| frame.page_id() == Some(0)).count(),
                1
            );
            assert_cache_consistent(&cache);
        });
    }

    #[test]
    fn cache_hit_racing_with_eviction_never_retargets_a_live_guard() {
        check_model(|| {
            let (_file, cache) = cache_with_pages(&[41, 73], 1);
            drop(cache.fetch_page(0).unwrap());
            let successes = Arc::new(AtomicUsize::new(0));

            let first_cache = cache.clone();
            let first_successes = Arc::clone(&successes);
            let first = thread::spawn(move || {
                if let Ok(guard) = first_cache.fetch_page(0) {
                    first_successes.fetch_add(1, LoomOrdering::Relaxed);
                    thread::yield_now();
                    assert_guard_matches_frame(&guard, 41);
                }
            });
            let second_cache = cache.clone();
            let second_successes = Arc::clone(&successes);
            let second = thread::spawn(move || {
                if let Ok(guard) = second_cache.fetch_page(1) {
                    second_successes.fetch_add(1, LoomOrdering::Relaxed);
                    thread::yield_now();
                    assert_guard_matches_frame(&guard, 73);
                }
            });

            first.join().unwrap();
            second.join().unwrap();
            assert!(successes.load(LoomOrdering::Relaxed) >= 1);
            assert_cache_consistent(&cache);
        });
    }

    #[test]
    fn flush_racing_with_write_does_not_lose_the_dirty_generation() {
        check_model(|| {
            let (file, cache) = cache_with_pages(&[113], 1);
            drop(cache.fetch_page(0).unwrap());

            let writer_cache = cache.clone();
            let writer = thread::spawn(move || {
                let guard = writer_cache.fetch_page(0).unwrap();
                let mut page = guard.write(None).unwrap();
                page.page_mut()[0] = 127;
                thread::yield_now();
            });
            let flush_cache = cache.clone();
            let flush = thread::spawn(move || flush_cache.flush_all());

            writer.join().unwrap();
            let flush_result = flush.join().unwrap();
            assert!(
                flush_result.is_ok()
                    || matches!(flush_result, Err(PageCacheError::PinnedPage { page_id: 0 }))
            );

            cache.flush_all().unwrap();
            let mut disk = DiskManager::new(file.path()).unwrap();
            let mut page = [0; PAGE_SIZE];
            disk.read_page(0, &mut page).unwrap();
            assert_eq!(page[0], 127);
            assert!(!cache.inner.frames[0].dirty.load(Ordering::Acquire));
        });
    }

    #[test]
    fn concurrent_misses_do_not_share_one_victim_frame() {
        check_model(|| {
            let (_file, cache) = cache_with_pages(&[89, 101], 1);
            let successes = Arc::new(AtomicUsize::new(0));

            let first_cache = cache.clone();
            let first_successes = Arc::clone(&successes);
            let first = thread::spawn(move || {
                if let Ok(guard) = first_cache.fetch_page(0) {
                    first_successes.fetch_add(1, LoomOrdering::Relaxed);
                    thread::yield_now();
                    assert_guard_matches_frame(&guard, 89);
                }
            });
            let second_cache = cache.clone();
            let second_successes = Arc::clone(&successes);
            let second = thread::spawn(move || {
                if let Ok(guard) = second_cache.fetch_page(1) {
                    second_successes.fetch_add(1, LoomOrdering::Relaxed);
                    thread::yield_now();
                    assert_guard_matches_frame(&guard, 101);
                }
            });

            first.join().unwrap();
            second.join().unwrap();
            assert!(successes.load(LoomOrdering::Relaxed) >= 1);
            assert_cache_consistent(&cache);
        });
    }
}
