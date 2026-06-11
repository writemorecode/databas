//! Single-threaded page cache with explicit pin and page-access guards.
//!
//! [`PageCache`] is a cheap-to-clone handle that shares cache state through
//! [`Rc`]. The cache is intentionally single-threaded today and uses interior
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
    cell::{Cell, Ref, RefCell, RefMut},
    collections::HashMap,
    rc::Rc,
};

use crate::core::{
    error::{PageCacheError, PageCacheResult},
    log_manager::{Lsn, ZERO_LSN},
    page::{NodeMarker, Page, PageResult, Read, Write},
    page_replacement::ClockPolicy,
    storage_runtime::StorageRuntime,
    transaction_manager::{PageUndo, page_lsn},
    {PAGE_SIZE, PageId},
};

pub(crate) type FrameId = usize;

#[derive(Debug)]
struct Frame {
    page_id: Cell<Option<PageId>>,
    data: RefCell<[u8; PAGE_SIZE]>,
    dirty: Cell<bool>,
    lsn: Cell<Lsn>,
    pin_count: Cell<u32>,
}

impl Frame {
    /// Creates an empty frame with zeroed page data and cleared metadata bits.
    fn empty() -> Self {
        Self {
            page_id: Cell::new(None),
            data: RefCell::new([0u8; PAGE_SIZE]),
            dirty: Cell::new(false),
            lsn: Cell::new(ZERO_LSN),
            pin_count: Cell::new(0),
        }
    }
}

struct CacheMeta {
    page_table: HashMap<PageId, FrameId>,
    replacement: ClockPolicy,
}

struct PageCacheInner {
    runtime: Rc<StorageRuntime>,
    meta: RefCell<CacheMeta>,
    frames: Vec<Frame>,
}

/// Shared handle to the single-threaded page cache.
///
/// Cloning the handle shares the same cache state through [`Rc`]. The handle
/// itself does not represent a pin or a page borrow; it only provides access to
/// cache operations. Use [`PinGuard`] to keep pages resident and use
/// [`PageReadGuard`] or [`PageWriteGuard`] for temporary access to the page
/// bytes.
pub(crate) struct PageCache {
    inner: Rc<PageCacheInner>,
}

impl Clone for PageCache {
    fn clone(&self) -> Self {
        Self { inner: Rc::clone(&self.inner) }
    }
}

impl PageCache {
    /// Creates a new page cache with a fixed number of preallocated frames.
    ///
    /// Returns an error when `frame_count` is zero.
    pub(crate) fn new(runtime: Rc<StorageRuntime>, frame_count: usize) -> PageCacheResult<Self> {
        if frame_count == 0 {
            return Err(PageCacheError::InvalidFrameCount { frame_count });
        }

        let mut frames = Vec::new();
        frames
            .try_reserve_exact(frame_count)
            .map_err(|source| PageCacheError::FrameAllocationFailed { frame_count, source })?;
        frames.extend((0..frame_count).map(|_| Frame::empty()));

        Ok(Self {
            inner: Rc::new(PageCacheInner {
                runtime,
                meta: RefCell::new(CacheMeta {
                    page_table: HashMap::new(),
                    replacement: ClockPolicy::new(frame_count),
                }),
                frames,
            }),
        })
    }

    /// Fetches an existing page into the cache and returns a pin guard.
    ///
    /// Cache hits update replacement state and increment pin count.
    /// Cache misses use CLOCK replacement and may evict a dirty page.
    pub(crate) fn fetch_page(&self, page_id: PageId) -> PageCacheResult<PinGuard> {
        if let Some(frame_id) = self.resident_frame_id(page_id)? {
            let frame = &self.inner.frames[frame_id];
            frame.pin_count.set(frame.pin_count.get().checked_add(1).expect("pin count overflow"));
            self.inner.meta.borrow_mut().replacement.record_access(frame_id);
            return Ok(PinGuard::new(Rc::clone(&self.inner), frame_id, page_id));
        }

        let frame_id = self.select_victim_frame().ok_or(PageCacheError::NoEvictableFrame)?;
        self.replace_frame(frame_id, page_id)?;
        Ok(PinGuard::new(Rc::clone(&self.inner), frame_id, page_id))
    }

    /// Allocates a new on-disk page and returns it pinned in the cache.
    ///
    /// A victim frame is selected before allocation so a full pinned cache
    /// returns `NoEvictableFrame` without growing the file.
    pub(crate) fn new_page(&self) -> PageCacheResult<(PageId, PinGuard)> {
        let frame_id = self.select_victim_frame().ok_or(PageCacheError::NoEvictableFrame)?;
        let page_id = self.inner.runtime.new_page()?;
        if let Err(err) = self.inner.runtime.record_page_alloc(page_id) {
            return Err(PageCacheError::Transaction(Box::new(err)));
        }
        self.replace_frame(frame_id, page_id)?;
        Ok((page_id, PinGuard::new(Rc::clone(&self.inner), frame_id, page_id)))
    }

    /// Flushes one resident page if dirty.
    ///
    /// Non-resident pages are a no-op. Pinned pages return `PinnedPage`.
    #[cfg(test)]
    pub(crate) fn flush_page(&self, page_id: PageId) -> PageCacheResult<()> {
        let Some(frame_id) = self.resident_frame_id(page_id)? else {
            return Ok(());
        };

        let frame = &self.inner.frames[frame_id];
        if frame.pin_count.get() > 0 {
            return Err(PageCacheError::PinnedPage { page_id });
        }

        self.flush_frame_if_dirty(frame_id)
    }

    /// Flushes all dirty pages that are currently unpinned.
    ///
    /// Returns `PinnedPage` if a dirty page is pinned.
    pub(crate) fn flush_all(&self) -> PageCacheResult<()> {
        for (frame_id, frame) in self.inner.frames.iter().enumerate() {
            let page_id = frame.page_id.get();
            let pin_count = frame.pin_count.get();
            let dirty = frame.dirty.get();

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

    fn resident_frame_id(&self, page_id: PageId) -> PageCacheResult<Option<FrameId>> {
        let meta = self.inner.meta.borrow();
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

    fn select_victim_frame(&self) -> Option<FrameId> {
        let frames = &self.inner.frames;
        self.inner
            .meta
            .borrow_mut()
            .replacement
            .select_victim(|frame_id| frames[frame_id].pin_count.get() > 0)
    }

    /// Replaces frame contents with `new_page_id`, flushing old dirty data first.
    fn replace_frame(&self, frame_id: FrameId, new_page_id: PageId) -> PageCacheResult<()> {
        self.flush_frame_if_dirty(frame_id)?;

        let frame = &self.inner.frames[frame_id];
        let old_page_id = frame.page_id.get();

        let mut data = [0u8; PAGE_SIZE];
        self.inner.runtime.read_page(new_page_id, &mut data)?;

        {
            let mut frame_data = frame.data.try_borrow_mut().map_err(|_| {
                PageCacheError::PageMutableBorrowConflict {
                    page_id: old_page_id.unwrap_or(new_page_id),
                }
            })?;
            *frame_data = data;
        }

        frame.page_id.set(Some(new_page_id));
        frame.dirty.set(false);
        frame.lsn.set(page_lsn(&data));
        frame.pin_count.set(1);

        let mut meta = self.inner.meta.borrow_mut();
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
        if !frame.dirty.get() {
            return Ok(());
        }

        let Some(page_id) = frame.page_id.get() else {
            return Ok(());
        };

        let page = frame
            .data
            .try_borrow()
            .map_err(|_| PageCacheError::PageImmutableBorrowConflict { page_id })?;
        let flush_lsn = frame.lsn.get().max(page_lsn(&page));
        self.inner.runtime.flush_wal_through(flush_lsn)?;
        self.inner.runtime.write_page(page_id, &page)?;
        frame.dirty.set(false);
        Ok(())
    }

    pub(crate) fn restore_rollback_pages(&self, undo_pages: Vec<PageUndo>) -> PageCacheResult<()> {
        for undo in undo_pages {
            let pin = self.fetch_page(undo.page_id)?;
            let frame = &self.inner.frames[pin.frame_id];
            {
                let mut data = frame.data.try_borrow_mut().map_err(|_| {
                    PageCacheError::PageMutableBorrowConflict { page_id: undo.page_id }
                })?;
                *data = undo.before;
            }
            frame.dirty.set(true);
            frame.lsn.set(frame.lsn.get().max(undo.lsn));
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
    page_cache: Rc<PageCacheInner>,
    frame_id: FrameId,
    page_id: PageId,
}

impl PinGuard {
    /// Creates a new pin guard for a specific frame.
    fn new(page_cache: Rc<PageCacheInner>, frame_id: FrameId, page_id: PageId) -> Self {
        Self { page_cache, frame_id, page_id }
    }

    /// Returns the page ID associated with this pin.
    #[cfg(test)]
    pub(crate) fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Borrows the pinned page immutably.
    ///
    /// Multiple read guards may coexist for the same page, but immutable access
    /// fails while a write guard is active.
    pub(crate) fn read(&self) -> PageCacheResult<PageReadGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = frame
            .data
            .try_borrow()
            .map_err(|_| PageCacheError::PageImmutableBorrowConflict { page_id: self.page_id })?;
        Ok(PageReadGuard { page })
    }

    /// Borrows the pinned page mutably and marks it dirty immediately.
    ///
    /// Mutable access fails while any read or write guard is active for the
    /// same frame. Acquiring a write guard marks the frame dirty even if the
    /// caller later decides not to mutate the page bytes.
    pub(crate) fn write(&self) -> PageCacheResult<PageWriteGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = frame
            .data
            .try_borrow_mut()
            .map_err(|_| PageCacheError::PageMutableBorrowConflict { page_id: self.page_id })?;
        let before = *page;
        let was_dirty = frame.dirty.get();
        frame.dirty.set(true);
        Ok(PageWriteGuard {
            page,
            before,
            was_dirty,
            runtime: Rc::clone(&self.page_cache.runtime),
            frame,
            page_id: self.page_id,
        })
    }
}

impl Drop for PinGuard {
    /// Decrements the frame pin count when the guard leaves scope.
    fn drop(&mut self) {
        let frame = &self.page_cache.frames[self.frame_id];
        debug_assert!(frame.pin_count.get() > 0, "pin count underflow");
        if frame.pin_count.get() > 0 {
            frame.pin_count.set(frame.pin_count.get() - 1);
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
    page: Ref<'a, [u8; PAGE_SIZE]>,
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
    page: RefMut<'a, [u8; PAGE_SIZE]>,
    before: [u8; PAGE_SIZE],
    was_dirty: bool,
    runtime: Rc<StorageRuntime>,
    frame: &'a Frame,
    page_id: PageId,
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

    /// Opens a typed immutable view over the page bytes.
    pub(crate) fn open<N>(&self) -> PageResult<Page<Read<'_>, N>>
    where
        N: NodeMarker,
    {
        Page::<Read<'_>, N>::open(self.page())
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
            return;
        }

        match self.runtime.record_page_update(self.page_id, &self.before, &self.page) {
            Ok(Some(update)) => {
                *self.page = update.redo;
                self.frame.lsn.set(update.lsn);
            }
            Ok(None) => {
                self.frame.lsn.set(self.frame.lsn.get().max(page_lsn(&self.page)));
            }
            Err(_) => {
                *self.page = self.before;
                self.frame.dirty.set(self.was_dirty);
                self.runtime.record_transaction_failure();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, rc::Rc};

    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::disk_manager::DiskManager;
    use crate::core::log_manager::{
        LogManagerFlushError, LogRecord, LogRecordKind, OwnedLogRecordKind,
        read_log_record_kinds_for_test,
    };
    use crate::core::page;
    use crate::core::page::format::PageKind;
    use crate::core::page::{Leaf, Page, Write};
    use crate::core::storage_runtime::StorageRuntime;

    /// Generates a deterministic page payload from a seed byte.
    fn page_with_pattern(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        for (index, byte) in page.iter_mut().enumerate() {
            *byte = seed.wrapping_add(index as u8);
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
    fn runtime_for_disk(path: &Path, disk_manager: DiskManager) -> Rc<StorageRuntime> {
        Rc::new(StorageRuntime::new(path.to_path_buf(), disk_manager).unwrap())
    }

    fn runtime_for_path(path: &Path) -> Rc<StorageRuntime> {
        let disk_manager = DiskManager::new(path).unwrap();
        runtime_for_disk(path, disk_manager)
    }

    /// Creates a temporary database file and writes the provided pages to it.
    fn create_disk_with_pages(pages: &[[u8; PAGE_SIZE]]) -> (NamedTempFile, Rc<StorageRuntime>) {
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

    fn append_wal_through(runtime: &StorageRuntime, target_lsn: Lsn) {
        for txn_id in 1..=target_lsn {
            runtime
                .append_log_transaction(txn_id, &[LogRecord { txn_id, kind: LogRecordKind::Begin }])
                .unwrap();
        }
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
            assert_eq!(frame.page_id.get(), None);
            assert!(!frame.dirty.get());
            assert_eq!(frame.pin_count.get(), 0);
            assert_eq!(*frame.data.borrow(), [0u8; PAGE_SIZE]);
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

        assert_eq!(cache.inner.frames[0].page_id.get(), Some(0));
        assert_eq!(cache.inner.frames[0].pin_count.get(), 0);
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

        assert_eq!(cache.inner.frames[0].pin_count.get(), 0);
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
        assert_eq!(cache.inner.frames[0].pin_count.get(), 1);
        assert_eq!(cache.inner.frames[1].pin_count.get(), 1);
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

        let mut write0 = guard0.write().unwrap();
        let mut write1 = guard1.write().unwrap();

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

        let (_page_id, guard) = cache.new_page().unwrap();

        {
            let mut write = guard.write().unwrap();
            let _ = Page::<Write<'_>, Leaf>::init(write.page_mut());

            assert_eq!(write.open::<Leaf>().unwrap().kind(), PageKind::RawLeaf);
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
        assert!(!cache.inner.frames[0].dirty.get());

        {
            let guard = cache.fetch_page(0).unwrap();
            let mut page = guard.write().unwrap();
            page.page_mut()[0] = 99;
        }

        assert!(cache.inner.frames[0].dirty.get());
    }

    #[test]
    fn read_returns_error_while_write_guard_is_active() {
        let page = page_with_pattern(14);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let _write = guard.write().unwrap();

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

        let result = guard.write();
        assert!(matches!(result, Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })));
    }

    #[test]
    fn write_returns_error_while_write_guard_is_active() {
        let page = page_with_pattern(16);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        let _first_write = guard.write().unwrap();

        let result = guard.write();
        assert!(matches!(result, Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })));
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
            guard.write().unwrap().page_mut()[0] = 222;
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

        let page_table = &cache.inner.meta.borrow().page_table;
        assert!(!page_table.contains_key(&0));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn eviction_skips_pinned_frames() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        cache.inner.frames[0].page_id.set(Some(0));
        *cache.inner.frames[0].data.borrow_mut() = pages[0];
        cache.inner.frames[0].dirty.set(false);
        cache.inner.frames[0].pin_count.set(1);

        cache.inner.frames[1].page_id.set(Some(1));
        *cache.inner.frames[1].data.borrow_mut() = pages[1];
        cache.inner.frames[1].dirty.set(false);
        cache.inner.frames[1].pin_count.set(0);

        let mut meta = cache.inner.meta.borrow_mut();
        meta.page_table.insert(0, 0);
        meta.page_table.insert(1, 1);
        drop(meta);

        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        assert_eq!(cache.inner.frames[0].page_id.get(), Some(0));
        let page_table = &cache.inner.meta.borrow().page_table;
        assert!(page_table.contains_key(&0));
        assert!(!page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn fetch_returns_error_when_all_frames_are_pinned() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        cache.inner.frames[0].page_id.set(Some(0));
        *cache.inner.frames[0].data.borrow_mut() = pages[0];
        cache.inner.frames[0].dirty.set(false);
        cache.inner.frames[0].pin_count.set(1);

        cache.inner.frames[1].page_id.set(Some(1));
        *cache.inner.frames[1].data.borrow_mut() = pages[1];
        cache.inner.frames[1].dirty.set(false);
        cache.inner.frames[1].pin_count.set(1);

        let mut meta = cache.inner.meta.borrow_mut();
        meta.page_table.insert(0, 0);
        meta.page_table.insert(1, 1);
        drop(meta);

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
            guard.write().unwrap().page_mut()[0] = 177;
        }
        assert!(cache.inner.frames[0].dirty.get());

        cache.flush_page(0).unwrap();

        assert!(!cache.inner.frames[0].dirty.get());
        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[0], 177);
    }

    #[test]
    fn flush_page_forces_wal_through_page_lsn_before_write() {
        let page = formatted_page_with_lsn(15, 7);
        let (file, runtime) = create_disk_with_pages(&[page]);
        append_wal_through(&runtime, 7);
        let cache = PageCache::new(runtime, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write().unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }

        cache.flush_page(0).unwrap();

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 177);
        assert!(!cache.inner.frames[0].dirty.get());
    }

    #[test]
    fn dirty_page_eviction_forces_wal_through_page_lsn_before_write() {
        let page0 = formatted_page_with_lsn(1, 13);
        let page1 = page_with_pattern(2);
        let (file, runtime) = create_disk_with_pages(&[page0, page1]);
        append_wal_through(&runtime, 13);
        let cache = PageCache::new(runtime, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write().unwrap().page_mut()[PAGE_SIZE - 1] = 222;
        }

        {
            let _guard = cache.fetch_page(1).unwrap();
        }

        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[PAGE_SIZE - 1], 222);
    }

    #[test]
    fn flush_all_forces_wal_through_each_dirty_page_lsn_before_each_write() {
        let page0 = formatted_page_with_lsn(4, 21);
        let page1 = formatted_page_with_lsn(5, 34);
        let (file, runtime) = create_disk_with_pages(&[page0, page1]);
        append_wal_through(&runtime, 34);
        let cache = PageCache::new(runtime, 2).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write().unwrap().page_mut()[PAGE_SIZE - 1] = 10;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.write().unwrap().page_mut()[PAGE_SIZE - 1] = 20;
        }

        cache.flush_all().unwrap();

        let flushed_page0 = read_disk_page(file.path(), 0);
        let flushed_page1 = read_disk_page(file.path(), 1);
        assert_eq!(flushed_page0[PAGE_SIZE - 1], 10);
        assert_eq!(flushed_page1[PAGE_SIZE - 1], 20);
        for frame in &cache.inner.frames {
            assert!(!frame.dirty.get());
        }
    }

    #[test]
    fn wal_flush_failure_prevents_page_write_and_leaves_frame_dirty() {
        let page = formatted_page_with_lsn(15, 55);
        let (file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(runtime, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.write().unwrap().page_mut()[PAGE_SIZE - 1] = 177;
        }

        let result = cache.flush_page(0);

        assert!(matches!(
            result,
            Err(PageCacheError::WalFlush(LogManagerFlushError::LsnNotAppended {
                requested_lsn: 55,
                highest_appended_lsn: None,
            }))
        ));
        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[PAGE_SIZE - 1], page[PAGE_SIZE - 1]);
        assert!(cache.inner.frames[0].dirty.get());
    }

    #[test]
    fn wal_logging_failure_restores_page_bytes_and_dirty_state() {
        let page = formatted_page_with_lsn(17, ZERO_LSN);
        let (_file, runtime) = create_disk_with_pages(&[page]);
        let cache = PageCache::new(Rc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        runtime.force_next_lsn_exhausted_for_test();
        let guard = cache.fetch_page(0).unwrap();

        {
            let mut write = guard.write().unwrap();
            write.page_mut()[PAGE_SIZE - 1] = 88;
        }

        assert_eq!(guard.read().unwrap().page(), &page);
        assert!(!cache.inner.frames[0].dirty.get());
        assert!(runtime.commit_transaction(txn_id).is_err());
    }

    #[test]
    fn page_write_failure_after_wal_flush_leaves_frame_dirty() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.frames[0].page_id.set(Some(99));
        *cache.inner.frames[0].data.borrow_mut() = page_with_pattern(15);
        cache.inner.frames[0].dirty.set(true);
        cache.inner.frames[0].pin_count.set(0);
        cache.inner.meta.borrow_mut().page_table.insert(99, 0);

        let result = cache.flush_page(99);

        assert!(matches!(
            result,
            Err(PageCacheError::Disk(crate::core::error::DiskManagerError::InvalidPageId {
                page_id: 99
            }))
        ));
        assert!(cache.inner.frames[0].dirty.get());
    }

    #[test]
    fn flush_page_returns_error_if_page_is_pinned() {
        let page = page_with_pattern(8);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.frames[0].page_id.set(Some(0));
        *cache.inner.frames[0].data.borrow_mut() = page;
        cache.inner.frames[0].dirty.set(true);
        cache.inner.frames[0].pin_count.set(1);
        cache.inner.meta.borrow_mut().page_table.insert(0, 0);

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
            guard.write().unwrap().page_mut()[0] = 10;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.write().unwrap().page_mut()[0] = 20;
        }

        cache.flush_all().unwrap();

        for frame in &cache.inner.frames {
            assert!(!frame.dirty.get());
        }

        let page0 = read_disk_page(file.path(), 0);
        let page1 = read_disk_page(file.path(), 1);
        assert_eq!(page0[0], 10);
        assert_eq!(page1[0], 20);
    }

    #[test]
    fn flush_all_returns_error_if_dirty_page_is_pinned() {
        let page = page_with_pattern(19);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.inner.frames[0].page_id.set(Some(0));
        *cache.inner.frames[0].data.borrow_mut() = page;
        cache.inner.frames[0].dirty.set(true);
        cache.inner.frames[0].pin_count.set(1);
        cache.inner.meta.borrow_mut().page_table.insert(0, 0);

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
                guard.write().unwrap().page_mut()[0] = 144;
            }
            assert!(cache.inner.frames[0].dirty.get());
        }

        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[0], page[0]);
    }

    #[test]
    fn new_page_returns_pinned_zero_initialized_page() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (page_id, guard) = cache.new_page().unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(guard.read().unwrap().page(), &[0u8; PAGE_SIZE]);
    }

    #[test]
    fn new_page_allocates_sequential_ids() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (first_page_id, first_guard) = cache.new_page().unwrap();
        assert_eq!(first_page_id, 0);
        drop(first_guard);

        let (second_page_id, second_guard) = cache.new_page().unwrap();
        assert_eq!(second_page_id, 1);
        drop(second_guard);
    }

    #[test]
    fn new_page_without_active_transaction_does_not_write_wal() {
        let file = NamedTempFile::new().unwrap();
        let runtime = runtime_for_path(file.path());
        let cache = PageCache::new(runtime, 1).unwrap();

        let (page_id, guard) = cache.new_page().unwrap();
        drop(guard);

        assert_eq!(page_id, 0);
        assert_eq!(read_log_record_kinds_for_test(file.path()), []);
    }

    #[test]
    fn new_page_with_active_transaction_writes_page_alloc_wal_record() {
        let file = NamedTempFile::new().unwrap();
        let runtime = runtime_for_path(file.path());
        let cache = PageCache::new(Rc::clone(&runtime), 1).unwrap();

        let txn_id = runtime.begin_transaction().unwrap();
        let (page_id, guard) = cache.new_page().unwrap();
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

        cache.inner.frames[0].pin_count.set(1);

        let result = cache.new_page();
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));

        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        let read_result = disk_manager.read_page(0, &mut page);
        assert!(matches!(
            read_result,
            Err(crate::core::error::DiskManagerError::InvalidPageId { page_id: 0 })
        ));
    }

    #[test]
    fn new_page_changes_are_durable_after_flush_and_reopen() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = runtime_for_path(file.path());

        let page_id = {
            let cache = PageCache::new(disk_manager, 1).unwrap();
            let (page_id, guard) = cache.new_page().unwrap();
            let mut page = guard.write().unwrap();
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

        cache.inner.meta.borrow_mut().page_table.insert(7, 99);

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

        cache.inner.meta.borrow_mut().page_table.insert(8, 100);

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
