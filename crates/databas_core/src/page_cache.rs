use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::HashMap,
};

use crate::{
    disk_manager::DiskManager,
    error::{PageCacheError, PageCacheResult},
    page_replacement::ClockPolicy,
    types::{PAGE_SIZE, PageId},
};

pub(crate) type FrameId = usize;

#[derive(Clone)]
struct Frame {
    page_id: Cell<Option<PageId>>,
    data: RefCell<[u8; PAGE_SIZE]>,
    dirty: Cell<bool>,
    pin_count: Cell<u32>,
}

impl Frame {
    /// Creates an empty frame with zeroed page data and cleared metadata bits.
    fn empty() -> Self {
        Self {
            page_id: Cell::new(None),
            data: RefCell::new([0u8; PAGE_SIZE]),
            dirty: Cell::new(false),
            pin_count: Cell::new(0),
        }
    }
}

pub(crate) struct PageCache {
    disk_manager: RefCell<DiskManager>,
    frames: Vec<Frame>,
    page_table: RefCell<HashMap<PageId, FrameId>>,
    replacement: RefCell<ClockPolicy>,
}

impl PageCache {
    /// Creates a new page cache with a fixed number of preallocated frames.
    ///
    /// Returns an error when `frame_count` is zero.
    pub(crate) fn new(disk_manager: DiskManager, frame_count: usize) -> PageCacheResult<Self> {
        if frame_count == 0 {
            return Err(PageCacheError::InvalidFrameCount(frame_count));
        }

        Ok(Self {
            disk_manager: RefCell::new(disk_manager),
            frames: vec![Frame::empty(); frame_count],
            page_table: RefCell::new(HashMap::new()),
            replacement: RefCell::new(ClockPolicy::new(frame_count)),
        })
    }

    /// Fetches an existing page into the cache and returns a pin guard.
    ///
    /// Cache hits update replacement state and increment pin count.
    /// Cache misses use CLOCK replacement and may evict a dirty page.
    pub(crate) fn fetch_page(&self, page_id: PageId) -> PageCacheResult<PinGuard<'_>> {
        let frame_id = { self.try_page_table()?.get(&page_id).copied() };
        if let Some(frame_id) = frame_id {
            let frame_count = self.frames.len();
            let frame = self.frames.get(frame_id).ok_or(PageCacheError::CorruptPageTableEntry {
                page_id,
                frame_id,
                frame_count,
            })?;
            let pin_count = frame.pin_count.get();
            let new_pin_count = pin_count.checked_add(1).expect("pin count overflow");
            let mut replacement = self.try_replacement_mut()?;
            frame.pin_count.set(new_pin_count);
            replacement.record_access(frame_id);
            return Ok(PinGuard::new(self, frame_id));
        }

        let frame_id = self.select_victim_frame()?.ok_or(PageCacheError::NoEvictableFrame)?;
        self.replace_frame(frame_id, page_id)?;
        Ok(PinGuard::new(self, frame_id))
    }

    /// Allocates a new on-disk page and returns it pinned in the cache.
    ///
    /// A victim frame is selected before allocation so a full pinned cache
    /// returns `NoEvictableFrame` without growing the file.
    pub(crate) fn new_page(&self) -> PageCacheResult<(PageId, PinGuard<'_>)> {
        let frame_id = self.select_victim_frame()?.ok_or(PageCacheError::NoEvictableFrame)?;
        let page_id = self.try_disk_manager_mut()?.new_page()?;
        self.replace_frame(frame_id, page_id)?;
        Ok((page_id, PinGuard::new(self, frame_id)))
    }

    /// Flushes one resident page if dirty.
    ///
    /// Non-resident pages are a no-op. Pinned pages return `PinnedPage`.
    pub(crate) fn flush_page(&self, page_id: PageId) -> PageCacheResult<()> {
        let frame_id = { self.try_page_table()?.get(&page_id).copied() };
        let Some(frame_id) = frame_id else {
            return Ok(());
        };

        let frame = self.frames.get(frame_id).ok_or(PageCacheError::CorruptPageTableEntry {
            page_id,
            frame_id,
            frame_count: self.frames.len(),
        })?;
        if frame.pin_count.get() > 0 {
            return Err(PageCacheError::PinnedPage(page_id));
        }

        self.flush_frame_if_dirty(frame_id)
    }

    /// Flushes all dirty pages that are currently unpinned.
    ///
    /// Returns `PinnedPage` if a dirty page is pinned.
    pub(crate) fn flush_all(&self) -> PageCacheResult<()> {
        for frame_id in 0..self.frames.len() {
            let frame = &self.frames[frame_id];
            if !frame.dirty.get() {
                continue;
            }

            let Some(page_id) = frame.page_id.get() else {
                continue;
            };

            if frame.pin_count.get() > 0 {
                return Err(PageCacheError::PinnedPage(page_id));
            }

            self.flush_frame_if_dirty(frame_id)?;
        }

        Ok(())
    }

    /// Returns a shared borrow of the page table.
    fn try_page_table(&self) -> PageCacheResult<Ref<'_, HashMap<PageId, FrameId>>> {
        self.page_table
            .try_borrow()
            .map_err(|_| PageCacheError::PageTableBorrowConflict { mutable: false })
    }

    /// Returns a mutable borrow of the page table.
    fn try_page_table_mut(&self) -> PageCacheResult<RefMut<'_, HashMap<PageId, FrameId>>> {
        self.page_table
            .try_borrow_mut()
            .map_err(|_| PageCacheError::PageTableBorrowConflict { mutable: true })
    }

    /// Returns a mutable borrow of the replacement policy.
    fn try_replacement_mut(&self) -> PageCacheResult<RefMut<'_, ClockPolicy>> {
        self.replacement
            .try_borrow_mut()
            .map_err(|_| PageCacheError::ReplacementBorrowConflict { mutable: true })
    }

    /// Returns a mutable borrow of the disk manager.
    fn try_disk_manager_mut(&self) -> PageCacheResult<RefMut<'_, DiskManager>> {
        self.disk_manager
            .try_borrow_mut()
            .map_err(|_| PageCacheError::DiskManagerBorrowConflict { mutable: true })
    }

    /// Returns a shared borrow of a frame's page data.
    fn try_frame_data<'a>(
        frame: &'a Frame,
        frame_id: FrameId,
    ) -> PageCacheResult<Ref<'a, [u8; PAGE_SIZE]>> {
        frame.data.try_borrow().map_err(|_| PageCacheError::PageBorrowConflict {
            page_id: frame.page_id.get(),
            frame_id,
            mutable: false,
        })
    }

    /// Returns a mutable borrow of a frame's page data.
    fn try_frame_data_mut<'a>(
        frame: &'a Frame,
        frame_id: FrameId,
    ) -> PageCacheResult<RefMut<'a, [u8; PAGE_SIZE]>> {
        frame.data.try_borrow_mut().map_err(|_| PageCacheError::PageBorrowConflict {
            page_id: frame.page_id.get(),
            frame_id,
            mutable: true,
        })
    }

    /// Selects a victim frame using CLOCK second-chance replacement.
    ///
    /// Pinned frames are skipped and referenced frames get one second chance.
    fn select_victim_frame(&self) -> PageCacheResult<Option<FrameId>> {
        let frames = &self.frames;
        Ok(self
            .try_replacement_mut()?
            .select_victim(|frame_id| frames[frame_id].pin_count.get() > 0))
    }

    /// Replaces frame contents with `new_page_id`, flushing old dirty data first.
    fn replace_frame(&self, frame_id: FrameId, new_page_id: PageId) -> PageCacheResult<()> {
        let frame = &self.frames[frame_id];
        let old_page_id = frame.page_id.get();
        let mut page_table = self.try_page_table_mut()?;
        let mut replacement = self.try_replacement_mut()?;
        let mut disk_manager = self.try_disk_manager_mut()?;
        let mut frame_data = Self::try_frame_data_mut(frame, frame_id)?;

        if let Some(old_page_id) = old_page_id
            && frame.dirty.get()
        {
            disk_manager.write_page(old_page_id, &frame_data)?;
            frame.dirty.set(false);
        }

        let mut data = [0u8; PAGE_SIZE];
        disk_manager.read_page(new_page_id, &mut data)?;

        if let Some(old_page_id) = old_page_id {
            page_table.remove(&old_page_id);
        }
        *frame_data = data;
        frame.page_id.set(Some(new_page_id));
        frame.dirty.set(false);
        frame.pin_count.set(1);
        replacement.record_insert(frame_id);
        page_table.insert(new_page_id, frame_id);
        Ok(())
    }

    /// Writes a dirty resident frame to disk and clears its dirty bit.
    fn flush_frame_if_dirty(&self, frame_id: FrameId) -> PageCacheResult<()> {
        let frame = &self.frames[frame_id];
        if !frame.dirty.get() {
            return Ok(());
        }
        let Some(page_id) = frame.page_id.get() else {
            return Ok(());
        };
        let frame_data = Self::try_frame_data(frame, frame_id)?;
        self.try_disk_manager_mut()?.write_page(page_id, &frame_data)?;
        frame.dirty.set(false);
        Ok(())
    }

    /// Attempts to flush all dirty unpinned frames and ignores write errors.
    fn flush_best_effort_on_drop(&self) {
        let Ok(mut disk_manager) = self.disk_manager.try_borrow_mut() else {
            return;
        };
        for frame in self.frames.iter() {
            if !frame.dirty.get() || frame.pin_count.get() > 0 {
                continue;
            }
            let Some(page_id) = frame.page_id.get() else {
                continue;
            };
            let Ok(frame_data) = frame.data.try_borrow() else {
                continue;
            };
            if disk_manager.write_page(page_id, &frame_data).is_ok() {
                frame.dirty.set(false);
            }
        }
    }
}

impl Drop for PageCache {
    /// Performs best-effort flushing for dirty unpinned frames.
    fn drop(&mut self) {
        self.flush_best_effort_on_drop();
    }
}

pub(crate) struct PinGuard<'a> {
    page_cache: &'a PageCache,
    frame_id: FrameId,
}

impl<'a> PinGuard<'a> {
    /// Creates a new pin guard for a specific frame.
    fn new(page_cache: &'a PageCache, frame_id: FrameId) -> Self {
        Self { page_cache, frame_id }
    }

    /// Returns an immutable reference to the pinned page bytes.
    pub(crate) fn page(&self) -> PageCacheResult<Ref<'_, [u8; PAGE_SIZE]>> {
        let frame = &self.page_cache.frames[self.frame_id];
        PageCache::try_frame_data(frame, self.frame_id)
    }

    /// Returns a mutable reference to the pinned page bytes and marks it dirty.
    pub(crate) fn page_mut(&self) -> PageCacheResult<RefMut<'_, [u8; PAGE_SIZE]>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let data = PageCache::try_frame_data_mut(frame, self.frame_id)?;
        frame.dirty.set(true);
        Ok(data)
    }

    /// Returns the pinned page id, if available.
    pub(crate) fn page_id(&self) -> Option<PageId> {
        self.page_cache.frames[self.frame_id].page_id.get()
    }

    /// Returns the frame id for this guard.
    pub(crate) fn frame_id(&self) -> FrameId {
        self.frame_id
    }
}

impl Drop for PinGuard<'_> {
    /// Decrements the frame pin count when the guard leaves scope.
    fn drop(&mut self) {
        let frame = &self.page_cache.frames[self.frame_id];
        let pin_count = frame.pin_count.get();
        debug_assert!(pin_count > 0, "pin count underflow");
        if pin_count > 0 {
            frame.pin_count.set(pin_count - 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::NamedTempFile;

    use crate::page::USABLE_SPACE_END;

    use super::*;

    /// Generates a deterministic page payload from a seed byte.
    fn page_with_pattern(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        for (index, byte) in page.iter_mut().enumerate() {
            *byte = seed.wrapping_add(index as u8);
        }
        page
    }

    /// Creates a temporary database file and writes the provided pages to it.
    fn create_disk_with_pages(pages: &[[u8; PAGE_SIZE]]) -> (NamedTempFile, DiskManager) {
        let file = NamedTempFile::new().unwrap();
        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        for page in pages {
            let page_id = disk_manager.new_page().unwrap();
            disk_manager.write_page(page_id, page).unwrap();
        }
        (file, disk_manager)
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
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let result = PageCache::new(disk_manager, 0);
        assert!(matches!(result, Err(PageCacheError::InvalidFrameCount(0))));
    }

    #[test]
    fn frames_are_preallocated_and_empty() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 3).unwrap();

        assert_eq!(cache.frames.len(), 3);
        for frame in &cache.frames {
            assert_eq!(frame.page_id.get(), None);
            assert!(!frame.dirty.get());
            assert_eq!(frame.pin_count.get(), 0);
            assert_eq!(*frame.data.try_borrow().unwrap(), [0u8; PAGE_SIZE]);
        }
    }

    #[test]
    fn fetch_page_loads_page_and_pins() {
        let page = page_with_pattern(7);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            let page_ref = guard.page().unwrap();
            assert_eq!(&*page_ref, &page);
        }

        assert_eq!(cache.frames[0].page_id.get(), Some(0));
        assert_eq!(cache.frames[0].pin_count.get(), 0);
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

        assert_eq!(cache.frames[0].pin_count.get(), 0);
    }

    #[test]
    fn page_read_does_not_mark_dirty_but_page_mut_does() {
        let page = page_with_pattern(13);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            let page_ref = guard.page().unwrap();
            assert_eq!(page_ref[0], page[0]);
        }
        assert!(!cache.frames[0].dirty.get());

        {
            let guard = cache.fetch_page(0).unwrap();
            let mut page = guard.page_mut().unwrap();
            page[0] = 99;
        }

        assert!(cache.frames[0].dirty.get());
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
            guard.page_mut().unwrap()[0] = 222;
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

        let page_table = cache.page_table.try_borrow().unwrap();
        assert!(!page_table.contains_key(&0));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn eviction_skips_pinned_frames() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        cache.frames[0] = Frame {
            page_id: Cell::new(Some(0)),
            data: RefCell::new(pages[0]),
            dirty: Cell::new(false),
            pin_count: Cell::new(1),
        };
        cache.frames[1] = Frame {
            page_id: Cell::new(Some(1)),
            data: RefCell::new(pages[1]),
            dirty: Cell::new(false),
            pin_count: Cell::new(0),
        };
        {
            let mut page_table = cache.page_table.try_borrow_mut().unwrap();
            page_table.insert(0, 0);
            page_table.insert(1, 1);
        }

        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        assert_eq!(cache.frames[0].page_id.get(), Some(0));
        let page_table = cache.page_table.try_borrow().unwrap();
        assert!(page_table.contains_key(&0));
        assert!(!page_table.contains_key(&1));
        assert!(page_table.contains_key(&2));
    }

    #[test]
    fn fetch_returns_error_when_all_frames_are_pinned() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        cache.frames[0] = Frame {
            page_id: Cell::new(Some(0)),
            data: RefCell::new(pages[0]),
            dirty: Cell::new(false),
            pin_count: Cell::new(1),
        };
        cache.frames[1] = Frame {
            page_id: Cell::new(Some(1)),
            data: RefCell::new(pages[1]),
            dirty: Cell::new(false),
            pin_count: Cell::new(1),
        };
        {
            let mut page_table = cache.page_table.try_borrow_mut().unwrap();
            page_table.insert(0, 0);
            page_table.insert(1, 1);
        }

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
            guard.page_mut().unwrap()[0] = 177;
        }
        assert!(cache.frames[0].dirty.get());

        cache.flush_page(0).unwrap();

        assert!(!cache.frames[0].dirty.get());
        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[0], 177);
    }

    #[test]
    fn flush_page_returns_error_if_page_is_pinned() {
        let page = page_with_pattern(8);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        cache.frames[0] = Frame {
            page_id: Cell::new(Some(0)),
            data: RefCell::new(page),
            dirty: Cell::new(true),
            pin_count: Cell::new(1),
        };
        cache.page_table.try_borrow_mut().unwrap().insert(0, 0);

        let result = cache.flush_page(0);
        assert!(matches!(result, Err(PageCacheError::PinnedPage(0))));
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
            guard.page_mut().unwrap()[0] = 10;
        }
        {
            let guard = cache.fetch_page(1).unwrap();
            guard.page_mut().unwrap()[0] = 20;
        }

        cache.flush_all().unwrap();

        for frame in &cache.frames {
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
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        cache.frames[0] = Frame {
            page_id: Cell::new(Some(0)),
            data: RefCell::new(page),
            dirty: Cell::new(true),
            pin_count: Cell::new(1),
        };
        cache.page_table.try_borrow_mut().unwrap().insert(0, 0);

        let result = cache.flush_all();
        assert!(matches!(result, Err(PageCacheError::PinnedPage(0))));
    }

    #[test]
    fn drop_flushes_dirty_unpinned_pages_best_effort() {
        let page = page_with_pattern(33);
        let pages = [page];
        let (file, disk_manager) = create_disk_with_pages(&pages);

        {
            let cache = PageCache::new(disk_manager, 1).unwrap();
            {
                let guard = cache.fetch_page(0).unwrap();
                guard.page_mut().unwrap()[0] = 144;
            }
            assert!(cache.frames[0].dirty.get());
        }

        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[0], 144);
    }

    #[test]
    fn new_page_returns_pinned_zero_initialized_page() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (page_id, guard) = cache.new_page().unwrap();
        assert_eq!(page_id, 0);
        let expected = [0u8; PAGE_SIZE];
        let page_ref = guard.page().unwrap();
        assert_eq!(&*page_ref, &expected);
    }

    #[test]
    fn new_page_allocates_sequential_ids() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let (first_page_id, first_guard) = cache.new_page().unwrap();
        assert_eq!(first_page_id, 0);
        drop(first_guard);

        let (second_page_id, second_guard) = cache.new_page().unwrap();
        assert_eq!(second_page_id, 1);
        drop(second_guard);
    }

    #[test]
    fn new_page_returns_error_when_all_frames_are_pinned() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        cache.frames[0] = Frame {
            page_id: Cell::new(None),
            data: RefCell::new([0u8; PAGE_SIZE]),
            dirty: Cell::new(false),
            pin_count: Cell::new(1),
        };

        let result = cache.new_page();
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));

        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        let read_result = disk_manager.read_page(0, &mut page);
        assert!(matches!(read_result, Err(crate::error::StorageError::InvalidPageId(0))));
    }

    #[test]
    fn new_page_changes_are_durable_after_flush_and_reopen() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();

        let page_id = {
            let cache = PageCache::new(disk_manager, 1).unwrap();
            let (page_id, guard) = cache.new_page().unwrap();
            {
                let mut page = guard.page_mut().unwrap();
                page[0] = 61;
                page[USABLE_SPACE_END - 1] = 142;
            }
            drop(guard);
            cache.flush_page(page_id).unwrap();
            page_id
        };

        let mut reopened_disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        reopened_disk_manager.read_page(page_id, &mut page).unwrap();

        assert_eq!(page[0], 61);
        assert_eq!(page[USABLE_SPACE_END - 1], 142);
    }

    #[test]
    fn fetch_page_returns_error_for_corrupt_page_table_entry() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.page_table.try_borrow_mut().unwrap().insert(7, 99);

        let result = cache.fetch_page(7);
        assert!(matches!(
            result,
            Err(PageCacheError::CorruptPageTableEntry { page_id: 7, frame_id: 99, frame_count: 1 })
        ));
    }

    #[test]
    fn flush_page_returns_error_for_corrupt_page_table_entry() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();

        cache.page_table.try_borrow_mut().unwrap().insert(8, 100);

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

    #[test]
    fn can_have_multiple_pinguards() {
        let pages = [page_with_pattern(7), page_with_pattern(9)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let guard1 = cache.fetch_page(0).unwrap();
        let guard2 = cache.fetch_page(1).unwrap();

        assert_eq!(guard1.page().unwrap()[0], pages[0][0]);
        assert_eq!(guard2.page().unwrap()[0], pages[1][0]);
    }

    #[test]
    fn borrow_conflict_for_same_page() {
        let pages = [page_with_pattern(42)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        let guard1 = cache.fetch_page(0).unwrap();
        let guard2 = cache.fetch_page(0).unwrap();

        let _page_mut = guard1.page_mut().unwrap();
        let shared_err = guard2.page().unwrap_err();
        assert!(matches!(
            shared_err,
            PageCacheError::PageBorrowConflict { page_id: Some(0), frame_id: 0, mutable: false }
        ));

        let mut_err = guard2.page_mut().unwrap_err();
        assert!(matches!(
            mut_err,
            PageCacheError::PageBorrowConflict { page_id: Some(0), frame_id: 0, mutable: true }
        ));
    }

    #[test]
    fn can_hold_multiple_distinct_pinned_pages() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 2).unwrap();

        let guard1 = cache.new_page();
        let guard2 = cache.new_page();

        assert!(guard1.is_ok());
        assert!(guard2.is_ok());
    }

    #[test]
    fn fetch_page_returns_error_for_page_table_borrow_conflict() {
        let pages = [page_with_pattern(23)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();
        let _page_table = cache.page_table.try_borrow_mut().unwrap();

        let result = cache.fetch_page(0);
        assert!(matches!(result, Err(PageCacheError::PageTableBorrowConflict { mutable: false })));
    }

    #[test]
    fn replace_frame_returns_error_without_side_effects_on_page_table_conflict() {
        let pages = [page_with_pattern(3), page_with_pattern(5)];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            guard.page_mut().unwrap()[0] = 99;
        }

        let page_table = cache.page_table.try_borrow_mut().unwrap();
        let result = cache.replace_frame(0, 1);
        assert!(matches!(result, Err(PageCacheError::PageTableBorrowConflict { mutable: true })));
        drop(page_table);

        assert_eq!(cache.frames[0].page_id.get(), Some(0));
        assert!(cache.frames[0].dirty.get());
        assert_eq!(cache.frames[0].pin_count.get(), 0);
        let cached_page = cache.frames[0].data.try_borrow().unwrap();
        assert_eq!(cached_page[0], 99);
        assert_eq!(&cached_page[1..], &pages[0][1..]);

        let page_table = cache.page_table.try_borrow().unwrap();
        assert_eq!(page_table.get(&0), Some(&0));
        assert!(!page_table.contains_key(&1));

        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk, pages[0]);
    }

    #[test]
    fn fetch_page_returns_error_for_replacement_borrow_conflict_without_pin_leak() {
        let pages = [page_with_pattern(29)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }

        let _replacement = cache.replacement.try_borrow_mut().unwrap();
        let result = cache.fetch_page(0);
        assert!(matches!(result, Err(PageCacheError::ReplacementBorrowConflict { mutable: true })));
        assert_eq!(cache.frames[0].pin_count.get(), 0);
    }

    #[test]
    fn new_page_returns_error_for_replacement_borrow_conflict() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();
        let _replacement = cache.replacement.try_borrow_mut().unwrap();

        let result = cache.new_page();
        assert!(matches!(result, Err(PageCacheError::ReplacementBorrowConflict { mutable: true })));
    }

    #[test]
    fn new_page_returns_error_for_disk_manager_borrow_conflict() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let cache = PageCache::new(disk_manager, 1).unwrap();
        let _disk_manager = cache.disk_manager.try_borrow_mut().unwrap();

        let result = cache.new_page();
        assert!(matches!(result, Err(PageCacheError::DiskManagerBorrowConflict { mutable: true })));
    }
}
