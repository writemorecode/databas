use std::collections::HashMap;

use crate::{
    disk_manager::DiskManager,
    error::{PageCacheError, PageCacheResult},
    types::{PAGE_SIZE, PageId},
};

pub(crate) type FrameId = usize;

#[derive(Debug, Clone)]
struct Frame {
    page_id: Option<PageId>,
    data: [u8; PAGE_SIZE],
    reference: bool,
    dirty: bool,
    pin_count: u32,
}

impl Frame {
    fn empty() -> Self {
        Self { page_id: None, data: [0u8; PAGE_SIZE], reference: false, dirty: false, pin_count: 0 }
    }
}

pub(crate) struct PageCache {
    disk_manager: DiskManager,
    frames: Vec<Frame>,
    page_table: HashMap<PageId, FrameId>,
    clock_hand: FrameId,
}

impl PageCache {
    pub(crate) fn new(disk_manager: DiskManager, frame_count: usize) -> PageCacheResult<Self> {
        if frame_count == 0 {
            return Err(PageCacheError::InvalidFrameCount(frame_count));
        }

        Ok(Self {
            disk_manager,
            frames: vec![Frame::empty(); frame_count],
            page_table: HashMap::new(),
            clock_hand: 0,
        })
    }

    pub(crate) fn fetch_page(&mut self, page_id: PageId) -> PageCacheResult<PinGuard<'_>> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_id];
            frame.reference = true;
            frame.pin_count = frame.pin_count.checked_add(1).expect("pin count overflow");
            return Ok(PinGuard::new(self, frame_id));
        }

        let frame_id = self.select_victim_frame().ok_or(PageCacheError::NoEvictableFrame)?;
        self.replace_frame(frame_id, page_id)?;
        Ok(PinGuard::new(self, frame_id))
    }

    pub(crate) fn new_page(&mut self) -> PageCacheResult<(PageId, PinGuard<'_>)> {
        let frame_id = self.select_victim_frame().ok_or(PageCacheError::NoEvictableFrame)?;
        let page_id = self.disk_manager.new_page()?;
        self.replace_frame(frame_id, page_id)?;
        Ok((page_id, PinGuard::new(self, frame_id)))
    }

    pub(crate) fn flush_page(&mut self, page_id: PageId) -> PageCacheResult<()> {
        let Some(&frame_id) = self.page_table.get(&page_id) else {
            return Ok(());
        };

        if self.frames[frame_id].pin_count > 0 {
            return Err(PageCacheError::PinnedPage(page_id));
        }

        self.flush_frame_if_dirty(frame_id)
    }

    pub(crate) fn flush_all(&mut self) -> PageCacheResult<()> {
        for frame_id in 0..self.frames.len() {
            let (page_id, pin_count, dirty) = {
                let frame = &self.frames[frame_id];
                (frame.page_id, frame.pin_count, frame.dirty)
            };

            if !dirty {
                continue;
            }

            let Some(page_id) = page_id else {
                continue;
            };

            if pin_count > 0 {
                return Err(PageCacheError::PinnedPage(page_id));
            }

            self.flush_frame_if_dirty(frame_id)?;
        }

        Ok(())
    }

    fn select_victim_frame(&mut self) -> Option<FrameId> {
        let max_scans = self.frames.len().saturating_mul(2);

        for _ in 0..max_scans {
            let frame_id = self.clock_hand;
            self.advance_clock_hand();

            let frame = &mut self.frames[frame_id];
            if frame.pin_count > 0 {
                continue;
            }

            if frame.reference {
                frame.reference = false;
                continue;
            }

            return Some(frame_id);
        }

        None
    }

    fn replace_frame(&mut self, frame_id: FrameId, new_page_id: PageId) -> PageCacheResult<()> {
        self.flush_frame_if_dirty(frame_id)?;

        if let Some(old_page_id) = self.frames[frame_id].page_id {
            self.page_table.remove(&old_page_id);
        }

        let mut data = [0u8; PAGE_SIZE];
        self.disk_manager.read_page(new_page_id, &mut data)?;
        self.frames[frame_id] =
            Frame { page_id: Some(new_page_id), data, reference: true, dirty: false, pin_count: 1 };

        self.page_table.insert(new_page_id, frame_id);
        Ok(())
    }

    fn flush_frame_if_dirty(&mut self, frame_id: FrameId) -> PageCacheResult<()> {
        let (disk_manager, frames) = (&mut self.disk_manager, &mut self.frames);
        let frame = &mut frames[frame_id];
        if !frame.dirty {
            return Ok(());
        }
        let Some(page_id) = frame.page_id else {
            return Ok(());
        };
        disk_manager.write_page(page_id, &frame.data)?;
        frame.dirty = false;
        Ok(())
    }

    fn advance_clock_hand(&mut self) {
        self.clock_hand = (self.clock_hand + 1) % self.frames.len();
    }

    fn flush_best_effort_on_drop(&mut self) {
        let (disk_manager, frames) = (&mut self.disk_manager, &mut self.frames);
        for frame in frames.iter_mut() {
            if !frame.dirty || frame.pin_count > 0 {
                continue;
            }
            let Some(page_id) = frame.page_id else {
                continue;
            };
            if disk_manager.write_page(page_id, &frame.data).is_ok() {
                frame.dirty = false;
            }
        }
    }
}

impl Drop for PageCache {
    fn drop(&mut self) {
        self.flush_best_effort_on_drop();
    }
}

pub(crate) struct PinGuard<'a> {
    page_cache: &'a mut PageCache,
    frame_id: FrameId,
}

impl<'a> PinGuard<'a> {
    fn new(page_cache: &'a mut PageCache, frame_id: FrameId) -> Self {
        Self { page_cache, frame_id }
    }

    pub(crate) fn page(&self) -> &[u8; PAGE_SIZE] {
        &self.page_cache.frames[self.frame_id].data
    }

    pub(crate) fn page_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        let frame = &mut self.page_cache.frames[self.frame_id];
        frame.dirty = true;
        &mut frame.data
    }
}

impl Drop for PinGuard<'_> {
    fn drop(&mut self) {
        let frame = &mut self.page_cache.frames[self.frame_id];
        debug_assert!(frame.pin_count > 0, "pin count underflow");
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::NamedTempFile;

    use super::*;

    fn page_with_pattern(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        for (index, byte) in page.iter_mut().enumerate() {
            *byte = seed.wrapping_add(index as u8);
        }
        page
    }

    fn create_disk_with_pages(pages: &[[u8; PAGE_SIZE]]) -> (NamedTempFile, DiskManager) {
        let file = NamedTempFile::new().unwrap();
        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        for page in pages {
            let page_id = disk_manager.new_page().unwrap();
            disk_manager.write_page(page_id, page).unwrap();
        }
        (file, disk_manager)
    }

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
            assert_eq!(frame.page_id, None);
            assert_eq!(frame.reference, false);
            assert_eq!(frame.dirty, false);
            assert_eq!(frame.pin_count, 0);
            assert_eq!(frame.data, [0u8; PAGE_SIZE]);
        }
    }

    #[test]
    fn fetch_page_loads_page_and_pins() {
        let page = page_with_pattern(7);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        let guard = cache.fetch_page(0).unwrap();
        assert_eq!(guard.page(), &page);
        drop(guard);

        assert_eq!(cache.frames[0].pin_count, 0);
        assert_eq!(cache.frames[0].reference, true);
    }

    #[test]
    fn pin_guard_drop_decrements_pin_count() {
        let page = page_with_pattern(11);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }

        assert_eq!(cache.frames[0].pin_count, 0);
    }

    #[test]
    fn page_read_does_not_mark_dirty_but_page_mut_does() {
        let page = page_with_pattern(13);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let guard = cache.fetch_page(0).unwrap();
            assert_eq!(guard.page()[0], page[0]);
        }
        assert!(!cache.frames[0].dirty);

        {
            let mut guard = cache.fetch_page(0).unwrap();
            let page = guard.page_mut();
            page[0] = 99;
        }

        assert!(cache.frames[0].dirty);
    }

    #[test]
    fn dirty_page_is_written_during_eviction() {
        let page0 = page_with_pattern(1);
        let page1 = page_with_pattern(2);
        let pages = [page0, page1];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let mut guard = cache.fetch_page(0).unwrap();
            guard.page_mut()[0] = 222;
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
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }
        {
            let _guard = cache.fetch_page(1).unwrap();
        }
        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        assert!(!cache.page_table.contains_key(&0));
        assert!(cache.page_table.contains_key(&1));
        assert!(cache.page_table.contains_key(&2));
    }

    #[test]
    fn eviction_skips_pinned_frames() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        cache.frames[0] = Frame {
            page_id: Some(0),
            data: pages[0],
            reference: false,
            dirty: false,
            pin_count: 1,
        };
        cache.frames[1] = Frame {
            page_id: Some(1),
            data: pages[1],
            reference: false,
            dirty: false,
            pin_count: 0,
        };
        cache.page_table.insert(0, 0);
        cache.page_table.insert(1, 1);
        cache.clock_hand = 0;

        {
            let _guard = cache.fetch_page(2).unwrap();
        }

        assert_eq!(cache.frames[0].page_id, Some(0));
        assert!(cache.page_table.contains_key(&0));
        assert!(!cache.page_table.contains_key(&1));
        assert!(cache.page_table.contains_key(&2));
    }

    #[test]
    fn fetch_returns_error_when_all_frames_are_pinned() {
        let pages = [page_with_pattern(1), page_with_pattern(2), page_with_pattern(3)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        cache.frames[0] = Frame {
            page_id: Some(0),
            data: pages[0],
            reference: false,
            dirty: false,
            pin_count: 1,
        };
        cache.frames[1] = Frame {
            page_id: Some(1),
            data: pages[1],
            reference: false,
            dirty: false,
            pin_count: 1,
        };
        cache.page_table.insert(0, 0);
        cache.page_table.insert(1, 1);
        cache.clock_hand = 0;

        let result = cache.fetch_page(2);
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));
    }

    #[test]
    fn flush_page_writes_dirty_data_and_clears_dirty_bit() {
        let page = page_with_pattern(15);
        let pages = [page];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let mut guard = cache.fetch_page(0).unwrap();
            guard.page_mut()[0] = 177;
        }
        assert!(cache.frames[0].dirty);

        cache.flush_page(0).unwrap();

        assert!(!cache.frames[0].dirty);
        let flushed_page = read_disk_page(file.path(), 0);
        assert_eq!(flushed_page[0], 177);
    }

    #[test]
    fn flush_page_returns_error_if_page_is_pinned() {
        let page = page_with_pattern(8);
        let pages = [page];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        cache.frames[0] =
            Frame { page_id: Some(0), data: page, reference: true, dirty: true, pin_count: 1 };
        cache.page_table.insert(0, 0);

        let result = cache.flush_page(0);
        assert!(matches!(result, Err(PageCacheError::PinnedPage(0))));
    }

    #[test]
    fn flush_page_is_noop_for_nonresident_page() {
        let pages = [page_with_pattern(1), page_with_pattern(2)];
        let (_file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        {
            let _guard = cache.fetch_page(0).unwrap();
        }

        assert!(cache.flush_page(1).is_ok());
    }

    #[test]
    fn flush_all_writes_all_dirty_unpinned_pages() {
        let pages = [page_with_pattern(4), page_with_pattern(5)];
        let (file, disk_manager) = create_disk_with_pages(&pages);
        let mut cache = PageCache::new(disk_manager, 2).unwrap();

        {
            let mut guard = cache.fetch_page(0).unwrap();
            guard.page_mut()[0] = 10;
        }
        {
            let mut guard = cache.fetch_page(1).unwrap();
            guard.page_mut()[0] = 20;
        }

        cache.flush_all().unwrap();

        for frame in &cache.frames {
            assert!(!frame.dirty);
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

        cache.frames[0] =
            Frame { page_id: Some(0), data: page, reference: true, dirty: true, pin_count: 1 };
        cache.page_table.insert(0, 0);

        let result = cache.flush_all();
        assert!(matches!(result, Err(PageCacheError::PinnedPage(0))));
    }

    #[test]
    fn drop_flushes_dirty_unpinned_pages_best_effort() {
        let page = page_with_pattern(33);
        let pages = [page];
        let (file, disk_manager) = create_disk_with_pages(&pages);

        {
            let mut cache = PageCache::new(disk_manager, 1).unwrap();
            {
                let mut guard = cache.fetch_page(0).unwrap();
                guard.page_mut()[0] = 144;
            }
            assert!(cache.frames[0].dirty);
        }

        let page_on_disk = read_disk_page(file.path(), 0);
        assert_eq!(page_on_disk[0], 144);
    }

    #[test]
    fn new_page_returns_pinned_zero_initialized_page() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

        let (page_id, guard) = cache.new_page().unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(guard.page(), &[0u8; PAGE_SIZE]);
    }

    #[test]
    fn new_page_allocates_sequential_ids() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let mut cache = PageCache::new(disk_manager, 1).unwrap();

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
            page_id: None,
            data: [0u8; PAGE_SIZE],
            reference: false,
            dirty: false,
            pin_count: 1,
        };

        let result = cache.new_page();
        assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)));

        let mut disk_manager = DiskManager::new(file.path()).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        let read_result = disk_manager.read_page(0, &mut page);
        assert!(matches!(read_result, Err(crate::error::StorageError::InvalidPageId(0))));
    }
}
