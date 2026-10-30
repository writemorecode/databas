//! Persistent extent bitmaps. One set bit reserves one physical page.
//!
//! Page zero embeds the first bitmap after its database header. Every subsequent
//! extent starts with a bitmap page. Bitmap changes use logical WAL records,
//! rather than shared full-page undo images, so transactions changing different
//! bits never undo one another. Uncommitted frees remain reserved.

use crate::disk_manager::DiskManager;
use crate::error::StorageResult;
use crate::{PAGE_SIZE, PageId};

mod bitmap;

use bitmap::AllocationBitmap;
pub use bitmap::BITMAP_OFFSET;
#[cfg(test)]
pub use bitmap::PAGES_PER_EXTENT;

pub struct PageAllocator {
    bitmap: AllocationBitmap,
    dirty: bool,
    next_candidate: PageId,
}

impl PageAllocator {
    pub(crate) fn load(disk: &mut DiskManager) -> StorageResult<Self> {
        let mut bitmap = AllocationBitmap::new();
        for page_id in AllocationBitmap::metadata_page_ids(disk.page_count()) {
            let mut bytes = [0; PAGE_SIZE];
            disk.read_page(page_id, &mut bytes)?;
            // A trailing bitmap with no data pages can be recreated after an
            // interrupted extent initialization. Existing extents are validated.
            if page_id != 0 && disk.page_count() == page_id + 1 {
                let expected = AllocationBitmap::empty_page(page_id);
                if bytes != expected {
                    disk.write_page(page_id, &expected)?;
                    disk.sync()?;
                    bytes = expected;
                }
            }
            bitmap.push_page(page_id, bytes)?;
        }
        Ok(Self { bitmap, dirty: false, next_candidate: 0 })
    }

    /// Validate the physical extent only after WAL has repaired allocations.
    /// A bitmap sector can reach disk before an unsynced file extension does.
    pub(crate) fn validate_bounds(&self, page_count: u64) -> StorageResult<()> {
        self.bitmap.validate_bounds(page_count)
    }

    pub(crate) fn contains_page(&self, page_id: PageId) -> bool {
        self.bitmap.contains_page(page_id)
    }

    pub(crate) fn is_metadata(page_id: PageId) -> bool {
        AllocationBitmap::is_metadata(page_id)
    }

    pub(crate) fn is_allocated(&self, page_id: PageId) -> bool {
        self.bitmap.is_allocated(page_id)
    }

    /// Select a free page, extending the file and initializing metadata if needed.
    /// Selection does not reserve it until its WAL append succeeds. The caller
    /// holds the allocation latch throughout selection, logging, and reservation.
    pub(crate) fn candidate(&mut self, disk: &mut DiskManager) -> StorageResult<PageId> {
        let page_id = if let Some(page_id) = self.bitmap.first_free_from(self.next_candidate) {
            if page_id == disk.page_count() {
                disk.new_page()?;
            }
            page_id
        } else {
            self.ensure_page(disk, self.bitmap.next_metadata_page_id())?;
            disk.new_page()?
        };
        self.next_candidate = page_id;
        Ok(page_id)
    }

    /// Recovery may need to recreate an extent lost with an unsynced extension.
    pub(crate) fn ensure_page(
        &mut self,
        disk: &mut DiskManager,
        page_id: PageId,
    ) -> StorageResult<()> {
        // Check impossible extents before iterating over metadata pages.
        page_id
            .checked_add(1)
            .and_then(|n| n.checked_mul(PAGE_SIZE as u64))
            .ok_or(crate::disk_manager::DiskManagerError::InvalidPageId { page_id })?;
        while !self.bitmap.contains_page(page_id) {
            let metadata_id = self.bitmap.next_metadata_page_id();
            let bytes = AllocationBitmap::empty_page(metadata_id);
            disk.ensure_page_exists(metadata_id)?;
            disk.write_page(metadata_id, &bytes)?;
            // Publish valid extent metadata before any data page can be logged.
            disk.sync()?;
            self.bitmap.push_page(metadata_id, bytes)?;
        }
        disk.ensure_page_exists(page_id)?;
        Ok(())
    }

    pub(crate) fn set_allocated(&mut self, page_id: PageId, allocated: bool) {
        self.bitmap.set_allocated(page_id, allocated);
        if allocated {
            if page_id == self.next_candidate {
                self.next_candidate += 1;
            }
        } else {
            self.next_candidate = self.next_candidate.min(page_id);
        }
        self.dirty = true;
    }

    /// The caller must flush the WAL first and sync the database before reclaiming it.
    pub(crate) fn flush(&mut self, disk: &mut DiskManager) -> StorageResult<()> {
        if self.dirty {
            for (page_id, bytes) in self.bitmap.pages() {
                disk.write_page(page_id, bytes)?;
            }
            // Keep dirty state on sync failure so an explicit retry writes again.
            disk.sync()?;
            self.dirty = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
