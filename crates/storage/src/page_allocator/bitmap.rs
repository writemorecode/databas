//! Maps physical page IDs to persisted extent bitmaps and owns their page format.

use crate::bitset::BitSet;
use crate::database_header::DatabaseHeader;
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::{PAGE_SIZE, PageId};

pub const BITMAP_OFFSET: usize = 16;
const BITMAP_BITS: usize = (PAGE_SIZE - BITMAP_OFFSET) * 8;
pub const PAGES_PER_EXTENT: u64 = BITMAP_BITS as u64;
const BITMAP_MAGIC: &[u8; 8] = b"DBBITMAP";

pub(super) struct AllocationBitmap {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl AllocationBitmap {
    pub(super) fn new() -> Self {
        Self { pages: Vec::new() }
    }

    pub(super) fn metadata_page_ids(page_count: u64) -> impl Iterator<Item = PageId> {
        (0..page_count).step_by(BITMAP_BITS)
    }

    pub(super) fn next_metadata_page_id(&self) -> PageId {
        self.pages.len() as u64 * PAGES_PER_EXTENT
    }

    /// Whether the bitmap covers this ID, regardless of allocation or file length.
    pub(super) fn contains_page(&self, page_id: PageId) -> bool {
        page_id / PAGES_PER_EXTENT < self.pages.len() as u64
    }

    pub(super) fn is_metadata(page_id: PageId) -> bool {
        page_id.is_multiple_of(PAGES_PER_EXTENT)
    }

    pub(super) fn empty_page(page_id: PageId) -> [u8; PAGE_SIZE] {
        if page_id == 0 {
            return DatabaseHeader::encode_page();
        }
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(BITMAP_MAGIC);
        bytes[8..16].copy_from_slice(&page_id.to_le_bytes());
        BitSet::new(&mut bytes[BITMAP_OFFSET..]).set(0, true);
        bytes
    }

    /// Validate and append the next bitmap page in physical order.
    pub(super) fn push_page(
        &mut self,
        page_id: PageId,
        bytes: [u8; PAGE_SIZE],
    ) -> StorageResult<()> {
        if page_id != self.next_metadata_page_id() {
            return Err(invalid_bitmap(page_id));
        }
        if page_id == 0 {
            DatabaseHeader::validate_page(&bytes)?;
        } else if &bytes[..8] != BITMAP_MAGIC || bytes[8..16] != page_id.to_le_bytes() {
            return Err(invalid_bitmap(page_id));
        }
        if !BitSet::new(&bytes[BITMAP_OFFSET..]).contains(0) {
            return Err(invalid_bitmap(page_id));
        }
        self.pages.push(bytes);
        Ok(())
    }

    pub(super) fn pages(&self) -> impl Iterator<Item = (PageId, &[u8; PAGE_SIZE])> {
        self.pages.iter().enumerate().map(|(index, page)| (index as u64 * PAGES_PER_EXTENT, page))
    }

    pub(super) fn is_allocated(&self, page_id: PageId) -> bool {
        let (extent, bit) = Self::location(page_id);
        self.pages.get(extent).is_some_and(|page| BitSet::new(&page[BITMAP_OFFSET..]).contains(bit))
    }

    pub(super) fn set_allocated(&mut self, page_id: PageId, allocated: bool) {
        let (extent, bit) = Self::location(page_id);
        BitSet::new(&mut self.pages[extent][BITMAP_OFFSET..]).set(bit, allocated);
    }

    pub(super) fn first_free_from(&self, page_id: PageId) -> Option<PageId> {
        let (first_extent, first_bit) = Self::location(page_id);
        for (extent, page) in self.pages.iter().enumerate().skip(first_extent) {
            let start = if extent == first_extent { first_bit } else { 0 };
            if let Some(bit) = BitSet::new(&page[BITMAP_OFFSET..]).first_clear_from(start) {
                return Some(extent as u64 * PAGES_PER_EXTENT + bit as u64);
            }
        }
        None
    }

    pub(super) fn validate_bounds(&self, page_count: u64) -> StorageResult<()> {
        for (page_id, bytes) in self.pages() {
            let end = usize::try_from(page_count.saturating_sub(page_id).min(PAGES_PER_EXTENT))
                .map_err(|_out_of_range| invalid_bitmap(page_id))?;
            if BitSet::new(&bytes[BITMAP_OFFSET..]).any_set_from(end) {
                return Err(invalid_bitmap(page_id));
            }
        }
        Ok(())
    }

    fn location(page_id: PageId) -> (usize, usize) {
        ((page_id / PAGES_PER_EXTENT) as usize, (page_id % PAGES_PER_EXTENT) as usize)
    }
}

fn invalid_bitmap(page_id: PageId) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::DatabaseFile,
        page_id: Some(page_id),
        kind: CorruptionKind::InvalidAllocationBitmap,
    })
}
