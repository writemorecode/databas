use std::{cmp::Ordering, ops::Range};

use crate::{PAGE_SIZE, RowId, SlotId};

use super::{
    PageError, PageResult,
    cell::{Cell, CellMut},
    core::{BoundResult, Index, Leaf, Page, PageAccess, PageAccessMut, SearchResult},
    format::{self, CELL_LENGTH_SIZE},
};

const ROW_ID_SIZE: usize = 8;
/// The fixed-size prefix of an index leaf cell: encoded length plus row reference.
pub const INDEX_LEAF_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + ROW_ID_SIZE;

fn encoded_len(key_len: usize) -> PageResult<usize> {
    let len = INDEX_LEAF_CELL_PREFIX_SIZE + key_len;
    if len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    Ok(len)
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, row_id: RowId, key: &[u8]) {
    let cell_len = INDEX_LEAF_CELL_PREFIX_SIZE + key.len();
    format::write_u16(bytes, cell_offset, cell_len as u16);
    format::write_u64(bytes, cell_offset + CELL_LENGTH_SIZE, row_id);
    bytes[cell_offset + INDEX_LEAF_CELL_PREFIX_SIZE..cell_offset + cell_len].copy_from_slice(key);
}

fn compare_key<A>(
    page: &Page<A, Leaf, Index>,
    slot_index: SlotId,
    key: &[u8],
) -> PageResult<Ordering>
where
    A: PageAccess,
{
    let cell_offset = page.slot_offset(slot_index)? as usize;
    Ok(Cell::<Leaf, Index>::new(page.bytes(), cell_offset, slot_index)?.key().cmp(key))
}

fn compare_entry<A>(
    page: &Page<A, Leaf, Index>,
    slot_index: SlotId,
    key: &[u8],
    row_id: RowId,
) -> PageResult<Ordering>
where
    A: PageAccess,
{
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell = Cell::<Leaf, Index>::new(page.bytes(), cell_offset, slot_index)?;
    let ordering = cell.key().cmp(key);
    Ok(if ordering == Ordering::Equal { cell.row_id().cmp(&row_id) } else { ordering })
}

fn bound_to_slot(bound: BoundResult, slot_count: SlotId) -> SlotId {
    match bound {
        BoundResult::At(slot_index) => slot_index,
        BoundResult::PastEnd => slot_count,
    }
}

impl<A> Page<A, Leaf, Index>
where
    A: PageAccess,
{
    /// Returns the first slot whose key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the first slot whose key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the half-open slot range containing all entries for `key`.
    pub fn equal_range(&self, key: &[u8]) -> PageResult<Range<SlotId>> {
        let start = bound_to_slot(self.lower_bound(key)?, self.slot_count());
        let end = bound_to_slot(self.upper_bound(key)?, self.slot_count());
        Ok(start..end)
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Leaf, Index>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        Cell::<Leaf, Index>::new(self.bytes(), cell_offset, slot_index)
    }
}

impl<A> Page<A, Leaf, Index>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<CellMut<'_, Leaf, Index>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        CellMut::<Leaf, Index>::new(self.bytes_mut(), cell_offset, slot_index)
    }

    /// Inserts a new `(key, row_id)` entry while preserving lexicographic key order.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> PageResult<SlotId> {
        let cell_len = encoded_len(key.len())?;
        let slot_index = match self
            .search_slots_by(|page, slot_index| compare_entry(page, slot_index, key, row_id))?
        {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey),
            SearchResult::InsertAt(slot_index) => slot_index,
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, row_id, key);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }

    /// Deletes an existing `(key, row_id)` entry and re-packs the page.
    pub fn delete(&mut self, key: &[u8], row_id: RowId) -> PageResult<SlotId> {
        let slot_index = match self
            .search_slots_by(|page, slot_index| compare_entry(page, slot_index, key, row_id))?
        {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let cell_offset = self.slot_offset(slot_index)?;
        let cell_len = self.cell_len(slot_index)?;
        self.remove_slot(slot_index)?;
        self.reclaim_space(cell_offset, cell_len)?;
        Ok(slot_index)
    }
}
