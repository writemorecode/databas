use std::{cmp::Ordering, ops::Range};

use crate::{PAGE_SIZE, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    cell::Cell,
    core::{BoundResult, Index, Leaf, Page, PageAccess, PageAccessMut, Read, SearchResult, Write},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
};

const ROW_ID_SIZE: usize = 8;
/// The fixed-size prefix of an index leaf cell: encoded length plus row reference.
pub const INDEX_LEAF_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexLeafCellParts {
    pub(crate) row_id: RowId,
    pub(crate) key_start: usize,
    pub(crate) key_end: usize,
}

pub(crate) fn cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    let cell_len = format::read_u16(bytes, cell_offset) as usize;
    if cell_len < INDEX_LEAF_CELL_PREFIX_SIZE {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }
    if cell_offset + cell_len > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }
    Ok(cell_len)
}

pub(crate) fn cell_parts<A>(
    page: &Page<A, Leaf, Index>,
    slot_index: SlotId,
) -> PageResult<IndexLeafCellParts>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = cell_len_at(page.bytes(), slot_index, cell_offset)?;
    let key_start = cell_offset + INDEX_LEAF_CELL_PREFIX_SIZE;
    let key_end = cell_offset + cell_len;

    Ok(IndexLeafCellParts {
        row_id: format::read_u64(page.bytes(), cell_offset + CELL_LENGTH_SIZE),
        key_start,
        key_end,
    })
}

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
    let parts = cell_parts(page, slot_index)?;
    Ok(page.bytes()[parts.key_start..parts.key_end].cmp(key))
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
    let parts = cell_parts(page, slot_index)?;
    let ordering = page.bytes()[parts.key_start..parts.key_end].cmp(key);
    Ok(if ordering == Ordering::Equal { parts.row_id.cmp(&row_id) } else { ordering })
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
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<Read<'_>, Leaf, Index>> {
        cell_parts(self, slot_index)?;
        Ok(Cell::new(Read { bytes: self.bytes() }, slot_index))
    }
}

impl<A> Page<A, Leaf, Index>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<Cell<Write<'_>, Leaf, Index>> {
        let page = Page::<Read<'_>, Leaf, Index>::open(self.bytes())?;
        cell_parts(&page, slot_index)?;
        Ok(Cell::new(Write { bytes: self.bytes_mut() }, slot_index))
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
