use std::cmp::Ordering;

use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    PageError, PageResult,
    cell::{Cell, CellMut},
    core::{BoundResult, Index, Interior, Page, PageAccess, PageAccessMut},
    format::{self, CELL_LENGTH_SIZE, RIGHTMOST_CHILD_OFFSET},
};

const PAGE_ID_SIZE: usize = 8;
const ROW_ID_SIZE: usize = 8;
/// The fixed-size header of an index interior cell: encoded length plus left-child page id.
pub const INDEX_INTERIOR_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + PAGE_ID_SIZE;

pub(crate) fn write_left_child(bytes: &mut [u8], page_id: PageId) {
    bytes[CELL_LENGTH_SIZE..CELL_LENGTH_SIZE + PAGE_ID_SIZE]
        .copy_from_slice(&page_id.to_le_bytes());
}

fn encoded_len(key_len: usize) -> PageResult<usize> {
    let len = INDEX_INTERIOR_CELL_PREFIX_SIZE + key_len + ROW_ID_SIZE;
    if len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    Ok(len)
}

fn write_cell(
    bytes: &mut [u8; PAGE_SIZE],
    cell_offset: usize,
    left_child: PageId,
    key: &[u8],
    row_id: RowId,
) {
    let cell_len = INDEX_INTERIOR_CELL_PREFIX_SIZE + key.len() + ROW_ID_SIZE;
    format::write_u16(bytes, cell_offset, cell_len as u16);
    write_left_child(&mut bytes[cell_offset..cell_offset + cell_len], left_child);
    let key_start = cell_offset + INDEX_INTERIOR_CELL_PREFIX_SIZE;
    let key_end = key_start + key.len();
    bytes[key_start..key_end].copy_from_slice(key);
    bytes[key_end..cell_offset + cell_len].copy_from_slice(&row_id.to_be_bytes());
}

fn compare_key<A>(
    page: &Page<A, Interior, Index>,
    slot_index: SlotId,
    key: &[u8],
) -> PageResult<Ordering>
where
    A: PageAccess,
{
    let cell_offset = page.slot_offset(slot_index)? as usize;
    Ok(Cell::<Interior, Index>::new(page.bytes(), cell_offset, slot_index)?.key().cmp(key))
}

fn compare_entry<A>(
    page: &Page<A, Interior, Index>,
    slot_index: SlotId,
    key: &[u8],
    row_id: RowId,
) -> PageResult<Ordering>
where
    A: PageAccess,
{
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell = Cell::<Interior, Index>::new(page.bytes(), cell_offset, slot_index)?;
    let ordering = cell.key().cmp(key);
    Ok(if ordering == Ordering::Equal { cell.row_id().cmp(&row_id) } else { ordering })
}

impl<A> Page<A, Interior, Index>
where
    A: PageAccess,
{
    /// Returns the page id stored in the rightmost-child header field.
    pub fn rightmost_child(&self) -> PageId {
        format::read_u64(self.bytes(), RIGHTMOST_CHILD_OFFSET)
    }

    /// Returns the first slot whose separator key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the first slot whose separator entry is greater than or equal to `(key, row_id)`.
    pub fn lower_bound_entry(&self, key: &[u8], row_id: RowId) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| compare_entry(page, slot_index, key, row_id))
    }

    /// Returns the first slot whose separator key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the child page that may contain `key`.
    pub fn child_for(&self, key: &[u8]) -> PageResult<PageId> {
        match self.lower_bound(key)? {
            BoundResult::At(slot_index) => Ok(self.cell(slot_index)?.left_child()),
            BoundResult::PastEnd => Ok(self.rightmost_child()),
        }
    }

    /// Returns the child page that may contain `(key, row_id)`.
    pub fn child_for_entry(&self, key: &[u8], row_id: RowId) -> PageResult<PageId> {
        match self.lower_bound_entry(key, row_id)? {
            BoundResult::At(slot_index) => Ok(self.cell(slot_index)?.left_child()),
            BoundResult::PastEnd => Ok(self.rightmost_child()),
        }
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Interior, Index>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        Cell::<Interior, Index>::new(self.bytes(), cell_offset, slot_index)
    }
}

impl<A> Page<A, Interior, Index>
where
    A: PageAccessMut,
{
    /// Updates the page id stored in the rightmost-child header field.
    pub fn set_rightmost_child(&mut self, page_id: PageId) {
        format::write_u64(self.bytes_mut(), RIGHTMOST_CHILD_OFFSET, page_id);
    }

    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<CellMut<'_, Interior, Index>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        CellMut::<Interior, Index>::new(self.bytes_mut(), cell_offset, slot_index)
    }

    /// Inserts a new separator key and its left-child pointer while preserving slot order.
    pub fn insert(&mut self, key: &[u8], row_id: RowId, left_child: PageId) -> PageResult<SlotId> {
        let cell_len = encoded_len(key.len())?;
        let slot_index = match self.lower_bound_entry(key, row_id)? {
            BoundResult::At(slot_index) => slot_index,
            BoundResult::PastEnd => self.slot_count(),
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, left_child, key, row_id);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }
}
