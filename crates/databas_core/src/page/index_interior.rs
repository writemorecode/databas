use crate::{PAGE_SIZE, PageId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Interior, Page, PageAccess},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
};

const PAGE_ID_SIZE: usize = 8;
/// The fixed-size prefix of an index interior cell: encoded length plus left-child page id.
pub const INDEX_INTERIOR_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + PAGE_ID_SIZE;

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexInteriorCellParts {
    pub(crate) cell_offset: usize,
    pub(crate) left_child: PageId,
    pub(crate) key_start: usize,
    pub(crate) key_end: usize,
}

pub(crate) fn cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    let cell_len = format::read_u16(bytes, cell_offset) as usize;
    if cell_len < INDEX_INTERIOR_CELL_PREFIX_SIZE {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }
    if cell_offset + cell_len > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }
    Ok(cell_len)
}

pub(crate) fn cell_parts<A>(
    page: &Page<A, Interior, Index>,
    slot_index: SlotId,
) -> PageResult<IndexInteriorCellParts>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = cell_len_at(page.bytes(), slot_index, cell_offset)?;
    let key_start = cell_offset + INDEX_INTERIOR_CELL_PREFIX_SIZE;
    let key_end = cell_offset + cell_len;

    Ok(IndexInteriorCellParts {
        cell_offset,
        left_child: format::read_u64(page.bytes(), cell_offset + CELL_LENGTH_SIZE),
        key_start,
        key_end,
    })
}

pub(crate) fn write_left_child(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, page_id: PageId) {
    format::write_u64(bytes, cell_offset + CELL_LENGTH_SIZE, page_id);
}
