use crate::{PAGE_SIZE, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Leaf, Page, PageAccess},
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
