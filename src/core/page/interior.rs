use std::ops::Range;

use crate::core::{PAGE_SIZE, PageId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    cell::{Cell, CellMut},
    core::{Interior, Page, PageAccess, PageAccessMut},
    format::{
        self, CELL_LENGTH_SIZE, CELL_OVERFLOW_PAGE_ID_SIZE, INTERIOR_CELL_PREFIX_SIZE,
        MIN_INLINE_OVERFLOW_PAYLOAD_BYTES, RIGHTMOST_CHILD_OFFSET, USABLE_SPACE_END,
    },
};

const PAGE_ID_SIZE: usize = 8;
const FIRST_OVERFLOW_PAGE_ID_OFFSET: usize = CELL_LENGTH_SIZE;
const LEFT_CHILD_OFFSET: usize = FIRST_OVERFLOW_PAGE_ID_OFFSET + CELL_OVERFLOW_PAGE_ID_SIZE;
const KEY_LENGTH_OFFSET: usize = LEFT_CHILD_OFFSET + PAGE_ID_SIZE;

#[derive(Debug, Clone)]
pub(crate) struct InteriorCellParts {
    pub(crate) payload_len: usize,
    pub(crate) first_overflow_page_id: Option<PageId>,
    pub(crate) inline_payload_range: Range<usize>,
    pub(crate) left_child: PageId,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedInteriorCell {
    pub(crate) cell_offset: usize,
    pub(crate) cell_len: usize,
    pub(crate) parts: InteriorCellParts,
}

pub(crate) fn cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    if cell_offset + INTERIOR_CELL_PREFIX_SIZE > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }

    let payload_len = format::read_u16(bytes, cell_offset) as usize;
    let first_overflow_page_id =
        format::read_optional_u64(bytes, cell_offset + FIRST_OVERFLOW_PAGE_ID_OFFSET);
    let key_len = format::read_u16(bytes, cell_offset + KEY_LENGTH_OFFSET) as usize;
    if key_len != payload_len {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }

    let Some(inline_payload_len) = format::inline_payload_len(payload_len, first_overflow_page_id)
    else {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    };
    if first_overflow_page_id.is_some() && inline_payload_len < MIN_INLINE_OVERFLOW_PAYLOAD_BYTES {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }

    let cell_len = INTERIOR_CELL_PREFIX_SIZE + inline_payload_len;
    if cell_offset + cell_len > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }
    Ok(cell_len)
}

pub(crate) fn cell_parts<A>(
    page: &Page<A, Interior>,
    slot_index: SlotId,
) -> PageResult<ParsedInteriorCell>
where
    A: PageAccess,
{
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = cell_len_at(page.bytes(), slot_index, cell_offset)?;
    let payload_len = format::read_u16(page.bytes(), cell_offset) as usize;
    let first_overflow_page_id =
        format::read_optional_u64(page.bytes(), cell_offset + FIRST_OVERFLOW_PAGE_ID_OFFSET);
    let inline_payload_len = cell_len - INTERIOR_CELL_PREFIX_SIZE;

    Ok(ParsedInteriorCell {
        cell_offset,
        cell_len,
        parts: InteriorCellParts {
            payload_len,
            first_overflow_page_id,
            inline_payload_range: INTERIOR_CELL_PREFIX_SIZE
                ..INTERIOR_CELL_PREFIX_SIZE + inline_payload_len,
            left_child: format::read_u64(page.bytes(), cell_offset + LEFT_CHILD_OFFSET),
        },
    })
}

pub(crate) fn write_cell_with_payload(
    bytes: &mut [u8; PAGE_SIZE],
    cell_offset: usize,
    left_child: PageId,
    key_len: usize,
    first_overflow_page_id: Option<PageId>,
    inline_payload: &[u8],
) {
    format::write_u16(bytes, cell_offset, key_len as u16);
    format::write_optional_u64(
        bytes,
        cell_offset + FIRST_OVERFLOW_PAGE_ID_OFFSET,
        first_overflow_page_id,
    );
    let cell_len = INTERIOR_CELL_PREFIX_SIZE + inline_payload.len();
    write_left_child(&mut bytes[cell_offset..cell_offset + cell_len], left_child);
    format::write_u16(bytes, cell_offset + KEY_LENGTH_OFFSET, key_len as u16);
    let payload_start = cell_offset + INTERIOR_CELL_PREFIX_SIZE;
    bytes[payload_start..payload_start + inline_payload.len()].copy_from_slice(inline_payload);
}

pub(crate) fn write_left_child(bytes: &mut [u8], page_id: PageId) {
    bytes[LEFT_CHILD_OFFSET..LEFT_CHILD_OFFSET + PAGE_ID_SIZE]
        .copy_from_slice(&page_id.to_le_bytes());
}

impl<A> Page<A, Interior>
where
    A: PageAccess,
{
    /// Returns the page id stored in the rightmost-child header field.
    pub(crate) fn rightmost_child(&self) -> PageId {
        format::read_u64(self.bytes(), RIGHTMOST_CHILD_OFFSET)
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub(crate) fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Interior>> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes = &self.bytes()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(Cell::new_interior(cell_bytes, parsed.parts))
    }

    /// Returns full payload metadata and the page-relative inline payload range for one cell.
    pub(crate) fn cell_payload_parts(
        &self,
        slot_index: SlotId,
    ) -> PageResult<(PageId, usize, Option<PageId>, Range<usize>)> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_offset = parsed.cell_offset;
        let inline_payload_range = cell_offset + parsed.parts.inline_payload_range.start
            ..cell_offset + parsed.parts.inline_payload_range.end;
        Ok((
            parsed.parts.left_child,
            parsed.parts.payload_len,
            parsed.parts.first_overflow_page_id,
            inline_payload_range,
        ))
    }
}

impl<A> Page<A, Interior>
where
    A: PageAccessMut,
{
    /// Updates the page id stored in the rightmost-child header field.
    pub(crate) fn set_rightmost_child(&mut self, page_id: PageId) {
        format::write_u64(self.bytes_mut(), RIGHTMOST_CHILD_OFFSET, page_id);
    }

    /// Returns a typed mutable view of the cell at `slot_index`.
    pub(crate) fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<CellMut<'_, Interior>> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes =
            &mut self.bytes_mut()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(CellMut::new_interior(cell_bytes, parsed.parts))
    }

    /// Inserts an interior cell whose separator key may continue in an overflow chain.
    pub(crate) fn insert_payload_at(
        &mut self,
        slot_index: SlotId,
        left_child: PageId,
        key_len: usize,
        first_overflow_page_id: Option<PageId>,
        inline_payload: &[u8],
    ) -> PageResult<SlotId> {
        if key_len > u16::MAX as usize {
            return Err(PageError::CellTooLarge { len: key_len, max: u16::MAX as usize });
        }
        let Some(expected_inline_len) = format::inline_payload_len(key_len, first_overflow_page_id)
        else {
            return Err(PageError::CellTooLarge { len: key_len, max: u16::MAX as usize });
        };
        if inline_payload.len() != expected_inline_len {
            return Err(PageError::CellTooLarge {
                len: INTERIOR_CELL_PREFIX_SIZE + inline_payload.len(),
                max: INTERIOR_CELL_PREFIX_SIZE + expected_inline_len,
            });
        }
        if slot_index > self.slot_count() {
            return Err(PageError::InvalidSlotIndex { slot_index, slot_count: self.slot_count() });
        }

        let cell_len = INTERIOR_CELL_PREFIX_SIZE + inline_payload.len();
        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell_with_payload(
            self.bytes_mut(),
            cell_offset as usize,
            left_child,
            key_len,
            first_overflow_page_id,
            inline_payload,
        );
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }
}
