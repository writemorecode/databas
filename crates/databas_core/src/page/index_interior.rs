use std::cmp::Ordering;

use crate::{PAGE_SIZE, PageId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    cell::{Cell, CellMut},
    core::{BoundResult, Index, Interior, Page, PageAccess, PageAccessMut},
    format::{self, CELL_LENGTH_SIZE, RIGHTMOST_CHILD_OFFSET, USABLE_SPACE_END},
};

const PAGE_ID_SIZE: usize = 8;
/// The fixed-size prefix of an index interior cell: encoded length plus left-child page id.
pub const INDEX_INTERIOR_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + PAGE_ID_SIZE;

#[derive(Debug, Clone)]
pub(crate) struct IndexInteriorCellParts {
    pub(crate) left_child: PageId,
    pub(crate) payload_range: std::ops::Range<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedIndexInteriorCell {
    pub(crate) cell_offset: usize,
    pub(crate) cell_len: usize,
    pub(crate) parts: IndexInteriorCellParts,
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
) -> PageResult<ParsedIndexInteriorCell>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = cell_len_at(page.bytes(), slot_index, cell_offset)?;

    Ok(ParsedIndexInteriorCell {
        cell_offset,
        cell_len,
        parts: IndexInteriorCellParts {
            left_child: format::read_u64(page.bytes(), cell_offset + CELL_LENGTH_SIZE),
            payload_range: INDEX_INTERIOR_CELL_PREFIX_SIZE..cell_len,
        },
    })
}

pub(crate) fn write_left_child(bytes: &mut [u8], page_id: PageId) {
    bytes[CELL_LENGTH_SIZE..CELL_LENGTH_SIZE + PAGE_ID_SIZE]
        .copy_from_slice(&page_id.to_le_bytes());
}

fn encoded_len(key_len: usize) -> PageResult<usize> {
    let len = INDEX_INTERIOR_CELL_PREFIX_SIZE + key_len;
    if len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    Ok(len)
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, left_child: PageId, key: &[u8]) {
    let cell_len = INDEX_INTERIOR_CELL_PREFIX_SIZE + key.len();
    format::write_u16(bytes, cell_offset, cell_len as u16);
    write_left_child(&mut bytes[cell_offset..cell_offset + cell_len], left_child);
    bytes[cell_offset + INDEX_INTERIOR_CELL_PREFIX_SIZE..cell_offset + cell_len]
        .copy_from_slice(key);
}

fn compare_key<A>(
    page: &Page<A, Interior, Index>,
    slot_index: SlotId,
    key: &[u8],
) -> PageResult<Ordering>
where
    A: PageAccess,
{
    let parsed = cell_parts(page, slot_index)?;
    let cell_offset = parsed.cell_offset;
    let payload_range = parsed.parts.payload_range;
    Ok(page.bytes()[cell_offset + payload_range.start..cell_offset + payload_range.end].cmp(key))
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

    /// Returns the first slot whose separator key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the child page that may contain `key`.
    pub fn child_for(&self, key: &[u8]) -> PageResult<PageId> {
        match self.lower_bound(key)? {
            BoundResult::At(slot_index) => Ok(cell_parts(self, slot_index)?.parts.left_child),
            BoundResult::PastEnd => Ok(self.rightmost_child()),
        }
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Interior, Index>> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes = &self.bytes()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(Cell::new_index_interior(cell_bytes, parsed.parts, slot_index))
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
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes =
            &mut self.bytes_mut()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(CellMut::new_index_interior(cell_bytes, parsed.parts, slot_index))
    }

    /// Inserts a new separator key and its left-child pointer while preserving slot order.
    pub fn insert(&mut self, key: &[u8], left_child: PageId) -> PageResult<SlotId> {
        let cell_len = encoded_len(key.len())?;
        let slot_index = match self.upper_bound(key)? {
            BoundResult::At(slot_index) => slot_index,
            BoundResult::PastEnd => self.slot_count(),
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, left_child, key);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{Read, Write};

    fn new_index_interior_page(rightmost_child: PageId) -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        let _ = Page::<Write<'_>, Interior, Index>::init(&mut bytes, rightmost_child);
        bytes
    }

    #[test]
    fn parses_valid_index_interior_cell() {
        let mut bytes = new_index_interior_page(99);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(b"mango", 5).unwrap();

        let page = page.as_ref();
        let cell = page.cell(0).unwrap();
        assert_eq!(cell.payload().unwrap(), b"mango");
        assert_eq!(cell.left_child().unwrap(), 5);
        assert_eq!(page.rightmost_child(), 99);
    }

    #[test]
    fn rightmost_child_accessors_round_trip() {
        let mut bytes = new_index_interior_page(7);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        assert_eq!(page.rightmost_child(), 7);

        page.set_rightmost_child(88);

        assert_eq!(page.rightmost_child(), 88);
        assert_eq!(page.as_ref().rightmost_child(), 88);
    }

    #[test]
    fn insert_keeps_separator_order_and_allows_duplicates() {
        let mut bytes = new_index_interior_page(90);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(b"pear", 4).unwrap();
        page.insert(b"apple", 1).unwrap();
        page.insert(b"mango", 2).unwrap();
        page.insert(b"mango", 3).unwrap();

        let page = page.as_ref();
        assert_eq!(page.cell(0).unwrap().payload().unwrap(), b"apple");
        assert_eq!(page.cell(0).unwrap().left_child().unwrap(), 1);
        assert_eq!(page.cell(1).unwrap().payload().unwrap(), b"mango");
        assert_eq!(page.cell(1).unwrap().left_child().unwrap(), 2);
        assert_eq!(page.cell(2).unwrap().payload().unwrap(), b"mango");
        assert_eq!(page.cell(2).unwrap().left_child().unwrap(), 3);
        assert_eq!(page.cell(3).unwrap().payload().unwrap(), b"pear");
        assert_eq!(page.cell(3).unwrap().left_child().unwrap(), 4);
    }

    #[test]
    fn bounds_return_past_end_on_empty_page() {
        let bytes = new_index_interior_page(7);
        let page = Page::<Read<'_>, Interior, Index>::open(&bytes).unwrap();

        assert_eq!(page.lower_bound(b"banana").unwrap(), BoundResult::PastEnd);
        assert_eq!(page.upper_bound(b"banana").unwrap(), BoundResult::PastEnd);
    }

    #[test]
    fn bounds_cover_exact_in_between_and_duplicate_separator_positions() {
        let mut bytes = new_index_interior_page(90);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(b"apple", 1).unwrap();
        page.insert(b"mango", 2).unwrap();
        page.insert(b"mango", 3).unwrap();
        page.insert(b"pear", 4).unwrap();

        let page = page.as_ref();
        assert_eq!(page.lower_bound(b"aardvark").unwrap(), BoundResult::At(0));
        assert_eq!(page.upper_bound(b"aardvark").unwrap(), BoundResult::At(0));
        assert_eq!(page.lower_bound(b"mango").unwrap(), BoundResult::At(1));
        assert_eq!(page.upper_bound(b"mango").unwrap(), BoundResult::At(3));
        assert_eq!(page.lower_bound(b"orange").unwrap(), BoundResult::At(3));
        assert_eq!(page.upper_bound(b"orange").unwrap(), BoundResult::At(3));
        assert_eq!(page.lower_bound(b"zebra").unwrap(), BoundResult::PastEnd);
        assert_eq!(page.upper_bound(b"zebra").unwrap(), BoundResult::PastEnd);
    }

    #[test]
    fn child_for_routes_by_first_separator_greater_than_or_equal_to_key() {
        let mut bytes = new_index_interior_page(90);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(b"apple", 1).unwrap();
        page.insert(b"mango", 2).unwrap();
        page.insert(b"mango", 3).unwrap();
        page.insert(b"pear", 4).unwrap();

        let page = page.as_ref();
        assert_eq!(page.child_for(b"aardvark").unwrap(), 1);
        assert_eq!(page.child_for(b"apple").unwrap(), 1);
        assert_eq!(page.child_for(b"banana").unwrap(), 2);
        assert_eq!(page.child_for(b"mango").unwrap(), 2);
        assert_eq!(page.child_for(b"orange").unwrap(), 4);
        assert_eq!(page.child_for(b"zebra").unwrap(), 90);
    }

    #[test]
    fn child_for_returns_rightmost_child_on_empty_page() {
        let bytes = new_index_interior_page(77);
        let page = Page::<Read<'_>, Interior, Index>::open(&bytes).unwrap();

        assert_eq!(page.child_for(b"apple").unwrap(), 77);
        assert_eq!(page.child_for(b"zebra").unwrap(), 77);
    }

    #[test]
    fn mutable_cell_view_updates_left_child() {
        let mut bytes = new_index_interior_page(9);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(b"mango", 5).unwrap();

        {
            let mut cell = page.cell_mut(0).unwrap();
            cell.set_left_child(66).unwrap();
        }

        let page = page.as_ref();
        assert_eq!(page.cell(0).unwrap().left_child().unwrap(), 66);
    }

    #[test]
    fn cell_payload_is_sliced_relative_to_cell_start() {
        let mut bytes = new_index_interior_page(9);
        let mut page = Page::<Write<'_>, Interior, Index>::open(&mut bytes).unwrap();
        page.insert(&[5_u8; 64], 1).unwrap();
        page.insert(b"mango", 2).unwrap();

        let page = page.as_ref();
        assert_eq!(page.cell(1).unwrap().payload().unwrap(), b"mango");
    }
}
