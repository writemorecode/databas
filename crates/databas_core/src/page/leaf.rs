use crate::types::{PAGE_SIZE, RowId};

use super::{
    CellCorruption, PageError, PageResult,
    cell::Cell,
    core::{Leaf, Page, PageAccess, PageAccessMut, Read, SearchResult, Write},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
};

const ROW_ID_SIZE: usize = 8;
/// The fixed-size prefix of a leaf cell: encoded length plus row id.
pub const LEAF_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeafCellParts {
    pub(crate) row_id: RowId,
    pub(crate) payload_start: usize,
    pub(crate) payload_end: usize,
}

pub(crate) fn cell_parts<A>(page: &Page<A, Leaf>, slot_index: u16) -> PageResult<LeafCellParts>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = page.cell_len(slot_index)?;
    if cell_len < LEAF_CELL_PREFIX_SIZE {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }

    let payload_start = cell_offset + LEAF_CELL_PREFIX_SIZE;
    let payload_end = cell_offset + cell_len;
    if payload_end > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }

    Ok(LeafCellParts {
        row_id: format::read_u64(page.bytes(), cell_offset + CELL_LENGTH_SIZE),
        payload_start,
        payload_end,
    })
}

fn encoded_len(payload_len: usize) -> PageResult<usize> {
    let len = LEAF_CELL_PREFIX_SIZE + payload_len;
    if len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    Ok(len)
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, row_id: RowId, payload: &[u8]) {
    let cell_len = LEAF_CELL_PREFIX_SIZE + payload.len();
    format::write_u16(bytes, cell_offset, cell_len as u16);
    format::write_u64(bytes, cell_offset + CELL_LENGTH_SIZE, row_id);
    bytes[cell_offset + LEAF_CELL_PREFIX_SIZE..cell_offset + cell_len].copy_from_slice(payload);
}

impl<A> Page<A, Leaf>
where
    A: PageAccess,
{
    /// Searches the leaf page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.search_slots_by(row_id, |page, slot_index| Ok(cell_parts(page, slot_index)?.row_id))
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: u16) -> PageResult<Cell<Read<'_>, Leaf>> {
        cell_parts(self, slot_index)?;
        Ok(Cell::new(Read { bytes: self.bytes() }, slot_index))
    }

    /// Looks up a row id and returns its cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<Cell<Read<'_>, Leaf>>> {
        match self.search(row_id)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> Page<A, Leaf>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: u16) -> PageResult<Cell<Write<'_>, Leaf>> {
        let page = Page::<Read<'_>, Leaf>::open(self.bytes())?;
        cell_parts(&page, slot_index)?;
        Ok(Cell::new(Write { bytes: self.bytes_mut() }, slot_index))
    }

    /// Inserts a new `(row_id, payload)` record while preserving slot order.
    pub fn insert(&mut self, row_id: RowId, payload: &[u8]) -> PageResult<u16> {
        let cell_len = encoded_len(payload.len())?;
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey { key: row_id }),
            SearchResult::InsertAt(slot_index) => slot_index,
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, row_id, payload);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }

    /// Deletes an existing `(row_id, payload)` record and re-packs the page.
    pub fn delete(&mut self, row_id: RowId) -> PageResult<u16> {
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound { key: row_id }),
        };

        self.remove_slot(slot_index)?;
        self.defragment()?;
        Ok(slot_index)
    }

    /// Replaces the payload for an existing `row_id`.
    pub fn update(&mut self, row_id: RowId, payload: &[u8]) -> PageResult<()> {
        let cell_len = encoded_len(payload.len())?;
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound { key: row_id }),
        };

        let old_offset = self.slot_offset(slot_index)? as usize;
        let old_len = self.cell_len(slot_index)?;
        if old_len == cell_len {
            write_cell(self.bytes_mut(), old_offset, row_id, payload);
            return Ok(());
        }

        let new_offset = self.reserve_space_for_rewrite(cell_len)?;
        write_cell(self.bytes_mut(), new_offset as usize, row_id, payload);
        self.set_slot_offset(slot_index, new_offset)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_leaf_page() -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        let _ = Page::<Write<'_>, Leaf>::initialize(&mut bytes);
        bytes
    }

    #[test]
    fn parses_valid_leaf_cell() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(7, b"hello").unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.lookup(7).unwrap().unwrap();
        assert_eq!(cell.row_id().unwrap(), 7);
        assert_eq!(cell.payload().unwrap(), b"hello");
    }

    #[test]
    fn rejects_leaf_cell_with_short_length() {
        let mut bytes = new_leaf_page();
        {
            let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
            page.set_content_start((USABLE_SPACE_END - 4) as u16);
            format::write_u16(page.bytes_mut(), USABLE_SPACE_END - 4, 4);
            page.insert_slot(0, (USABLE_SPACE_END - 4) as u16).unwrap();
        }
        let page = Page::<Read<'_>, Leaf>::open(&bytes).unwrap();
        let err = page.cell(0).unwrap_err();
        assert_eq!(
            err,
            PageError::CorruptCell { slot_index: 0, kind: CellCorruption::LengthTooSmall }
        );
    }

    #[test]
    fn rejects_leaf_cell_with_payload_running_past_page() {
        let mut bytes = new_leaf_page();
        {
            let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
            page.set_content_start((USABLE_SPACE_END - LEAF_CELL_PREFIX_SIZE) as u16);
            format::write_u16(page.bytes_mut(), USABLE_SPACE_END - LEAF_CELL_PREFIX_SIZE, 64);
            page.insert_slot(0, (USABLE_SPACE_END - LEAF_CELL_PREFIX_SIZE) as u16).unwrap();
        }
        let page = Page::<Read<'_>, Leaf>::open(&bytes).unwrap();
        let err = page.cell(0).unwrap_err();
        assert_eq!(
            err,
            PageError::CorruptCell { slot_index: 0, kind: CellCorruption::LengthOutOfBounds }
        );
    }

    #[test]
    fn insert_keeps_slot_order_sorted() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();

        page.insert(20, b"twenty").unwrap();
        page.insert(10, b"ten").unwrap();
        page.insert(30, b"thirty").unwrap();

        let page = page.as_ref();
        assert_eq!(page.cell(0).unwrap().row_id().unwrap(), 10);
        assert_eq!(page.cell(1).unwrap().row_id().unwrap(), 20);
        assert_eq!(page.cell(2).unwrap().row_id().unwrap(), 30);
    }

    #[test]
    fn insert_rejects_duplicate_keys() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();

        page.insert(10, b"ten").unwrap();
        let err = page.insert(10, b"again").unwrap_err();
        assert_eq!(err, PageError::DuplicateKey { key: 10 });
    }

    #[test]
    fn lookup_returns_inserted_payloads() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(5, b"a").unwrap();
        page.insert(15, b"bbb").unwrap();
        page.insert(25, b"cc").unwrap();

        let page_ref = page.as_ref();
        assert_eq!(page_ref.lookup(5).unwrap().unwrap().payload().unwrap(), b"a");
        assert_eq!(page_ref.lookup(15).unwrap().unwrap().payload().unwrap(), b"bbb");
        assert!(page_ref.lookup(99).unwrap().is_none());
    }

    #[test]
    fn update_same_size_overwrites_in_place() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"abc").unwrap();
        let offset_before = page.slot_offset(0).unwrap();

        page.update(10, b"xyz").unwrap();

        assert_eq!(page.slot_offset(0).unwrap(), offset_before);
        let page_ref = page.as_ref();
        assert_eq!(page_ref.lookup(10).unwrap().unwrap().payload().unwrap(), b"xyz");
    }

    #[test]
    fn update_larger_payload_rewrites_at_end() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"a").unwrap();
        let offset_before = page.slot_offset(0).unwrap();

        page.update(10, b"much-larger").unwrap();

        assert_ne!(page.slot_offset(0).unwrap(), offset_before);
        let page_ref = page.as_ref();
        assert_eq!(page_ref.lookup(10).unwrap().unwrap().payload().unwrap(), b"much-larger");
    }

    #[test]
    fn insert_uses_defragmentation_retry_once() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        let filler = [9_u8; 1500];
        page.insert(10, &filler).unwrap();
        page.insert(20, &filler).unwrap();
        page.insert(30, b"small").unwrap();
        page.update(10, b"x").unwrap();
        page.update(20, b"y").unwrap();

        let fragmented_free = page.free_space();
        assert!(fragmented_free < 3000);

        page.insert(25, &[7_u8; 1200]).unwrap();

        let page = page.as_ref();
        assert_eq!(page.cell(0).unwrap().row_id().unwrap(), 10);
        assert_eq!(page.cell(1).unwrap().row_id().unwrap(), 20);
        assert_eq!(page.cell(2).unwrap().row_id().unwrap(), 25);
        assert_eq!(page.cell(3).unwrap().row_id().unwrap(), 30);
    }

    #[test]
    fn update_returns_not_found_for_missing_key() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        let err = page.update(88, b"missing").unwrap_err();
        assert_eq!(err, PageError::KeyNotFound { key: 88 });
    }

    #[test]
    fn delete_returns_removed_slot_index() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"ten").unwrap();
        page.insert(20, b"twenty").unwrap();
        page.insert(30, b"thirty").unwrap();

        let removed = page.delete(20).unwrap();

        assert_eq!(removed, 1);
    }

    #[test]
    fn delete_removes_existing_key_and_preserves_order() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"ten").unwrap();
        page.insert(20, b"twenty").unwrap();
        page.insert(30, b"thirty").unwrap();

        page.delete(20).unwrap();

        let page = page.as_ref();
        assert!(page.lookup(20).unwrap().is_none());
        assert_eq!(page.search(20).unwrap(), SearchResult::InsertAt(1));
        assert_eq!(page.cell(0).unwrap().row_id().unwrap(), 10);
        assert_eq!(page.cell(1).unwrap().row_id().unwrap(), 30);
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();

        let err = page.delete(99).unwrap_err();

        assert_eq!(err, PageError::KeyNotFound { key: 99 });
    }

    #[test]
    fn delete_from_single_cell_page_restores_empty_layout() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        let empty_free = page.free_space();
        page.insert(10, b"abc").unwrap();

        let removed = page.delete(10).unwrap();

        assert_eq!(removed, 0);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.content_start(), USABLE_SPACE_END as u16);
        assert_eq!(page.free_space(), empty_free);
        assert!(page.lookup(10).unwrap().is_none());
    }

    #[test]
    fn delete_reclaims_fragmented_space_and_keeps_survivors_readable() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        let filler = [7_u8; 900];
        page.insert(10, &filler).unwrap();
        page.insert(20, &filler).unwrap();
        page.insert(30, &filler).unwrap();
        page.update(20, b"tiny").unwrap();
        let fragmented_free = page.free_space();

        page.delete(10).unwrap();

        assert!(page.free_space() > fragmented_free);
        let page = page.as_ref();
        assert!(page.lookup(10).unwrap().is_none());
        assert_eq!(page.lookup(20).unwrap().unwrap().payload().unwrap(), b"tiny");
        assert_eq!(page.lookup(30).unwrap().unwrap().payload().unwrap(), filler);
    }

    #[test]
    fn delete_produces_canonical_packed_layout() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"abc").unwrap();
        page.insert(20, b"longer-payload").unwrap();
        page.insert(30, b"z").unwrap();
        page.update(20, b"x").unwrap();

        page.delete(10).unwrap();

        let page = Page::<Read<'_>, Leaf>::open(&bytes).unwrap();
        assert_eq!(page.slot_count(), 2);
        let offset0 = page.slot_offset(0).unwrap() as usize;
        let len0 = page.cell_len(0).unwrap();
        let offset1 = page.slot_offset(1).unwrap() as usize;
        let len1 = page.cell_len(1).unwrap();
        assert_eq!(offset0, page.content_start() as usize);
        assert_eq!(offset1, offset0 + len0);
        assert_eq!(offset1 + len1, USABLE_SPACE_END);
        assert!(page.bytes()[USABLE_SPACE_END..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn update_returns_page_full_after_defrag() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        let large = [1_u8; 1900];
        page.insert(10, &large).unwrap();
        page.insert(20, &large).unwrap();
        page.update(10, b"x").unwrap();

        let err = page.update(20, &[2_u8; 2500]).unwrap_err();
        assert!(matches!(err, PageError::PageFull { .. }));
    }

    #[test]
    fn mutable_cell_view_exposes_payload_slice() {
        let mut bytes = new_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert(10, b"abc").unwrap();

        {
            let mut cell = page.cell_mut(0).unwrap();
            cell.payload_mut().unwrap().copy_from_slice(b"xyz");
        }

        let page_ref = page.as_ref();
        assert_eq!(page_ref.lookup(10).unwrap().unwrap().payload().unwrap(), b"xyz");
    }
}
