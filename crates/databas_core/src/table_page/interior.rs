use crate::{
    error::{TablePageError, TablePageResult},
    types::{PAGE_SIZE, PageId, RowId},
};

use super::layout::{self, PageSpec, SearchResult, SpaceError};

const INTERIOR_SPEC: PageSpec =
    PageSpec { page_type: layout::INTERIOR_PAGE_TYPE, header_size: layout::INTERIOR_HEADER_SIZE };

const LEFT_CHILD_SIZE: usize = 8;
const ROW_ID_SIZE: usize = 8;
const INTERIOR_CELL_SIZE: usize = LEFT_CHILD_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InteriorCell {
    pub(crate) left_child: PageId,
    pub(crate) row_id: RowId,
}

#[derive(Debug)]
pub(crate) struct TableInteriorPageRef<'a> {
    page: &'a [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub(crate) struct TableInteriorPageMut<'a> {
    page: &'a mut [u8; PAGE_SIZE],
}

impl<'a> TableInteriorPageRef<'a> {
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, INTERIOR_SPEC)?;
        Ok(Self { page })
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<InteriorCell>> {
        let slot_index =
            match layout::find_row_id(self.page, INTERIOR_SPEC, row_id, interior_row_id_from_cell)?
            {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Ok(None),
            };

        decode_interior_cell_at_slot(self.page, slot_index).map(Some)
    }

    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    pub(crate) fn rightmost_child(&self) -> PageId {
        layout::read_u64_at(self.page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET)
    }

    pub(crate) fn free_space(&self) -> usize {
        layout::free_space(self.page, INTERIOR_SPEC).expect("interior page must remain valid")
    }
}

impl<'a> TableInteriorPageMut<'a> {
    pub(crate) fn init_empty(
        page: &'a mut [u8; PAGE_SIZE],
        rightmost_child: PageId,
    ) -> TablePageResult<Self> {
        layout::init_empty(page, INTERIOR_SPEC)?;
        layout::write_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, rightmost_child);
        Ok(Self { page })
    }

    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, INTERIOR_SPEC)?;
        Ok(Self { page })
    }

    pub(crate) fn as_ref(&self) -> TableInteriorPageRef<'_> {
        TableInteriorPageRef { page: self.page }
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<InteriorCell>> {
        self.as_ref().search(row_id)
    }

    pub(crate) fn insert(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let insertion_index =
            match layout::find_row_id(self.page, INTERIOR_SPEC, row_id, interior_row_id_from_cell)?
            {
                SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId(row_id)),
                SearchResult::NotFound(insertion_index) => insertion_index,
            };

        let cell = encode_interior_cell(left_child, row_id);
        let cell_offset = write_interior_cell_with_retry(self.page, &cell, 1)?;
        layout::insert_slot(self.page, INTERIOR_SPEC, insertion_index, cell_offset)
    }

    pub(crate) fn update(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, INTERIOR_SPEC, row_id, interior_row_id_from_cell)?
            {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        let cell = encode_interior_cell(left_child, row_id);
        let cell_offset = write_interior_cell_with_retry(self.page, &cell, 0)?;
        layout::set_slot_offset(self.page, INTERIOR_SPEC, slot_index, cell_offset)
    }

    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, INTERIOR_SPEC, row_id, interior_row_id_from_cell)?
            {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        layout::remove_slot(self.page, INTERIOR_SPEC, slot_index)
    }

    pub(crate) fn rightmost_child(&self) -> PageId {
        self.as_ref().rightmost_child()
    }

    pub(crate) fn set_rightmost_child(&mut self, page_id: PageId) -> TablePageResult<()> {
        layout::validate(self.page, INTERIOR_SPEC)?;
        layout::write_u64_at(self.page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, page_id);
        Ok(())
    }

    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        layout::defragment(self.page, INTERIOR_SPEC, interior_cell_len)
    }
}

fn decode_interior_cell_at_slot(
    page: &[u8; PAGE_SIZE],
    slot_index: u16,
) -> TablePageResult<InteriorCell> {
    let cell = layout::cell_bytes_at_slot(page, INTERIOR_SPEC, slot_index)?;
    interior_cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;

    let left_child = read_u64(cell, 0);
    let row_id = read_u64(cell, LEFT_CHILD_SIZE);

    Ok(InteriorCell { left_child, row_id })
}

fn interior_cell_len(cell: &[u8]) -> TablePageResult<usize> {
    if cell.len() < INTERIOR_CELL_SIZE {
        return Err(TablePageError::CorruptPage("interior cell too short"));
    }

    Ok(INTERIOR_CELL_SIZE)
}

fn interior_row_id_from_cell(cell: &[u8]) -> TablePageResult<RowId> {
    if cell.len() < INTERIOR_CELL_SIZE {
        return Err(TablePageError::CorruptPage("interior cell too short"));
    }

    Ok(read_u64(cell, LEFT_CHILD_SIZE))
}

fn encode_interior_cell(left_child: PageId, row_id: RowId) -> [u8; INTERIOR_CELL_SIZE] {
    let mut cell = [0u8; INTERIOR_CELL_SIZE];
    cell[0..LEFT_CHILD_SIZE].copy_from_slice(&left_child.to_le_bytes());
    cell[LEFT_CHILD_SIZE..INTERIOR_CELL_SIZE].copy_from_slice(&row_id.to_le_bytes());
    cell
}

fn write_interior_cell_with_retry(
    page: &mut [u8; PAGE_SIZE],
    cell: &[u8],
    extra_slots: usize,
) -> TablePageResult<u16> {
    if let Ok(offset) = layout::try_append_cell(page, INTERIOR_SPEC, cell, extra_slots)? {
        return Ok(offset);
    }

    layout::defragment(page, INTERIOR_SPEC, interior_cell_len)?;

    match layout::try_append_cell(page, INTERIOR_SPEC, cell, extra_slots)? {
        Ok(offset) => Ok(offset),
        Err(SpaceError { needed, available }) => {
            Err(TablePageError::PageFull { needed, available })
        }
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized_interior_page(rightmost_child: PageId) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        {
            let _interior = TableInteriorPageMut::init_empty(&mut page, rightmost_child).unwrap();
        }
        page
    }

    fn max_cell_count() -> usize {
        (PAGE_SIZE - layout::INTERIOR_HEADER_SIZE) / (INTERIOR_CELL_SIZE + 2)
    }

    #[test]
    fn init_empty_and_from_bytes_validate_page_type() {
        let mut page = [0u8; PAGE_SIZE];
        {
            let interior = TableInteriorPageMut::init_empty(&mut page, 77).unwrap();
            assert_eq!(interior.as_ref().cell_count(), 0);
            assert_eq!(interior.as_ref().rightmost_child(), 77);
        }

        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        assert_eq!(interior_ref.rightmost_child(), 77);

        page[0] = 99;
        let err = TableInteriorPageRef::from_bytes(&page).unwrap_err();
        assert!(matches!(err, TablePageError::InvalidPageType(99)));
    }

    #[test]
    fn search_on_empty_page_returns_none() {
        let page = initialized_interior_page(7);
        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        assert_eq!(interior_ref.search(5).unwrap(), None);
    }

    #[test]
    fn insert_search_and_duplicate_behavior() {
        let mut page = initialized_interior_page(100);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();

        for row_id in [50, 10, 40, 20, 30] {
            interior.insert(row_id, row_id + 1_000).unwrap();
        }

        for row_id in [10, 20, 30, 40, 50] {
            let cell = interior.search(row_id).unwrap().unwrap();
            assert_eq!(cell.row_id, row_id);
            assert_eq!(cell.left_child, row_id + 1_000);
        }

        let err = interior.insert(20, 1).unwrap_err();
        assert!(matches!(err, TablePageError::DuplicateRowId(20)));
    }

    #[test]
    fn update_delete_and_missing_row_id_behavior() {
        let mut page = initialized_interior_page(100);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();

        interior.insert(1, 11).unwrap();
        interior.insert(2, 22).unwrap();

        interior.update(2, 222).unwrap();
        assert_eq!(interior.search(2).unwrap().unwrap().left_child, 222);

        let update_err = interior.update(99, 999).unwrap_err();
        assert!(matches!(update_err, TablePageError::RowIdNotFound(99)));

        interior.delete(1).unwrap();
        assert_eq!(interior.search(1).unwrap(), None);

        let delete_err = interior.delete(1).unwrap_err();
        assert!(matches!(delete_err, TablePageError::RowIdNotFound(1)));
    }

    #[test]
    fn rightmost_child_roundtrip() {
        let mut page = initialized_interior_page(9);
        {
            let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();
            assert_eq!(interior.rightmost_child(), 9);
            interior.set_rightmost_child(42).unwrap();
            assert_eq!(interior.rightmost_child(), 42);
        }

        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        assert_eq!(interior_ref.rightmost_child(), 42);
    }

    #[test]
    fn insert_defrag_retry_path_succeeds() {
        let mut page = initialized_interior_page(123);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();

        let max_cells = max_cell_count();
        for row_id in 0..max_cells {
            interior.insert(row_id as RowId, (row_id as PageId) + 10_000).unwrap();
        }

        interior.delete(100).unwrap();
        assert!(interior.as_ref().free_space() < INTERIOR_CELL_SIZE + 2);

        interior.insert(max_cells as RowId, 99_999).unwrap();
        assert_eq!(interior.search(max_cells as RowId).unwrap().unwrap().left_child, 99_999);
    }

    #[test]
    fn insert_fails_with_page_full_when_no_space_even_after_defrag() {
        let mut page = initialized_interior_page(123);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();

        let max_cells = max_cell_count();
        for row_id in 0..max_cells {
            interior.insert(row_id as RowId, row_id as PageId).unwrap();
        }

        let err = interior.insert(max_cells as RowId, 1).unwrap_err();
        assert!(matches!(err, TablePageError::PageFull { .. }));
    }

    #[test]
    fn corrupt_slot_offset_and_malformed_cell_are_detected() {
        let mut page = initialized_interior_page(55);
        {
            let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();
            interior.insert(1, 11).unwrap();
        }

        let first_slot = layout::INTERIOR_HEADER_SIZE;
        page[first_slot..first_slot + 2].copy_from_slice(&1u16.to_le_bytes());

        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        let err = interior_ref.search(1).unwrap_err();
        assert!(matches!(err, TablePageError::CorruptCell { slot_index: 0 }));

        let mut page = initialized_interior_page(55);
        {
            let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();
            interior.insert(1, 11).unwrap();
        }

        let malformed_offset = (PAGE_SIZE - 8) as u16;
        page[first_slot..first_slot + 2].copy_from_slice(&malformed_offset.to_le_bytes());

        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        let err = interior_ref.search(1).unwrap_err();
        assert!(matches!(err, TablePageError::CorruptCell { slot_index: 0 }));
    }
}
