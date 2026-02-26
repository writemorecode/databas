use std::cmp::Ordering;

use crate::{
    error::{TablePageError, TablePageResult},
    types::{PAGE_SIZE, PageId, RowId},
};

use super::{
    layout::{self, PageSpec, SearchResult, SpaceError},
    read_u64,
};

const INTERIOR_SPEC: PageSpec =
    PageSpec { page_type: layout::INTERIOR_PAGE_TYPE, header_size: layout::INTERIOR_HEADER_SIZE };

const LEFT_CHILD_SIZE: usize = 8;
const ROW_ID_SIZE: usize = 8;
const INTERIOR_CELL_SIZE: usize = LEFT_CHILD_SIZE + ROW_ID_SIZE;

/// Decoded interior cell mapping a separator key to its left child page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InteriorCell {
    /// Child page id for keys less than `row_id` in this separator cell.
    pub(crate) left_child: PageId,
    /// Separator row id for this interior cell.
    pub(crate) row_id: RowId,
}

/// Immutable wrapper over a validated table-interior page.
#[derive(Debug)]
pub(crate) struct TableInteriorPageRef<'a> {
    page: &'a [u8; PAGE_SIZE],
}

/// Mutable wrapper over a validated table-interior page.
#[derive(Debug)]
pub(crate) struct TableInteriorPageMut<'a> {
    page: &'a mut [u8; PAGE_SIZE],
}

impl<'a> TableInteriorPageRef<'a> {
    /// Validates and wraps raw page bytes as an interior page.
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, INTERIOR_SPEC)?;
        Ok(Self { page })
    }

    /// Looks up an interior cell by row id without mutable access.
    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<InteriorCell>> {
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        decode_interior_cell_at_slot(self.page, slot_index).map(Some)
    }

    /// Returns the child page id to descend into for `row_id`.
    ///
    /// This uses the first separator key that is greater than or equal to `row_id`.
    /// If no such separator exists, the page header's rightmost child is returned.
    pub(crate) fn child_for_row_id(&self, row_id: RowId) -> TablePageResult<PageId> {
        let slot_count = usize::from(self.cell_count());
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => Some(slot_index),
            SearchResult::NotFound(insertion_index) => {
                (usize::from(insertion_index) < slot_count).then_some(insertion_index)
            }
        };

        if let Some(slot_index) = slot_index {
            return Ok(decode_interior_cell_at_slot(self.page, slot_index)?.left_child);
        }

        Ok(self.rightmost_child())
    }

    /// Returns the number of slot entries currently stored on the page.
    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    /// Returns the page header's rightmost child pointer.
    pub(crate) fn rightmost_child(&self) -> PageId {
        layout::read_u64_at(self.page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET)
    }

    /// Returns free bytes between the slot directory and cell-content region.
    pub(crate) fn free_space(&self) -> TablePageResult<usize> {
        layout::free_space(self.page, INTERIOR_SPEC)
    }
}

impl<'a> TableInteriorPageMut<'a> {
    /// Initializes an empty interior page and seeds the rightmost child pointer.
    pub(crate) fn init_empty(
        page: &'a mut [u8; PAGE_SIZE],
        rightmost_child: PageId,
    ) -> TablePageResult<Self> {
        layout::init_empty(page, INTERIOR_SPEC)?;
        layout::write_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, rightmost_child);
        Ok(Self { page })
    }

    /// Validates and wraps existing page bytes as a mutable interior page.
    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, INTERIOR_SPEC)?;
        Ok(Self { page })
    }

    /// Immutable row-id lookup convenience method for mutable wrappers.
    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<InteriorCell>> {
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        decode_interior_cell_at_slot(self.page, slot_index).map(Some)
    }

    /// Returns the child page id to descend into for `row_id`.
    pub(crate) fn child_for_row_id(&self, row_id: RowId) -> TablePageResult<PageId> {
        let slot_count = usize::from(self.cell_count());
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => Some(slot_index),
            SearchResult::NotFound(insertion_index) => {
                (usize::from(insertion_index) < slot_count).then_some(insertion_index)
            }
        };

        if let Some(slot_index) = slot_index {
            return Ok(decode_interior_cell_at_slot(self.page, slot_index)?.left_child);
        }

        Ok(self.rightmost_child())
    }

    /// Returns the number of slot entries currently stored on the page.
    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    /// Returns free bytes between the slot directory and cell-content region.
    pub(crate) fn free_space(&self) -> TablePageResult<usize> {
        layout::free_space(self.page, INTERIOR_SPEC)
    }

    /// Inserts a new `(left_child, row_id)` cell in sorted order.
    ///
    /// Fails with [`TablePageError::DuplicateRowId`] if `row_id` already exists.
    pub(crate) fn insert(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let insertion_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId(row_id)),
            SearchResult::NotFound(insertion_index) => insertion_index,
        };

        let cell = encode_interior_cell(left_child, row_id);
        let cell_offset = insert_interior_cell(self.page, &cell)?;
        layout::insert_slot(self.page, INTERIOR_SPEC, insertion_index, cell_offset)
    }

    /// Replaces the `left_child` pointer for an existing separator key.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when `row_id` is absent.
    pub(crate) fn update(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
        };

        let existing_cell = layout::cell_bytes_at_slot(self.page, INTERIOR_SPEC, slot_index)?;
        interior_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;

        let cell_offset = usize::from(layout::slot_offset(self.page, INTERIOR_SPEC, slot_index)?);
        let cell = encode_interior_cell(left_child, row_id);
        let cell_end = cell_offset + INTERIOR_CELL_SIZE;
        self.page[cell_offset..cell_end].copy_from_slice(&cell);
        Ok(())
    }

    /// Deletes the interior cell identified by `row_id`.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when `row_id` is absent.
    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index = match find_interior_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
        };

        layout::remove_slot(self.page, INTERIOR_SPEC, slot_index)
    }

    /// Returns the current rightmost child pointer from the page header.
    pub(crate) fn rightmost_child(&self) -> PageId {
        layout::read_u64_at(self.page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET)
    }

    /// Updates the page header's rightmost child pointer.
    pub(crate) fn set_rightmost_child(&mut self, page_id: PageId) -> TablePageResult<()> {
        layout::validate(self.page, INTERIOR_SPEC)?;
        layout::write_u64_at(self.page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, page_id);
        Ok(())
    }

    /// Compacts live cells toward the page end and rewrites slot offsets.
    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        defragment_interior_page(self.page)
    }
}

/// Decodes and validates the interior cell referenced by `slot_index`.
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

/// Returns the encoded byte length of one interior cell.
fn interior_cell_len(cell: &[u8]) -> TablePageResult<usize> {
    if cell.len() < INTERIOR_CELL_SIZE {
        return Err(TablePageError::CorruptPage("interior cell too short"));
    }

    Ok(INTERIOR_CELL_SIZE)
}

/// Extracts the separator row id from an encoded interior cell.
fn interior_row_id_from_cell(cell: &[u8]) -> TablePageResult<RowId> {
    if cell.len() < INTERIOR_CELL_SIZE {
        return Err(TablePageError::CorruptPage("interior cell too short"));
    }

    Ok(read_u64(cell, LEFT_CHILD_SIZE))
}

/// Performs row-id lookup on interior pages with interior-specific spec and decoder.
fn find_interior_row_id(page: &[u8; PAGE_SIZE], row_id: RowId) -> TablePageResult<SearchResult> {
    layout::validate(page, INTERIOR_SPEC)?;

    let cell_count = usize::from(layout::cell_count(page));
    let mut left = 0usize;
    let mut right = cell_count;

    while left < right {
        let mid = left + ((right - left) / 2);
        let mid_u16 = mid as u16;
        let cell = layout::cell_bytes_at_slot_on_valid_page(page, INTERIOR_SPEC, mid_u16)?;
        let current_row_id = interior_row_id_from_cell(cell)
            .map_err(|_| TablePageError::CorruptCell { slot_index: mid_u16 })?;

        match current_row_id.cmp(&row_id) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(SearchResult::Found(mid_u16)),
        }
    }

    let insertion_index = left as u16;
    Ok(SearchResult::NotFound(insertion_index))
}

/// Encodes `(left_child, row_id)` into the fixed-width interior cell format.
fn encode_interior_cell(left_child: PageId, row_id: RowId) -> [u8; INTERIOR_CELL_SIZE] {
    let mut cell = [0u8; INTERIOR_CELL_SIZE];
    cell[0..LEFT_CHILD_SIZE].copy_from_slice(&left_child.to_le_bytes());
    cell[LEFT_CHILD_SIZE..INTERIOR_CELL_SIZE].copy_from_slice(&row_id.to_le_bytes());
    cell
}

/// Inserts an interior cell, defragmenting once before returning page-full.
fn insert_interior_cell(page: &mut [u8; PAGE_SIZE], cell: &[u8]) -> TablePageResult<u16> {
    if let Ok(offset) = layout::try_append_cell_for_insert(page, INTERIOR_SPEC, cell)? {
        return Ok(offset);
    }

    defragment_interior_page(page)?;

    match layout::try_append_cell_for_insert(page, INTERIOR_SPEC, cell)? {
        Ok(offset) => Ok(offset),
        Err(SpaceError { needed, available }) => {
            Err(TablePageError::PageFull { needed, available })
        }
    }
}

/// Rewrites live interior cells contiguously and refreshes slot offsets.
fn defragment_interior_page(page: &mut [u8; PAGE_SIZE]) -> TablePageResult<()> {
    layout::validate(page, INTERIOR_SPEC)?;

    let rightmost_child = layout::read_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET);
    let cell_count = usize::from(layout::cell_count(page));
    let mut cells = Vec::with_capacity(cell_count);

    for slot in 0..cell_count {
        let slot_u16 = slot as u16;
        let cell = layout::cell_bytes_at_slot_on_valid_page(page, INTERIOR_SPEC, slot_u16)?;
        let cell_len = interior_cell_len(cell)
            .map_err(|_| TablePageError::CorruptCell { slot_index: slot_u16 })?;

        cells.push(cell[..cell_len].to_vec());
    }

    layout::init_empty(page, INTERIOR_SPEC)?;
    layout::write_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, rightmost_child);

    for (slot, cell) in cells.into_iter().enumerate() {
        let slot_u16 = slot as u16;
        let cell_offset = match layout::try_append_cell_for_insert(page, INTERIOR_SPEC, &cell)? {
            Ok(offset) => offset,
            Err(_) => return Err(TablePageError::CorruptPage("cell content underflow")),
        };
        layout::insert_slot(page, INTERIOR_SPEC, slot_u16, cell_offset)?;
    }

    Ok(())
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
            assert_eq!(interior.cell_count(), 0);
            assert_eq!(interior.rightmost_child(), 77);
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
    fn child_for_row_id_routes_by_separator_and_rightmost_fallback() {
        let mut page = initialized_interior_page(400);
        {
            let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();
            interior.insert(10, 100).unwrap();
            interior.insert(20, 200).unwrap();
            interior.insert(30, 300).unwrap();
        }

        let interior_ref = TableInteriorPageRef::from_bytes(&page).unwrap();
        assert_eq!(interior_ref.child_for_row_id(5).unwrap(), 100);
        assert_eq!(interior_ref.child_for_row_id(10).unwrap(), 100);
        assert_eq!(interior_ref.child_for_row_id(15).unwrap(), 200);
        assert_eq!(interior_ref.child_for_row_id(20).unwrap(), 200);
        assert_eq!(interior_ref.child_for_row_id(29).unwrap(), 300);
        assert_eq!(interior_ref.child_for_row_id(30).unwrap(), 300);
        assert_eq!(interior_ref.child_for_row_id(31).unwrap(), 400);
    }

    #[test]
    fn mutable_child_for_row_id_delegates_to_immutable_routing() {
        let mut page = initialized_interior_page(999);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();
        interior.insert(50, 500).unwrap();
        interior.insert(100, 600).unwrap();

        assert_eq!(interior.child_for_row_id(10).unwrap(), 500);
        assert_eq!(interior.child_for_row_id(75).unwrap(), 600);
        assert_eq!(interior.child_for_row_id(101).unwrap(), 999);
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
    fn update_on_full_page_overwrites_in_place() {
        let mut page = initialized_interior_page(123);
        let mut interior = TableInteriorPageMut::from_bytes(&mut page).unwrap();

        let max_cells = max_cell_count();
        for row_id in 0..max_cells {
            interior.insert(row_id as RowId, row_id as PageId).unwrap();
        }

        let free_before = interior.free_space().unwrap();
        assert!(free_before < INTERIOR_CELL_SIZE);

        let target_row_id = (max_cells / 2) as RowId;
        interior.update(target_row_id, 777_777).unwrap();

        assert_eq!(interior.free_space().unwrap(), free_before);
        assert_eq!(interior.search(target_row_id).unwrap().unwrap().left_child, 777_777);
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
        assert!(interior.free_space().unwrap() < INTERIOR_CELL_SIZE + 2);

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
