use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    PageError, PageResult,
    cell::Cell,
    core::{
        BoundResult, Interior, Page, PageAccess, PageAccessMut, Read, SearchResult, Table, Write,
    },
    format::{self, RIGHTMOST_CHILD_OFFSET},
};

const PAGE_ID_SIZE: usize = 8;
const ROW_ID_SIZE: usize = 8;
/// The fixed-size size of an interior cell: child id and separator key.
pub const INTERIOR_CELL_SIZE: usize = PAGE_ID_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy)]
pub(crate) struct InteriorCellParts {
    pub(crate) cell_offset: usize,
    pub(crate) left_child: PageId,
    pub(crate) row_id: RowId,
}

pub(crate) fn cell_parts<A>(
    page: &Page<A, Interior, Table>,
    slot_index: SlotId,
) -> PageResult<InteriorCellParts>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;

    Ok(InteriorCellParts {
        cell_offset,
        left_child: format::read_u64(page.bytes(), cell_offset),
        row_id: format::read_u64(page.bytes(), cell_offset + PAGE_ID_SIZE),
    })
}

fn encoded_len() -> usize {
    INTERIOR_CELL_SIZE
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, left_child: PageId, row_id: RowId) {
    write_left_child(bytes, cell_offset, left_child);
    format::write_u64(bytes, cell_offset + PAGE_ID_SIZE, row_id);
}

pub(crate) fn write_left_child(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, page_id: PageId) {
    format::write_u64(bytes, cell_offset, page_id);
}

impl<A> Page<A, Interior, Table>
where
    A: PageAccess,
{
    /// Returns the page id stored in the rightmost-child header field.
    pub fn rightmost_child(&self) -> PageId {
        format::read_u64(self.bytes(), RIGHTMOST_CHILD_OFFSET)
    }

    /// Returns the first slot whose separator row id is greater than or equal to `row_id`.
    pub fn lower_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| {
            Ok(cell_parts(page, slot_index)?.row_id.cmp(&row_id))
        })
    }

    /// Returns the first slot whose separator row id is strictly greater than `row_id`.
    pub fn upper_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| {
            Ok(cell_parts(page, slot_index)?.row_id.cmp(&row_id))
        })
    }

    /// Returns the child page that may contain `row_id`.
    pub fn child_for(&self, row_id: RowId) -> PageResult<PageId> {
        match self.lower_bound(row_id)? {
            BoundResult::At(slot_index) => Ok(cell_parts(self, slot_index)?.left_child),
            BoundResult::PastEnd => Ok(self.rightmost_child()),
        }
    }

    /// Searches the interior page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.search_slots_by(|page, slot_index| {
            Ok(cell_parts(page, slot_index)?.row_id.cmp(&row_id))
        })
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<Read<'_>, Interior, Table>> {
        cell_parts(self, slot_index)?;
        Ok(Cell::new(Read { bytes: self.bytes() }, slot_index))
    }

    /// Looks up a separator key and returns its cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<Cell<Read<'_>, Interior, Table>>> {
        match self.search(row_id)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> Page<A, Interior, Table>
where
    A: PageAccessMut,
{
    /// Updates the page id stored in the rightmost-child header field.
    pub fn set_rightmost_child(&mut self, page_id: PageId) {
        format::write_u64(self.bytes_mut(), RIGHTMOST_CHILD_OFFSET, page_id);
    }

    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<Cell<Write<'_>, Interior, Table>> {
        let page = Page::<Read<'_>, Interior, Table>::open(self.bytes())?;
        cell_parts(&page, slot_index)?;
        Ok(Cell::new(Write { bytes: self.bytes_mut() }, slot_index))
    }

    /// Inserts a new separator key and its left-child pointer while preserving slot order.
    pub fn insert(&mut self, row_id: RowId, left_child: PageId) -> PageResult<SlotId> {
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey),
            SearchResult::InsertAt(slot_index) => slot_index,
        };

        let cell_offset = self.reserve_space_for_insert(encoded_len())?;
        write_cell(self.bytes_mut(), cell_offset as usize, left_child, row_id);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }

    /// Rewrites the left-child pointer for an existing separator key.
    pub fn update(&mut self, row_id: RowId, left_child: PageId) -> PageResult<()> {
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let cell_offset = self.slot_offset(slot_index)? as usize;
        write_cell(self.bytes_mut(), cell_offset, left_child, row_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_interior_page(rightmost_child: PageId) -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        let _ = Page::<Write<'_>, Interior>::initialize_with_rightmost(&mut bytes, rightmost_child);
        bytes
    }

    #[test]
    fn parses_valid_interior_cell() {
        let mut bytes = new_interior_page(99);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(20, 5).unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.lookup(20).unwrap().unwrap();
        assert_eq!(cell.row_id().unwrap(), 20);
        assert_eq!(cell.left_child().unwrap(), 5);
        assert_eq!(page_ref.rightmost_child(), 99);
    }

    #[test]
    fn rejects_interior_cell_that_runs_past_page_end() {
        let mut bytes = new_interior_page(10);
        {
            let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
            page.set_content_start((format::USABLE_SPACE_END - 8) as u16);
            page.insert_slot(0, (format::USABLE_SPACE_END - 8) as u16).unwrap();
        }

        let err = Page::<Read<'_>, Interior>::open(&bytes).unwrap_err();
        assert_eq!(
            err,
            PageError::MalformedPage(super::super::PageCorruption::InteriorCellOutOfBounds)
        );
    }

    #[test]
    fn rightmost_child_accessors_round_trip() {
        let mut bytes = new_interior_page(7);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        assert_eq!(page.rightmost_child(), 7);
        assert_eq!(page.prev_page_id(), None);
        assert_eq!(page.next_page_id(), None);

        page.set_prev_page_id(Some(5));
        page.set_next_page_id(Some(9));
        page.set_rightmost_child(88);

        assert_eq!(page.rightmost_child(), 88);
        assert_eq!(page.prev_page_id(), Some(5));
        assert_eq!(page.next_page_id(), Some(9));
        assert_eq!(page.as_ref().rightmost_child(), 88);
        assert_eq!(page.as_ref().prev_page_id(), Some(5));
        assert_eq!(page.as_ref().next_page_id(), Some(9));
    }

    #[test]
    fn bounds_return_past_end_on_empty_page() {
        let bytes = new_interior_page(7);
        let page = Page::<Read<'_>, Interior>::open(&bytes).unwrap();

        assert_eq!(page.lower_bound(10).unwrap(), BoundResult::PastEnd);
        assert_eq!(page.upper_bound(10).unwrap(), BoundResult::PastEnd);
    }

    #[test]
    fn bounds_locate_exact_and_insertion_positions() {
        let mut bytes = new_interior_page(90);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(10, 1).unwrap();
        page.insert(20, 2).unwrap();
        page.insert(30, 3).unwrap();

        let page = page.as_ref();
        assert_eq!(page.lower_bound(5).unwrap(), BoundResult::At(0));
        assert_eq!(page.upper_bound(5).unwrap(), BoundResult::At(0));
        assert_eq!(page.lower_bound(20).unwrap(), BoundResult::At(1));
        assert_eq!(page.upper_bound(20).unwrap(), BoundResult::At(2));
        assert_eq!(page.lower_bound(25).unwrap(), BoundResult::At(2));
        assert_eq!(page.upper_bound(25).unwrap(), BoundResult::At(2));
        assert_eq!(page.lower_bound(30).unwrap(), BoundResult::At(2));
        assert_eq!(page.upper_bound(30).unwrap(), BoundResult::PastEnd);
        assert_eq!(page.lower_bound(99).unwrap(), BoundResult::PastEnd);
        assert_eq!(page.upper_bound(99).unwrap(), BoundResult::PastEnd);
    }

    #[test]
    fn child_for_uses_first_separator_greater_than_or_equal_to_target() {
        let mut bytes = new_interior_page(90);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(10, 1).unwrap();
        page.insert(20, 2).unwrap();
        page.insert(30, 3).unwrap();

        let page = page.as_ref();
        assert_eq!(page.child_for(5).unwrap(), 1);
        assert_eq!(page.child_for(10).unwrap(), 1);
        assert_eq!(page.child_for(15).unwrap(), 2);
        assert_eq!(page.child_for(20).unwrap(), 2);
        assert_eq!(page.child_for(25).unwrap(), 3);
        assert_eq!(page.child_for(30).unwrap(), 3);
        assert_eq!(page.child_for(35).unwrap(), 90);
    }

    #[test]
    fn child_for_returns_rightmost_child_when_there_are_no_separators() {
        let bytes = new_interior_page(77);
        let page = Page::<Read<'_>, Interior>::open(&bytes).unwrap();

        assert_eq!(page.child_for(0).unwrap(), 77);
        assert_eq!(page.child_for(99).unwrap(), 77);
    }

    #[test]
    fn lower_bound_agrees_with_search_result() {
        let mut bytes = new_interior_page(90);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(10, 1).unwrap();
        page.insert(20, 2).unwrap();
        page.insert(30, 3).unwrap();

        let page = page.as_ref();
        for key in [5, 10, 15, 20, 25, 30, 35] {
            match page.search(key).unwrap() {
                SearchResult::Found(slot) | SearchResult::InsertAt(slot) => {
                    let expected = if slot == page.slot_count() {
                        BoundResult::PastEnd
                    } else {
                        BoundResult::At(slot)
                    };
                    assert_eq!(page.lower_bound(key).unwrap(), expected, "key {key}");
                }
            }
        }
    }

    #[test]
    fn insert_keeps_separator_order_sorted() {
        let mut bytes = new_interior_page(90);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(40, 4).unwrap();
        page.insert(10, 1).unwrap();
        page.insert(25, 2).unwrap();

        let page = page.as_ref();
        assert_eq!(page.cell(0).unwrap().row_id().unwrap(), 10);
        assert_eq!(page.cell(1).unwrap().row_id().unwrap(), 25);
        assert_eq!(page.cell(2).unwrap().row_id().unwrap(), 40);
        assert_eq!(page.rightmost_child(), 90);
    }

    #[test]
    fn update_changes_left_child_for_existing_separator() {
        let mut bytes = new_interior_page(9);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(50, 5).unwrap();

        page.update(50, 77).unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.lookup(50).unwrap().unwrap();
        assert_eq!(cell.left_child().unwrap(), 77);
    }

    #[test]
    fn mutable_cell_view_updates_left_child() {
        let mut bytes = new_interior_page(9);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.insert(50, 5).unwrap();

        {
            let mut cell = page.cell_mut(0).unwrap();
            cell.set_left_child(66).unwrap();
        }

        let page_ref = page.as_ref();
        let cell = page_ref.lookup(50).unwrap().unwrap();
        assert_eq!(cell.left_child().unwrap(), 66);
    }

    #[test]
    fn update_returns_not_found_for_missing_separator() {
        let mut bytes = new_interior_page(1);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        let err = page.update(77, 9).unwrap_err();
        assert_eq!(err, PageError::RowIdNotFound { row_id: 77 });
    }

    #[test]
    fn defragmentation_preserves_order_sibling_pointers_rightmost_child_and_footer() {
        let mut bytes = new_interior_page(444);
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();
        page.set_prev_page_id(Some(111));
        page.set_next_page_id(Some(222));
        page.insert(10, 1).unwrap();
        page.insert(20, 2).unwrap();
        page.insert(30, 3).unwrap();

        let dead_offset = page.reserve_space_for_rewrite(INTERIOR_CELL_SIZE).unwrap();
        write_cell(page.bytes_mut(), dead_offset as usize, 999, 999);
        page.defragment().unwrap();

        let page_ref = page.as_ref();
        assert_eq!(page_ref.rightmost_child(), 444);
        assert_eq!(page_ref.prev_page_id(), Some(111));
        assert_eq!(page_ref.next_page_id(), Some(222));
        assert_eq!(page_ref.cell(0).unwrap().row_id().unwrap(), 10);
        assert_eq!(page_ref.cell(1).unwrap().row_id().unwrap(), 20);
        assert_eq!(page_ref.cell(2).unwrap().row_id().unwrap(), 30);
        assert!(page_ref.bytes()[format::USABLE_SPACE_END..].iter().all(|byte| *byte == 0));
    }
}
