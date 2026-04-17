use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    PageError, PageResult,
    cell::{Cell, CellRead, CellWrite},
    core::{BoundResult, Interior, Page, PageAccess, PageAccessMut, SearchResult, Table},
    format::{self, RIGHTMOST_CHILD_OFFSET},
};

const PAGE_ID_SIZE: usize = 8;
const ROW_ID_SIZE: usize = 8;
/// The fixed-size size of an interior cell: child id and separator key.
pub const INTERIOR_CELL_SIZE: usize = PAGE_ID_SIZE + ROW_ID_SIZE;

fn encoded_len() -> usize {
    INTERIOR_CELL_SIZE
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, left_child: PageId, row_id: RowId) {
    write_left_child(&mut bytes[cell_offset..cell_offset + INTERIOR_CELL_SIZE], left_child);
    format::write_u64(bytes, cell_offset + PAGE_ID_SIZE, row_id);
}

pub(crate) fn write_left_child(bytes: &mut [u8], page_id: PageId) {
    bytes[..PAGE_ID_SIZE].copy_from_slice(&page_id.to_le_bytes());
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
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<CellRead<'_>, Interior, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Returns the first slot whose separator row id is strictly greater than `row_id`.
    pub fn upper_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| {
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<CellRead<'_>, Interior, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Returns the child page that may contain `row_id`.
    pub fn child_for(&self, row_id: RowId) -> PageResult<PageId> {
        match self.lower_bound(row_id)? {
            BoundResult::At(slot_index) => Ok(self.cell(slot_index)?.left_child()),
            BoundResult::PastEnd => Ok(self.rightmost_child()),
        }
    }

    /// Searches the interior page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.search_slots_by(|page, slot_index| {
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<CellRead<'_>, Interior, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<CellRead<'_>, Interior, Table>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        Cell::<CellRead<'_>, Interior, Table>::new(self.bytes(), cell_offset, slot_index)
    }

    /// Looks up a separator key and returns its cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<Cell<CellRead<'_>, Interior, Table>>> {
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
    pub fn cell_mut(
        &mut self,
        slot_index: SlotId,
    ) -> PageResult<Cell<CellWrite<'_>, Interior, Table>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        Cell::<CellWrite<'_>, Interior, Table>::new(self.bytes_mut(), cell_offset, slot_index)
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
