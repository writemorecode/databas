use crate::{PAGE_SIZE, RowId, SlotId};

use super::{
    PageError, PageResult,
    cell::{Cell, CellMut},
    core::{BoundResult, Leaf, Page, PageAccess, PageAccessMut, SearchResult, Table},
    format::{self, CELL_LENGTH_SIZE},
};

const ROW_ID_SIZE: usize = 8;
/// The fixed-size prefix of a leaf cell: encoded length plus row id.
pub const LEAF_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + ROW_ID_SIZE;

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

impl<A> Page<A, Leaf, Table>
where
    A: PageAccess,
{
    /// Returns the first slot whose row id is greater than or equal to `row_id`.
    pub fn lower_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| {
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<Leaf, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Returns the first slot whose row id is strictly greater than `row_id`.
    pub fn upper_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| {
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<Leaf, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Searches the leaf page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.search_slots_by(|page, slot_index| {
            let cell_offset = page.slot_offset(slot_index)? as usize;
            Ok(Cell::<Leaf, Table>::new(page.bytes(), cell_offset, slot_index)?
                .row_id()
                .cmp(&row_id))
        })
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Leaf, Table>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        Cell::<Leaf, Table>::new(self.bytes(), cell_offset, slot_index)
    }

    /// Looks up a row id and returns its cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<Cell<'_, Leaf, Table>>> {
        match self.search(row_id)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> Page<A, Leaf, Table>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<CellMut<'_, Leaf, Table>> {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        CellMut::<Leaf, Table>::new(self.bytes_mut(), cell_offset, slot_index)
    }

    /// Inserts a new `(row_id, payload)` record while preserving slot order.
    pub fn insert(&mut self, row_id: RowId, payload: &[u8]) -> PageResult<SlotId> {
        let cell_len = encoded_len(payload.len())?;
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey),
            SearchResult::InsertAt(slot_index) => slot_index,
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, row_id, payload);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }

    /// Deletes an existing `(row_id, payload)` record and re-packs the page.
    pub fn delete(&mut self, row_id: RowId) -> PageResult<SlotId> {
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let cell_offset = self.slot_offset(slot_index)?;
        let cell_len = self.cell_len(slot_index)?;
        self.remove_slot(slot_index)?;
        self.reclaim_space(cell_offset, cell_len)?;
        Ok(slot_index)
    }

    /// Replaces the payload for an existing `row_id`.
    pub fn update(&mut self, row_id: RowId, payload: &[u8]) -> PageResult<()> {
        let cell_len = encoded_len(payload.len())?;
        let slot_index = match self.search(row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let old_len = self.cell_len(slot_index)?;
        if old_len == cell_len {
            let old_offset = self.slot_offset(slot_index)? as usize;
            write_cell(self.bytes_mut(), old_offset, row_id, payload);
            return Ok(());
        }

        let new_offset = self.reserve_space_for_rewrite(cell_len)?;
        // Reserving rewrite space may defragment the page and rewrite slot offsets,
        // so re-read the old cell location before reclaiming it.
        let old_offset = self.slot_offset(slot_index)?;
        write_cell(self.bytes_mut(), new_offset as usize, row_id, payload);
        self.set_slot_offset(slot_index, new_offset)?;
        self.reclaim_space(old_offset, old_len)?;
        Ok(())
    }
}
