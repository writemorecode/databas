use std::ops::Range;

use crate::{PAGE_SIZE, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    cell::{Cell, CellMut},
    core::{BoundResult, Leaf, Page, PageAccess, PageAccessMut, SearchResult},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
};

const KEY_LENGTH_SIZE: usize = 2;
const VALUE_LENGTH_SIZE: usize = 2;
const KEY_LENGTH_OFFSET: usize = CELL_LENGTH_SIZE;
const VALUE_LENGTH_OFFSET: usize = KEY_LENGTH_OFFSET + KEY_LENGTH_SIZE;
/// The fixed-size prefix of a raw leaf cell: cell length, key length, and value length.
pub const LEAF_CELL_PREFIX_SIZE: usize = CELL_LENGTH_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE;

#[derive(Debug, Clone)]
pub(crate) struct LeafCellParts {
    pub(crate) key_range: Range<usize>,
    pub(crate) value_range: Range<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedLeafCell {
    pub(crate) cell_offset: usize,
    pub(crate) cell_len: usize,
    pub(crate) parts: LeafCellParts,
}

pub(crate) fn cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    let cell_len = format::read_u16(bytes, cell_offset) as usize;
    if cell_len < LEAF_CELL_PREFIX_SIZE {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }
    if cell_offset + cell_len > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }

    let key_len = format::read_u16(bytes, cell_offset + KEY_LENGTH_OFFSET) as usize;
    let value_len = format::read_u16(bytes, cell_offset + VALUE_LENGTH_OFFSET) as usize;
    if LEAF_CELL_PREFIX_SIZE + key_len + value_len != cell_len {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }

    Ok(cell_len)
}

pub(crate) fn cell_parts<A>(page: &Page<A, Leaf>, slot_index: SlotId) -> PageResult<ParsedLeafCell>
where
    A: PageAccess,
{
    page.validate_slot_index(slot_index)?;
    let cell_offset = page.slot_offset(slot_index)? as usize;
    let cell_len = cell_len_at(page.bytes(), slot_index, cell_offset)?;
    let key_len = format::read_u16(page.bytes(), cell_offset + KEY_LENGTH_OFFSET) as usize;
    let value_len = format::read_u16(page.bytes(), cell_offset + VALUE_LENGTH_OFFSET) as usize;
    let key_start = LEAF_CELL_PREFIX_SIZE;
    let value_start = key_start + key_len;
    let value_end = value_start + value_len;

    Ok(ParsedLeafCell {
        cell_offset,
        cell_len,
        parts: LeafCellParts {
            key_range: key_start..value_start,
            value_range: value_start..value_end,
        },
    })
}

fn encoded_len(key_len: usize, value_len: usize) -> PageResult<usize> {
    let len = LEAF_CELL_PREFIX_SIZE + key_len + value_len;
    if key_len > u16::MAX as usize || value_len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    if len > u16::MAX as usize {
        return Err(PageError::CellTooLarge { len, max: u16::MAX as usize });
    }
    Ok(len)
}

fn write_cell(bytes: &mut [u8; PAGE_SIZE], cell_offset: usize, key: &[u8], value: &[u8]) {
    let cell_len = LEAF_CELL_PREFIX_SIZE + key.len() + value.len();
    format::write_u16(bytes, cell_offset, cell_len as u16);
    format::write_u16(bytes, cell_offset + KEY_LENGTH_OFFSET, key.len() as u16);
    format::write_u16(bytes, cell_offset + VALUE_LENGTH_OFFSET, value.len() as u16);
    let key_start = cell_offset + LEAF_CELL_PREFIX_SIZE;
    let value_start = key_start + key.len();
    let value_end = value_start + value.len();
    bytes[key_start..value_start].copy_from_slice(key);
    bytes[value_start..value_end].copy_from_slice(value);
}

fn compare_key<A>(
    page: &Page<A, Leaf>,
    slot_index: SlotId,
    key: &[u8],
) -> PageResult<std::cmp::Ordering>
where
    A: PageAccess,
{
    let parsed = cell_parts(page, slot_index)?;
    let cell_offset = parsed.cell_offset;
    let key_range = parsed.parts.key_range;
    Ok(page.bytes()[cell_offset + key_range.start..cell_offset + key_range.end].cmp(key))
}

impl<A> Page<A, Leaf>
where
    A: PageAccess,
{
    /// Returns the first slot whose key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.lower_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns the first slot whose key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.upper_bound_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Searches the leaf page for `key`.
    pub fn search(&self, key: &[u8]) -> PageResult<SearchResult> {
        self.search_slots_by(|page, slot_index| compare_key(page, slot_index, key))
    }

    /// Returns a typed immutable view of the cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<Cell<'_, Leaf>> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes = &self.bytes()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(Cell::new_leaf(cell_bytes, parsed.parts, slot_index))
    }

    /// Returns page-relative byte ranges for the key and value in one cell.
    pub(crate) fn cell_key_value_ranges(
        &self,
        slot_index: SlotId,
    ) -> PageResult<(Range<usize>, Range<usize>)> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_offset = parsed.cell_offset;
        let key_range =
            cell_offset + parsed.parts.key_range.start..cell_offset + parsed.parts.key_range.end;
        let value_range = cell_offset + parsed.parts.value_range.start
            ..cell_offset + parsed.parts.value_range.end;
        Ok((key_range, value_range))
    }

    /// Looks up a key and returns its cell if present.
    pub fn lookup(&self, key: &[u8]) -> PageResult<Option<Cell<'_, Leaf>>> {
        match self.search(key)? {
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
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<CellMut<'_, Leaf>> {
        let parsed = cell_parts(self, slot_index)?;
        let cell_bytes =
            &mut self.bytes_mut()[parsed.cell_offset..parsed.cell_offset + parsed.cell_len];
        Ok(CellMut::new_leaf(cell_bytes, parsed.parts, slot_index))
    }

    /// Inserts a new `(key, value)` cell while preserving key order.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> PageResult<SlotId> {
        let cell_len = encoded_len(key.len(), value.len())?;
        let slot_index = match self.search(key)? {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey),
            SearchResult::InsertAt(slot_index) => slot_index,
        };

        let cell_offset = self.reserve_space_for_insert(cell_len)?;
        write_cell(self.bytes_mut(), cell_offset as usize, key, value);
        self.insert_slot(slot_index, cell_offset)?;
        Ok(slot_index)
    }

    /// Deletes an existing key/value cell and re-packs the page.
    pub fn delete(&mut self, key: &[u8]) -> PageResult<SlotId> {
        let slot_index = match self.search(key)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let cell_offset = self.slot_offset(slot_index)?;
        let cell_len = self.cell_len(slot_index)?;
        self.remove_slot(slot_index)?;
        self.reclaim_space(cell_offset, cell_len)?;
        Ok(slot_index)
    }

    /// Replaces the value for an existing key.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> PageResult<()> {
        let cell_len = encoded_len(key.len(), value.len())?;
        let slot_index = match self.search(key)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
        };

        let old_len = self.cell_len(slot_index)?;
        if old_len == cell_len {
            let old_offset = self.slot_offset(slot_index)? as usize;
            write_cell(self.bytes_mut(), old_offset, key, value);
            return Ok(());
        }

        let new_offset = self.reserve_space_for_rewrite(cell_len)?;
        let old_offset = self.slot_offset(slot_index)?;
        write_cell(self.bytes_mut(), new_offset as usize, key, value);
        self.set_slot_offset(slot_index, new_offset)?;
        self.reclaim_space(old_offset, old_len)?;
        Ok(())
    }
}
