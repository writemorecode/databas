use crate::{
    table_page::{TablePageCorruptionKind, TablePageError, TablePageResult},
    types::PageId,
};
use std::cmp::Ordering;

use crate::types::{PAGE_SIZE, RowId};

use super::{
    layout::{self, PageSpec, SearchResult},
    read_u64,
};

const LEAF_SPEC: PageSpec =
    PageSpec { page_type: layout::LEAF_PAGE_TYPE, header_size: layout::LEAF_HEADER_SIZE };

const PAYLOAD_LEN_SIZE: usize = 2;
const ROW_ID_SIZE: usize = 8;
const LEAF_CELL_PREFIX_SIZE: usize = PAYLOAD_LEN_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy)]
struct LeafUpdateAllocation {
    offset: u16,
    old_cell_reclaimed: bool,
}

/// Borrowed view of one leaf cell decoded from a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeafCellRef<'a> {
    /// Row identifier used as the key inside the page.
    pub(crate) row_id: RowId,
    /// Borrowed payload bytes for the row.
    pub(crate) payload: &'a [u8],
}

impl<'a> LeafCellRef<'a> {
    /// Deserializes and validates the leaf cell referenced by `slot_index`.
    pub(crate) fn try_deserialize_at_slot(
        page: &'a [u8; PAGE_SIZE],
        slot_index: u16,
    ) -> TablePageResult<Self> {
        let cell = layout::cell_bytes_at_slot(page, LEAF_SPEC, slot_index)?;
        let cell_len =
            leaf_cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;

        let payload_len = usize::from(read_u16(cell, 0));
        let row_id = read_u64(cell, PAYLOAD_LEN_SIZE);
        let payload_start = LEAF_CELL_PREFIX_SIZE;
        let payload_end = payload_start + payload_len;

        debug_assert!(payload_end <= cell_len);

        Ok(Self { row_id, payload: &cell[payload_start..payload_end] })
    }
}

/// Immutable wrapper over a validated table-leaf page.
#[derive(Debug)]
pub(crate) struct TableLeafPageRef<'a> {
    page: &'a [u8; PAGE_SIZE],
}

/// Mutable wrapper over a validated table-leaf page.
#[derive(Debug)]
pub(crate) struct TableLeafPageMut<'a> {
    page: &'a mut [u8; PAGE_SIZE],
}

impl<'a> TableLeafPageRef<'a> {
    /// Validates and wraps raw page bytes as a leaf page.
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    /// Looks up a cell by row id without requiring mutable access.
    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<LeafCellRef<'a>>> {
        let slot_index = match find_leaf_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        LeafCellRef::try_deserialize_at_slot(self.page, slot_index).map(Some)
    }

    /// Returns the number of slot entries currently stored on the page.
    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    /// Returns total reusable space on the page, including the unallocated gap,
    /// freeblocks, and fragmented bytes.
    pub(crate) fn free_space(&self) -> TablePageResult<usize> {
        layout::free_space(self.page, LEAF_SPEC)
    }

    /// Returns the previous leaf sibling page id, if any.
    pub(crate) fn prev_sibling(&self) -> Option<PageId> {
        layout::prev_sibling(self.page)
    }

    /// Returns the next leaf sibling page id, if any.
    pub(crate) fn next_sibling(&self) -> Option<PageId> {
        layout::next_sibling(self.page)
    }
}

impl<'a> TableLeafPageMut<'a> {
    /// Initializes an empty leaf page in-place and returns a mutable wrapper.
    pub(crate) fn init_empty(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::init_empty(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    /// Validates and wraps existing page bytes as a mutable leaf page.
    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    /// Immutable row-id lookup convenience method for mutable wrappers.
    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<LeafCellRef<'_>>> {
        let slot_index = match find_leaf_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        LeafCellRef::try_deserialize_at_slot(self.page, slot_index).map(Some)
    }

    /// Returns the number of slot entries currently stored on the page.
    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    /// Returns total reusable space on the page, including the unallocated gap,
    /// freeblocks, and fragmented bytes.
    pub(crate) fn free_space(&self) -> TablePageResult<usize> {
        layout::free_space(self.page, LEAF_SPEC)
    }

    /// Inserts a new cell keyed by `row_id`, preserving sorted slot order.
    ///
    /// Fails with [`TablePageError::DuplicateRowId`] if the key already exists.
    pub(crate) fn insert(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let insertion_index = match find_leaf_row_id(self.page, row_id)? {
            SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId { row_id }),
            SearchResult::NotFound(insertion_index) => insertion_index,
        };

        let cell_offset = insert_leaf_cell(self.page, row_id, payload)?;
        layout::insert_slot(self.page, LEAF_SPEC, insertion_index, cell_offset)
    }

    /// Replaces the payload for an existing row id.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when the key is absent.
    pub(crate) fn update(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let slot_index = match find_leaf_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let mut existing_cell = layout::cell_bytes_at_slot(self.page, LEAF_SPEC, slot_index)?;
        let mut existing_len =
            leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let new_len = leaf_cell_encoded_len(payload)?;
        let mut cell_offset = layout::slot_offset(self.page, LEAF_SPEC, slot_index)?;

        if new_len < existing_len {
            let released_tail = existing_len - new_len;
            if released_tail <= 3
                && layout::fragmented_free_bytes(self.page) + (released_tail as u8) > 60
            {
                defragment_leaf_page(self.page)?;
                existing_cell = layout::cell_bytes_at_slot(self.page, LEAF_SPEC, slot_index)?;
                existing_len = leaf_cell_len(existing_cell)
                    .map_err(|_| TablePageError::CorruptCell { slot_index })?;
                cell_offset = layout::slot_offset(self.page, LEAF_SPEC, slot_index)?;
            }
        }

        if existing_len == new_len {
            write_leaf_cell_at(self.page, usize::from(cell_offset), row_id, payload)?;
            return Ok(());
        }

        if new_len < existing_len {
            let cell_offset = usize::from(cell_offset);
            write_leaf_cell_at(self.page, cell_offset, row_id, payload)?;
            layout::release_space(
                self.page,
                LEAF_SPEC,
                (cell_offset + new_len) as u16,
                existing_len - new_len,
            )?;
            return Ok(());
        }

        let replacement = update_leaf_cell(self.page, slot_index, row_id, payload)?;
        layout::set_slot_offset(self.page, LEAF_SPEC, slot_index, replacement.offset)?;
        if replacement.old_cell_reclaimed {
            return Ok(());
        }
        layout::release_space(self.page, LEAF_SPEC, cell_offset, existing_len)
    }

    /// Deletes the cell for `row_id`.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when the key is absent.
    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index = match find_leaf_row_id(self.page, row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let existing_cell = layout::cell_bytes_at_slot(self.page, LEAF_SPEC, slot_index)?;
        let existing_len =
            leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let cell_offset = layout::slot_offset(self.page, LEAF_SPEC, slot_index)?;

        layout::remove_slot(self.page, LEAF_SPEC, slot_index)?;
        layout::release_space(self.page, LEAF_SPEC, cell_offset, existing_len)
    }

    /// Compacts live cells toward the page end and rewrites slot offsets.
    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        defragment_leaf_page(self.page)
    }
}

/// Returns the encoded byte length of a leaf cell.
fn leaf_cell_len(cell: &[u8]) -> TablePageResult<usize> {
    if cell.len() < LEAF_CELL_PREFIX_SIZE {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellTooShort));
    }

    let payload_len = usize::from(read_u16(cell, 0));
    let cell_len = LEAF_CELL_PREFIX_SIZE + payload_len;

    if cell_len > cell.len() {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellPayloadOutOfBounds));
    }

    Ok(cell_len)
}

/// Performs row-id lookup on leaf pages with leaf-specific spec and decoder.
fn find_leaf_row_id(page: &[u8; PAGE_SIZE], row_id: RowId) -> TablePageResult<SearchResult> {
    let cell_count = usize::from(layout::cell_count(page));
    let mut left = 0usize;
    let mut right = cell_count;

    while left < right {
        let mid = left + ((right - left) / 2);
        let mid_u16 = mid as u16;
        let cell = layout::cell_bytes_at_slot_on_valid_page(page, LEAF_SPEC, mid_u16)?;
        if cell.len() < LEAF_CELL_PREFIX_SIZE {
            return Err(TablePageError::CorruptCell { slot_index: mid_u16 });
        }
        let current_row_id = read_u64(cell, PAYLOAD_LEN_SIZE);

        match current_row_id.cmp(&row_id) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(SearchResult::Found(mid_u16)),
        }
    }

    let insertion_index = left as u16;
    Ok(SearchResult::NotFound(insertion_index))
}

/// Computes the serialized cell length for a payload.
fn leaf_cell_encoded_len(payload: &[u8]) -> TablePageResult<usize> {
    if payload.len() > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge {
            len: payload.len(),
            max: usize::from(u16::MAX),
        });
    }

    Ok(LEAF_CELL_PREFIX_SIZE + payload.len())
}

/// Encodes one leaf cell into owned bytes.
fn encode_leaf_cell(row_id: RowId, payload: &[u8]) -> TablePageResult<Vec<u8>> {
    let cell_len = leaf_cell_encoded_len(payload)?;
    let payload_len = u16::try_from(payload.len()).map_err(|_| TablePageError::CellTooLarge {
        len: payload.len(),
        max: usize::from(u16::MAX),
    })?;

    let mut cell = vec![0u8; cell_len];
    cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
    cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
    cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
    Ok(cell)
}

/// Updates a leaf cell value, defragmenting once before reporting page-full.
fn update_leaf_cell(
    page: &mut [u8; PAGE_SIZE],
    slot_index: u16,
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<LeafUpdateAllocation> {
    let cell = encode_leaf_cell(row_id, payload)?;
    if let Ok(offset) = layout::try_append_cell(page, LEAF_SPEC, &cell)? {
        return Ok(LeafUpdateAllocation { offset, old_cell_reclaimed: false });
    }

    let existing_cell = layout::cell_bytes_at_slot(page, LEAF_SPEC, slot_index)?;
    let existing_len =
        leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
    let space_error = layout::page_full_for_update(page, LEAF_SPEC, cell.len(), existing_len)?;
    if space_error.needed > space_error.available {
        return Err(TablePageError::PageFull {
            needed: space_error.needed,
            available: space_error.available,
        });
    }

    rewrite_leaf_page(page, Some((slot_index, &cell)))?;
    Ok(LeafUpdateAllocation {
        offset: layout::slot_offset(page, LEAF_SPEC, slot_index)?,
        old_cell_reclaimed: true,
    })
}

/// Inserts a leaf cell, defragmenting once before reporting page-full.
fn insert_leaf_cell(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<u16> {
    let cell = encode_leaf_cell(row_id, payload)?;
    if let Ok(offset) = layout::try_append_cell_for_insert(page, LEAF_SPEC, &cell)? {
        return Ok(offset);
    }

    let space_error = layout::page_full_for_insert(page, LEAF_SPEC, cell.len())?;
    if space_error.needed > space_error.available {
        return Err(TablePageError::PageFull {
            needed: space_error.needed,
            available: space_error.available,
        });
    }

    defragment_leaf_page(page)?;

    match layout::try_append_cell_for_insert(page, LEAF_SPEC, &cell)? {
        Ok(offset) => Ok(offset),
        Err(_) => Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellContentUnderflow)),
    }
}

/// Rewrites live leaf cells contiguously and refreshes slot offsets.
fn defragment_leaf_page(page: &mut [u8; PAGE_SIZE]) -> TablePageResult<()> {
    rewrite_leaf_page(page, None)
}

fn copy_leaf_cell_into_scratch(
    cell: &[u8],
    scratch: &mut [u8; PAGE_SIZE],
    scratch_len: &mut usize,
) -> TablePageResult<()> {
    let next = *scratch_len + cell.len();
    if next > scratch.len() {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellContentUnderflow));
    }

    scratch[*scratch_len..next].copy_from_slice(cell);
    *scratch_len = next;
    Ok(())
}

fn copy_leaf_slot_into_scratch(
    page: &[u8; PAGE_SIZE],
    slot_index: u16,
    scratch: &mut [u8; PAGE_SIZE],
    scratch_len: &mut usize,
) -> TablePageResult<()> {
    let cell = layout::cell_bytes_at_slot_on_valid_page(page, LEAF_SPEC, slot_index)?;
    let cell_len = leaf_cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
    copy_leaf_cell_into_scratch(&cell[..cell_len], scratch, scratch_len)
}

fn rewrite_leaf_page(
    page: &mut [u8; PAGE_SIZE],
    replacement: Option<(u16, &[u8])>,
) -> TablePageResult<()> {
    let prev_sibling = layout::prev_sibling(page);
    let next_sibling = layout::next_sibling(page);
    let cell_count = usize::from(layout::cell_count(page));
    let mut scratch = [0u8; PAGE_SIZE];
    let mut scratch_len = 0usize;

    match replacement {
        None => {
            for slot in 0..cell_count {
                copy_leaf_slot_into_scratch(page, slot as u16, &mut scratch, &mut scratch_len)?;
            }
        }
        Some((replacement_slot_u16, replacement_cell)) => {
            let replacement_slot = usize::from(replacement_slot_u16);
            if replacement_slot >= cell_count {
                for slot in 0..cell_count {
                    copy_leaf_slot_into_scratch(page, slot as u16, &mut scratch, &mut scratch_len)?;
                }
            } else {
                for slot in 0..replacement_slot {
                    copy_leaf_slot_into_scratch(page, slot as u16, &mut scratch, &mut scratch_len)?;
                }

                copy_leaf_cell_into_scratch(replacement_cell, &mut scratch, &mut scratch_len)?;

                for slot in (replacement_slot + 1)..cell_count {
                    copy_leaf_slot_into_scratch(page, slot as u16, &mut scratch, &mut scratch_len)?;
                }
            }
        }
    }

    layout::init_empty(page, LEAF_SPEC)?;
    layout::set_prev_sibling(page, prev_sibling);
    layout::set_next_sibling(page, next_sibling);

    let mut scratch_offset = 0usize;
    for slot in 0..cell_count {
        let slot_u16 = slot as u16;
        let cell_len = leaf_cell_len(&scratch[scratch_offset..scratch_len])
            .map_err(|_| TablePageError::CorruptCell { slot_index: slot_u16 })?;
        let next = scratch_offset + cell_len;
        let cell_offset = match layout::try_append_cell_for_insert(
            page,
            LEAF_SPEC,
            &scratch[scratch_offset..next],
        )? {
            Ok(offset) => offset,
            Err(_) => {
                return Err(TablePageError::CorruptPage(
                    TablePageCorruptionKind::CellContentUnderflow,
                ));
            }
        };
        layout::insert_slot(page, LEAF_SPEC, slot_u16, cell_offset)?;
        scratch_offset = next;
    }

    debug_assert_eq!(scratch_offset, scratch_len);
    Ok(())
}

fn write_leaf_cell_at(
    page: &mut [u8; PAGE_SIZE],
    cell_offset: usize,
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<()> {
    let payload_len = u16::try_from(payload.len()).map_err(|_| TablePageError::CellTooLarge {
        len: payload.len(),
        max: usize::from(u16::MAX),
    })?;
    let cell_end = cell_offset + LEAF_CELL_PREFIX_SIZE + payload.len();
    let cell = &mut page[cell_offset..cell_end];

    cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
    cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
    cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
    Ok(())
}

/// Reads a little-endian `u16` from `bytes` at `offset`.
fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(out)
}

#[cfg(test)]
fn initialized_leaf_page() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    {
        let _leaf = TableLeafPageMut::init_empty(&mut page).unwrap();
    }
    page
}

#[cfg(test)]
mod tests {
    use crate::page_checksum::PAGE_DATA_END;

    use super::*;

    fn payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    #[test]
    fn init_empty_and_from_bytes_validate_page_type() {
        let mut page = [0u8; PAGE_SIZE];
        {
            let leaf = TableLeafPageMut::init_empty(&mut page).unwrap();
            assert_eq!(leaf.cell_count(), 0);
        }

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.cell_count(), 0);

        page[0] = 99;
        let err = TableLeafPageRef::from_bytes(&page).unwrap_err();
        assert!(matches!(err, TablePageError::InvalidPageType { page_type: 99 }));
    }

    #[test]
    fn search_on_empty_page_returns_none() {
        let page = initialized_leaf_page();
        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.search(7).unwrap(), None);
    }

    #[test]
    fn search_returns_borrowed_payload() {
        let mut page = initialized_leaf_page();

        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(42, &[1, 2, 3, 4]).unwrap();
        }

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        let cell = leaf_ref.search(42).unwrap().unwrap();
        assert_eq!(cell.row_id, 42);
        assert_eq!(cell.payload, &[1, 2, 3, 4]);
    }

    #[test]
    fn inserts_are_searchable_after_random_order_inserts() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        for row_id in [50, 10, 40, 20, 30] {
            leaf.insert(row_id, &[row_id as u8]).unwrap();
        }

        for row_id in [10, 20, 30, 40, 50] {
            let found = leaf.search(row_id).unwrap().unwrap();
            assert_eq!(found.row_id, row_id);
            assert_eq!(found.payload, &[row_id as u8]);
        }
    }

    #[test]
    fn insert_duplicate_row_id_fails() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(11, &[1]).unwrap();
        let err = leaf.insert(11, &[2]).unwrap_err();
        assert!(matches!(err, TablePageError::DuplicateRowId { row_id: 11 }));
    }

    #[test]
    fn update_existing_and_missing_row_ids() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(9, &[1, 2]).unwrap();
        leaf.update(9, &[8, 8, 8]).unwrap();
        assert_eq!(leaf.search(9).unwrap().unwrap().payload, &[8, 8, 8]);

        let err = leaf.update(99, &[1]).unwrap_err();
        assert!(matches!(err, TablePageError::RowIdNotFound { row_id: 99 }));
    }

    #[test]
    fn delete_existing_and_missing_row_ids() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &[1]).unwrap();
        leaf.insert(2, &[2]).unwrap();

        leaf.delete(1).unwrap();
        assert_eq!(leaf.search(1).unwrap(), None);

        let err = leaf.delete(1).unwrap_err();
        assert!(matches!(err, TablePageError::RowIdNotFound { row_id: 1 }));
    }

    #[test]
    fn insert_reuses_deleted_cell_space_before_defragmenting() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &payload(1, 256)).unwrap();
        leaf.insert(2, &payload(2, 256)).unwrap();
        leaf.insert(3, &payload(3, 256)).unwrap();

        let deleted_slot = find_leaf_row_id(leaf.page, 2).unwrap();
        let deleted_slot = match deleted_slot {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => panic!("row should exist"),
        };
        let deleted_offset = layout::slot_offset(leaf.page, LEAF_SPEC, deleted_slot).unwrap();

        leaf.delete(2).unwrap();
        leaf.insert(4, &payload(4, 256)).unwrap();

        let inserted_slot = find_leaf_row_id(leaf.page, 4).unwrap();
        let inserted_slot = match inserted_slot {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => panic!("row should exist"),
        };
        let inserted_offset = layout::slot_offset(leaf.page, LEAF_SPEC, inserted_slot).unwrap();

        assert_eq!(inserted_offset, deleted_offset);
    }

    #[test]
    fn shrinking_update_returns_tail_space() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(7, &payload(1, 128)).unwrap();
        let free_before = leaf.free_space().unwrap();

        leaf.update(7, &payload(9, 80)).unwrap();

        assert_eq!(leaf.search(7).unwrap().unwrap().payload, payload(9, 80).as_slice());
        assert_eq!(leaf.free_space().unwrap(), free_before + 48);
    }

    #[test]
    fn growing_update_can_succeed_by_reclaiming_old_cell_during_rewrite() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &payload(1, 1_200)).unwrap();
        leaf.insert(2, &payload(2, 1_200)).unwrap();
        leaf.insert(3, &payload(3, 1_200)).unwrap();

        let free_before = leaf.free_space().unwrap();
        assert!(free_before < leaf_cell_encoded_len(&payload(9, 1_400)).unwrap());

        leaf.update(2, &payload(9, 1_400)).unwrap();

        assert_eq!(leaf.search(2).unwrap().unwrap().payload, payload(9, 1_400).as_slice());
        assert_eq!(leaf.search(1).unwrap().unwrap().payload, payload(1, 1_200).as_slice());
        assert_eq!(leaf.search(3).unwrap().unwrap().payload, payload(3, 1_200).as_slice());
    }

    #[test]
    fn failed_growing_update_leaves_old_cell_intact() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &payload(1, 1_200)).unwrap();
        leaf.insert(2, &payload(2, 1_200)).unwrap();
        leaf.insert(3, &payload(3, 1_200)).unwrap();

        let err = leaf.update(2, &payload(9, 2_000)).unwrap_err();
        assert!(matches!(err, TablePageError::PageFull { .. }));
        assert_eq!(leaf.search(2).unwrap().unwrap().payload, payload(2, 1_200).as_slice());
    }

    #[test]
    fn insert_defrag_retry_path_succeeds() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &payload(1, 1_200)).unwrap();
        leaf.insert(2, &payload(2, 1_200)).unwrap();
        leaf.insert(3, &payload(3, 1_200)).unwrap();
        leaf.delete(2).unwrap();

        leaf.insert(4, &payload(4, 1_000)).unwrap();

        assert_eq!(leaf.search(4).unwrap().unwrap().payload.len(), 1_000);
    }

    #[test]
    fn update_defrag_retry_path_succeeds() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(1, &payload(1, 1_200)).unwrap();
        leaf.insert(2, &payload(2, 1_200)).unwrap();
        leaf.insert(3, &payload(3, 1_200)).unwrap();
        leaf.delete(2).unwrap();

        leaf.update(1, &payload(9, 1_000)).unwrap();
        assert_eq!(leaf.search(1).unwrap().unwrap().payload.len(), 1_000);
    }

    #[test]
    fn same_size_update_on_full_page_overwrites_in_place() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        let max_payload = PAGE_DATA_END - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
        leaf.insert(1, &payload(1, max_payload)).unwrap();

        let free_before = leaf.free_space().unwrap();
        assert_eq!(free_before, 0);

        let updated_payload = payload(9, max_payload);
        leaf.update(1, &updated_payload).unwrap();

        assert_eq!(leaf.free_space().unwrap(), free_before);
        assert_eq!(leaf.search(1).unwrap().unwrap().payload, updated_payload.as_slice());
    }

    #[test]
    fn insert_and_update_fail_with_page_full() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        let max_payload = PAGE_DATA_END - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
        leaf.insert(1, &payload(1, max_payload)).unwrap();

        let insert_err = leaf.insert(2, &[1]).unwrap_err();
        assert!(matches!(insert_err, TablePageError::PageFull { .. }));

        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
        leaf.insert(1, &payload(1, 100)).unwrap();
        leaf.insert(2, &payload(2, 100)).unwrap();

        let update_err = leaf.update(1, &payload(3, 4_000)).unwrap_err();
        assert!(matches!(update_err, TablePageError::PageFull { .. }));
    }

    #[test]
    fn largest_payload_and_cell_too_large_boundaries() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        let max_payload = PAGE_DATA_END - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
        leaf.insert(1, &payload(5, max_payload)).unwrap();
        assert_eq!(leaf.search(1).unwrap().unwrap().payload.len(), max_payload);

        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
        let err = leaf.insert(2, &payload(7, 70_000)).unwrap_err();
        assert!(matches!(err, TablePageError::CellTooLarge { len: 70_000, .. }));
    }

    #[test]
    fn corrupt_slot_offset_is_detected() {
        let mut page = initialized_leaf_page();
        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(1, &[9, 9]).unwrap();
        }

        page[layout::LEAF_HEADER_SIZE..layout::LEAF_HEADER_SIZE + 2]
            .copy_from_slice(&1u16.to_le_bytes());

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        let err = leaf_ref.search(1).unwrap_err();
        assert!(matches!(err, TablePageError::CorruptCell { slot_index: 0 }));
    }

    #[test]
    fn malformed_leaf_cell_payload_length_is_detected() {
        let mut page = initialized_leaf_page();
        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(1, &[1, 2, 3]).unwrap();
        }

        let slot = layout::LEAF_HEADER_SIZE;
        let cell_offset = usize::from(u16::from_le_bytes([page[slot], page[slot + 1]]));
        page[cell_offset..cell_offset + 2].copy_from_slice(&u16::MAX.to_le_bytes());

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        let err = leaf_ref.search(1).unwrap_err();
        assert!(matches!(err, TablePageError::CorruptCell { slot_index: 0 }));
    }

    #[test]
    fn sibling_pointers_roundtrip_and_allow_none() {
        let mut page = initialized_leaf_page();
        {
            let leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            assert_eq!(leaf.search(1).unwrap(), None);
        }
        layout::set_prev_sibling(&mut page, Some(7));
        layout::set_next_sibling(&mut page, Some(13));

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.prev_sibling(), Some(7));
        assert_eq!(leaf_ref.next_sibling(), Some(13));

        layout::set_prev_sibling(&mut page, None);
        layout::set_next_sibling(&mut page, None);

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.prev_sibling(), None);
        assert_eq!(leaf_ref.next_sibling(), None);
    }

    #[test]
    fn sibling_pointers_survive_defragmentation() {
        let mut page = initialized_leaf_page();
        layout::set_prev_sibling(&mut page, Some(111));
        layout::set_next_sibling(&mut page, Some(222));
        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(1, &payload(1, 1_200)).unwrap();
            leaf.insert(2, &payload(2, 1_200)).unwrap();
            leaf.insert(3, &payload(3, 1_200)).unwrap();
            leaf.delete(2).unwrap();

            leaf.insert(4, &payload(4, 1_000)).unwrap();
        }

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.prev_sibling(), Some(111));
        assert_eq!(leaf_ref.next_sibling(), Some(222));
    }

    #[test]
    fn rewrite_leaf_page_replacement_handles_first_and_last_slots() {
        let mut page = initialized_leaf_page();
        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(1, &[1]).unwrap();
            leaf.insert(2, &[2]).unwrap();
            leaf.insert(3, &[3]).unwrap();
        }

        let replacement_first = encode_leaf_cell(1, &[9, 9]).unwrap();
        rewrite_leaf_page(&mut page, Some((0, &replacement_first))).unwrap();

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.search(1).unwrap().unwrap().payload, &[9, 9]);
        assert_eq!(leaf_ref.search(2).unwrap().unwrap().payload, &[2]);
        assert_eq!(leaf_ref.search(3).unwrap().unwrap().payload, &[3]);

        let replacement_last = encode_leaf_cell(3, &[7, 7, 7]).unwrap();
        rewrite_leaf_page(&mut page, Some((2, &replacement_last))).unwrap();

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.search(1).unwrap().unwrap().payload, &[9, 9]);
        assert_eq!(leaf_ref.search(2).unwrap().unwrap().payload, &[2]);
        assert_eq!(leaf_ref.search(3).unwrap().unwrap().payload, &[7, 7, 7]);
    }
}

#[cfg(test)]
mod test_prop {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;

    use crate::page_checksum::PAGE_DATA_END;

    use super::*;

    const SLOT_ENTRY_SIZE: usize = 2;
    const MAX_GENERATED_PAYLOAD_LEN: usize = 96;
    const MAX_GENERATED_ENTRY_COUNT: usize = 48;
    const EMPTY_ORACLE_MISS_PROBE_LOW: RowId = 0;
    const EMPTY_ORACLE_MISS_PROBE_NEXT: RowId = 1;
    const ORACLE_MISS_SENTINEL: RowId = RowId::MAX;
    type LeafEntry = (RowId, Vec<u8>);
    type LeafEntries = Vec<LeafEntry>;
    type LeafInsertThenUpdateCase = (LeafEntries, LeafEntries);
    type LeafOpSequence = Vec<LeafOp>;

    #[derive(Debug, Clone)]
    enum LeafOp {
        Insert(RowId, Vec<u8>),
        Update(RowId, Vec<u8>),
        Delete(RowId),
    }

    fn compact_leaf_usage(entries: &[LeafEntry]) -> usize {
        layout::LEAF_HEADER_SIZE
            + entries
                .iter()
                .map(|(_, payload)| payload.len() + LEAF_CELL_PREFIX_SIZE + SLOT_ENTRY_SIZE)
                .sum::<usize>()
    }

    fn compact_leaf_fits(entries: &[LeafEntry]) -> bool {
        compact_leaf_usage(entries) <= PAGE_DATA_END
    }

    fn unique_row_ids(entries: &[LeafEntry]) -> bool {
        let mut seen = BTreeSet::new();
        entries.iter().all(|(row_id, _)| seen.insert(*row_id))
    }

    fn leaf_payload_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 0..=MAX_GENERATED_PAYLOAD_LEN)
    }

    fn leaf_entries_strategy() -> impl Strategy<Value = LeafEntries> {
        let payloads = leaf_payload_strategy();
        let entries = (any::<RowId>(), payloads);
        prop::collection::vec(entries, 0..=MAX_GENERATED_ENTRY_COUNT).prop_filter(
            "entries must have unique row ids and fit on one compact leaf page",
            |entries| unique_row_ids(entries) && compact_leaf_fits(entries),
        )
    }

    fn non_empty_leaf_entries_strategy() -> impl Strategy<Value = LeafEntries> {
        let payloads = leaf_payload_strategy();
        let entries = (any::<RowId>(), payloads);
        prop::collection::vec(entries, 1..=MAX_GENERATED_ENTRY_COUNT).prop_filter(
            "entries must have unique row ids and fit on one compact leaf page",
            |entries| unique_row_ids(entries) && compact_leaf_fits(entries),
        )
    }

    fn update_sequence_succeeds(entries: &[LeafEntry], updates: &[LeafEntry]) -> bool {
        let mut current: BTreeMap<RowId, Vec<u8>> = entries.iter().cloned().collect();
        let mut live_usage = compact_leaf_usage(entries);
        let mut dead_bytes = 0usize;

        for (row_id, new_payload) in updates {
            let old_payload = match current.get(row_id) {
                Some(payload) => payload.clone(),
                None => return false,
            };

            if old_payload == *new_payload {
                return false;
            }

            let old_cell_len = LEAF_CELL_PREFIX_SIZE + old_payload.len();
            let new_cell_len = LEAF_CELL_PREFIX_SIZE + new_payload.len();

            if new_cell_len != old_cell_len {
                let available_without_defrag =
                    PAGE_DATA_END.saturating_sub(live_usage.saturating_add(dead_bytes));

                if new_cell_len <= available_without_defrag {
                    dead_bytes += old_cell_len;
                } else {
                    let available_after_defrag = PAGE_DATA_END.saturating_sub(live_usage);
                    if new_cell_len > available_after_defrag {
                        return false;
                    }

                    dead_bytes = old_cell_len;
                }

                live_usage = live_usage - old_cell_len + new_cell_len;
            }

            current.insert(*row_id, new_payload.clone());
        }

        true
    }

    fn leaf_insert_then_update_strategy() -> impl Strategy<Value = LeafInsertThenUpdateCase> {
        non_empty_leaf_entries_strategy()
            .prop_flat_map(|entries| {
                let entry_count = entries.len();
                prop::sample::subsequence(entries.clone(), 1..=entry_count).prop_flat_map(
                    move |selected_entries| {
                        let entries = entries.clone();
                        prop::collection::vec(leaf_payload_strategy(), selected_entries.len())
                            .prop_map(move |replacement_payloads| {
                                let updates: LeafEntries = selected_entries
                                    .iter()
                                    .zip(replacement_payloads)
                                    .map(|((row_id, _), payload)| (*row_id, payload))
                                    .collect();
                                (entries.clone(), updates)
                            })
                    },
                )
            })
            .prop_filter(
                "updates must be executable under the current leaf-page update algorithm",
                |(entries, updates)| update_sequence_succeeds(entries, updates),
            )
    }

    fn leaf_op_strategy() -> impl Strategy<Value = LeafOp> {
        prop_oneof![
            (any::<RowId>(), leaf_payload_strategy())
                .prop_map(|(row_id, payload)| LeafOp::Insert(row_id, payload)),
            (any::<RowId>(), leaf_payload_strategy())
                .prop_map(|(row_id, payload)| LeafOp::Update(row_id, payload)),
            any::<RowId>().prop_map(LeafOp::Delete),
        ]
    }

    fn leaf_op_sequence_strategy() -> impl Strategy<Value = LeafOpSequence> {
        prop::collection::vec(leaf_op_strategy(), 1..=96)
    }

    proptest! {
        // This checks the core leaf-page lookup invariant: any generated set of unique entries
        // that fits on one compact page should round-trip through insert/search exactly as the
        // BTreeMap oracle predicts, regardless of insertion order.
        #[test]
        fn prop_insert_and_search_match_btreemap_oracle(entries in leaf_entries_strategy()) {
            let oracle: BTreeMap<RowId, Vec<u8>> = entries.iter().cloned().collect();
            let mut page = initialized_leaf_page();

            {
                let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

                for (row_id, payload) in &entries {
                    leaf.insert(*row_id, payload).unwrap();
                }

                prop_assert_eq!(leaf.cell_count() as usize, oracle.len());

                for (row_id, payload) in &oracle {
                    let cell = leaf.search(*row_id).unwrap().unwrap();
                    prop_assert_eq!(cell.row_id, *row_id);
                    prop_assert_eq!(cell.payload, payload.as_slice());
                }

                let miss_probes = if let Some((&first_key, _)) = oracle.first_key_value() {
                    let last_key = *oracle.last_key_value().unwrap().0;
                    [
                        first_key.saturating_sub(1),
                        last_key.saturating_add(1),
                        ORACLE_MISS_SENTINEL,
                    ]
                } else {
                    [
                        EMPTY_ORACLE_MISS_PROBE_LOW,
                        EMPTY_ORACLE_MISS_PROBE_NEXT,
                        ORACLE_MISS_SENTINEL,
                    ]
                };

                for probe in miss_probes {
                    if !oracle.contains_key(&probe) {
                        prop_assert_eq!(leaf.search(probe).unwrap(), None);
                    }
                }
            }

            let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
            for (row_id, payload) in &oracle {
                let cell = leaf_ref.search(*row_id).unwrap().unwrap();
                prop_assert_eq!(cell.row_id, *row_id);
                prop_assert_eq!(cell.payload, payload.as_slice());
            }
        }

        // This checks the update path against the same BTreeMap oracle used by insert/search:
        // rows are inserted once, a subset is updated in place by row id, and the final page
        // state must match the oracle even when updates rely on defragmentation to succeed.
        #[test]
        fn prop_insert_then_update_match_btreemap_oracle(
            (entries, updates) in leaf_insert_then_update_strategy()
        ) {
            let mut oracle: BTreeMap<RowId, Vec<u8>> = entries.iter().cloned().collect();
            let original_row_count = oracle.len();
            let mut page = initialized_leaf_page();

            {
                let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

                for (row_id, payload) in &entries {
                    leaf.insert(*row_id, payload).unwrap();
                }

                prop_assert_eq!(leaf.cell_count() as usize, original_row_count);

                for (row_id, payload) in &updates {
                    leaf.update(*row_id, payload).unwrap();
                    oracle.insert(*row_id, payload.clone());
                }

                prop_assert_eq!(leaf.cell_count() as usize, original_row_count);

                for (row_id, payload) in &oracle {
                    let cell = leaf.search(*row_id).unwrap().unwrap();
                    prop_assert_eq!(cell.row_id, *row_id);
                    prop_assert_eq!(cell.payload, payload.as_slice());
                }

                let miss_probes = if let Some((&first_key, _)) = oracle.first_key_value() {
                    let last_key = *oracle.last_key_value().unwrap().0;
                    [
                        first_key.saturating_sub(1),
                        last_key.saturating_add(1),
                        ORACLE_MISS_SENTINEL,
                    ]
                } else {
                    [
                        EMPTY_ORACLE_MISS_PROBE_LOW,
                        EMPTY_ORACLE_MISS_PROBE_NEXT,
                        ORACLE_MISS_SENTINEL,
                    ]
                };

                for probe in miss_probes {
                    if !oracle.contains_key(&probe) {
                        prop_assert_eq!(leaf.search(probe).unwrap(), None);
                    }
                }
            }

            let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
            for (row_id, payload) in &oracle {
                let cell = leaf_ref.search(*row_id).unwrap().unwrap();
                prop_assert_eq!(cell.row_id, *row_id);
                prop_assert_eq!(cell.payload, payload.as_slice());
            }
        }

        #[test]
        fn prop_interleaved_mutations_preserve_search_oracle(ops in leaf_op_sequence_strategy()) {
            let mut oracle: BTreeMap<RowId, Vec<u8>> = BTreeMap::new();
            let mut page = initialized_leaf_page();
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

            for op in ops {
                match op {
                    LeafOp::Insert(row_id, payload) => {
                        match leaf.insert(row_id, &payload) {
                            Ok(()) => {
                                oracle.insert(row_id, payload);
                            }
                            Err(TablePageError::DuplicateRowId { .. }) => {
                                prop_assert!(oracle.contains_key(&row_id));
                            }
                            Err(TablePageError::PageFull { .. }) => {}
                            Err(err) => return Err(TestCaseError::fail(format!("unexpected insert error: {err}"))),
                        }
                    }
                    LeafOp::Update(row_id, payload) => {
                        match leaf.update(row_id, &payload) {
                            Ok(()) => {
                                prop_assert!(oracle.contains_key(&row_id));
                                oracle.insert(row_id, payload);
                            }
                            Err(TablePageError::RowIdNotFound { .. }) => {
                                prop_assert!(!oracle.contains_key(&row_id));
                            }
                            Err(TablePageError::PageFull { .. }) => {
                                prop_assert!(oracle.contains_key(&row_id));
                            }
                            Err(err) => return Err(TestCaseError::fail(format!("unexpected update error: {err}"))),
                        }
                    }
                    LeafOp::Delete(row_id) => {
                        match leaf.delete(row_id) {
                            Ok(()) => {
                                prop_assert!(oracle.remove(&row_id).is_some());
                            }
                            Err(TablePageError::RowIdNotFound { .. }) => {
                                prop_assert!(!oracle.contains_key(&row_id));
                            }
                            Err(err) => return Err(TestCaseError::fail(format!("unexpected delete error: {err}"))),
                        }
                    }
                }
            }

            leaf.defragment().unwrap();
            prop_assert_eq!(leaf.cell_count() as usize, oracle.len());

            for (row_id, payload) in &oracle {
                let cell = leaf.search(*row_id).unwrap().unwrap();
                prop_assert_eq!(cell.row_id, *row_id);
                prop_assert_eq!(cell.payload, payload.as_slice());
            }
        }
    }
}
