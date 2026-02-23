use crate::{
    error::{TablePageError, TablePageResult},
    types::{PAGE_SIZE, RowId},
};

use super::layout::{self, PageSpec, SearchResult, SpaceError};

const LEAF_SPEC: PageSpec =
    PageSpec { page_type: layout::LEAF_PAGE_TYPE, header_size: layout::LEAF_HEADER_SIZE };

const PAYLOAD_LEN_SIZE: usize = 2;
const ROW_ID_SIZE: usize = 8;
const LEAF_CELL_PREFIX_SIZE: usize = PAYLOAD_LEN_SIZE + ROW_ID_SIZE;

/// Borrowed view of one leaf cell decoded from a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeafCellRef<'a> {
    /// Row identifier used as the key inside the page.
    pub(crate) row_id: RowId,
    /// Borrowed payload bytes for the row.
    pub(crate) payload: &'a [u8],
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
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Ok(None),
            };

        decode_leaf_cell_at_slot(self.page, slot_index).map(Some)
    }

    /// Returns the number of slot entries currently stored on the page.
    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    /// Returns free bytes between the slot directory and cell-content region.
    pub(crate) fn free_space(&self) -> usize {
        layout::free_space(self.page, LEAF_SPEC).expect("leaf page must remain valid")
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

    /// Returns an immutable view over the same underlying page bytes.
    pub(crate) fn as_ref(&self) -> TableLeafPageRef<'_> {
        TableLeafPageRef { page: self.page }
    }

    /// Immutable row-id lookup convenience method for mutable wrappers.
    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<LeafCellRef<'_>>> {
        self.as_ref().search(row_id)
    }

    /// Inserts a new cell keyed by `row_id`, preserving sorted slot order.
    ///
    /// Fails with [`TablePageError::DuplicateRowId`] if the key already exists.
    pub(crate) fn insert(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let insertion_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId(row_id)),
                SearchResult::NotFound(insertion_index) => insertion_index,
            };

        let cell_offset = write_leaf_cell_for_insert_with_retry(self.page, row_id, payload)?;
        layout::insert_slot(self.page, LEAF_SPEC, insertion_index, cell_offset)
    }

    /// Replaces the payload for an existing row id.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when the key is absent.
    pub(crate) fn update(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        let existing_cell = layout::cell_bytes_at_slot(self.page, LEAF_SPEC, slot_index)?;
        let existing_len =
            leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let new_len = leaf_cell_encoded_len(payload)?;

        if existing_len == new_len {
            let cell_offset = usize::from(layout::slot_offset(self.page, LEAF_SPEC, slot_index)?);
            let cell_end = cell_offset + existing_len;
            let cell = &mut self.page[cell_offset..cell_end];
            let payload_len = u16::try_from(payload.len()).expect("payload length must fit in u16");

            cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
            cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
            cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
            return Ok(());
        }

        let cell_offset = write_leaf_cell_for_update_with_retry(self.page, row_id, payload)?;
        layout::set_slot_offset(self.page, LEAF_SPEC, slot_index, cell_offset)
    }

    /// Deletes the cell for `row_id`.
    ///
    /// Fails with [`TablePageError::RowIdNotFound`] when the key is absent.
    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                SearchResult::Found(slot_index) => slot_index,
                SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        layout::remove_slot(self.page, LEAF_SPEC, slot_index)
    }

    /// Compacts live cells toward the page end and rewrites slot offsets.
    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        layout::defragment(self.page, LEAF_SPEC, leaf_cell_len)
    }
}

/// Decodes and validates the leaf cell referenced by `slot_index`.
fn decode_leaf_cell_at_slot<'a>(
    page: &'a [u8; PAGE_SIZE],
    slot_index: u16,
) -> TablePageResult<LeafCellRef<'a>> {
    let cell = layout::cell_bytes_at_slot(page, LEAF_SPEC, slot_index)?;
    let cell_len = leaf_cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;

    let payload_len = usize::from(read_u16(cell, 0));
    let row_id = read_u64(cell, PAYLOAD_LEN_SIZE);
    let payload_start = LEAF_CELL_PREFIX_SIZE;
    let payload_end = payload_start + payload_len;

    debug_assert!(payload_end <= cell_len);

    Ok(LeafCellRef { row_id, payload: &cell[payload_start..payload_end] })
}

/// Returns the encoded byte length of a leaf cell.
fn leaf_cell_len(cell: &[u8]) -> TablePageResult<usize> {
    if cell.len() < LEAF_CELL_PREFIX_SIZE {
        return Err(TablePageError::CorruptPage("leaf cell too short"));
    }

    let payload_len = usize::from(read_u16(cell, 0));
    let cell_len = LEAF_CELL_PREFIX_SIZE
        .checked_add(payload_len)
        .ok_or(TablePageError::CorruptPage("leaf cell length overflow"))?;

    if cell_len > cell.len() {
        return Err(TablePageError::CorruptPage("leaf cell payload out of bounds"));
    }

    Ok(cell_len)
}

/// Extracts the row id key from an encoded leaf cell.
fn leaf_row_id_from_cell(cell: &[u8]) -> TablePageResult<RowId> {
    if cell.len() < LEAF_CELL_PREFIX_SIZE {
        return Err(TablePageError::CorruptPage("leaf cell too short"));
    }

    Ok(read_u64(cell, PAYLOAD_LEN_SIZE))
}

/// Computes the serialized cell length for a payload.
fn leaf_cell_encoded_len(payload: &[u8]) -> TablePageResult<usize> {
    if payload.len() > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: payload.len() });
    }

    LEAF_CELL_PREFIX_SIZE
        .checked_add(payload.len())
        .ok_or(TablePageError::CorruptPage("leaf cell length overflow"))
}

/// Attempts to append a leaf cell without defragmenting.
fn try_append_leaf_cell(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<Result<u16, SpaceError>> {
    let cell_len = leaf_cell_encoded_len(payload)?;
    let payload_len = u16::try_from(payload.len()).expect("payload length must fit in u16");

    layout::try_append_cell_with_writer(page, LEAF_SPEC, cell_len, |cell| {
        cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
        cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
        cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
    })
}

/// Attempts to append a leaf cell while reserving space for one new slot.
fn try_append_leaf_cell_for_insert(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<Result<u16, SpaceError>> {
    let cell_len = leaf_cell_encoded_len(payload)?;
    let payload_len = u16::try_from(payload.len()).expect("payload length must fit in u16");

    layout::try_append_cell_with_writer_for_insert(page, LEAF_SPEC, cell_len, |cell| {
        cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
        cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
        cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
    })
}

/// Appends a replacement leaf cell, defragmenting once before reporting page-full.
fn write_leaf_cell_for_update_with_retry(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<u16> {
    write_leaf_cell_with_retry(page, row_id, payload, try_append_leaf_cell)
}

/// Appends a newly inserted leaf cell, defragmenting once before reporting page-full.
fn write_leaf_cell_for_insert_with_retry(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
) -> TablePageResult<u16> {
    write_leaf_cell_with_retry(page, row_id, payload, try_append_leaf_cell_for_insert)
}

/// Shared retry helper for leaf-cell appends.
fn write_leaf_cell_with_retry<F>(
    page: &mut [u8; PAGE_SIZE],
    row_id: RowId,
    payload: &[u8],
    try_append: F,
) -> TablePageResult<u16>
where
    F: Fn(&mut [u8; PAGE_SIZE], RowId, &[u8]) -> TablePageResult<Result<u16, SpaceError>>,
{
    if let Ok(offset) = try_append(page, row_id, payload)? {
        return Ok(offset);
    }

    layout::defragment(page, LEAF_SPEC, leaf_cell_len)?;

    match try_append(page, row_id, payload)? {
        Ok(offset) => Ok(offset),
        Err(SpaceError { needed, available }) => {
            Err(TablePageError::PageFull { needed, available })
        }
    }
}

/// Reads a little-endian `u16` from `bytes` at `offset`.
fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(out)
}

/// Reads a little-endian `u64` from `bytes` at `offset`.
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized_leaf_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        {
            let _leaf = TableLeafPageMut::init_empty(&mut page).unwrap();
        }
        page
    }

    fn payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    #[test]
    fn init_empty_and_from_bytes_validate_page_type() {
        let mut page = [0u8; PAGE_SIZE];
        {
            let leaf = TableLeafPageMut::init_empty(&mut page).unwrap();
            assert_eq!(leaf.as_ref().cell_count(), 0);
        }

        let leaf_ref = TableLeafPageRef::from_bytes(&page).unwrap();
        assert_eq!(leaf_ref.cell_count(), 0);

        page[0] = 99;
        let err = TableLeafPageRef::from_bytes(&page).unwrap_err();
        assert!(matches!(err, TablePageError::InvalidPageType(99)));
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
        assert!(matches!(err, TablePageError::DuplicateRowId(11)));
    }

    #[test]
    fn update_existing_and_missing_row_ids() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        leaf.insert(9, &[1, 2]).unwrap();
        leaf.update(9, &[8, 8, 8]).unwrap();
        assert_eq!(leaf.search(9).unwrap().unwrap().payload, &[8, 8, 8]);

        let err = leaf.update(99, &[1]).unwrap_err();
        assert!(matches!(err, TablePageError::RowIdNotFound(99)));
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
        assert!(matches!(err, TablePageError::RowIdNotFound(1)));
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

        let max_payload = PAGE_SIZE - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
        leaf.insert(1, &payload(1, max_payload)).unwrap();

        let free_before = leaf.as_ref().free_space();
        assert_eq!(free_before, 0);

        let updated_payload = payload(9, max_payload);
        leaf.update(1, &updated_payload).unwrap();

        assert_eq!(leaf.as_ref().free_space(), free_before);
        assert_eq!(leaf.search(1).unwrap().unwrap().payload, updated_payload.as_slice());
    }

    #[test]
    fn insert_and_update_fail_with_page_full() {
        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();

        let max_payload = PAGE_SIZE - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
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

        let max_payload = PAGE_SIZE - layout::LEAF_HEADER_SIZE - 2 - LEAF_CELL_PREFIX_SIZE;
        leaf.insert(1, &payload(5, max_payload)).unwrap();
        assert_eq!(leaf.search(1).unwrap().unwrap().payload.len(), max_payload);

        let mut page = initialized_leaf_page();
        let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
        let err = leaf.insert(2, &payload(7, 70_000)).unwrap_err();
        assert!(matches!(err, TablePageError::CellTooLarge { len: 70_000 }));
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
}
