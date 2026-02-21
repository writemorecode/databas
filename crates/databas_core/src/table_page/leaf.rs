use crate::{
    error::{TablePageError, TablePageResult},
    types::{PAGE_SIZE, RowId},
};

use super::layout::{self, PageSpec, SpaceError};

const LEAF_SPEC: PageSpec =
    PageSpec { page_type: layout::LEAF_PAGE_TYPE, header_size: layout::LEAF_HEADER_SIZE };

const PAYLOAD_LEN_SIZE: usize = 2;
const ROW_ID_SIZE: usize = 8;
const LEAF_CELL_PREFIX_SIZE: usize = PAYLOAD_LEN_SIZE + ROW_ID_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeafCellRef<'a> {
    pub(crate) row_id: RowId,
    pub(crate) payload: &'a [u8],
}

#[derive(Debug)]
pub(crate) struct TableLeafPageRef<'a> {
    page: &'a [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub(crate) struct TableLeafPageMut<'a> {
    page: &'a mut [u8; PAGE_SIZE],
}

impl<'a> TableLeafPageRef<'a> {
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<LeafCellRef<'a>>> {
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                Ok(slot_index) => slot_index,
                Err(_) => return Ok(None),
            };

        decode_leaf_cell_at_slot(self.page, slot_index).map(Some)
    }

    pub(crate) fn cell_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    pub(crate) fn free_space(&self) -> usize {
        layout::free_space(self.page, LEAF_SPEC).expect("leaf page must remain valid")
    }
}

impl<'a> TableLeafPageMut<'a> {
    pub(crate) fn init_empty(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::init_empty(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate(page, LEAF_SPEC)?;
        Ok(Self { page })
    }

    pub(crate) fn as_ref(&self) -> TableLeafPageRef<'_> {
        TableLeafPageRef { page: self.page }
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<LeafCellRef<'_>>> {
        self.as_ref().search(row_id)
    }

    pub(crate) fn insert(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let insertion_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                Ok(_) => return Err(TablePageError::DuplicateRowId(row_id)),
                Err(insertion_index) => insertion_index,
            };

        let cell = encode_leaf_cell(row_id, payload)?;
        let cell_offset = write_leaf_cell_with_retry(self.page, &cell, 1)?;
        layout::insert_slot(self.page, LEAF_SPEC, insertion_index, cell_offset)
    }

    pub(crate) fn update(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                Ok(slot_index) => slot_index,
                Err(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        let cell = encode_leaf_cell(row_id, payload)?;
        let cell_offset = write_leaf_cell_with_retry(self.page, &cell, 0)?;
        layout::set_slot_offset(self.page, LEAF_SPEC, slot_index, cell_offset)
    }

    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index =
            match layout::find_row_id(self.page, LEAF_SPEC, row_id, leaf_row_id_from_cell)? {
                Ok(slot_index) => slot_index,
                Err(_) => return Err(TablePageError::RowIdNotFound(row_id)),
            };

        layout::remove_slot(self.page, LEAF_SPEC, slot_index)
    }

    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        layout::defragment(self.page, LEAF_SPEC, leaf_cell_len)
    }
}

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

fn leaf_row_id_from_cell(cell: &[u8]) -> TablePageResult<RowId> {
    if cell.len() < LEAF_CELL_PREFIX_SIZE {
        return Err(TablePageError::CorruptPage("leaf cell too short"));
    }

    Ok(read_u64(cell, PAYLOAD_LEN_SIZE))
}

fn encode_leaf_cell(row_id: RowId, payload: &[u8]) -> TablePageResult<Vec<u8>> {
    if payload.len() > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: payload.len() });
    }

    let payload_len = u16::try_from(payload.len()).expect("payload length must fit in u16");
    let mut cell = Vec::with_capacity(LEAF_CELL_PREFIX_SIZE + payload.len());
    cell.extend_from_slice(&payload_len.to_le_bytes());
    cell.extend_from_slice(&row_id.to_le_bytes());
    cell.extend_from_slice(payload);
    Ok(cell)
}

fn write_leaf_cell_with_retry(
    page: &mut [u8; PAGE_SIZE],
    cell: &[u8],
    extra_slots: usize,
) -> TablePageResult<u16> {
    match layout::try_append_cell(page, LEAF_SPEC, cell, extra_slots)? {
        Ok(offset) => return Ok(offset),
        Err(_) => {}
    }

    layout::defragment(page, LEAF_SPEC, leaf_cell_len)?;

    match layout::try_append_cell(page, LEAF_SPEC, cell, extra_slots)? {
        Ok(offset) => Ok(offset),
        Err(SpaceError { needed, available }) => {
            Err(TablePageError::PageFull { needed, available })
        }
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(out)
}

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
