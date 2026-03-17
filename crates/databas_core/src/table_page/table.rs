use std::cmp::Ordering;

use crate::{
    table_page::{
        Interior, Leaf, Page, PageAccess, PageAccessMut, Read, Table, TablePageCorruptionKind,
        TablePageError, TablePageResult, Write,
        layout::{
            self, CellWriteMode, FREEBLOCK_HEADER_SIZE, MAX_FRAGMENTED_FREE_BYTES, SearchResult,
        },
    },
    types::{PAGE_SIZE, PageId, RowId},
};

use super::read_u64;

const PAYLOAD_LEN_SIZE: usize = 2;
const ROW_ID_SIZE: usize = 8;
const LEAF_CELL_PREFIX_SIZE: usize = PAYLOAD_LEN_SIZE + ROW_ID_SIZE;

const LEFT_CHILD_SIZE: usize = 8;
const INTERIOR_CELL_SIZE: usize = LEFT_CHILD_SIZE + ROW_ID_SIZE;

/// Borrowed view of one leaf cell decoded from a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableLeafCell<'a> {
    /// Row identifier used as the key inside the page.
    pub(crate) row_id: RowId,
    /// Borrowed payload bytes for the row.
    pub(crate) payload: &'a [u8],
}

impl<'a> TableLeafCell<'a> {
    /// Deserializes and validates the leaf cell referenced by `slot_index`.
    pub(crate) fn try_deserialize_at_slot(
        page: &'a [u8; PAGE_SIZE],
        slot_index: u16,
    ) -> TablePageResult<Self> {
        let cell = layout::cell_bytes_at_slot::<Table, Leaf>(page, slot_index)?;
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

/// Decoded interior cell mapping a separator key to its left child page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableInteriorCell {
    /// Child page id for keys in the half-open range `[prev_row_id, row_id)`.
    ///
    /// For the first separator cell on the page, this child covers all keys
    /// strictly less than `row_id`.
    pub(crate) left_child: PageId,
    /// Separator row id for this interior cell.
    pub(crate) row_id: RowId,
}

impl TableInteriorCell {
    /// Deserializes and validates the interior cell referenced by `slot_index`.
    pub(crate) fn try_deserialize_at_slot(
        page: &[u8; PAGE_SIZE],
        slot_index: u16,
    ) -> TablePageResult<Self> {
        let cell = layout::cell_bytes_at_slot::<Table, Interior>(page, slot_index)?;
        if cell.len() < INTERIOR_CELL_SIZE {
            return Err(TablePageError::CorruptCell { slot_index });
        }

        let left_child = read_u64(cell, 0);
        let row_id = read_u64(cell, LEFT_CHILD_SIZE);

        Ok(Self { left_child, row_id })
    }
}

impl<'a> Page<Read<'a>, Table, Leaf> {
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate::<Table, Leaf>(page)?;
        Ok(Self::new(Read { bytes: page }))
    }
}

impl<'a> Page<Write<'a>, Table, Leaf> {
    pub(crate) fn init_empty(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::init_empty::<Table, Leaf>(page)?;
        Ok(Self::new(Write { bytes: page }))
    }

    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate::<Table, Leaf>(page)?;
        Ok(Self::new(Write { bytes: page }))
    }
}

impl<A> Page<A, Table, Leaf>
where
    A: PageAccess,
{
    pub(crate) fn len(&self) -> u16 {
        self.slot_count()
    }

    pub(crate) fn cell_count(&self) -> u16 {
        self.slot_count()
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<TableLeafCell<'_>>> {
        let slot_index = match find_leaf_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        TableLeafCell::try_deserialize_at_slot(self.bytes(), slot_index).map(Some)
    }

    pub(crate) fn rowid_at(&self, slot_index: u16) -> TablePageResult<RowId> {
        Ok(self.cell_at(slot_index)?.row_id)
    }

    pub(crate) fn cell_at(&self, slot_index: u16) -> TablePageResult<TableLeafCell<'_>> {
        TableLeafCell::try_deserialize_at_slot(self.bytes(), slot_index)
    }

    pub(crate) fn prev_sibling(&self) -> Option<PageId> {
        layout::prev_sibling(self.bytes())
    }

    pub(crate) fn next_sibling(&self) -> Option<PageId> {
        layout::next_sibling(self.bytes())
    }
}

impl<A> Page<A, Table, Leaf>
where
    A: PageAccessMut,
{
    pub(crate) fn insert(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let insertion_index = match find_leaf_row_id(self.bytes(), row_id)? {
            SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId { row_id }),
            SearchResult::NotFound(insertion_index) => insertion_index,
        };

        let cell_len = leaf_cell_encoded_len(payload)?;
        let cell_offset = match layout::try_allocate_space::<Table, Leaf>(
            self.bytes_mut(),
            cell_len,
            CellWriteMode::Insert,
        )? {
            Ok(offset) => offset,
            Err(space_error) => {
                if space_error.needed > space_error.available {
                    return Err(TablePageError::PageFull {
                        needed: space_error.needed,
                        available: space_error.available,
                    });
                }

                rewrite_leaf_page(self.bytes_mut(), None)?;

                match layout::try_allocate_space::<Table, Leaf>(
                    self.bytes_mut(),
                    cell_len,
                    CellWriteMode::Insert,
                )? {
                    Ok(offset) => offset,
                    Err(_) => {
                        return Err(TablePageError::CorruptPage(
                            TablePageCorruptionKind::CellContentUnderflow,
                        ));
                    }
                }
            }
        };
        write_leaf_cell_at(self.bytes_mut(), usize::from(cell_offset), row_id, payload)?;
        layout::insert_slot::<Table, Leaf>(self.bytes_mut(), insertion_index, cell_offset)
    }

    pub(crate) fn update(&mut self, row_id: RowId, payload: &[u8]) -> TablePageResult<()> {
        let slot_index = match find_leaf_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let mut working_page = *self.bytes();
        let mut existing_cell =
            layout::cell_bytes_at_slot::<Table, Leaf>(&working_page, slot_index)?;
        let mut existing_len =
            leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let new_len = leaf_cell_encoded_len(payload)?;
        let mut cell_offset = layout::slot_offset::<Table, Leaf>(&working_page, slot_index)?;

        if new_len < existing_len {
            let released_tail = existing_len - new_len;
            let new_fragmented_free_bytes =
                layout::fragmented_free_bytes(&working_page) + (released_tail as u8);
            if released_tail < FREEBLOCK_HEADER_SIZE
                && new_fragmented_free_bytes > MAX_FRAGMENTED_FREE_BYTES
            {
                rewrite_leaf_page(&mut working_page, None)?;
                existing_cell =
                    layout::cell_bytes_at_slot::<Table, Leaf>(&working_page, slot_index)?;
                existing_len = leaf_cell_len(existing_cell)
                    .map_err(|_| TablePageError::CorruptCell { slot_index })?;
                cell_offset = layout::slot_offset::<Table, Leaf>(&working_page, slot_index)?;
            }
        }

        if existing_len == new_len {
            write_leaf_cell_at(&mut working_page, usize::from(cell_offset), row_id, payload)?;
            *self.bytes_mut() = working_page;
            return Ok(());
        }

        if new_len < existing_len {
            let cell_offset = usize::from(cell_offset);
            write_leaf_cell_at(&mut working_page, cell_offset, row_id, payload)?;
            layout::release_space::<Table, Leaf>(
                &mut working_page,
                (cell_offset + new_len) as u16,
                existing_len - new_len,
            )?;
            *self.bytes_mut() = working_page;
            return Ok(());
        }

        if let Ok(offset) = layout::try_allocate_space::<Table, Leaf>(
            &mut working_page,
            new_len,
            CellWriteMode::Update,
        )? {
            write_leaf_cell_at(&mut working_page, usize::from(offset), row_id, payload)?;
            layout::set_slot_offset::<Table, Leaf>(&mut working_page, slot_index, offset)?;
            layout::release_space::<Table, Leaf>(&mut working_page, cell_offset, existing_len)?;
            *self.bytes_mut() = working_page;
            return Ok(());
        }

        let space_error =
            layout::page_full_for_update::<Table, Leaf>(&working_page, new_len, existing_len)?;
        if space_error.needed > space_error.available {
            return Err(TablePageError::PageFull {
                needed: space_error.needed,
                available: space_error.available,
            });
        }

        rewrite_leaf_page(
            &mut working_page,
            Some(LeafCellReplacement { slot_index, row_id, payload }),
        )?;
        *self.bytes_mut() = working_page;
        Ok(())
    }

    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index = match find_leaf_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let existing_cell = layout::cell_bytes_at_slot::<Table, Leaf>(self.bytes(), slot_index)?;
        let existing_len =
            leaf_cell_len(existing_cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let cell_offset = layout::slot_offset::<Table, Leaf>(self.bytes(), slot_index)?;

        layout::remove_slot::<Table, Leaf>(self.bytes_mut(), slot_index)?;
        layout::release_space::<Table, Leaf>(self.bytes_mut(), cell_offset, existing_len)
    }

    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        rewrite_leaf_page(self.bytes_mut(), None)
    }
}

impl<'a> Page<Read<'a>, Table, Interior> {
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate::<Table, Interior>(page)?;
        Ok(Self::new(Read { bytes: page }))
    }
}

impl<'a> Page<Write<'a>, Table, Interior> {
    pub(crate) fn init_empty(
        page: &'a mut [u8; PAGE_SIZE],
        rightmost_child: PageId,
    ) -> TablePageResult<Self> {
        layout::init_empty::<Table, Interior>(page)?;
        layout::write_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, rightmost_child);
        Ok(Self::new(Write { bytes: page }))
    }

    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        layout::validate::<Table, Interior>(page)?;
        Ok(Self::new(Write { bytes: page }))
    }
}

impl<A> Page<A, Table, Interior>
where
    A: PageAccess,
{
    pub(crate) fn cell_count(&self) -> u16 {
        self.slot_count()
    }

    pub(crate) fn search(&self, row_id: RowId) -> TablePageResult<Option<TableInteriorCell>> {
        let slot_index = match find_interior_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Ok(None),
        };

        TableInteriorCell::try_deserialize_at_slot(self.bytes(), slot_index).map(Some)
    }

    pub(crate) fn rowid_at(&self, slot_index: u16) -> TablePageResult<RowId> {
        Ok(TableInteriorCell::try_deserialize_at_slot(self.bytes(), slot_index)?.row_id)
    }

    pub(crate) fn child_at(&self, slot_index: u16) -> TablePageResult<PageId> {
        Ok(TableInteriorCell::try_deserialize_at_slot(self.bytes(), slot_index)?.left_child)
    }

    pub(crate) fn child_for_row_id(&self, row_id: RowId) -> TablePageResult<PageId> {
        let slot_count = usize::from(self.cell_count());
        let slot_index = match find_interior_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index.checked_add(1),
            SearchResult::NotFound(insertion_index) => Some(insertion_index),
        }
        .filter(|slot_index| usize::from(*slot_index) < slot_count);

        if let Some(slot_index) = slot_index {
            return Ok(
                TableInteriorCell::try_deserialize_at_slot(self.bytes(), slot_index)?.left_child
            );
        }

        Ok(self.rightmost_child())
    }

    pub(crate) fn rightmost_child(&self) -> PageId {
        layout::read_u64_at(self.bytes(), layout::INTERIOR_RIGHTMOST_CHILD_OFFSET)
    }

    pub(crate) fn prev_sibling(&self) -> Option<PageId> {
        layout::prev_sibling(self.bytes())
    }

    pub(crate) fn next_sibling(&self) -> Option<PageId> {
        layout::next_sibling(self.bytes())
    }
}

impl<A> Page<A, Table, Interior>
where
    A: PageAccessMut,
{
    pub(crate) fn insert(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let insertion_index = match find_interior_row_id(self.bytes(), row_id)? {
            SearchResult::Found(_) => return Err(TablePageError::DuplicateRowId { row_id }),
            SearchResult::NotFound(insertion_index) => insertion_index,
        };

        let cell_offset = match layout::try_allocate_space::<Table, Interior>(
            self.bytes_mut(),
            INTERIOR_CELL_SIZE,
            layout::CellWriteMode::Insert,
        )? {
            Ok(offset) => offset,
            Err(space_error) => {
                if space_error.needed > space_error.available {
                    return Err(TablePageError::PageFull {
                        needed: space_error.needed,
                        available: space_error.available,
                    });
                }

                rewrite_interior_page(self.bytes_mut())?;

                match layout::try_allocate_space::<Table, Interior>(
                    self.bytes_mut(),
                    INTERIOR_CELL_SIZE,
                    layout::CellWriteMode::Insert,
                )? {
                    Ok(offset) => offset,
                    Err(_) => {
                        return Err(TablePageError::CorruptPage(
                            TablePageCorruptionKind::CellContentUnderflow,
                        ));
                    }
                }
            }
        };
        write_interior_cell_at(self.bytes_mut(), usize::from(cell_offset), left_child, row_id);
        layout::insert_slot::<Table, Interior>(self.bytes_mut(), insertion_index, cell_offset)
    }

    pub(crate) fn update(&mut self, row_id: RowId, left_child: PageId) -> TablePageResult<()> {
        let slot_index = match find_interior_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let existing_cell =
            layout::cell_bytes_at_slot::<Table, Interior>(self.bytes(), slot_index)?;
        if existing_cell.len() < INTERIOR_CELL_SIZE {
            return Err(TablePageError::CorruptCell { slot_index });
        }

        let cell_offset =
            usize::from(layout::slot_offset::<Table, Interior>(self.bytes(), slot_index)?);
        write_interior_cell_at(self.bytes_mut(), cell_offset, left_child, row_id);
        Ok(())
    }

    pub(crate) fn delete(&mut self, row_id: RowId) -> TablePageResult<()> {
        let slot_index = match find_interior_row_id(self.bytes(), row_id)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::NotFound(_) => return Err(TablePageError::RowIdNotFound { row_id }),
        };

        let cell_offset = layout::slot_offset::<Table, Interior>(self.bytes(), slot_index)?;
        layout::remove_slot::<Table, Interior>(self.bytes_mut(), slot_index)?;
        layout::release_space::<Table, Interior>(self.bytes_mut(), cell_offset, INTERIOR_CELL_SIZE)
    }

    pub(crate) fn set_rightmost_child(&mut self, page_id: PageId) -> TablePageResult<()> {
        layout::write_u64_at(self.bytes_mut(), layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, page_id);
        Ok(())
    }

    pub(crate) fn defragment(&mut self) -> TablePageResult<()> {
        rewrite_interior_page(self.bytes_mut())
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
        let cell = layout::cell_bytes_at_slot_on_valid_page::<Table, Leaf>(page, mid_u16)?;
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

#[derive(Debug, Clone, Copy)]
struct LeafCellReplacement<'a> {
    slot_index: u16,
    row_id: RowId,
    payload: &'a [u8],
}

fn rewrite_leaf_page(
    page: &mut [u8; PAGE_SIZE],
    replacement: Option<LeafCellReplacement<'_>>,
) -> TablePageResult<()> {
    let prev_sibling = layout::prev_sibling(page);
    let next_sibling = layout::next_sibling(page);
    let cell_count = usize::from(layout::cell_count(page));
    let mut scratch = [0u8; PAGE_SIZE];
    let mut scratch_len = 0usize;
    let copy_slot = |slot_index: u16,
                     scratch: &mut [u8; PAGE_SIZE],
                     scratch_len: &mut usize|
     -> TablePageResult<()> {
        let cell = layout::cell_bytes_at_slot_on_valid_page::<Table, Leaf>(page, slot_index)?;
        let cell_len =
            leaf_cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index })?;
        let next = *scratch_len + cell_len;
        if next > scratch.len() {
            return Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellContentUnderflow));
        }

        scratch[*scratch_len..next].copy_from_slice(&cell[..cell_len]);
        *scratch_len = next;
        Ok(())
    };

    match replacement {
        None => {
            for slot in 0..cell_count {
                copy_slot(slot as u16, &mut scratch, &mut scratch_len)?;
            }
        }
        Some(replacement_cell) => {
            let replacement_slot = usize::from(replacement_cell.slot_index);
            if replacement_slot >= cell_count {
                for slot in 0..cell_count {
                    copy_slot(slot as u16, &mut scratch, &mut scratch_len)?;
                }
            } else {
                for slot in 0..replacement_slot {
                    copy_slot(slot as u16, &mut scratch, &mut scratch_len)?;
                }

                let cell_len = leaf_cell_encoded_len(replacement_cell.payload)?;
                let next = scratch_len + cell_len;
                if next > scratch.len() {
                    return Err(TablePageError::CorruptPage(
                        TablePageCorruptionKind::CellContentUnderflow,
                    ));
                }
                let cell = &mut scratch[scratch_len..next];
                let payload_len = u16::try_from(replacement_cell.payload.len())
                    .expect("payload length validated before writing leaf cell");
                cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
                cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE]
                    .copy_from_slice(&replacement_cell.row_id.to_le_bytes());
                cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(replacement_cell.payload);
                scratch_len = next;

                for slot in (replacement_slot + 1)..cell_count {
                    copy_slot(slot as u16, &mut scratch, &mut scratch_len)?;
                }
            }
        }
    }

    layout::init_empty::<Table, Leaf>(page)?;
    layout::set_prev_sibling(page, prev_sibling);
    layout::set_next_sibling(page, next_sibling);

    let mut scratch_offset = 0usize;
    for slot in 0..cell_count {
        let slot_u16 = slot as u16;
        let cell_len = leaf_cell_len(&scratch[scratch_offset..scratch_len])
            .map_err(|_| TablePageError::CorruptCell { slot_index: slot_u16 })?;
        let next = scratch_offset + cell_len;
        let cell_offset =
            match layout::try_allocate_space::<Table, Leaf>(page, cell_len, CellWriteMode::Insert)?
            {
                Ok(offset) => offset,
                Err(_) => {
                    return Err(TablePageError::CorruptPage(
                        TablePageCorruptionKind::CellContentUnderflow,
                    ));
                }
            };
        page[usize::from(cell_offset)..usize::from(cell_offset) + cell_len]
            .copy_from_slice(&scratch[scratch_offset..next]);
        layout::insert_slot::<Table, Leaf>(page, slot_u16, cell_offset)?;
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
    let cell_end = cell_offset + leaf_cell_encoded_len(payload)?;
    let cell = &mut page[cell_offset..cell_end];
    debug_assert_eq!(cell.len(), LEAF_CELL_PREFIX_SIZE + payload.len());
    let payload_len =
        u16::try_from(payload.len()).expect("payload length validated before writing leaf cell");
    cell[0..PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len.to_le_bytes());
    cell[PAYLOAD_LEN_SIZE..LEAF_CELL_PREFIX_SIZE].copy_from_slice(&row_id.to_le_bytes());
    cell[LEAF_CELL_PREFIX_SIZE..].copy_from_slice(payload);
    Ok(())
}

/// Performs row-id lookup on interior pages with interior-specific spec and decoder.
fn find_interior_row_id(page: &[u8; PAGE_SIZE], row_id: RowId) -> TablePageResult<SearchResult> {
    let cell_count = usize::from(layout::cell_count(page));
    let mut left = 0usize;
    let mut right = cell_count;

    while left < right {
        let mid = left + ((right - left) / 2);
        let mid_u16 = mid as u16;
        let cell = layout::cell_bytes_at_slot_on_valid_page::<Table, Interior>(page, mid_u16)?;
        if cell.len() < INTERIOR_CELL_SIZE {
            return Err(TablePageError::CorruptCell { slot_index: mid_u16 });
        }
        let current_row_id = read_u64(cell, LEFT_CHILD_SIZE);

        match current_row_id.cmp(&row_id) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(SearchResult::Found(mid_u16)),
        }
    }

    let insertion_index = left as u16;
    Ok(SearchResult::NotFound(insertion_index))
}

fn rewrite_interior_page(page: &mut [u8; PAGE_SIZE]) -> TablePageResult<()> {
    let prev_sibling = layout::prev_sibling(page);
    let next_sibling = layout::next_sibling(page);
    let rightmost_child = layout::read_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET);
    let cell_count = usize::from(layout::cell_count(page));
    let mut scratch = [0u8; PAGE_SIZE];
    let mut scratch_len = 0usize;

    for slot in 0..cell_count {
        let slot_u16 = slot as u16;
        let cell = layout::cell_bytes_at_slot_on_valid_page::<Table, Interior>(page, slot_u16)?;

        if cell.len() < INTERIOR_CELL_SIZE {
            return Err(TablePageError::CorruptCell { slot_index: slot_u16 });
        }
        let next = scratch_len + INTERIOR_CELL_SIZE;
        if next > scratch.len() {
            return Err(TablePageError::CorruptPage(TablePageCorruptionKind::CellContentUnderflow));
        }
        scratch[scratch_len..next].copy_from_slice(&cell[..INTERIOR_CELL_SIZE]);
        scratch_len = next;
    }

    layout::init_empty::<Table, Interior>(page)?;
    layout::set_prev_sibling(page, prev_sibling);
    layout::set_next_sibling(page, next_sibling);
    layout::write_u64_at(page, layout::INTERIOR_RIGHTMOST_CHILD_OFFSET, rightmost_child);

    let mut scratch_offset = 0usize;
    for slot in 0..cell_count {
        let slot_u16 = slot as u16;
        let next = scratch_offset + INTERIOR_CELL_SIZE;
        let cell = &scratch[scratch_offset..next];
        let cell_offset = match layout::try_allocate_space::<Table, Interior>(
            page,
            cell.len(),
            layout::CellWriteMode::Insert,
        )? {
            Ok(offset) => offset,
            Err(_) => {
                return Err(TablePageError::CorruptPage(
                    TablePageCorruptionKind::CellContentUnderflow,
                ));
            }
        };
        page[usize::from(cell_offset)..usize::from(cell_offset) + cell.len()].copy_from_slice(cell);
        layout::insert_slot::<Table, Interior>(page, slot_u16, cell_offset)?;
        scratch_offset = next;
    }

    Ok(())
}

fn write_interior_cell_at(
    page: &mut [u8; PAGE_SIZE],
    cell_offset: usize,
    left_child: PageId,
    row_id: RowId,
) {
    let cell_end = cell_offset + INTERIOR_CELL_SIZE;
    let cell = &mut page[cell_offset..cell_end];
    cell[0..LEFT_CHILD_SIZE].copy_from_slice(&left_child.to_le_bytes());
    cell[LEFT_CHILD_SIZE..INTERIOR_CELL_SIZE].copy_from_slice(&row_id.to_le_bytes());
}

/// Reads a little-endian `u16` from `bytes` at `offset`.
fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_checksum::PAGE_DATA_END;

    const CONTENT_START_OFFSET: usize = 4;
    const LEAF_SLOT_DIR_OFFSET: usize = 24;
    const INTERIOR_SLOT_DIR_OFFSET: usize = 32;

    fn fill_leaf_page_with_payload(
        page: &mut Page<Write<'_>, Table, Leaf>,
        payload: &[u8],
    ) -> Vec<RowId> {
        let mut row_ids = Vec::new();
        let mut next_row_id = 1;

        loop {
            match page.insert(next_row_id, payload) {
                Ok(()) => {
                    row_ids.push(next_row_id);
                    next_row_id += 1;
                }
                Err(TablePageError::PageFull { .. }) => return row_ids,
                Err(err) => panic!("unexpected insert error while filling page: {err:?}"),
            }
        }
    }

    #[test]
    fn leaf_defragment_compacts_page_and_preserves_siblings() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();
            page.insert(10, b"ten").unwrap();
            page.insert(20, b"twenty").unwrap();
            page.insert(30, b"thirty").unwrap();
        }

        layout::set_prev_sibling(&mut bytes, Some(7));
        layout::set_next_sibling(&mut bytes, Some(8));

        let free_space_before = {
            let mut page = Page::<Write<'_>, Table, Leaf>::from_bytes(&mut bytes).unwrap();
            page.delete(20).unwrap();
            page.free_space().unwrap()
        };

        assert_ne!(layout::first_freeblock(&bytes), 0);

        let mut page = Page::<Write<'_>, Table, Leaf>::from_bytes(&mut bytes).unwrap();
        page.defragment().unwrap();

        assert_eq!(page.prev_sibling(), Some(7));
        assert_eq!(page.next_sibling(), Some(8));
        assert_eq!(page.len(), 2);
        assert_eq!(page.rowid_at(0).unwrap(), 10);
        assert_eq!(page.rowid_at(1).unwrap(), 30);
        assert_eq!(page.search(10).unwrap().unwrap().payload, b"ten");
        assert_eq!(page.search(20).unwrap(), None);
        assert_eq!(page.search(30).unwrap().unwrap().payload, b"thirty");
        assert_eq!(page.free_space().unwrap(), free_space_before);
        drop(page);
        assert_eq!(layout::first_freeblock(&bytes), 0);
        assert_eq!(layout::fragmented_free_bytes(&bytes), 0);
    }

    #[test]
    fn leaf_update_rewrites_first_when_fragmented_bytes_would_overflow() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();
            page.insert(10, b"rust").unwrap();
        }

        layout::set_prev_sibling(&mut bytes, Some(11));
        layout::set_next_sibling(&mut bytes, Some(12));
        layout::set_fragmented_free_bytes(&mut bytes, MAX_FRAGMENTED_FREE_BYTES);

        let mut page = Page::<Write<'_>, Table, Leaf>::from_bytes(&mut bytes).unwrap();
        page.update(10, b"rus").unwrap();

        assert_eq!(page.search(10).unwrap().unwrap().payload, b"rus");
        assert_eq!(page.prev_sibling(), Some(11));
        assert_eq!(page.next_sibling(), Some(12));
        drop(page);
        assert_eq!(layout::fragmented_free_bytes(&bytes), 1);
    }

    #[test]
    fn leaf_update_can_grow_cell_after_rewriting_page_with_reclaimable_space() {
        let mut bytes = [0u8; PAGE_SIZE];
        let original_payload = vec![b'a'; 100];
        let inserted_row_ids = {
            let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();
            fill_leaf_page_with_payload(&mut page, &original_payload)
        };

        assert!(inserted_row_ids.len() > 3);

        {
            let mut page = Page::<Write<'_>, Table, Leaf>::from_bytes(&mut bytes).unwrap();
            page.delete(2).unwrap();
        }
        assert_ne!(layout::first_freeblock(&bytes), 0);

        let expanded_payload = vec![b'b'; 150];
        let mut page = Page::<Write<'_>, Table, Leaf>::from_bytes(&mut bytes).unwrap();
        page.update(1, &expanded_payload).unwrap();

        assert_eq!(page.search(1).unwrap().unwrap().payload, expanded_payload.as_slice());
        assert_eq!(page.search(2).unwrap(), None);
        drop(page);
        assert_eq!(layout::first_freeblock(&bytes), 0);
        assert_eq!(layout::fragmented_free_bytes(&bytes), 0);
    }

    #[test]
    fn leaf_search_reports_corrupt_cell_when_payload_runs_past_cell_bytes() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();
            page.insert(10, b"abc").unwrap();
        }

        let cell_offset = usize::from(layout::slot_offset::<Table, Leaf>(&bytes, 0).unwrap());
        bytes[cell_offset..cell_offset + PAYLOAD_LEN_SIZE].copy_from_slice(&1000u16.to_le_bytes());

        let page = Page::<Read<'_>, Table, Leaf>::from_bytes(&bytes).unwrap();
        assert!(matches!(page.search(10), Err(TablePageError::CorruptCell { slot_index: 0 })));
    }

    #[test]
    fn interior_mutators_and_defragment_preserve_routing_and_siblings() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Interior>::init_empty(&mut bytes, 99).unwrap();
            page.insert(10, 1).unwrap();
            page.insert(20, 2).unwrap();
            page.insert(30, 3).unwrap();
            page.insert(40, 4).unwrap();
        }

        layout::set_prev_sibling(&mut bytes, Some(21));
        layout::set_next_sibling(&mut bytes, Some(22));

        {
            let mut page = Page::<Write<'_>, Table, Interior>::from_bytes(&mut bytes).unwrap();
            page.delete(20).unwrap();
            page.update(30, 300).unwrap();
            page.set_rightmost_child(123).unwrap();
        }

        assert_ne!(layout::first_freeblock(&bytes), 0);

        let mut page = Page::<Write<'_>, Table, Interior>::from_bytes(&mut bytes).unwrap();
        page.defragment().unwrap();

        assert_eq!(page.prev_sibling(), Some(21));
        assert_eq!(page.next_sibling(), Some(22));
        assert_eq!(page.rightmost_child(), 123);
        assert_eq!(page.cell_count(), 3);
        assert_eq!(page.search(10).unwrap(), Some(TableInteriorCell { left_child: 1, row_id: 10 }));
        assert_eq!(
            page.search(30).unwrap(),
            Some(TableInteriorCell { left_child: 300, row_id: 30 })
        );
        assert_eq!(page.search(20).unwrap(), None);
        assert_eq!(page.child_for_row_id(0).unwrap(), 1);
        assert_eq!(page.child_for_row_id(10).unwrap(), 300);
        assert_eq!(page.child_for_row_id(25).unwrap(), 300);
        assert_eq!(page.child_for_row_id(30).unwrap(), 4);
        assert_eq!(page.child_for_row_id(40).unwrap(), 123);
        assert_eq!(page.child_for_row_id(50).unwrap(), 123);
        drop(page);
        assert_eq!(layout::first_freeblock(&bytes), 0);
        assert_eq!(layout::fragmented_free_bytes(&bytes), 0);
    }

    #[test]
    fn interior_search_reports_corrupt_cell_when_slot_is_truncated() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Interior>::init_empty(&mut bytes, 99).unwrap();
            page.insert(10, 1).unwrap();
        }

        let truncated_offset = (PAGE_DATA_END - (INTERIOR_CELL_SIZE - 1)) as u16;
        bytes[CONTENT_START_OFFSET..CONTENT_START_OFFSET + 2]
            .copy_from_slice(&truncated_offset.to_le_bytes());
        bytes[INTERIOR_SLOT_DIR_OFFSET..INTERIOR_SLOT_DIR_OFFSET + 2]
            .copy_from_slice(&truncated_offset.to_le_bytes());

        let page = Page::<Read<'_>, Table, Interior>::from_bytes(&bytes).unwrap();
        assert!(matches!(page.search(10), Err(TablePageError::CorruptCell { slot_index: 0 })));
    }

    #[test]
    fn leaf_search_reports_corrupt_cell_when_slot_is_too_short_for_row_id_lookup() {
        let mut bytes = [0u8; PAGE_SIZE];
        {
            let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();
            page.insert(10, b"abc").unwrap();
        }

        let truncated_offset = (PAGE_DATA_END - (LEAF_CELL_PREFIX_SIZE - 1)) as u16;
        bytes[CONTENT_START_OFFSET..CONTENT_START_OFFSET + 2]
            .copy_from_slice(&truncated_offset.to_le_bytes());
        bytes[LEAF_SLOT_DIR_OFFSET..LEAF_SLOT_DIR_OFFSET + 2]
            .copy_from_slice(&truncated_offset.to_le_bytes());

        let page = Page::<Read<'_>, Table, Leaf>::from_bytes(&bytes).unwrap();
        assert!(matches!(page.search(10), Err(TablePageError::CorruptCell { slot_index: 0 })));
    }
}
