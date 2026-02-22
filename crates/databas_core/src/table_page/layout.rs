use std::cmp::Ordering;

use crate::{
    error::{TablePageError, TablePageResult},
    types::{PAGE_SIZE, RowId},
};

pub(super) const LEAF_PAGE_TYPE: u8 = 1;
pub(super) const INTERIOR_PAGE_TYPE: u8 = 2;

pub(super) const LEAF_HEADER_SIZE: usize = 8;
pub(super) const INTERIOR_HEADER_SIZE: usize = 16;
pub(super) const INTERIOR_RIGHTMOST_CHILD_OFFSET: usize = 8;

const PAGE_TYPE_OFFSET: usize = 0;
const CELL_COUNT_OFFSET: usize = 2;
const CONTENT_START_OFFSET: usize = 4;
const SLOT_WIDTH: usize = 2;

/// Static properties for one table-page kind used by shared layout helpers.
#[derive(Debug, Clone, Copy)]
pub(super) struct PageSpec {
    /// Discriminant written into the page-type header byte.
    pub(super) page_type: u8,
    /// Total size of the fixed header before the slot directory starts.
    pub(super) header_size: usize,
}

/// Space accounting returned when append-at-end allocation cannot fit a cell.
#[derive(Debug, Clone, Copy)]
pub(super) struct SpaceError {
    /// Bytes required by the requested write.
    pub(super) needed: usize,
    /// Bytes currently available in the unallocated region.
    pub(super) available: usize,
}

/// Row-id lookup result containing either the matching slot or insertion point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchResult {
    /// The row id exists and maps to this slot index.
    Found(u16),
    /// The row id does not exist; the payload is the sorted insertion index.
    NotFound(u16),
}

/// Returns the raw page type byte from the fixed page header.
pub(super) fn page_type(page: &[u8; PAGE_SIZE]) -> u8 {
    page[PAGE_TYPE_OFFSET]
}

pub(super) fn init_empty(page: &mut [u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<()> {
    validate_spec(spec)?;

    page.fill(0);
    page[PAGE_TYPE_OFFSET] = spec.page_type;
    set_cell_count(page, 0);
    set_content_start(page, PAGE_SIZE)
}

pub(super) fn validate(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<()> {
    validate_spec(spec)?;

    let page_type = page[PAGE_TYPE_OFFSET];
    if page_type != spec.page_type {
        return Err(TablePageError::InvalidPageType(page_type));
    }

    let cell_count = usize::from(cell_count(page));
    let slot_dir_end = slot_dir_end_for_count(spec, cell_count)?;
    let content_start = usize::from(content_start(page));

    if content_start < slot_dir_end || content_start > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("invalid cell content start"));
    }

    Ok(())
}

pub(super) fn cell_count(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CELL_COUNT_OFFSET)
}

pub(super) fn content_start(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CONTENT_START_OFFSET)
}

pub(super) fn free_space(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<usize> {
    validate(page, spec)?;

    let slot_dir_end = slot_dir_end_for_count(spec, usize::from(cell_count(page)))?;
    let content_start = usize::from(content_start(page));
    Ok(content_start - slot_dir_end)
}

pub(super) fn find_row_id<F>(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    row_id: RowId,
    row_id_from_cell: F,
) -> TablePageResult<SearchResult>
where
    F: Fn(&[u8]) -> TablePageResult<RowId>,
{
    validate(page, spec)?;

    let cell_count = usize::from(cell_count(page));
    let mut left = 0usize;
    let mut right = cell_count;

    while left < right {
        let mid = left + ((right - left) / 2);
        let mid_u16 =
            u16::try_from(mid).map_err(|_| TablePageError::CorruptPage("slot index overflow"))?;
        let cell = cell_bytes_at_slot_impl(page, spec, mid_u16)?;
        let current_row_id = row_id_from_cell(cell)
            .map_err(|_| TablePageError::CorruptCell { slot_index: mid_u16 })?;

        match current_row_id.cmp(&row_id) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(SearchResult::Found(mid_u16)),
        }
    }

    let insertion_index =
        u16::try_from(left).map_err(|_| TablePageError::CorruptPage("slot index overflow"))?;
    Ok(SearchResult::NotFound(insertion_index))
}

pub(super) fn cell_bytes_at_slot(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&[u8]> {
    validate(page, spec)?;
    cell_bytes_at_slot_impl(page, spec, slot_index)
}

pub(super) fn try_append_cell(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    cell: &[u8],
    extra_slots: usize,
) -> TablePageResult<Result<u16, SpaceError>> {
    validate(page, spec)?;

    if cell.len() > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: cell.len() });
    }

    let current_count = usize::from(cell_count(page));
    let required_count = current_count
        .checked_add(extra_slots)
        .ok_or(TablePageError::CorruptPage("cell count overflow"))?;

    let slot_dir_end_after = slot_dir_end_for_count(spec, required_count)?;
    let content_start = usize::from(content_start(page));
    let available = content_start.saturating_sub(slot_dir_end_after);

    if cell.len() > available {
        return Ok(Err(SpaceError { needed: cell.len(), available }));
    }

    let new_start = content_start - cell.len();
    page[new_start..content_start].copy_from_slice(cell);
    set_content_start(page, new_start)?;

    let offset_u16 = u16::try_from(new_start)
        .map_err(|_| TablePageError::CorruptPage("cell offset overflow"))?;
    Ok(Ok(offset_u16))
}

pub(super) fn set_slot_offset(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
    cell_offset: u16,
) -> TablePageResult<()> {
    validate(page, spec)?;

    let cell_count = usize::from(cell_count(page));
    let slot_index_usize = usize::from(slot_index);
    if slot_index_usize >= cell_count {
        return Err(TablePageError::CorruptPage("slot index out of bounds"));
    }

    write_slot_offset_raw(page, spec, slot_index_usize, cell_offset)
}

pub(super) fn insert_slot(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    insert_index: u16,
    cell_offset: u16,
) -> TablePageResult<()> {
    validate(page, spec)?;

    let cell_count = usize::from(cell_count(page));
    let insert_index_usize = usize::from(insert_index);
    if insert_index_usize > cell_count {
        return Err(TablePageError::CorruptPage("slot index out of bounds"));
    }

    let new_count =
        cell_count.checked_add(1).ok_or(TablePageError::CorruptPage("cell count overflow"))?;
    let slot_dir_end_after = slot_dir_end_for_count(spec, new_count)?;
    let content_start = usize::from(content_start(page));
    if slot_dir_end_after > content_start {
        return Err(TablePageError::CorruptPage("slot directory overlaps cell content"));
    }

    for slot in (insert_index_usize..cell_count).rev() {
        let offset = slot_offset(page, spec, u16::try_from(slot).expect("slot index overflow"))?;
        write_slot_offset_raw(page, spec, slot + 1, offset)?;
    }

    write_slot_offset_raw(page, spec, insert_index_usize, cell_offset)?;

    let new_count_u16 =
        u16::try_from(new_count).map_err(|_| TablePageError::CorruptPage("cell count overflow"))?;
    set_cell_count(page, new_count_u16);
    Ok(())
}

pub(super) fn remove_slot(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    remove_index: u16,
) -> TablePageResult<()> {
    validate(page, spec)?;

    let cell_count = usize::from(cell_count(page));
    let remove_index_usize = usize::from(remove_index);
    if remove_index_usize >= cell_count {
        return Err(TablePageError::CorruptPage("slot index out of bounds"));
    }

    for slot in remove_index_usize..(cell_count - 1) {
        let next_offset =
            slot_offset(page, spec, u16::try_from(slot + 1).expect("slot index overflow"))?;
        write_slot_offset_raw(page, spec, slot, next_offset)?;
    }

    write_slot_offset_raw(page, spec, cell_count - 1, 0)?;

    let new_count = u16::try_from(cell_count - 1)
        .map_err(|_| TablePageError::CorruptPage("cell count overflow"))?;
    set_cell_count(page, new_count);
    Ok(())
}

pub(super) fn defragment<F>(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    cell_len: F,
) -> TablePageResult<()>
where
    F: Fn(&[u8]) -> TablePageResult<usize>,
{
    validate(page, spec)?;

    let cell_count = usize::from(cell_count(page));
    if cell_count == 0 {
        set_content_start(page, PAGE_SIZE)?;
        return Ok(());
    }

    let mut cells = Vec::with_capacity(cell_count);

    for slot in 0..cell_count {
        let slot_u16 =
            u16::try_from(slot).map_err(|_| TablePageError::CorruptPage("slot index overflow"))?;
        let cell = cell_bytes_at_slot_impl(page, spec, slot_u16)?;
        let len =
            cell_len(cell).map_err(|_| TablePageError::CorruptCell { slot_index: slot_u16 })?;

        if len == 0 || len > cell.len() {
            return Err(TablePageError::CorruptCell { slot_index: slot_u16 });
        }

        cells.push(cell[..len].to_vec());
    }

    let mut next_start = PAGE_SIZE;
    for (slot, cell) in cells.into_iter().enumerate() {
        next_start = next_start
            .checked_sub(cell.len())
            .ok_or(TablePageError::CorruptPage("cell content underflow"))?;

        let end = next_start + cell.len();
        page[next_start..end].copy_from_slice(&cell);
        let cell_offset = u16::try_from(next_start)
            .map_err(|_| TablePageError::CorruptPage("offset overflow"))?;
        write_slot_offset_raw(page, spec, slot, cell_offset)?;
    }

    set_content_start(page, next_start)
}

pub(super) fn read_u64_at(page: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&page[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

pub(super) fn write_u64_at(page: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    page[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn validate_spec(spec: PageSpec) -> TablePageResult<()> {
    if spec.header_size < CONTENT_START_OFFSET + 2 || spec.header_size > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("invalid page header size"));
    }

    Ok(())
}

fn set_cell_count(page: &mut [u8; PAGE_SIZE], cell_count: u16) {
    write_u16(page, CELL_COUNT_OFFSET, cell_count);
}

fn set_content_start(page: &mut [u8; PAGE_SIZE], content_start: usize) -> TablePageResult<()> {
    let content_start = u16::try_from(content_start)
        .map_err(|_| TablePageError::CorruptPage("content start overflow"))?;
    write_u16(page, CONTENT_START_OFFSET, content_start);
    Ok(())
}

fn slot_dir_end_for_count(spec: PageSpec, cell_count: usize) -> TablePageResult<usize> {
    let slots_bytes = cell_count
        .checked_mul(SLOT_WIDTH)
        .ok_or(TablePageError::CorruptPage("slot directory overflow"))?;
    let slot_dir_end = spec
        .header_size
        .checked_add(slots_bytes)
        .ok_or(TablePageError::CorruptPage("slot directory overflow"))?;

    if slot_dir_end > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("slot directory exceeds page size"));
    }

    Ok(slot_dir_end)
}

fn slot_position(spec: PageSpec, slot_index: usize) -> TablePageResult<usize> {
    let slot_bytes = slot_index
        .checked_mul(SLOT_WIDTH)
        .ok_or(TablePageError::CorruptPage("slot directory overflow"))?;
    let position = spec
        .header_size
        .checked_add(slot_bytes)
        .ok_or(TablePageError::CorruptPage("slot directory overflow"))?;

    if position + SLOT_WIDTH > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("slot directory exceeds page size"));
    }

    Ok(position)
}

fn slot_offset(page: &[u8; PAGE_SIZE], spec: PageSpec, slot_index: u16) -> TablePageResult<u16> {
    let cell_count = usize::from(cell_count(page));
    let slot_index = usize::from(slot_index);
    if slot_index >= cell_count {
        return Err(TablePageError::CorruptPage("slot index out of bounds"));
    }

    let position = slot_position(spec, slot_index)?;
    Ok(read_u16(page, position))
}

fn write_slot_offset_raw(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: usize,
    cell_offset: u16,
) -> TablePageResult<()> {
    let position = slot_position(spec, slot_index)?;
    write_u16(page, position, cell_offset);
    Ok(())
}

fn cell_bytes_at_slot_impl(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&[u8]> {
    let content_start = usize::from(content_start(page));
    let cell_offset = usize::from(slot_offset(page, spec, slot_index)?);

    if cell_offset < content_start || cell_offset >= PAGE_SIZE {
        return Err(TablePageError::CorruptCell { slot_index });
    }

    Ok(&page[cell_offset..])
}

fn read_u16(page: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&page[offset..offset + 2]);
    u16::from_le_bytes(bytes)
}

fn write_u16(page: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    page[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}
