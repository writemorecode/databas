use crate::{
    error::{TablePageError, TablePageResult},
    types::PAGE_SIZE,
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

const _: () = {
    assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");
    assert!(LEAF_PAGE_TYPE != INTERIOR_PAGE_TYPE, "table page types must be distinct");
    assert!(
        LEAF_HEADER_SIZE >= CONTENT_START_OFFSET + 2 && LEAF_HEADER_SIZE <= PAGE_SIZE,
        "leaf header layout is invalid"
    );
    assert!(
        INTERIOR_HEADER_SIZE >= CONTENT_START_OFFSET + 2 && INTERIOR_HEADER_SIZE <= PAGE_SIZE,
        "interior header layout is invalid"
    );
    assert!(
        INTERIOR_RIGHTMOST_CHILD_OFFSET + 8 <= INTERIOR_HEADER_SIZE,
        "interior rightmost-child pointer must fit in the fixed header"
    );
};

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

/// Initializes `page` as an empty instance of the page kind described by `spec`.
pub(super) fn init_empty(page: &mut [u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<()> {
    page.fill(0);
    page[PAGE_TYPE_OFFSET] = spec.page_type;
    set_cell_count(page, 0);
    set_content_start(page, PAGE_SIZE);
    Ok(())
}

/// Validates that `page` matches `spec` and has internally consistent header bounds.
pub(super) fn validate(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<()> {
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

/// Returns the number of slot entries currently stored in the page header.
pub(super) fn cell_count(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CELL_COUNT_OFFSET)
}

/// Returns the offset where the cell-content region currently begins.
pub(super) fn content_start(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CONTENT_START_OFFSET)
}

/// Returns free bytes between the slot directory end and the cell-content start.
pub(super) fn free_space(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<usize> {
    validate(page, spec)?;

    let slot_dir_end = slot_dir_end_for_count(spec, usize::from(cell_count(page)))?;
    let content_start = usize::from(content_start(page));
    Ok(content_start - slot_dir_end)
}

/// Returns the raw cell byte slice referenced by `slot_index`.
pub(super) fn cell_bytes_at_slot(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&[u8]> {
    validate(page, spec)?;
    cell_bytes_at_slot_on_valid_page(page, spec, slot_index)
}

/// Returns the raw cell bytes for `slot_index` on a page that already passed [`validate`].
pub(super) fn cell_bytes_at_slot_on_valid_page<'a>(
    page: &'a [u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&'a [u8]> {
    let content_start = usize::from(content_start(page));
    let cell_offset = usize::from(slot_offset(page, spec, slot_index)?);
    if cell_offset < content_start || cell_offset >= PAGE_SIZE {
        return Err(TablePageError::CorruptCell { slot_index });
    }
    Ok(&page[cell_offset..])
}

/// Attempts to append a pre-encoded cell into the cell-content region.
pub(super) fn try_append_cell(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    cell: &[u8],
) -> TablePageResult<Result<u16, SpaceError>> {
    validate(page, spec)?;

    let cell_len = cell.len();
    if cell_len > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: cell_len });
    }

    let current_count = usize::from(cell_count(page));
    let slot_dir_end_after = slot_dir_end_for_count(spec, current_count)?;
    let content_start = usize::from(content_start(page));
    let available = content_start.saturating_sub(slot_dir_end_after);

    if cell_len > available {
        return Ok(Err(SpaceError { needed: cell_len, available }));
    }

    let new_start = content_start - cell_len;
    page[new_start..content_start].copy_from_slice(cell);
    set_content_start(page, new_start);
    Ok(Ok(new_start as u16))
}

/// Attempts to append a pre-encoded cell while reserving one additional slot entry.
pub(super) fn try_append_cell_for_insert(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    cell: &[u8],
) -> TablePageResult<Result<u16, SpaceError>> {
    validate(page, spec)?;

    let cell_len = cell.len();
    if cell_len > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: cell_len });
    }

    let current_count = usize::from(cell_count(page));
    let required_count = current_count + 1;
    let slot_dir_end_after = slot_dir_end_for_count(spec, required_count)?;
    let content_start = usize::from(content_start(page));
    let available = content_start.saturating_sub(slot_dir_end_after);

    if cell_len > available {
        return Ok(Err(SpaceError { needed: cell_len, available }));
    }

    let new_start = content_start - cell_len;
    page[new_start..content_start].copy_from_slice(cell);
    set_content_start(page, new_start);
    Ok(Ok(new_start as u16))
}

/// Updates an existing slot to reference `cell_offset`.
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

/// Inserts a new slot at `insert_index` pointing to `cell_offset`.
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

    let new_count = cell_count + 1;
    let slot_dir_end_after = slot_dir_end_for_count(spec, new_count)?;
    let content_start = usize::from(content_start(page));
    if slot_dir_end_after > content_start {
        return Err(TablePageError::CorruptPage("slot directory overlaps cell content"));
    }

    for slot in (insert_index_usize..cell_count).rev() {
        let offset = slot_offset(page, spec, slot as u16)?;
        write_slot_offset_raw(page, spec, slot + 1, offset)?;
    }

    write_slot_offset_raw(page, spec, insert_index_usize, cell_offset)?;

    set_cell_count(page, new_count as u16);
    Ok(())
}

/// Removes the slot at `remove_index` and shifts following entries left.
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
        let slot_index = (slot + 1) as u16;
        let next_offset = slot_offset(page, spec, slot_index)?;
        write_slot_offset_raw(page, spec, slot, next_offset)?;
    }

    write_slot_offset_raw(page, spec, cell_count - 1, 0)?;

    set_cell_count(page, (cell_count - 1) as u16);
    Ok(())
}

/// Reads a little-endian `u64` from `page` at `offset`.
pub(super) fn read_u64_at(page: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&page[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

/// Writes `value` as little-endian `u64` into `page` at `offset`.
pub(super) fn write_u64_at(page: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    page[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

/// Writes the in-header slot count field.
fn set_cell_count(page: &mut [u8; PAGE_SIZE], cell_count: u16) {
    write_u16(page, CELL_COUNT_OFFSET, cell_count);
}

/// Writes the in-header content-start field.
fn set_content_start(page: &mut [u8; PAGE_SIZE], content_start: usize) {
    write_u16(page, CONTENT_START_OFFSET, content_start as u16);
}

/// Computes the byte offset where the slot directory ends for `cell_count` entries.
fn slot_dir_end_for_count(spec: PageSpec, cell_count: usize) -> TablePageResult<usize> {
    let slot_dir_end = spec.header_size + (cell_count * SLOT_WIDTH);

    if slot_dir_end > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("slot directory exceeds page size"));
    }

    Ok(slot_dir_end)
}

/// Computes the byte position of one slot entry inside the slot directory.
fn slot_position(spec: PageSpec, slot_index: usize) -> TablePageResult<usize> {
    let position = spec.header_size + (slot_index * SLOT_WIDTH);

    if position + SLOT_WIDTH > PAGE_SIZE {
        return Err(TablePageError::CorruptPage("slot directory exceeds page size"));
    }

    Ok(position)
}

/// Reads the cell-content offset stored in `slot_index`.
pub(super) fn slot_offset(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<u16> {
    let cell_count = usize::from(cell_count(page));
    let slot_index = usize::from(slot_index);
    if slot_index >= cell_count {
        return Err(TablePageError::CorruptPage("slot index out of bounds"));
    }

    let position = slot_position(spec, slot_index)?;
    Ok(read_u16(page, position))
}

/// Writes `cell_offset` into `slot_index` without checking current slot count.
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

/// Reads a little-endian `u16` from `page` at `offset`.
fn read_u16(page: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&page[offset..offset + 2]);
    u16::from_le_bytes(bytes)
}

/// Writes `value` as little-endian `u16` into `page` at `offset`.
fn write_u16(page: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    page[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}
