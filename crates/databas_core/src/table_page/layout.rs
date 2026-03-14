use crate::types::PageId;
use crate::{page_checksum::PAGE_DATA_END, types::PAGE_SIZE};

use crate::table_page::{TablePageCorruptionKind, TablePageError, TablePageResult};
/// Current fixed header layout (all multi-byte fields are little-endian):
///
/// Shared prefix (both leaf and interior):
/// - `0`: page type (`LEAF_PAGE_TYPE` or `INTERIOR_PAGE_TYPE`)
/// - `1`: fragmented free byte count (`u8`)
/// - `2..4`: `cell_count` (`u16`)
/// - `4..6`: `content_start` (`u16`)
/// - `6..8`: first freeblock offset (`u16`, `0` means none)
/// - `8..16`: previous sibling page id (`u64`, `0` means none)
/// - `16..24`: next sibling page id (`u64`, `0` means none)
///
/// Interior-only extension:
/// - `24..32`: rightmost child page id (`u64`)
///
/// Fixed header size is `24` for leaf pages and `32` for interior pages.
/// The slot directory starts immediately after the fixed header; each slot is
/// a `u16` cell offset.
pub(super) const LEAF_PAGE_TYPE: u8 = 1;
pub(super) const INTERIOR_PAGE_TYPE: u8 = 2;

pub(super) const LEAF_HEADER_SIZE: usize = 24;
pub(super) const INTERIOR_HEADER_SIZE: usize = 32;
pub(super) const PREV_SIBLING_OFFSET: usize = 8;
pub(super) const NEXT_SIBLING_OFFSET: usize = 16;
pub(super) const INTERIOR_RIGHTMOST_CHILD_OFFSET: usize = 24;

const PAGE_TYPE_OFFSET: usize = 0;
const FRAGMENTED_FREE_BYTES_OFFSET: usize = 1;
const CELL_COUNT_OFFSET: usize = 2;
const CONTENT_START_OFFSET: usize = 4;
const FIRST_FREEBLOCK_OFFSET: usize = 6;
const SLOT_WIDTH: usize = 2;
pub(super) const FREEBLOCK_HEADER_SIZE: usize = 4;
pub(super) const MAX_FRAGMENTED_FREE_BYTES: u8 = 60;

const NO_SIBLING_PAGE_ID: PageId = 0;

const _: () = {
    assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");
    assert!(LEAF_PAGE_TYPE != INTERIOR_PAGE_TYPE, "table page types must be distinct");
    assert!(
        LEAF_HEADER_SIZE >= CONTENT_START_OFFSET + 2 && LEAF_HEADER_SIZE <= PAGE_DATA_END,
        "leaf header layout is invalid"
    );
    assert!(
        INTERIOR_HEADER_SIZE >= CONTENT_START_OFFSET + 2 && INTERIOR_HEADER_SIZE <= PAGE_DATA_END,
        "interior header layout is invalid"
    );
    assert!(
        PREV_SIBLING_OFFSET + 8 <= LEAF_HEADER_SIZE
            && PREV_SIBLING_OFFSET + 8 <= INTERIOR_HEADER_SIZE,
        "previous-sibling pointer must fit in both fixed headers"
    );
    assert!(
        NEXT_SIBLING_OFFSET + 8 <= LEAF_HEADER_SIZE
            && NEXT_SIBLING_OFFSET + 8 <= INTERIOR_HEADER_SIZE,
        "next-sibling pointer must fit in both fixed headers"
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

#[derive(Debug, Clone, Copy)]
struct Freeblock {
    offset: usize,
    next: u16,
    size: usize,
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
    set_fragmented_free_bytes(page, 0);
    set_cell_count(page, 0);
    set_content_start(page, PAGE_DATA_END);
    set_first_freeblock(page, 0);
    Ok(())
}

/// Validates that `page` matches `spec` and has internally consistent header bounds.
pub(super) fn validate(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<()> {
    let page_type = page[PAGE_TYPE_OFFSET];
    if page_type != spec.page_type {
        return Err(TablePageError::InvalidPageType { page_type });
    }

    if fragmented_free_bytes(page) > MAX_FRAGMENTED_FREE_BYTES {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::InvalidFragmentedFreeBytes,
        ));
    }

    let cell_count = usize::from(cell_count(page));
    let slot_dir_end = slot_dir_end_for_count(spec, cell_count)?;
    let content_start = usize::from(content_start(page));

    if content_start < slot_dir_end || content_start > PAGE_DATA_END {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::InvalidCellContentStart));
    }

    walk_freeblocks(page, spec, |_| Ok(()))?;

    Ok(())
}

/// Returns the number of fragmented free bytes recorded in the page header.
pub(super) fn fragmented_free_bytes(page: &[u8; PAGE_SIZE]) -> u8 {
    page[FRAGMENTED_FREE_BYTES_OFFSET]
}

/// Returns the number of slot entries currently stored in the page header.
pub(super) fn cell_count(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CELL_COUNT_OFFSET)
}

/// Returns the offset where the cell-content region currently begins.
pub(super) fn content_start(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, CONTENT_START_OFFSET)
}

/// Returns the first freeblock offset recorded in the page header.
pub(super) fn first_freeblock(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, FIRST_FREEBLOCK_OFFSET)
}

/// Returns free bytes between the slot directory end and the cell-content start.
pub(super) fn unallocated_gap(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<usize> {
    let slot_dir_end = slot_dir_end_for_count(spec, usize::from(cell_count(page)))?;
    let content_start = usize::from(content_start(page));
    Ok(content_start - slot_dir_end)
}

/// Returns total reusable space, including the gap, freeblocks, and fragments.
pub(super) fn free_space(page: &[u8; PAGE_SIZE], spec: PageSpec) -> TablePageResult<usize> {
    let mut freeblock_bytes = 0usize;
    walk_freeblocks(page, spec, |freeblock| {
        freeblock_bytes += freeblock.size;
        Ok(())
    })?;
    Ok(unallocated_gap(page, spec)? + freeblock_bytes + usize::from(fragmented_free_bytes(page)))
}

/// Returns the raw cell byte slice referenced by `slot_index`.
pub(super) fn cell_bytes_at_slot(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&[u8]> {
    cell_bytes_at_slot_on_valid_page(page, spec, slot_index)
}

/// Returns the raw cell bytes for `slot_index` on a page that already passed [`validate`].
pub(super) fn cell_bytes_at_slot_on_valid_page(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
) -> TablePageResult<&[u8]> {
    let content_start = usize::from(content_start(page));
    let cell_offset = usize::from(slot_offset(page, spec, slot_index)?);
    if cell_offset < content_start || cell_offset >= PAGE_DATA_END {
        return Err(TablePageError::CorruptCell { slot_index });
    }
    Ok(&page[cell_offset..PAGE_DATA_END])
}

pub(super) enum CellWriteMode {
    Insert,
    Update,
}

/// Attempts to reserve `size` bytes from a freeblock or the gap.
pub(super) fn try_allocate_space(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    size: usize,
    mode: CellWriteMode,
) -> TablePageResult<Result<u16, SpaceError>> {
    if size > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: size, max: usize::from(u16::MAX) });
    }

    absorb_freeblocks_into_gap(page)?;
    let additional_slots = match mode {
        CellWriteMode::Insert => 1,
        CellWriteMode::Update => 0,
    };
    let slot_dir_end_after =
        slot_dir_end_for_count(spec, usize::from(cell_count(page)) + additional_slots)?;
    let content_start = usize::from(content_start(page));
    if slot_dir_end_after > content_start {
        return Ok(Err(Ok(SpaceError {
            needed: size + (additional_slots * SLOT_WIDTH),
            available: free_space(page, spec)?,
        })?));
    }

    if let Some(offset) = allocate_from_freeblocks(page, size)? {
        return Ok(Ok(offset));
    }

    let available_gap = content_start - slot_dir_end_after;
    if size > available_gap {
        return Ok(Err(Ok(SpaceError {
            needed: size + (additional_slots * SLOT_WIDTH),
            available: free_space(page, spec)?,
        })?));
    }

    let new_start = content_start - size;
    set_content_start(page, new_start);
    Ok(Ok(new_start as u16))
}

/// Releases a previously used byte range back into the free-space structure.
pub(super) fn release_space(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    offset: u16,
    size: usize,
) -> TablePageResult<()> {
    if size == 0 {
        return Ok(());
    }

    let offset = usize::from(offset);
    let slot_dir_end = slot_dir_end_for_count(spec, usize::from(cell_count(page)))?;
    if offset < usize::from(content_start(page))
        || offset < slot_dir_end
        || offset + size > PAGE_DATA_END
    {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::InvalidFreeblockOffset));
    }

    absorb_freeblocks_into_gap(page)?;
    if offset == usize::from(content_start(page)) {
        set_content_start(page, offset + size);
        absorb_freeblocks_into_gap(page)?;
        return Ok(());
    }

    let (prev_offset, next_offset) = locate_freeblock_neighbors(page, offset)?;
    if let Some(prev_offset) = prev_offset {
        let prev = read_freeblock(page, prev_offset)?;
        if prev.offset + prev.size > offset {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::FreeblockChainOutOfOrder,
            ));
        }
    }

    if next_offset != 0 {
        let next = read_freeblock(page, usize::from(next_offset))?;
        if offset + size > next.offset {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::FreeblockChainOutOfOrder,
            ));
        }
    }

    let merges_prev = prev_offset
        .map(|prev_offset| {
            let prev = read_freeblock(page, prev_offset)?;
            Ok(prev.offset + prev.size == offset)
        })
        .transpose()?
        .unwrap_or(false);
    let merges_next = if next_offset == 0 {
        false
    } else {
        let next = read_freeblock(page, usize::from(next_offset))?;
        offset + size == next.offset
    };

    if merges_prev {
        let prev_offset = prev_offset.expect("prev offset exists when merging predecessor");
        let prev = read_freeblock(page, prev_offset)?;
        let mut merged_size = prev.size + size;
        let mut next = prev.next;

        if merges_next {
            let successor = read_freeblock(page, usize::from(next_offset))?;
            merged_size += successor.size;
            next = successor.next;
        }

        write_freeblock(page, prev.offset, next, merged_size)?;
        absorb_freeblocks_into_gap(page)?;
        return Ok(());
    }

    if merges_next {
        let successor = read_freeblock(page, usize::from(next_offset))?;
        let merged_size = size + successor.size;
        if merged_size < FREEBLOCK_HEADER_SIZE {
            return add_fragmented_bytes(page, merged_size as u8);
        }

        write_freeblock(page, offset, successor.next, merged_size)?;
        set_link_target(page, prev_offset, offset as u16);
        absorb_freeblocks_into_gap(page)?;
        return Ok(());
    }

    if size < FREEBLOCK_HEADER_SIZE {
        return add_fragmented_bytes(page, size as u8);
    }

    write_freeblock(page, offset, next_offset, size)?;
    set_link_target(page, prev_offset, offset as u16);
    absorb_freeblocks_into_gap(page)?;
    Ok(())
}

/// Computes `PageFull` accounting for updating an existing cell.
pub(super) fn page_full_for_update(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    new_cell_len: usize,
    reclaimable_bytes: usize,
) -> TablePageResult<SpaceError> {
    Ok(SpaceError {
        needed: new_cell_len,
        available: free_space(page, spec)?.saturating_add(reclaimable_bytes),
    })
}

/// Updates an existing slot to reference `cell_offset`.
pub(super) fn set_slot_offset(
    page: &mut [u8; PAGE_SIZE],
    spec: PageSpec,
    slot_index: u16,
    cell_offset: u16,
) -> TablePageResult<()> {
    let cell_count = usize::from(cell_count(page));
    let slot_index_usize = usize::from(slot_index);
    if slot_index_usize >= cell_count {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds));
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
    let cell_count = usize::from(cell_count(page));
    let insert_index_usize = usize::from(insert_index);
    if insert_index_usize > cell_count {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds));
    }

    let new_count = cell_count + 1;
    let slot_dir_end_after = slot_dir_end_for_count(spec, new_count)?;
    let content_start = usize::from(content_start(page));
    if slot_dir_end_after > content_start {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::SlotDirectoryOverlapsCellContent,
        ));
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
    let cell_count = usize::from(cell_count(page));
    let remove_index_usize = usize::from(remove_index);
    if remove_index_usize >= cell_count {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds));
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

/// Returns the previous sibling page id from the shared page-header location.
pub(super) fn prev_sibling(page: &[u8; PAGE_SIZE]) -> Option<PageId> {
    decode_sibling_page_id(read_u64_at(page, PREV_SIBLING_OFFSET))
}

/// Returns the next sibling page id from the shared page-header location.
pub(super) fn next_sibling(page: &[u8; PAGE_SIZE]) -> Option<PageId> {
    decode_sibling_page_id(read_u64_at(page, NEXT_SIBLING_OFFSET))
}

/// Writes the previous sibling page id into the shared page-header location.
pub(super) fn set_prev_sibling(page: &mut [u8; PAGE_SIZE], page_id: Option<PageId>) {
    write_u64_at(page, PREV_SIBLING_OFFSET, encode_sibling_page_id(page_id));
}

/// Writes the next sibling page id into the shared page-header location.
pub(super) fn set_next_sibling(page: &mut [u8; PAGE_SIZE], page_id: Option<PageId>) {
    write_u64_at(page, NEXT_SIBLING_OFFSET, encode_sibling_page_id(page_id));
}

/// Writes the in-header slot count field.
fn set_cell_count(page: &mut [u8; PAGE_SIZE], cell_count: u16) {
    write_u16(page, CELL_COUNT_OFFSET, cell_count);
}

/// Writes the fragmented free byte count field.
pub(super) fn set_fragmented_free_bytes(page: &mut [u8; PAGE_SIZE], fragmented: u8) {
    page[FRAGMENTED_FREE_BYTES_OFFSET] = fragmented;
}

/// Writes the in-header content-start field.
pub(super) fn set_content_start(page: &mut [u8; PAGE_SIZE], content_start: usize) {
    write_u16(page, CONTENT_START_OFFSET, content_start as u16);
}

/// Writes the first-freeblock offset field.
pub(super) fn set_first_freeblock(page: &mut [u8; PAGE_SIZE], offset: u16) {
    write_u16(page, FIRST_FREEBLOCK_OFFSET, offset);
}

/// Computes the byte offset where the slot directory ends for `cell_count` entries.
fn slot_dir_end_for_count(spec: PageSpec, cell_count: usize) -> TablePageResult<usize> {
    let slot_dir_end = spec.header_size + (cell_count * SLOT_WIDTH);

    if slot_dir_end > PAGE_DATA_END {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::SlotDirectoryExceedsPageSize,
        ));
    }

    Ok(slot_dir_end)
}

/// Computes the byte position of one slot entry inside the slot directory.
fn slot_position(spec: PageSpec, slot_index: usize) -> TablePageResult<usize> {
    let position = spec.header_size + (slot_index * SLOT_WIDTH);

    if position + SLOT_WIDTH > PAGE_DATA_END {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::SlotDirectoryExceedsPageSize,
        ));
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
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds));
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

fn walk_freeblocks(
    page: &[u8; PAGE_SIZE],
    spec: PageSpec,
    mut visitor: impl FnMut(Freeblock) -> TablePageResult<()>,
) -> TablePageResult<()> {
    let content_start = usize::from(content_start(page));
    let mut current = usize::from(first_freeblock(page));
    let mut previous_end = None;

    while current != 0 {
        let freeblock = read_freeblock(page, current)?;
        if freeblock.offset < content_start {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::InvalidFreeblockOffset,
            ));
        }
        if freeblock.offset == content_start {
            return Err(TablePageError::CorruptPage(TablePageCorruptionKind::AdjacentFreeblocks));
        }
        if let Some(previous_end) = previous_end {
            if freeblock.offset < previous_end {
                return Err(TablePageError::CorruptPage(
                    TablePageCorruptionKind::FreeblockChainOutOfOrder,
                ));
            }
            if freeblock.offset == previous_end {
                return Err(TablePageError::CorruptPage(
                    TablePageCorruptionKind::AdjacentFreeblocks,
                ));
            }
        }
        if freeblock.offset + freeblock.size > PAGE_DATA_END {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::InvalidFreeblockOffset,
            ));
        }
        if usize::from(freeblock.next) != 0 && usize::from(freeblock.next) <= freeblock.offset {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::FreeblockChainOutOfOrder,
            ));
        }
        let slot_dir_end = slot_dir_end_for_count(spec, usize::from(cell_count(page)))?;
        if freeblock.offset < slot_dir_end {
            return Err(TablePageError::CorruptPage(
                TablePageCorruptionKind::InvalidFreeblockOffset,
            ));
        }

        visitor(freeblock)?;
        previous_end = Some(freeblock.offset + freeblock.size);
        current = usize::from(freeblock.next);
    }

    Ok(())
}

fn read_freeblock(page: &[u8; PAGE_SIZE], offset: usize) -> TablePageResult<Freeblock> {
    if offset + FREEBLOCK_HEADER_SIZE > PAGE_DATA_END {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::InvalidFreeblockOffset));
    }

    let next = read_u16(page, offset);
    let size = usize::from(read_u16(page, offset + 2));
    if size < FREEBLOCK_HEADER_SIZE {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::FreeblockTooSmall));
    }

    Ok(Freeblock { offset, next, size })
}

fn write_freeblock(
    page: &mut [u8; PAGE_SIZE],
    offset: usize,
    next: u16,
    size: usize,
) -> TablePageResult<()> {
    if size > usize::from(u16::MAX) {
        return Err(TablePageError::CellTooLarge { len: size, max: usize::from(u16::MAX) });
    }
    if size < FREEBLOCK_HEADER_SIZE || offset + size > PAGE_DATA_END {
        return Err(TablePageError::CorruptPage(TablePageCorruptionKind::InvalidFreeblockOffset));
    }

    write_u16(page, offset, next);
    write_u16(page, offset + 2, size as u16);
    Ok(())
}

fn allocate_from_freeblocks(
    page: &mut [u8; PAGE_SIZE],
    size: usize,
) -> TablePageResult<Option<u16>> {
    let mut previous = None;
    let mut current = first_freeblock(page);

    while current != 0 {
        let freeblock = read_freeblock(page, usize::from(current))?;
        if freeblock.size >= size {
            let remaining = freeblock.size - size;
            if remaining == 0 {
                set_link_target(page, previous, freeblock.next);
                return Ok(Some(current));
            }

            if remaining >= FREEBLOCK_HEADER_SIZE {
                let allocated_offset = freeblock.offset + remaining;
                write_freeblock(page, freeblock.offset, freeblock.next, remaining)?;
                return Ok(Some(allocated_offset as u16));
            }

            let remaining_u8 = remaining as u8;
            let fragments = fragmented_free_bytes(page);
            if fragments.saturating_add(remaining_u8) > MAX_FRAGMENTED_FREE_BYTES {
                previous = Some(freeblock.offset);
                current = freeblock.next;
                continue;
            }

            let allocated_offset = freeblock.offset + remaining;
            set_link_target(page, previous, freeblock.next);
            add_fragmented_bytes(page, remaining_u8)?;
            return Ok(Some(allocated_offset as u16));
        }

        previous = Some(freeblock.offset);
        current = freeblock.next;
    }

    Ok(None)
}

fn locate_freeblock_neighbors(
    page: &[u8; PAGE_SIZE],
    offset: usize,
) -> TablePageResult<(Option<usize>, u16)> {
    let mut previous = None;
    let mut current = first_freeblock(page);

    while current != 0 && usize::from(current) < offset {
        let freeblock = read_freeblock(page, usize::from(current))?;
        previous = Some(freeblock.offset);
        current = freeblock.next;
    }

    Ok((previous, current))
}

fn absorb_freeblocks_into_gap(page: &mut [u8; PAGE_SIZE]) -> TablePageResult<()> {
    loop {
        let head = first_freeblock(page);
        if head == 0 {
            return Ok(());
        }

        let gap_start = usize::from(content_start(page));
        if usize::from(head) != gap_start {
            return Ok(());
        }

        let freeblock = read_freeblock(page, gap_start)?;
        set_first_freeblock(page, freeblock.next);
        set_content_start(page, gap_start + freeblock.size);
    }
}

fn set_link_target(page: &mut [u8; PAGE_SIZE], previous: Option<usize>, next: u16) {
    if let Some(previous) = previous {
        write_u16(page, previous, next);
    } else {
        set_first_freeblock(page, next);
    }
}

fn add_fragmented_bytes(page: &mut [u8; PAGE_SIZE], additional: u8) -> TablePageResult<()> {
    let fragmented = fragmented_free_bytes(page);
    let Some(next) = fragmented.checked_add(additional) else {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::InvalidFragmentedFreeBytes,
        ));
    };
    if next > MAX_FRAGMENTED_FREE_BYTES {
        return Err(TablePageError::CorruptPage(
            TablePageCorruptionKind::InvalidFragmentedFreeBytes,
        ));
    }
    set_fragmented_free_bytes(page, next);
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

fn decode_sibling_page_id(page_id: PageId) -> Option<PageId> {
    if page_id == NO_SIBLING_PAGE_ID { None } else { Some(page_id) }
}

fn encode_sibling_page_id(page_id: Option<PageId>) -> PageId {
    page_id.unwrap_or(NO_SIBLING_PAGE_ID)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SPEC: PageSpec =
        PageSpec { page_type: LEAF_PAGE_TYPE, header_size: LEAF_HEADER_SIZE };

    fn initialized_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        init_empty(&mut page, TEST_SPEC).unwrap();
        page
    }

    #[test]
    fn released_space_is_reused_before_gap_space() {
        let mut page = initialized_page();
        let first_cell = [1u8; 12];
        let second_cell = [2u8; 12];

        let first_offset =
            try_allocate_space(&mut page, TEST_SPEC, first_cell.len(), CellWriteMode::Insert)
                .unwrap()
                .unwrap();
        page[usize::from(first_offset)..usize::from(first_offset) + first_cell.len()]
            .copy_from_slice(&first_cell);
        insert_slot(&mut page, TEST_SPEC, 0, first_offset).unwrap();

        let second_offset =
            try_allocate_space(&mut page, TEST_SPEC, second_cell.len(), CellWriteMode::Insert)
                .unwrap()
                .unwrap();
        page[usize::from(second_offset)..usize::from(second_offset) + second_cell.len()]
            .copy_from_slice(&second_cell);
        insert_slot(&mut page, TEST_SPEC, 1, second_offset).unwrap();

        release_space(&mut page, TEST_SPEC, first_offset, first_cell.len()).unwrap();
        assert_eq!(first_freeblock(&page), first_offset);

        let reused_offset =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(reused_offset)..usize::from(reused_offset) + 12]
            .copy_from_slice(&[9u8; 12]);
        assert_eq!(reused_offset, first_offset);
        assert_eq!(first_freeblock(&page), 0);
    }

    #[test]
    fn allocating_from_freeblock_can_leave_fragment_bytes() {
        let mut page = initialized_page();
        let reusable_offset =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(reusable_offset)..usize::from(reusable_offset) + 12]
            .copy_from_slice(&[1u8; 12]);
        let _live_offset =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(_live_offset)..usize::from(_live_offset) + 12].copy_from_slice(&[2u8; 12]);

        release_space(&mut page, TEST_SPEC, reusable_offset, 12).unwrap();
        let allocated_offset =
            try_allocate_space(&mut page, TEST_SPEC, 10, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(allocated_offset)..usize::from(allocated_offset) + 10]
            .copy_from_slice(&[2u8; 10]);

        assert_eq!(allocated_offset, reusable_offset + 2);
        assert_eq!(fragmented_free_bytes(&page), 2);
        assert_eq!(first_freeblock(&page), 0);
    }

    #[test]
    fn allocated_space_can_be_written_after_reservation() {
        let mut page = initialized_page();

        let offset =
            try_allocate_space(&mut page, TEST_SPEC, 4, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(offset)..usize::from(offset) + 4].copy_from_slice(&[1, 2, 3, 4]);

        assert_eq!(&page[usize::from(offset)..usize::from(offset) + 4], &[1, 2, 3, 4]);
    }

    #[test]
    fn releasing_adjacent_ranges_coalesces_freeblocks() {
        let mut page = initialized_page();
        let higher =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(higher)..usize::from(higher) + 12].copy_from_slice(&[1u8; 12]);
        let middle =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(middle)..usize::from(middle) + 12].copy_from_slice(&[2u8; 12]);
        let lower =
            try_allocate_space(&mut page, TEST_SPEC, 12, CellWriteMode::Update).unwrap().unwrap();
        page[usize::from(lower)..usize::from(lower) + 12].copy_from_slice(&[3u8; 12]);

        assert_eq!(usize::from(middle) + 12, usize::from(higher));
        assert_eq!(usize::from(lower) + 12, usize::from(middle));

        release_space(&mut page, TEST_SPEC, higher, 12).unwrap();
        release_space(&mut page, TEST_SPEC, middle, 12).unwrap();

        let freeblock = read_freeblock(&page, usize::from(first_freeblock(&page))).unwrap();
        assert_eq!(freeblock.offset, usize::from(middle));
        assert_eq!(freeblock.size, 24);
        assert_eq!(freeblock.next, 0);
    }

    #[test]
    fn validate_rejects_fragment_count_above_maximum() {
        let mut page = initialized_page();
        set_fragmented_free_bytes(&mut page, MAX_FRAGMENTED_FREE_BYTES + 1);

        let err = validate(&page, TEST_SPEC).unwrap_err();
        assert!(matches!(
            err,
            TablePageError::CorruptPage(TablePageCorruptionKind::InvalidFragmentedFreeBytes)
        ));
    }

    #[test]
    fn validate_rejects_out_of_bounds_freeblock() {
        let mut page = initialized_page();
        set_content_start(&mut page, PAGE_DATA_END - 32);
        set_first_freeblock(&mut page, (PAGE_DATA_END - 2) as u16);

        let err = validate(&page, TEST_SPEC).unwrap_err();
        assert!(matches!(
            err,
            TablePageError::CorruptPage(TablePageCorruptionKind::InvalidFreeblockOffset)
        ));
    }

    #[test]
    fn validate_rejects_too_small_freeblock() {
        let mut page = initialized_page();
        let offset = PAGE_DATA_END - 24;
        set_content_start(&mut page, PAGE_DATA_END - 32);
        set_first_freeblock(&mut page, offset as u16);
        write_u16(&mut page, offset, 0);
        write_u16(&mut page, offset + 2, 3);

        let err = validate(&page, TEST_SPEC).unwrap_err();
        assert!(matches!(
            err,
            TablePageError::CorruptPage(TablePageCorruptionKind::FreeblockTooSmall)
        ));
    }

    #[test]
    fn validate_rejects_out_of_order_freeblock_chain() {
        let mut page = initialized_page();
        let first = PAGE_DATA_END - 24;
        let second = PAGE_DATA_END - 40;
        set_content_start(&mut page, PAGE_DATA_END - 48);
        set_first_freeblock(&mut page, first as u16);
        write_freeblock(&mut page, first, second as u16, 12).unwrap();
        write_freeblock(&mut page, second, 0, 12).unwrap();

        let err = validate(&page, TEST_SPEC).unwrap_err();
        assert!(matches!(
            err,
            TablePageError::CorruptPage(TablePageCorruptionKind::FreeblockChainOutOfOrder)
        ));
    }

    #[test]
    fn validate_rejects_adjacent_freeblocks() {
        let mut page = initialized_page();
        let first = PAGE_DATA_END - 24;
        let second = PAGE_DATA_END - 12;
        set_content_start(&mut page, PAGE_DATA_END - 40);
        set_first_freeblock(&mut page, first as u16);
        write_freeblock(&mut page, first, second as u16, 12).unwrap();
        write_freeblock(&mut page, second, 0, 12).unwrap();

        let err = validate(&page, TEST_SPEC).unwrap_err();
        assert!(matches!(
            err,
            TablePageError::CorruptPage(TablePageCorruptionKind::AdjacentFreeblocks)
        ));
    }
}
