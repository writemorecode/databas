use core::marker::PhantomData;

use crate::types::{PAGE_SIZE, PageId, RowId};

use super::{
    error::{PageCorruption, PageError, PageResult},
    format::{
        self, CELL_LENGTH_SIZE, CONTENT_START_OFFSET, FORMAT_VERSION, KIND_OFFSET,
        SLOT_COUNT_OFFSET, USABLE_SPACE_END, VERSION_OFFSET,
    },
};

#[derive(Debug)]
pub enum Leaf {}

#[derive(Debug)]
pub enum Interior {}

pub trait NodeMarker {
    const KIND: format::PageKind;
}

impl NodeMarker for Leaf {
    const KIND: format::PageKind = format::PageKind::Leaf;
}

impl NodeMarker for Interior {
    const KIND: format::PageKind = format::PageKind::Interior;
}

#[derive(Debug, Clone, Copy)]
pub struct Read<'a> {
    pub(crate) bytes: &'a [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub struct Write<'a> {
    pub(crate) bytes: &'a mut [u8; PAGE_SIZE],
}

pub trait PageAccess {
    fn bytes(&self) -> &[u8; PAGE_SIZE];
}

pub trait PageAccessMut: PageAccess {
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}

impl PageAccess for Read<'_> {
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.bytes
    }
}

impl PageAccess for Write<'_> {
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.bytes
    }
}

impl PageAccessMut for Write<'_> {
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.bytes
    }
}

#[derive(Debug)]
pub struct Page<A, N> {
    access: A,
    _marker: PhantomData<N>,
}

#[derive(Debug)]
pub enum AnyPage<A> {
    Leaf(Page<A, Leaf>),
    Interior(Page<A, Interior>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    Found(u16),
    InsertAt(u16),
}

impl<A, N> Page<A, N> {
    fn new(access: A) -> Self {
        Self { access, _marker: PhantomData }
    }
}

impl<A, N> Page<A, N>
where
    A: PageAccess,
{
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.access.bytes()
    }

    pub fn kind(&self) -> format::PageKind
    where
        N: NodeMarker,
    {
        N::KIND
    }

    pub fn version(&self) -> u8 {
        self.bytes()[VERSION_OFFSET]
    }

    pub fn slot_count(&self) -> u16 {
        format::read_u16(self.bytes(), SLOT_COUNT_OFFSET)
    }

    pub fn content_start(&self) -> u16 {
        format::read_u16(self.bytes(), CONTENT_START_OFFSET)
    }

    pub fn free_space(&self) -> usize
    where
        N: NodeMarker,
    {
        self.content_start() as usize - self.slot_directory_end()
    }

    pub(crate) fn slot_directory_end(&self) -> usize
    where
        N: NodeMarker,
    {
        N::KIND.header_size() + self.slot_count() as usize * format::SLOT_ENTRY_SIZE
    }

    pub(crate) fn slot_offset(&self, slot_index: u16) -> PageResult<u16>
    where
        N: NodeMarker,
    {
        self.validate_slot_index(slot_index)?;
        let offset = format::slot_entry_offset(N::KIND.header_size(), slot_index);
        Ok(format::read_u16(self.bytes(), offset))
    }

    pub(crate) fn search_slots_by<F>(&self, key: RowId, mut read_key: F) -> PageResult<SearchResult>
    where
        N: NodeMarker,
        F: FnMut(&Self, u16) -> PageResult<RowId>,
    {
        let mut low = 0_u16;
        let mut high = self.slot_count();

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_key = read_key(self, mid)?;
            if mid_key < key {
                low = mid + 1;
            } else if mid_key > key {
                high = mid;
            } else {
                return Ok(SearchResult::Found(mid));
            }
        }

        Ok(SearchResult::InsertAt(low))
    }

    pub(crate) fn validate_slot_index(&self, slot_index: u16) -> PageResult<()> {
        let slot_count = self.slot_count();
        if slot_index >= slot_count {
            return Err(PageError::InvalidSlotIndex { slot_index, slot_count });
        }
        Ok(())
    }

    pub(crate) fn raw_cell_length(&self, slot_index: u16) -> PageResult<usize>
    where
        N: NodeMarker,
    {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        let cell_len = format::read_u16(self.bytes(), cell_offset) as usize;
        if cell_len < CELL_LENGTH_SIZE {
            return Err(PageError::CorruptCell {
                slot_index,
                kind: super::CellCorruption::LengthTooSmall,
            });
        }
        if cell_offset + cell_len > USABLE_SPACE_END {
            return Err(PageError::CorruptCell {
                slot_index,
                kind: super::CellCorruption::LengthOutOfBounds,
            });
        }
        Ok(cell_len)
    }
}

impl<A, N> Page<A, N>
where
    A: PageAccessMut,
    N: NodeMarker,
{
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.access.bytes_mut()
    }

    pub(crate) fn set_slot_count(&mut self, slot_count: u16) {
        format::write_u16(self.bytes_mut(), SLOT_COUNT_OFFSET, slot_count);
    }

    pub(crate) fn set_content_start(&mut self, content_start: u16) {
        format::write_u16(self.bytes_mut(), CONTENT_START_OFFSET, content_start);
    }

    pub(crate) fn set_slot_offset(&mut self, slot_index: u16, cell_offset: u16) -> PageResult<()> {
        self.validate_slot_index(slot_index)?;
        let offset = format::slot_entry_offset(N::KIND.header_size(), slot_index);
        format::write_u16(self.bytes_mut(), offset, cell_offset);
        Ok(())
    }

    pub(crate) fn insert_slot(&mut self, slot_index: u16, cell_offset: u16) -> PageResult<()> {
        let slot_count = self.slot_count();
        if slot_index > slot_count {
            return Err(PageError::InvalidSlotIndex { slot_index, slot_count });
        }

        let header_size = N::KIND.header_size();
        let insert_at = format::slot_entry_offset(header_size, slot_index);
        let tail_start = insert_at;
        let tail_end = format::slot_entry_offset(header_size, slot_count);

        self.bytes_mut().copy_within(tail_start..tail_end, tail_start + format::SLOT_ENTRY_SIZE);
        format::write_u16(self.bytes_mut(), insert_at, cell_offset);
        self.set_slot_count(slot_count + 1);
        Ok(())
    }

    pub(crate) fn remove_slot(&mut self, slot_index: u16) -> PageResult<u16> {
        self.validate_slot_index(slot_index)?;
        let slot_count = self.slot_count();
        let header_size = N::KIND.header_size();
        let remove_at = format::slot_entry_offset(header_size, slot_index);
        let removed = format::read_u16(self.bytes(), remove_at);
        let tail_start = remove_at + format::SLOT_ENTRY_SIZE;
        let tail_end = format::slot_entry_offset(header_size, slot_count);

        self.bytes_mut().copy_within(tail_start..tail_end, remove_at);
        let last_slot = format::slot_entry_offset(header_size, slot_count - 1);
        self.bytes_mut()[last_slot..last_slot + format::SLOT_ENTRY_SIZE].fill(0);
        self.set_slot_count(slot_count - 1);
        Ok(removed)
    }

    pub(crate) fn defragment(&mut self) -> PageResult<()> {
        let slot_count = self.slot_count();
        let header_size = N::KIND.header_size();
        let bytes = self.bytes();
        let mut packed = [0_u8; PAGE_SIZE];
        packed[..header_size].copy_from_slice(&bytes[..header_size]);

        let mut cursor = USABLE_SPACE_END;
        for slot_index in (0..slot_count).rev() {
            let cell_offset = self.slot_offset(slot_index)? as usize;
            let cell_len = self.raw_cell_length(slot_index)?;
            cursor -= cell_len;
            packed[cursor..cursor + cell_len]
                .copy_from_slice(&bytes[cell_offset..cell_offset + cell_len]);
            format::write_u16(
                &mut packed,
                format::slot_entry_offset(header_size, slot_index),
                cursor as u16,
            );
        }
        format::write_u16(&mut packed, CONTENT_START_OFFSET, cursor as u16);

        *self.bytes_mut() = packed;
        Ok(())
    }

    pub(crate) fn reserve_space_for_insert(&mut self, cell_len: usize) -> PageResult<u16> {
        self.reserve_space(cell_len, format::SLOT_ENTRY_SIZE)
    }

    pub(crate) fn reserve_space_for_rewrite(&mut self, cell_len: usize) -> PageResult<u16> {
        self.reserve_space(cell_len, 0)
    }

    fn reserve_space(&mut self, cell_len: usize, extra_bytes: usize) -> PageResult<u16> {
        self.ensure_cell_fits(cell_len)?;
        let needed = cell_len + extra_bytes;
        if self.free_space() < needed {
            self.defragment()?;
        }
        let available = self.free_space();
        if available < needed {
            return Err(PageError::PageFull { needed, available });
        }

        let new_content_start = self.content_start() as usize - cell_len;
        self.set_content_start(new_content_start as u16);
        Ok(new_content_start as u16)
    }

    fn ensure_cell_fits(&self, cell_len: usize) -> PageResult<()> {
        let max = USABLE_SPACE_END - N::KIND.header_size();
        if cell_len > max || cell_len > u16::MAX as usize {
            return Err(PageError::CellTooLarge { len: cell_len, max });
        }
        Ok(())
    }
}

impl<'a, N> Page<Read<'a>, N>
where
    N: NodeMarker,
{
    pub fn open(bytes: &'a [u8; PAGE_SIZE]) -> PageResult<Self> {
        validate_page(bytes, N::KIND)?;
        Ok(Self::new(Read { bytes }))
    }
}

impl<'a, N> Page<Write<'a>, N>
where
    N: NodeMarker,
{
    pub fn open(bytes: &'a mut [u8; PAGE_SIZE]) -> PageResult<Self> {
        validate_page(bytes, N::KIND)?;
        Ok(Self::new(Write { bytes }))
    }

    pub fn as_ref(&self) -> Page<Read<'_>, N> {
        Page::new(Read { bytes: self.bytes() })
    }

    pub(crate) fn initialize(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        bytes.fill(0);
        bytes[KIND_OFFSET] = N::KIND as u8;
        bytes[VERSION_OFFSET] = FORMAT_VERSION;
        format::write_u16(bytes, SLOT_COUNT_OFFSET, 0);
        format::write_u16(bytes, CONTENT_START_OFFSET, USABLE_SPACE_END as u16);
        Self::new(Write { bytes })
    }
}

impl<'a> Page<Write<'a>, Leaf> {
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self::initialize(bytes)
    }
}

impl<'a> Page<Write<'a>, Interior> {
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE], rightmost_child: PageId) -> Self {
        Self::initialize_with_rightmost(bytes, rightmost_child)
    }

    pub(crate) fn initialize_with_rightmost(
        bytes: &'a mut [u8; PAGE_SIZE],
        page_id: PageId,
    ) -> Self {
        let mut page = Self::initialize(bytes);
        format::write_u64(page.bytes_mut(), format::RIGHTMOST_CHILD_OFFSET, page_id);
        page
    }
}

impl<'a> TryFrom<&'a [u8; PAGE_SIZE]> for AnyPage<Read<'a>> {
    type Error = PageError;

    fn try_from(bytes: &'a [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        match format::PageKind::from_raw(bytes[KIND_OFFSET]) {
            Some(format::PageKind::Leaf) => Ok(Self::Leaf(Page::<Read<'a>, Leaf>::open(bytes)?)),
            Some(format::PageKind::Interior) => {
                Ok(Self::Interior(Page::<Read<'a>, Interior>::open(bytes)?))
            }
            None => Err(PageError::InvalidPageKind {
                expected: format::PageKind::Leaf,
                actual: bytes[KIND_OFFSET],
            }),
        }
    }
}

impl<'a> TryFrom<&'a mut [u8; PAGE_SIZE]> for AnyPage<Write<'a>> {
    type Error = PageError;

    fn try_from(bytes: &'a mut [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        match format::PageKind::from_raw(bytes[KIND_OFFSET]) {
            Some(format::PageKind::Leaf) => Ok(Self::Leaf(Page::<Write<'a>, Leaf>::open(bytes)?)),
            Some(format::PageKind::Interior) => {
                Ok(Self::Interior(Page::<Write<'a>, Interior>::open(bytes)?))
            }
            None => Err(PageError::InvalidPageKind {
                expected: format::PageKind::Leaf,
                actual: bytes[KIND_OFFSET],
            }),
        }
    }
}

fn validate_page(bytes: &[u8; PAGE_SIZE], expected_kind: format::PageKind) -> PageResult<()> {
    let Some(actual_kind) = format::PageKind::from_raw(bytes[KIND_OFFSET]) else {
        return Err(PageError::InvalidPageKind {
            expected: expected_kind,
            actual: bytes[KIND_OFFSET],
        });
    };
    if actual_kind != expected_kind {
        return Err(PageError::InvalidPageKind {
            expected: expected_kind,
            actual: bytes[KIND_OFFSET],
        });
    }
    if bytes[VERSION_OFFSET] != FORMAT_VERSION {
        return Err(PageError::InvalidPageVersion {
            expected: FORMAT_VERSION,
            actual: bytes[VERSION_OFFSET],
        });
    }
    if bytes[USABLE_SPACE_END..].iter().any(|byte| *byte != 0) {
        return Err(PageError::MalformedPage(PageCorruption::ReservedFooterNotZero));
    }

    let header_size = expected_kind.header_size();
    let slot_count = format::read_u16(bytes, SLOT_COUNT_OFFSET) as usize;
    let slot_directory_end = header_size + slot_count * format::SLOT_ENTRY_SIZE;
    if slot_directory_end > USABLE_SPACE_END {
        return Err(PageError::MalformedPage(PageCorruption::SlotDirectoryExceedsUsableSpace));
    }

    let content_start = format::read_u16(bytes, CONTENT_START_OFFSET) as usize;
    if !(slot_directory_end..=USABLE_SPACE_END).contains(&content_start) {
        return Err(PageError::MalformedPage(if content_start > USABLE_SPACE_END {
            PageCorruption::ContentStartOutOfBounds
        } else {
            PageCorruption::SlotDirectoryOverlapsContent
        }));
    }

    let mut cell_ranges = Vec::with_capacity(slot_count);
    for slot_index in 0..slot_count as u16 {
        let slot_offset =
            format::read_u16(bytes, format::slot_entry_offset(header_size, slot_index)) as usize;
        if slot_offset < content_start || slot_offset >= USABLE_SPACE_END {
            return Err(PageError::MalformedPage(PageCorruption::SlotOffsetOutOfBounds));
        }
        if slot_offset + CELL_LENGTH_SIZE > USABLE_SPACE_END {
            return Err(PageError::MalformedPage(PageCorruption::CellLengthPrefixOutOfBounds));
        }

        let cell_len = format::read_u16(bytes, slot_offset) as usize;
        if expected_kind == format::PageKind::Interior
            && cell_len >= CELL_LENGTH_SIZE
            && cell_len <= USABLE_SPACE_END - slot_offset
            && cell_len != super::interior::INTERIOR_CELL_PREFIX_SIZE
        {
            return Err(PageError::CorruptCell {
                slot_index,
                kind: super::CellCorruption::UnexpectedLength,
            });
        }
        if cell_len >= CELL_LENGTH_SIZE && cell_len <= USABLE_SPACE_END - slot_offset {
            cell_ranges.push((slot_offset, slot_offset + cell_len));
        }
    }

    cell_ranges.sort_unstable_by_key(|(start, _)| *start);
    if cell_ranges.windows(2).any(|window| window[0].1 > window[1].0) {
        return Err(PageError::MalformedPage(PageCorruption::CellRangesOverlap));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized_leaf_page() -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        let _ = Page::<Write<'_>, Leaf>::initialize(&mut bytes);
        bytes
    }

    fn initialized_interior_page() -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        let _ = Page::<Write<'_>, Interior>::initialize_with_rightmost(&mut bytes, 7);
        bytes
    }

    #[test]
    fn initialize_sets_header_and_zero_footer() {
        let bytes = initialized_leaf_page();
        assert_eq!(bytes[KIND_OFFSET], format::PageKind::Leaf as u8);
        assert_eq!(bytes[VERSION_OFFSET], FORMAT_VERSION);
        assert_eq!(format::read_u16(&bytes, SLOT_COUNT_OFFSET), 0);
        assert_eq!(format::read_u16(&bytes, CONTENT_START_OFFSET), USABLE_SPACE_END as u16);
        assert_eq!(&bytes[USABLE_SPACE_END..], &[0_u8; PAGE_SIZE - USABLE_SPACE_END]);
    }

    #[test]
    fn open_rejects_invalid_kind() {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[KIND_OFFSET] = 99;
        bytes[VERSION_OFFSET] = FORMAT_VERSION;
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, USABLE_SPACE_END as u16);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::InvalidPageKind { expected: format::PageKind::Leaf, actual: 99 }
        );
    }

    #[test]
    fn open_rejects_invalid_version() {
        let mut bytes = initialized_leaf_page();
        bytes[VERSION_OFFSET] = FORMAT_VERSION + 1;

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::InvalidPageVersion { expected: FORMAT_VERSION, actual: FORMAT_VERSION + 1 }
        );
    }

    #[test]
    fn open_rejects_slot_directory_past_usable_space() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(
            &mut bytes,
            SLOT_COUNT_OFFSET,
            ((USABLE_SPACE_END - format::LEAF_HEADER_SIZE) / format::SLOT_ENTRY_SIZE + 1) as u16,
        );

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::SlotDirectoryExceedsUsableSpace)
        );
    }

    #[test]
    fn open_rejects_content_start_out_of_bounds() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, (USABLE_SPACE_END + 1) as u16);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::ContentStartOutOfBounds)
        );
    }

    #[test]
    fn open_rejects_slot_directory_overlap() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 2);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, format::LEAF_HEADER_SIZE as u16 + 1);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::SlotDirectoryOverlapsContent)
        );
    }

    #[test]
    fn open_rejects_non_zero_footer() {
        let mut bytes = initialized_leaf_page();
        bytes[PAGE_SIZE - 1] = 1;

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::ReservedFooterNotZero)
        );
    }

    #[test]
    fn open_rejects_slot_offset_before_content_region() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 1);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, 100);
        format::write_u16(&mut bytes, format::slot_entry_offset(format::LEAF_HEADER_SIZE, 0), 90);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::SlotOffsetOutOfBounds)
        );
    }

    #[test]
    fn open_rejects_length_prefix_past_usable_space() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 1);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, (USABLE_SPACE_END - 1) as u16);
        format::write_u16(
            &mut bytes,
            format::slot_entry_offset(format::LEAF_HEADER_SIZE, 0),
            (USABLE_SPACE_END - 1) as u16,
        );

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::CellLengthPrefixOutOfBounds)
        );
    }

    #[test]
    fn open_rejects_aliased_cells() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 2);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, 100);
        format::write_u16(&mut bytes, format::slot_entry_offset(format::LEAF_HEADER_SIZE, 0), 100);
        format::write_u16(&mut bytes, format::slot_entry_offset(format::LEAF_HEADER_SIZE, 1), 100);
        format::write_u16(&mut bytes, 100, 10);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::CellRangesOverlap)
        );
    }

    #[test]
    fn open_rejects_interior_cell_with_unexpected_length() {
        let mut bytes = initialized_interior_page();
        let invalid_len = super::super::interior::INTERIOR_CELL_PREFIX_SIZE + 4;
        let cell_offset = USABLE_SPACE_END - invalid_len;

        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 1);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, cell_offset as u16);
        format::write_u16(
            &mut bytes,
            format::slot_entry_offset(format::INTERIOR_HEADER_SIZE, 0),
            cell_offset as u16,
        );
        format::write_u16(&mut bytes, cell_offset, invalid_len as u16);

        let result = Page::<Read<'_>, Interior>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::CorruptCell {
                slot_index: 0,
                kind: crate::page::CellCorruption::UnexpectedLength,
            }
        );
    }

    #[test]
    fn slot_helpers_shift_and_remove_entries() {
        let mut bytes = initialized_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();

        page.insert_slot(0, 300).unwrap();
        page.insert_slot(1, 320).unwrap();
        page.insert_slot(1, 310).unwrap();

        assert_eq!(page.slot_count(), 3);
        assert_eq!(page.slot_offset(0).unwrap(), 300);
        assert_eq!(page.slot_offset(1).unwrap(), 310);
        assert_eq!(page.slot_offset(2).unwrap(), 320);

        assert_eq!(page.remove_slot(1).unwrap(), 310);
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.slot_offset(0).unwrap(), 300);
        assert_eq!(page.slot_offset(1).unwrap(), 320);
    }

    #[test]
    fn free_space_tracks_header_and_slot_directory() {
        let mut bytes = initialized_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
        page.insert_slot(0, 1000).unwrap();
        page.insert_slot(1, 1100).unwrap();
        page.set_content_start(900);

        assert_eq!(
            page.free_space(),
            900 - (format::LEAF_HEADER_SIZE + 2 * format::SLOT_ENTRY_SIZE)
        );
    }

    #[test]
    fn binary_search_reports_found_and_insert_positions() {
        let mut bytes = initialized_leaf_page();
        {
            let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();
            page.set_content_start(1000);
            page.insert_slot(0, 1000).unwrap();
            page.insert_slot(1, 1010).unwrap();
            page.insert_slot(2, 1020).unwrap();
            page.insert_slot(3, 1030).unwrap();
        }
        let page = Page::<Read<'_>, Leaf>::open(&bytes).unwrap();
        let keys = [10_u64, 20, 40, 80];

        let read_key = |_: &Page<Read<'_>, Leaf>, slot_index: u16| Ok(keys[slot_index as usize]);

        assert_eq!(page.search_slots_by(5, read_key).unwrap(), SearchResult::InsertAt(0));
        assert_eq!(page.search_slots_by(10, read_key).unwrap(), SearchResult::Found(0));
        assert_eq!(page.search_slots_by(30, read_key).unwrap(), SearchResult::InsertAt(2));
        assert_eq!(page.search_slots_by(40, read_key).unwrap(), SearchResult::Found(2));
        assert_eq!(page.search_slots_by(90, read_key).unwrap(), SearchResult::InsertAt(4));
    }
}
