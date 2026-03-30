use core::marker::PhantomData;
use std::cmp::Ordering;

use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    error::{PageCorruption, PageError, PageResult},
    format::{
        self, CELL_LENGTH_SIZE, CONTENT_START_OFFSET, FIRST_FREEBLOCK_OFFSET, FORMAT_VERSION,
        FRAGMENTED_FREE_BYTES_OFFSET, FREEBLOCK_HEADER_SIZE, KIND_OFFSET,
        MAX_FRAGMENTED_FREE_BYTES, NEXT_PAGE_ID_OFFSET, PREV_PAGE_ID_OFFSET, SLOT_COUNT_OFFSET,
        USABLE_SPACE_END, VERSION_OFFSET,
    },
};

/// Marker type for leaf pages that store `(row_id, payload)` records.
#[derive(Debug)]
pub enum Leaf {}

/// Marker type for interior pages that store separator keys and child pointers.
#[derive(Debug)]
pub enum Interior {}

/// Associates a typed page marker with its encoded [`format::PageKind`].
pub trait NodeMarker {
    /// The page kind represented by this marker.
    const KIND: format::PageKind;
}

impl NodeMarker for Leaf {
    const KIND: format::PageKind = format::PageKind::Leaf;
}

impl NodeMarker for Interior {
    const KIND: format::PageKind = format::PageKind::Interior;
}

/// Shared immutable access to a page-sized byte buffer.
#[derive(Debug, Clone, Copy)]
pub struct Read<'a> {
    pub(crate) bytes: &'a [u8; PAGE_SIZE],
}

/// Shared mutable access to a page-sized byte buffer.
#[derive(Debug)]
pub struct Write<'a> {
    pub(crate) bytes: &'a mut [u8; PAGE_SIZE],
}

/// Abstraction over page access modes that can expose immutable bytes.
pub trait PageAccess {
    /// Returns the underlying fixed-size page buffer.
    fn bytes(&self) -> &[u8; PAGE_SIZE];
}

/// Extension of [`PageAccess`] for access modes that can mutate the page buffer.
pub trait PageAccessMut: PageAccess {
    /// Returns the underlying page buffer mutably.
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

/// A typed view over an encoded page.
///
/// `A` controls the access mode ([`Read`] or [`Write`]), while `N` controls the
/// logical page kind ([`Leaf`] or [`Interior`]).
#[derive(Debug)]
pub struct Page<A, N> {
    access: A,
    _marker: PhantomData<N>,
}

/// A page whose concrete node kind is determined by the encoded page header.
#[derive(Debug)]
pub enum AnyPage<A> {
    /// A leaf page.
    Leaf(Page<A, Leaf>),
    /// An interior page.
    Interior(Page<A, Interior>),
}

/// Result of searching a sorted slot directory by key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    /// The key already exists at the returned slot index.
    Found(SlotId),
    /// The key is absent and should be inserted at the returned slot index.
    InsertAt(SlotId),
}

/// Result of locating a bound within a sorted slot directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundResult {
    /// The bound resolves to an existing slot index.
    At(SlotId),
    /// The bound lies past the last slot on the page.
    PastEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Freeblock {
    pub(crate) offset: u16,
    pub(crate) size: u16,
    pub(crate) next: Option<u16>,
}

impl Freeblock {
    fn end(self) -> usize {
        self.offset as usize + self.size as usize
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FreeblockIter<'a> {
    bytes: &'a [u8; PAGE_SIZE],
    content_start: u16,
    next: Option<u16>,
}

impl<'a> FreeblockIter<'a> {
    fn new(bytes: &'a [u8; PAGE_SIZE], content_start: u16, next: Option<u16>) -> Self {
        Self { bytes, content_start, next }
    }
}

impl Iterator for FreeblockIter<'_> {
    type Item = PageResult<Freeblock>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.next?;
        match read_freeblock(self.bytes, self.content_start, offset) {
            Ok(freeblock) => {
                self.next = freeblock.next;
                Some(Ok(freeblock))
            }
            Err(err) => {
                self.next = None;
                Some(Err(err))
            }
        }
    }
}

fn read_freeblock(
    bytes: &[u8; PAGE_SIZE],
    content_start: u16,
    offset: u16,
) -> PageResult<Freeblock> {
    let offset = offset as usize;
    if offset < content_start as usize || offset + FREEBLOCK_HEADER_SIZE > USABLE_SPACE_END {
        return Err(PageError::MalformedPage(PageCorruption::FreeblockOffsetOutOfBounds));
    }

    let size = format::read_u16(bytes, offset + 2);
    if (size as usize) < FREEBLOCK_HEADER_SIZE {
        return Err(PageError::MalformedPage(PageCorruption::FreeblockTooSmall));
    }
    if offset + size as usize > USABLE_SPACE_END {
        return Err(PageError::MalformedPage(PageCorruption::FreeblockOutOfBounds));
    }

    Ok(Freeblock { offset: offset as u16, size, next: format::read_optional_u16(bytes, offset) })
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
    /// Returns the raw page bytes.
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.access.bytes()
    }

    /// Returns the statically known kind of this page.
    pub fn kind(&self) -> format::PageKind
    where
        N: NodeMarker,
    {
        N::KIND
    }

    /// Returns the encoded page-format version.
    pub fn version(&self) -> u8 {
        self.bytes()[VERSION_OFFSET]
    }

    /// Returns the number of live slots in the slot directory.
    pub fn slot_count(&self) -> u16 {
        format::read_u16(self.bytes(), SLOT_COUNT_OFFSET)
    }

    /// Returns the start offset of the packed cell-content region.
    pub fn content_start(&self) -> u16 {
        format::read_u16(self.bytes(), CONTENT_START_OFFSET)
    }

    /// Returns the first freeblock on the page, if one exists.
    pub fn first_freeblock(&self) -> Option<u16> {
        format::read_optional_u16(self.bytes(), FIRST_FREEBLOCK_OFFSET)
    }

    /// Returns the previous sibling page id on the same tree level, if present.
    pub fn prev_page_id(&self) -> Option<PageId> {
        format::read_optional_u64(self.bytes(), PREV_PAGE_ID_OFFSET)
    }

    /// Returns the next sibling page id on the same tree level, if present.
    pub fn next_page_id(&self) -> Option<PageId> {
        format::read_optional_u64(self.bytes(), NEXT_PAGE_ID_OFFSET)
    }

    /// Returns the contiguous free space between the slot directory and cell content.
    pub fn free_space(&self) -> usize
    where
        N: NodeMarker,
    {
        self.content_start() as usize - self.slot_directory_end()
    }

    pub(crate) fn fragmented_free_bytes(&self) -> u16 {
        format::read_u16(self.bytes(), FRAGMENTED_FREE_BYTES_OFFSET)
    }

    pub(crate) fn freeblocks(&self) -> FreeblockIter<'_> {
        FreeblockIter::new(self.bytes(), self.content_start(), self.first_freeblock())
    }

    pub(crate) fn total_reclaimable_space(&self) -> PageResult<usize>
    where
        N: NodeMarker,
    {
        let mut total = self.free_space() + self.fragmented_free_bytes() as usize;
        for freeblock in self.freeblocks() {
            total += freeblock?.size as usize;
        }
        Ok(total)
    }

    pub(crate) fn slot_directory_end(&self) -> usize
    where
        N: NodeMarker,
    {
        N::KIND.header_size() + self.slot_count() as usize * format::SLOT_ENTRY_SIZE
    }

    pub(crate) fn slot_offset(&self, slot_index: SlotId) -> PageResult<u16>
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
        F: FnMut(&Self, SlotId) -> PageResult<RowId>,
    {
        let mut low: SlotId = 0;
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

    pub(crate) fn bound_slots_by<F, P>(
        &self,
        key: RowId,
        mut read_key: F,
        mut go_right: P,
    ) -> PageResult<BoundResult>
    where
        N: NodeMarker,
        F: FnMut(&Self, SlotId) -> PageResult<RowId>,
        P: FnMut(Ordering) -> bool,
    {
        let mut low: SlotId = 0;
        let mut high = self.slot_count();

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_key = read_key(self, mid)?;
            if go_right(mid_key.cmp(&key)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == self.slot_count() { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    pub(crate) fn lower_bound_slots_by<F>(&self, key: RowId, read_key: F) -> PageResult<BoundResult>
    where
        N: NodeMarker,
        F: FnMut(&Self, SlotId) -> PageResult<RowId>,
    {
        self.bound_slots_by(key, read_key, |ordering| ordering == Ordering::Less)
    }

    pub(crate) fn upper_bound_slots_by<F>(&self, key: RowId, read_key: F) -> PageResult<BoundResult>
    where
        N: NodeMarker,
        F: FnMut(&Self, SlotId) -> PageResult<RowId>,
    {
        self.bound_slots_by(key, read_key, |ordering| ordering != Ordering::Greater)
    }

    pub(crate) fn validate_slot_index(&self, slot_index: SlotId) -> PageResult<()> {
        let slot_count = self.slot_count();
        if slot_index >= slot_count {
            return Err(PageError::InvalidSlotIndex { slot_index, slot_count });
        }
        Ok(())
    }

    pub(crate) fn cell_len(&self, slot_index: SlotId) -> PageResult<usize>
    where
        N: NodeMarker,
    {
        let cell_offset = self.slot_offset(slot_index)? as usize;
        match N::KIND {
            format::PageKind::Leaf => {
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
            format::PageKind::Interior => Ok(super::interior::INTERIOR_CELL_SIZE),
        }
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

    pub(crate) fn set_first_freeblock(&mut self, first_freeblock: Option<u16>) {
        format::write_optional_u16(self.bytes_mut(), FIRST_FREEBLOCK_OFFSET, first_freeblock);
    }

    /// Updates the previous sibling page id stored in the page header.
    pub fn set_prev_page_id(&mut self, page_id: Option<PageId>) {
        format::write_optional_u64(self.bytes_mut(), PREV_PAGE_ID_OFFSET, page_id);
    }

    /// Updates the next sibling page id stored in the page header.
    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        format::write_optional_u64(self.bytes_mut(), NEXT_PAGE_ID_OFFSET, page_id);
    }

    pub(crate) fn set_fragmented_free_bytes(&mut self, fragmented_free_bytes: u16) {
        format::write_u16(self.bytes_mut(), FRAGMENTED_FREE_BYTES_OFFSET, fragmented_free_bytes);
    }

    pub(crate) fn set_slot_offset(
        &mut self,
        slot_index: SlotId,
        cell_offset: u16,
    ) -> PageResult<()> {
        self.validate_slot_index(slot_index)?;
        let offset = format::slot_entry_offset(N::KIND.header_size(), slot_index);
        format::write_u16(self.bytes_mut(), offset, cell_offset);
        Ok(())
    }

    pub(crate) fn insert_slot(&mut self, slot_index: SlotId, cell_offset: u16) -> PageResult<()> {
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

    pub(crate) fn remove_slot(&mut self, slot_index: SlotId) -> PageResult<u16> {
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
            let cell_len = self.cell_len(slot_index)?;
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
        format::write_optional_u16(&mut packed, FIRST_FREEBLOCK_OFFSET, None);
        format::write_u16(&mut packed, FRAGMENTED_FREE_BYTES_OFFSET, 0);

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

        if self.free_space() >= extra_bytes
            && let Some((previous, freeblock)) = self.find_first_fit_freeblock(cell_len)?
        {
            let remainder = freeblock.size as usize - cell_len;
            if remainder == 0
                || remainder >= FREEBLOCK_HEADER_SIZE
                || self.can_store_fragmented_bytes(remainder)
            {
                return self.allocate_from_freeblock(previous, freeblock, cell_len);
            }
        }

        if self.free_space() >= needed {
            return Ok(self.allocate_from_gap(cell_len));
        }

        let available = self.total_reclaimable_space()?;
        if available < needed {
            return Err(PageError::PageFull { needed, available });
        }

        self.defragment()?;
        let available = self.free_space();
        if available < needed {
            return Err(PageError::PageFull { needed, available });
        }

        Ok(self.allocate_from_gap(cell_len))
    }

    fn ensure_cell_fits(&self, cell_len: usize) -> PageResult<()> {
        let max = USABLE_SPACE_END - N::KIND.header_size();
        if cell_len > max || cell_len > u16::MAX as usize {
            return Err(PageError::CellTooLarge { len: cell_len, max });
        }
        Ok(())
    }

    fn find_first_fit_freeblock(
        &self,
        cell_len: usize,
    ) -> PageResult<Option<(Option<Freeblock>, Freeblock)>> {
        let mut previous = None;
        for freeblock in self.freeblocks() {
            let freeblock = freeblock?;
            if freeblock.size as usize >= cell_len {
                return Ok(Some((previous, freeblock)));
            }
            previous = Some(freeblock);
        }
        Ok(None)
    }

    fn allocate_from_gap(&mut self, cell_len: usize) -> u16 {
        let new_content_start = self.content_start() as usize - cell_len;
        self.set_content_start(new_content_start as u16);
        new_content_start as u16
    }

    fn can_store_fragmented_bytes(&self, extra: usize) -> bool {
        self.fragmented_free_bytes() as usize + extra <= MAX_FRAGMENTED_FREE_BYTES as usize
    }

    fn write_freeblock(&mut self, offset: u16, next: Option<u16>, size: u16) {
        format::write_optional_u16(self.bytes_mut(), offset as usize, next);
        format::write_u16(self.bytes_mut(), offset as usize + 2, size);
    }

    fn set_chain_link(&mut self, previous: Option<Freeblock>, next: Option<u16>) {
        match previous {
            Some(previous) => {
                format::write_optional_u16(self.bytes_mut(), previous.offset as usize, next)
            }
            None => self.set_first_freeblock(next),
        }
    }

    fn add_fragmented_bytes(&mut self, extra: u16) -> PageResult<()> {
        let total = self.fragmented_free_bytes() + extra;
        if total > MAX_FRAGMENTED_FREE_BYTES {
            self.defragment()?;
            return Ok(());
        }
        self.set_fragmented_free_bytes(total);
        Ok(())
    }

    fn allocate_from_freeblock(
        &mut self,
        previous: Option<Freeblock>,
        freeblock: Freeblock,
        cell_len: usize,
    ) -> PageResult<u16> {
        let remainder = freeblock.size as usize - cell_len;
        if remainder == 0 {
            self.set_chain_link(previous, freeblock.next);
            return Ok(freeblock.offset);
        }

        if remainder >= FREEBLOCK_HEADER_SIZE {
            self.write_freeblock(freeblock.offset, freeblock.next, remainder as u16);
            return Ok(freeblock.offset + remainder as u16);
        }

        self.set_chain_link(previous, freeblock.next);
        self.add_fragmented_bytes(remainder as u16)?;
        Ok(freeblock.offset + remainder as u16)
    }

    fn absorb_freeblocks_into_gap(&mut self) -> PageResult<()> {
        while let Some(first_freeblock) = self.first_freeblock() {
            if first_freeblock as usize != self.content_start() as usize {
                break;
            }

            let freeblock = read_freeblock(self.bytes(), self.content_start(), first_freeblock)?;
            self.set_first_freeblock(freeblock.next);
            self.set_content_start(freeblock.end() as u16);
        }
        Ok(())
    }

    pub(crate) fn reclaim_space(&mut self, cell_offset: u16, cell_len: usize) -> PageResult<()> {
        if self.slot_count() == 0 {
            self.reset_empty_page();
            return Ok(());
        }

        let reclaim_start = cell_offset as usize;
        // If the reclaimed bytes touch the unallocated gap, grow the gap upward and
        // then keep folding in any freeblocks that now become adjacent to it.
        if reclaim_start == self.content_start() as usize {
            self.set_content_start((reclaim_start + cell_len) as u16);
            self.absorb_freeblocks_into_gap()?;
            return Ok(());
        }

        let reclaim_end = reclaim_start + cell_len;
        let mut previous = None;
        let mut next = None;
        // The freeblock chain stays sorted by offset, so a single walk finds both
        // neighbors we may need to merge with.
        for freeblock in self.freeblocks() {
            let freeblock = freeblock?;
            if freeblock.offset as usize >= reclaim_end {
                next = Some(freeblock);
                break;
            }
            previous = Some(freeblock);
        }

        let merged_with_previous = previous.filter(|freeblock| freeblock.end() == reclaim_start);
        let merged_with_next = next.filter(|freeblock| reclaim_end == freeblock.offset as usize);

        if let Some(previous) = merged_with_previous {
            // Reuse the previous freeblock header when the reclaimed region bridges
            // into it, optionally absorbing the next freeblock too.
            let merged_end = merged_with_next.map_or(reclaim_end, Freeblock::end);
            let next_link = match merged_with_next {
                Some(freeblock) => freeblock.next,
                None => previous.next,
            };
            self.write_freeblock(
                previous.offset,
                next_link,
                (merged_end - previous.offset as usize) as u16,
            );
            return Ok(());
        }

        let merged_start = cell_offset;
        let merged_end = merged_with_next.map_or(reclaim_end, Freeblock::end);
        let merged_size = merged_end - merged_start as usize;
        if merged_size < FREEBLOCK_HEADER_SIZE {
            // Tiny isolated holes cannot encode a freeblock header, so they count
            // toward fragmented free bytes until a later defragmentation pass.
            return self.add_fragmented_bytes(merged_size as u16);
        }

        let next_link = match merged_with_next {
            Some(freeblock) => freeblock.next,
            None => next.map(|freeblock| freeblock.offset),
        };
        self.write_freeblock(merged_start, next_link, merged_size as u16);
        self.set_chain_link(previous, Some(merged_start));
        Ok(())
    }

    fn reset_empty_page(&mut self) {
        self.set_content_start(USABLE_SPACE_END as u16);
        self.set_first_freeblock(None);
        self.set_fragmented_free_bytes(0);
    }
}

impl<'a, N> Page<Read<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens an immutable typed page view over an initialized buffer.
    pub fn open(bytes: &'a [u8; PAGE_SIZE]) -> PageResult<Self> {
        validate_page(bytes, N::KIND)?;
        Ok(Self::new(Read { bytes }))
    }
}

impl<'a, N> Page<Write<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens a mutable typed page view over an initialized buffer.
    pub fn open(bytes: &'a mut [u8; PAGE_SIZE]) -> PageResult<Self> {
        validate_page(bytes, N::KIND)?;
        Ok(Self::new(Write { bytes }))
    }

    /// Borrows this mutable page as an immutable page view.
    pub fn as_ref(&self) -> Page<Read<'_>, N> {
        Page::new(Read { bytes: self.bytes() })
    }

    pub(crate) fn initialize(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        bytes.fill(0);
        bytes[KIND_OFFSET] = N::KIND as u8;
        bytes[VERSION_OFFSET] = FORMAT_VERSION;
        format::write_u16(bytes, SLOT_COUNT_OFFSET, 0);
        format::write_u16(bytes, CONTENT_START_OFFSET, USABLE_SPACE_END as u16);
        format::write_optional_u16(bytes, FIRST_FREEBLOCK_OFFSET, None);
        format::write_u16(bytes, FRAGMENTED_FREE_BYTES_OFFSET, 0);
        format::write_optional_u64(bytes, PREV_PAGE_ID_OFFSET, None);
        format::write_optional_u64(bytes, NEXT_PAGE_ID_OFFSET, None);
        Self::new(Write { bytes })
    }
}

impl<'a> Page<Write<'a>, Leaf> {
    /// Initializes a fresh empty leaf page in-place.
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self::initialize(bytes)
    }
}

impl<'a> Page<Write<'a>, Interior> {
    /// Initializes a fresh empty interior page with its rightmost child pointer set.
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
            None => Err(PageError::UnknownPageKind { actual: bytes[KIND_OFFSET] }),
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
            None => Err(PageError::UnknownPageKind { actual: bytes[KIND_OFFSET] }),
        }
    }
}

fn validate_page(bytes: &[u8; PAGE_SIZE], expected_kind: format::PageKind) -> PageResult<()> {
    let Some(actual_kind) = format::PageKind::from_raw(bytes[KIND_OFFSET]) else {
        return Err(PageError::UnknownPageKind { actual: bytes[KIND_OFFSET] });
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
    if format::read_u16(bytes, FRAGMENTED_FREE_BYTES_OFFSET) > MAX_FRAGMENTED_FREE_BYTES {
        return Err(PageError::MalformedPage(PageCorruption::FragmentedFreeBytesTooLarge));
    }

    let first_freeblock = format::read_optional_u16(bytes, FIRST_FREEBLOCK_OFFSET);
    // Bound the number of freeblocks we traverse to avoid infinite loops
    // on malformed pages where the freeblock chain contains a cycle.
    let max_freeblocks = USABLE_SPACE_END / FREEBLOCK_HEADER_SIZE;
    for freeblock in
        FreeblockIter::new(bytes, content_start as u16, first_freeblock).take(max_freeblocks)
    {
        let _ = freeblock?;
    }

    for slot_index in 0..slot_count as SlotId {
        let slot_offset =
            format::read_u16(bytes, format::slot_entry_offset(header_size, slot_index)) as usize;
        if slot_offset < content_start || slot_offset >= USABLE_SPACE_END {
            return Err(PageError::MalformedPage(PageCorruption::SlotOffsetOutOfBounds));
        }
        if expected_kind == format::PageKind::Leaf {
            if slot_offset + CELL_LENGTH_SIZE > USABLE_SPACE_END {
                return Err(PageError::MalformedPage(PageCorruption::CellLengthPrefixOutOfBounds));
            }
        } else if slot_offset + super::interior::INTERIOR_CELL_SIZE > USABLE_SPACE_END {
            return Err(PageError::MalformedPage(PageCorruption::InteriorCellOutOfBounds));
        }
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
        assert_eq!(format::read_optional_u16(&bytes, FIRST_FREEBLOCK_OFFSET), None);
        assert_eq!(format::read_u16(&bytes, FRAGMENTED_FREE_BYTES_OFFSET), 0);
        assert_eq!(format::read_optional_u64(&bytes, PREV_PAGE_ID_OFFSET), None);
        assert_eq!(format::read_optional_u64(&bytes, NEXT_PAGE_ID_OFFSET), None);
        assert_eq!(&bytes[USABLE_SPACE_END..], &[0_u8; PAGE_SIZE - USABLE_SPACE_END]);
    }

    #[test]
    fn leaf_sibling_accessors_round_trip() {
        let mut bytes = initialized_leaf_page();
        let mut page = Page::<Write<'_>, Leaf>::open(&mut bytes).unwrap();

        assert_eq!(page.prev_page_id(), None);
        assert_eq!(page.next_page_id(), None);

        page.set_prev_page_id(Some(11));
        page.set_next_page_id(Some(22));

        let page = page.as_ref();
        assert_eq!(page.prev_page_id(), Some(11));
        assert_eq!(page.next_page_id(), Some(22));
    }

    #[test]
    fn interior_sibling_accessors_round_trip() {
        let mut bytes = initialized_interior_page();
        let mut page = Page::<Write<'_>, Interior>::open(&mut bytes).unwrap();

        assert_eq!(page.prev_page_id(), None);
        assert_eq!(page.next_page_id(), None);

        page.set_prev_page_id(Some(33));
        page.set_next_page_id(Some(44));

        let page = page.as_ref();
        assert_eq!(page.prev_page_id(), Some(33));
        assert_eq!(page.next_page_id(), Some(44));
    }

    #[test]
    fn first_freeblock_sentinel_parses_as_none() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, FIRST_FREEBLOCK_OFFSET, u16::MAX);

        let page = Page::<Read<'_>, Leaf>::open(&bytes).unwrap();

        assert_eq!(page.first_freeblock(), None);
    }

    #[test]
    fn open_rejects_unknown_kind() {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[KIND_OFFSET] = 99;
        bytes[VERSION_OFFSET] = FORMAT_VERSION;
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, USABLE_SPACE_END as u16);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(result.unwrap_err(), PageError::UnknownPageKind { actual: 99 });
    }

    #[test]
    fn open_rejects_mismatched_kind() {
        let bytes = initialized_interior_page();

        let result = Page::<Read<'_>, Leaf>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::InvalidPageKind { expected: format::PageKind::Leaf, actual: 2 }
        );
    }

    #[test]
    fn any_page_try_from_rejects_unknown_kind() {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[KIND_OFFSET] = 99;
        bytes[VERSION_OFFSET] = FORMAT_VERSION;
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, USABLE_SPACE_END as u16);

        let result = AnyPage::<Read<'_>>::try_from(&bytes);
        assert_eq!(result.unwrap_err(), PageError::UnknownPageKind { actual: 99 });
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
    fn open_rejects_fragmented_free_bytes_past_maximum() {
        let mut bytes = initialized_leaf_page();
        format::write_u16(&mut bytes, FRAGMENTED_FREE_BYTES_OFFSET, MAX_FRAGMENTED_FREE_BYTES + 1);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);

        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::FragmentedFreeBytesTooLarge)
        );
    }

    #[test]
    fn open_rejects_freeblock_smaller_than_header() {
        let mut bytes = initialized_leaf_page();
        let freeblock_offset = (USABLE_SPACE_END - FREEBLOCK_HEADER_SIZE) as u16;
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, freeblock_offset);
        format::write_optional_u16(&mut bytes, FIRST_FREEBLOCK_OFFSET, Some(freeblock_offset));
        format::write_optional_u16(&mut bytes, freeblock_offset as usize, None);
        format::write_u16(&mut bytes, freeblock_offset as usize + 2, 3);

        let result = Page::<Read<'_>, Leaf>::open(&bytes);

        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::FreeblockTooSmall)
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
    fn open_rejects_interior_cell_past_usable_space() {
        let mut bytes = initialized_interior_page();
        let cell_offset = USABLE_SPACE_END - super::super::interior::INTERIOR_CELL_SIZE + 1;

        format::write_u16(&mut bytes, SLOT_COUNT_OFFSET, 1);
        format::write_u16(&mut bytes, CONTENT_START_OFFSET, cell_offset as u16);
        format::write_u16(
            &mut bytes,
            format::slot_entry_offset(format::INTERIOR_HEADER_SIZE, 0),
            cell_offset as u16,
        );

        let result = Page::<Read<'_>, Interior>::open(&bytes);
        assert_eq!(
            result.unwrap_err(),
            PageError::MalformedPage(PageCorruption::InteriorCellOutOfBounds)
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

        let read_key = |_: &Page<Read<'_>, Leaf>, slot_index: SlotId| Ok(keys[slot_index as usize]);

        assert_eq!(page.search_slots_by(5, read_key).unwrap(), SearchResult::InsertAt(0));
        assert_eq!(page.search_slots_by(10, read_key).unwrap(), SearchResult::Found(0));
        assert_eq!(page.search_slots_by(30, read_key).unwrap(), SearchResult::InsertAt(2));
        assert_eq!(page.search_slots_by(40, read_key).unwrap(), SearchResult::Found(2));
        assert_eq!(page.search_slots_by(90, read_key).unwrap(), SearchResult::InsertAt(4));
    }
}
