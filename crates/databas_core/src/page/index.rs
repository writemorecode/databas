use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    Cell, CellCorruption, CellMut, Interior, Leaf, Page, PageError, PageResult, Read, SearchResult,
    Write,
    core::{BoundResult, NodeMarker, PageAccess, PageAccessMut},
};

const ROW_ID_VALUE_SIZE: usize = size_of::<RowId>();

/// Domain wrapper over raw B+-tree pages that store secondary-index entries.
#[derive(Debug)]
pub struct IndexPage<A, N> {
    raw: Page<A, N>,
}

/// Immutable index cell view.
#[derive(Debug)]
pub struct IndexCell<'a, N> {
    raw: Cell<'a, N>,
}

/// Mutable index cell view.
#[derive(Debug)]
pub struct IndexCellMut<'a, N> {
    raw: CellMut<'a, N>,
}

/// Convenience alias for index leaf pages.
pub type IndexLeafPage<A> = IndexPage<A, Leaf>;
/// Convenience alias for index interior pages.
pub type IndexInteriorPage<A> = IndexPage<A, Interior>;
/// Convenience alias for index leaf cells.
pub type IndexLeafCell<'a> = IndexCell<'a, Leaf>;
/// Convenience alias for index interior cells.
pub type IndexInteriorCell<'a> = IndexCell<'a, Interior>;
/// Convenience alias for mutable index leaf cells.
pub type IndexLeafCellMut<'a> = IndexCellMut<'a, Leaf>;
/// Convenience alias for mutable index interior cells.
pub type IndexInteriorCellMut<'a> = IndexCellMut<'a, Interior>;

fn encode_row_id(row_id: RowId) -> [u8; ROW_ID_VALUE_SIZE] {
    row_id.to_le_bytes()
}

fn decode_row_id(value: &[u8], slot_index: SlotId) -> PageResult<RowId> {
    let bytes: [u8; ROW_ID_VALUE_SIZE] = value.try_into().map_err(|_| PageError::CorruptCell {
        slot_index,
        kind: CellCorruption::InvalidIndexRowIdValueLength { actual: value.len() },
    })?;
    Ok(RowId::from_le_bytes(bytes))
}

impl<A, N> IndexPage<A, N> {
    /// Wraps a raw typed page as an index page.
    pub fn from_raw(raw: Page<A, N>) -> Self {
        Self { raw }
    }

    /// Consumes this index wrapper and returns the raw typed page.
    pub fn into_raw(self) -> Page<A, N> {
        self.raw
    }

    /// Returns the underlying raw typed page.
    pub fn raw(&self) -> &Page<A, N> {
        &self.raw
    }
}

impl<A, N> IndexPage<A, N>
where
    A: PageAccessMut,
    N: NodeMarker,
{
    /// Returns the underlying raw typed page mutably.
    pub fn raw_mut(&mut self) -> &mut Page<A, N> {
        &mut self.raw
    }
}

impl<A, N> IndexPage<A, N>
where
    A: PageAccess,
    N: NodeMarker,
{
    /// Returns the raw page bytes.
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.raw.bytes()
    }

    /// Returns the number of live index cells in the slot directory.
    pub fn slot_count(&self) -> u16 {
        self.raw.slot_count()
    }

    /// Returns the contiguous free space between the slot directory and cell content.
    pub fn free_space(&self) -> usize {
        self.raw.free_space()
    }

    /// Returns the previous index page at this tree level, if present.
    pub fn prev_page_id(&self) -> Option<PageId> {
        self.raw.prev_page_id()
    }

    /// Returns the next index page at this tree level, if present.
    pub fn next_page_id(&self) -> Option<PageId> {
        self.raw.next_page_id()
    }
}

impl<A, N> IndexPage<A, N>
where
    A: PageAccessMut,
    N: NodeMarker,
{
    /// Updates the previous sibling page id stored in the page header.
    pub fn set_prev_page_id(&mut self, page_id: Option<PageId>) {
        self.raw.set_prev_page_id(page_id);
    }

    /// Updates the next sibling page id stored in the page header.
    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        self.raw.set_next_page_id(page_id);
    }
}

impl<'a, N> IndexPage<Read<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens an immutable index page view over an initialized buffer.
    pub fn open(bytes: &'a [u8; PAGE_SIZE]) -> PageResult<Self> {
        Page::<Read<'a>, N>::open(bytes).map(Self::from_raw)
    }
}

impl<'a, N> IndexPage<Write<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens a mutable index page view over an initialized buffer.
    pub fn open(bytes: &'a mut [u8; PAGE_SIZE]) -> PageResult<Self> {
        Page::<Write<'a>, N>::open(bytes).map(Self::from_raw)
    }

    /// Borrows this mutable index page as an immutable index page view.
    pub fn as_ref(&self) -> IndexPage<Read<'_>, N> {
        IndexPage::from_raw(self.raw.as_ref())
    }
}

impl<'a> IndexPage<Write<'a>, Leaf> {
    /// Initializes a fresh empty index leaf page in-place.
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self::from_raw(Page::<Write<'a>, Leaf>::init(bytes))
    }
}

impl<'a> IndexPage<Write<'a>, Interior> {
    /// Initializes a fresh empty index interior page with its rightmost child pointer set.
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE], rightmost_child: PageId) -> Self {
        Self::from_raw(Page::<Write<'a>, Interior>::init(bytes, rightmost_child))
    }
}

impl<A> IndexPage<A, Leaf>
where
    A: PageAccess,
{
    /// Returns the first slot whose index key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.raw.lower_bound(key)
    }

    /// Returns the first slot whose index key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.raw.upper_bound(key)
    }

    /// Searches the index leaf page for `key`.
    pub fn search(&self, key: &[u8]) -> PageResult<SearchResult> {
        self.raw.search(key)
    }

    /// Returns a typed immutable view of the index cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<IndexCell<'_, Leaf>> {
        self.raw.cell(slot_index).map(IndexCell::new)
    }

    /// Looks up an index key and returns its cell if present.
    pub fn lookup(&self, key: &[u8]) -> PageResult<Option<IndexCell<'_, Leaf>>> {
        match self.search(key)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> IndexPage<A, Leaf>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the index cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<IndexCellMut<'_, Leaf>> {
        self.raw.cell_mut(slot_index).map(IndexCellMut::new)
    }

    /// Inserts an index entry from `key` to `row_id`.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> PageResult<SlotId> {
        self.raw.insert(key, &encode_row_id(row_id))
    }

    /// Deletes an index entry by key.
    pub fn delete(&mut self, key: &[u8]) -> PageResult<SlotId> {
        self.raw.delete(key)
    }
}

impl<A> IndexPage<A, Interior>
where
    A: PageAccess,
{
    /// Returns the page id stored in the rightmost-child header field.
    pub fn rightmost_child(&self) -> PageId {
        self.raw.rightmost_child()
    }

    /// Returns the first slot whose separator key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.raw.lower_bound(key)
    }

    /// Returns the first slot whose separator key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> PageResult<BoundResult> {
        self.raw.upper_bound(key)
    }

    /// Returns the child page that may contain `key`.
    pub fn child_for(&self, key: &[u8]) -> PageResult<PageId> {
        self.raw.child_for(key)
    }

    /// Searches the index interior page for `key`.
    pub fn search(&self, key: &[u8]) -> PageResult<SearchResult> {
        self.raw.search(key)
    }

    /// Returns a typed immutable view of the index cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<IndexCell<'_, Interior>> {
        self.raw.cell(slot_index).map(IndexCell::new)
    }

    /// Looks up a separator key and returns its index cell if present.
    pub fn lookup(&self, key: &[u8]) -> PageResult<Option<IndexCell<'_, Interior>>> {
        match self.search(key)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> IndexPage<A, Interior>
where
    A: PageAccessMut,
{
    /// Updates the page id stored in the rightmost-child header field.
    pub fn set_rightmost_child(&mut self, page_id: PageId) {
        self.raw.set_rightmost_child(page_id);
    }

    /// Returns a typed mutable view of the index cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<IndexCellMut<'_, Interior>> {
        self.raw.cell_mut(slot_index).map(IndexCellMut::new)
    }

    /// Inserts a separator key and its left-child pointer.
    pub fn insert(&mut self, key: &[u8], left_child: PageId) -> PageResult<SlotId> {
        self.raw.insert(key, left_child)
    }
}

impl<'a, N> IndexCell<'a, N> {
    fn new(raw: Cell<'a, N>) -> Self {
        Self { raw }
    }

    /// Consumes this index cell wrapper and returns the raw cell view.
    pub fn into_raw(self) -> Cell<'a, N> {
        self.raw
    }

    /// Returns the underlying raw cell view.
    pub fn raw(&self) -> &Cell<'a, N> {
        &self.raw
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.raw.slot_index()
    }
}

impl<'a, N> IndexCellMut<'a, N> {
    fn new(raw: CellMut<'a, N>) -> Self {
        Self { raw }
    }

    /// Consumes this index cell wrapper and returns the raw mutable cell view.
    pub fn into_raw(self) -> CellMut<'a, N> {
        self.raw
    }

    /// Returns the underlying raw mutable cell view.
    pub fn raw(&self) -> &CellMut<'a, N> {
        &self.raw
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.raw.slot_index()
    }

    /// Borrows this mutable index cell as an immutable index cell view.
    pub fn as_ref(&self) -> IndexCell<'_, N> {
        IndexCell::new(self.raw.as_ref())
    }
}

impl IndexCell<'_, Leaf> {
    /// Returns the index key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        self.raw.key()
    }

    /// Returns the row id stored as this index leaf cell's value.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.value()?, self.raw.slot_index())
    }
}

impl IndexCellMut<'_, Leaf> {
    /// Returns the index key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        self.raw.key()
    }

    /// Returns the row id stored as this index leaf cell's value.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.value()?, self.raw.slot_index())
    }

    /// Updates the row id stored as this index leaf cell's value.
    pub fn set_row_id(&mut self, row_id: RowId) -> PageResult<()> {
        self.raw.value_mut()?.copy_from_slice(&encode_row_id(row_id));
        Ok(())
    }
}

impl IndexCell<'_, Interior> {
    /// Returns the separator key stored in this index interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        self.raw.key()
    }

    /// Returns the left-child page id referenced by this index interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        self.raw.left_child()
    }
}

impl IndexCellMut<'_, Interior> {
    /// Returns the separator key stored in this index interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        self.raw.key()
    }

    /// Returns the left-child page id referenced by this index interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        self.raw.left_child()
    }

    /// Updates the left-child page id stored in this index interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        self.raw.set_left_child(page_id)
    }
}
