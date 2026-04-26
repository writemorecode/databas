use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    Cell, CellCorruption, CellMut, Interior, Leaf, Page, PageError, PageResult, Read, SearchResult,
    Write,
    core::{BoundResult, NodeMarker, PageAccess, PageAccessMut},
};

const ROW_ID_KEY_SIZE: usize = size_of::<RowId>();

/// Domain wrapper over raw B+-tree pages that store table records.
#[derive(Debug)]
pub struct TablePage<A, N> {
    raw: Page<A, N>,
}

/// Immutable table cell view.
#[derive(Debug)]
pub struct TableCell<'a, N> {
    raw: Cell<'a, N>,
}

/// Mutable table cell view.
#[derive(Debug)]
pub struct TableCellMut<'a, N> {
    raw: CellMut<'a, N>,
}

/// Convenience alias for table leaf pages.
pub type TableLeafPage<A> = TablePage<A, Leaf>;
/// Convenience alias for table interior pages.
pub type TableInteriorPage<A> = TablePage<A, Interior>;
/// Convenience alias for table leaf cells.
pub type TableLeafCell<'a> = TableCell<'a, Leaf>;
/// Convenience alias for table interior cells.
pub type TableInteriorCell<'a> = TableCell<'a, Interior>;
/// Convenience alias for mutable table leaf cells.
pub type TableLeafCellMut<'a> = TableCellMut<'a, Leaf>;
/// Convenience alias for mutable table interior cells.
pub type TableInteriorCellMut<'a> = TableCellMut<'a, Interior>;

fn encode_row_id(row_id: RowId) -> [u8; ROW_ID_KEY_SIZE] {
    row_id.to_be_bytes()
}

fn decode_row_id(key: &[u8], slot_index: SlotId) -> PageResult<RowId> {
    let bytes: [u8; ROW_ID_KEY_SIZE] = key.try_into().map_err(|_| PageError::CorruptCell {
        slot_index,
        kind: CellCorruption::InvalidTableRowIdKeyLength { actual: key.len() },
    })?;
    Ok(RowId::from_be_bytes(bytes))
}

impl<A, N> TablePage<A, N> {
    /// Wraps a raw typed page as a table page.
    pub fn from_raw(raw: Page<A, N>) -> Self {
        Self { raw }
    }

    /// Consumes this table wrapper and returns the raw typed page.
    pub fn into_raw(self) -> Page<A, N> {
        self.raw
    }

    /// Returns the underlying raw typed page.
    pub fn raw(&self) -> &Page<A, N> {
        &self.raw
    }
}

impl<A, N> TablePage<A, N>
where
    A: PageAccessMut,
    N: NodeMarker,
{
    /// Returns the underlying raw typed page mutably.
    pub fn raw_mut(&mut self) -> &mut Page<A, N> {
        &mut self.raw
    }
}

impl<A, N> TablePage<A, N>
where
    A: PageAccess,
    N: NodeMarker,
{
    /// Returns the raw page bytes.
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.raw.bytes()
    }

    /// Returns the number of live table cells in the slot directory.
    pub fn slot_count(&self) -> u16 {
        self.raw.slot_count()
    }

    /// Returns the contiguous free space between the slot directory and cell content.
    pub fn free_space(&self) -> usize {
        self.raw.free_space()
    }

    /// Returns the previous table page at this tree level, if present.
    pub fn prev_page_id(&self) -> Option<PageId> {
        self.raw.prev_page_id()
    }

    /// Returns the next table page at this tree level, if present.
    pub fn next_page_id(&self) -> Option<PageId> {
        self.raw.next_page_id()
    }
}

impl<A, N> TablePage<A, N>
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

impl<'a, N> TablePage<Read<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens an immutable table page view over an initialized buffer.
    pub fn open(bytes: &'a [u8; PAGE_SIZE]) -> PageResult<Self> {
        Page::<Read<'a>, N>::open(bytes).map(Self::from_raw)
    }
}

impl<'a, N> TablePage<Write<'a>, N>
where
    N: NodeMarker,
{
    /// Validates and opens a mutable table page view over an initialized buffer.
    pub fn open(bytes: &'a mut [u8; PAGE_SIZE]) -> PageResult<Self> {
        Page::<Write<'a>, N>::open(bytes).map(Self::from_raw)
    }

    /// Borrows this mutable table page as an immutable table page view.
    pub fn as_ref(&self) -> TablePage<Read<'_>, N> {
        TablePage::from_raw(self.raw.as_ref())
    }
}

impl<'a> TablePage<Write<'a>, Leaf> {
    /// Initializes a fresh empty table leaf page in-place.
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self::from_raw(Page::<Write<'a>, Leaf>::init(bytes))
    }
}

impl<'a> TablePage<Write<'a>, Interior> {
    /// Initializes a fresh empty table interior page with its rightmost child pointer set.
    pub fn init(bytes: &'a mut [u8; PAGE_SIZE], rightmost_child: PageId) -> Self {
        Self::from_raw(Page::<Write<'a>, Interior>::init(bytes, rightmost_child))
    }
}

impl<A> TablePage<A, Leaf>
where
    A: PageAccess,
{
    /// Returns the first slot whose row id is greater than or equal to `row_id`.
    pub fn lower_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.raw.lower_bound(&encode_row_id(row_id))
    }

    /// Returns the first slot whose row id is strictly greater than `row_id`.
    pub fn upper_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.raw.upper_bound(&encode_row_id(row_id))
    }

    /// Searches the table leaf page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.raw.search(&encode_row_id(row_id))
    }

    /// Returns a typed immutable view of the table cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<TableCell<'_, Leaf>> {
        self.raw.cell(slot_index).map(TableCell::new)
    }

    /// Looks up a row id and returns its table cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<TableCell<'_, Leaf>>> {
        match self.search(row_id)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> TablePage<A, Leaf>
where
    A: PageAccessMut,
{
    /// Returns a typed mutable view of the table cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<TableCellMut<'_, Leaf>> {
        self.raw.cell_mut(slot_index).map(TableCellMut::new)
    }

    /// Inserts a table record keyed by `row_id`.
    pub fn insert(&mut self, row_id: RowId, record: &[u8]) -> PageResult<SlotId> {
        self.raw.insert(&encode_row_id(row_id), record)
    }

    /// Deletes a table record by row id.
    pub fn delete(&mut self, row_id: RowId) -> PageResult<SlotId> {
        self.raw.delete(&encode_row_id(row_id))
    }
}

impl<A> TablePage<A, Interior>
where
    A: PageAccess,
{
    /// Returns the page id stored in the rightmost-child header field.
    pub fn rightmost_child(&self) -> PageId {
        self.raw.rightmost_child()
    }

    /// Returns the first slot whose separator row id is greater than or equal to `row_id`.
    pub fn lower_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.raw.lower_bound(&encode_row_id(row_id))
    }

    /// Returns the first slot whose separator row id is strictly greater than `row_id`.
    pub fn upper_bound(&self, row_id: RowId) -> PageResult<BoundResult> {
        self.raw.upper_bound(&encode_row_id(row_id))
    }

    /// Returns the child page that may contain `row_id`.
    pub fn child_for(&self, row_id: RowId) -> PageResult<PageId> {
        self.raw.child_for(&encode_row_id(row_id))
    }

    /// Searches the table interior page for `row_id`.
    pub fn search(&self, row_id: RowId) -> PageResult<SearchResult> {
        self.raw.search(&encode_row_id(row_id))
    }

    /// Returns a typed immutable view of the table cell at `slot_index`.
    pub fn cell(&self, slot_index: SlotId) -> PageResult<TableCell<'_, Interior>> {
        self.raw.cell(slot_index).map(TableCell::new)
    }

    /// Looks up a separator row id and returns its table cell if present.
    pub fn lookup(&self, row_id: RowId) -> PageResult<Option<TableCell<'_, Interior>>> {
        match self.search(row_id)? {
            SearchResult::Found(slot_index) => self.cell(slot_index).map(Some),
            SearchResult::InsertAt(_) => Ok(None),
        }
    }
}

impl<A> TablePage<A, Interior>
where
    A: PageAccessMut,
{
    /// Updates the page id stored in the rightmost-child header field.
    pub fn set_rightmost_child(&mut self, page_id: PageId) {
        self.raw.set_rightmost_child(page_id);
    }

    /// Returns a typed mutable view of the table cell at `slot_index`.
    pub fn cell_mut(&mut self, slot_index: SlotId) -> PageResult<TableCellMut<'_, Interior>> {
        self.raw.cell_mut(slot_index).map(TableCellMut::new)
    }

    /// Inserts a separator row id and its left-child pointer.
    pub fn insert(&mut self, row_id: RowId, left_child: PageId) -> PageResult<SlotId> {
        self.raw.insert(&encode_row_id(row_id), left_child)
    }
}

impl<'a, N> TableCell<'a, N> {
    fn new(raw: Cell<'a, N>) -> Self {
        Self { raw }
    }

    /// Consumes this table cell wrapper and returns the raw cell view.
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

impl<'a, N> TableCellMut<'a, N> {
    fn new(raw: CellMut<'a, N>) -> Self {
        Self { raw }
    }

    /// Consumes this table cell wrapper and returns the raw mutable cell view.
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

    /// Borrows this mutable table cell as an immutable table cell view.
    pub fn as_ref(&self) -> TableCell<'_, N> {
        TableCell::new(self.raw.as_ref())
    }
}

impl TableCell<'_, Leaf> {
    /// Returns the row id stored in this table leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.key()?, self.raw.slot_index())
    }

    /// Returns the encoded record bytes stored in this table leaf cell.
    pub fn record(&self) -> PageResult<&[u8]> {
        self.raw.value()
    }
}

impl TableCellMut<'_, Leaf> {
    /// Returns the row id stored in this table leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.key()?, self.raw.slot_index())
    }

    /// Returns the encoded record bytes stored in this table leaf cell.
    pub fn record(&self) -> PageResult<&[u8]> {
        self.raw.value()
    }

    /// Returns the encoded record bytes stored in this table leaf cell mutably.
    pub fn record_mut(&mut self) -> PageResult<&mut [u8]> {
        self.raw.value_mut()
    }
}

impl TableCell<'_, Interior> {
    /// Returns the separator row id stored in this table interior cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.key()?, self.raw.slot_index())
    }

    /// Returns the left-child page id referenced by this table interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        self.raw.left_child()
    }
}

impl TableCellMut<'_, Interior> {
    /// Returns the separator row id stored in this table interior cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_row_id(self.raw.key()?, self.raw.slot_index())
    }

    /// Returns the left-child page id referenced by this table interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        self.raw.left_child()
    }

    /// Updates the left-child page id stored in this table interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        self.raw.set_left_child(page_id)
    }
}
