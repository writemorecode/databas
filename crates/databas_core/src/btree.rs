//! Public b+-tree cursor skeletons.
//!
//! The intended layering is:
//!
//! - [`crate::Pager`] owns the file-backed page cache.
//! - [`TreeCursor`] is the public handle for one table or index tree.
//! - The cursor owns the tree root page id and its current logical position.
//!
//! This module is intentionally a skeleton. It fixes the outer API shape and
//! the core data-model types before the full tree algorithms are implemented.

use core::marker::PhantomData;
use std::fmt;

use crate::{
    PageId, RowId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    page::{
        self, Page, Write,
        format::{KIND_OFFSET, PageKind},
    },
    page_cache::{PageCache, PinGuard},
};

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for the supported public tree flavors.
///
/// The trait is sealed: only [`Table`] and [`Index`] implement it.
pub trait TreeKind: sealed::Sealed + 'static {}

/// Marker for a table b+-tree keyed by [`RowId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {}

/// Marker for an index b+-tree keyed by arbitrary bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {}

impl sealed::Sealed for Table {}
impl sealed::Sealed for Index {}

impl TreeKind for Table {}
impl TreeKind for Index {}

pub(crate) trait TreeKindExt: TreeKind {
    type PageTree: page::TreeMarker;

    const ROOT_KIND_NAME: &'static str;

    fn matches_page_kind(kind: PageKind) -> bool {
        kind.tree_kind() == <Self::PageTree as page::TreeMarker>::KIND
    }
}

impl TreeKindExt for Table {
    type PageTree = page::Table;

    const ROOT_KIND_NAME: &'static str = "table root page";
}

impl TreeKindExt for Index {
    type PageTree = page::Index;

    const ROOT_KIND_NAME: &'static str = "index root page";
}

/// Guard-backed table record view returned by table-tree reads and cursor iteration.
pub struct TableRecord {
    /// The primary row identifier used as the table-tree key.
    pub row_id: RowId,
    pin: PinGuard,
    slot_index: u16,
}

/// Guard-backed index entry view returned by index-tree reads and cursor iteration.
pub struct IndexEntry {
    /// The referenced row identifier.
    pub row_id: RowId,
    pin: PinGuard,
    slot_index: u16,
}

impl TableRecord {
    pub(crate) fn new(pin: PinGuard, slot_index: u16) -> StorageResult<Self> {
        let row_id = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Table>()?;
            let cell = leaf.cell(slot_index)?;
            cell.row_id()?
        };
        Ok(Self { row_id, pin, slot_index })
    }

    /// Executes `f` with a borrowed view of the row payload while the backing
    /// page remains pinned and immutably borrowed.
    pub fn with_payload<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let page = self.pin.read()?;
        let leaf = page.open_typed::<page::Leaf, page::Table>()?;
        let cell = leaf.cell(self.slot_index)?;
        Ok(f(cell.payload()?))
    }
}

impl fmt::Debug for TableRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRecord")
            .field("row_id", &self.row_id)
            .field("slot_index", &self.slot_index)
            .finish_non_exhaustive()
    }
}

impl IndexEntry {
    pub(crate) fn new(pin: PinGuard, slot_index: u16) -> StorageResult<Self> {
        let row_id = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Index>()?;
            let cell = leaf.cell(slot_index)?;
            cell.row_id()?
        };
        Ok(Self { row_id, pin, slot_index })
    }

    /// Executes `f` with a borrowed view of the indexed key while the backing
    /// page remains pinned and immutably borrowed.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let page = self.pin.read()?;
        let leaf = page.open_typed::<page::Leaf, page::Index>()?;
        let cell = leaf.cell(self.slot_index)?;
        Ok(f(cell.key()?))
    }
}

impl fmt::Debug for IndexEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexEntry")
            .field("row_id", &self.row_id)
            .field("slot_index", &self.slot_index)
            .finish_non_exhaustive()
    }
}

/// Logical cursor state exposed by the public cursor API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// The cursor is anchored to a page but not yet to a specific slot.
    Page {
        /// The page currently referenced by the cursor.
        page_id: PageId,
    },
    /// The cursor currently references a slot inside a leaf page.
    Positioned {
        /// The leaf page currently holding the cursor record.
        page_id: PageId,
        /// The slot index within the leaf page.
        slot_index: u16,
    },
    /// The cursor ran past the end of the tree.
    Exhausted,
}

/// Public handle to a single b+-tree rooted at `root_page_id`.
///
/// `Pager` returns this cursor already anchored at the root page. Future
/// searches, scans, and mutations update the cursor state in place.
#[derive(Clone)]
pub struct TreeCursor<K> {
    page_cache: PageCache,
    root_page_id: PageId,
    state: CursorState,
    _marker: PhantomData<K>,
}

/// Typed alias for a table-tree cursor.
pub type TableCursor = TreeCursor<Table>;
/// Typed alias for an index-tree cursor.
pub type IndexCursor = TreeCursor<Index>;

impl<K> TreeCursor<K>
where
    K: TreeKind,
{
    pub(crate) fn new(page_cache: PageCache, root_page_id: PageId) -> Self {
        Self {
            page_cache,
            root_page_id,
            state: CursorState::Page { page_id: root_page_id },
            _marker: PhantomData,
        }
    }

    /// Returns the root page id that anchors this tree.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Returns the cursor's current logical state.
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Returns the page currently referenced by the cursor, if any.
    pub fn current_page_id(&self) -> Option<PageId> {
        match self.state {
            CursorState::Page { page_id } | CursorState::Positioned { page_id, .. } => {
                Some(page_id)
            }
            CursorState::Exhausted => None,
        }
    }

    /// Returns `true` when the cursor is currently positioned on a record.
    pub fn is_positioned(&self) -> bool {
        matches!(self.state, CursorState::Positioned { .. })
    }

    /// Resets the cursor back to the tree root page.
    pub fn seek_to_root(&mut self) {
        self.state = CursorState::Page { page_id: self.root_page_id };
    }
}

impl TableCursor {
    /// Searches the table tree for `row_id`.
    ///
    /// The cursor is expected to end on the matching row when found, or on the
    /// leaf page where `row_id` would be inserted when absent.
    pub fn get(&mut self, row_id: RowId) -> StorageResult<Option<TableRecord>> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("table-tree lookup is not implemented yet")
    }

    /// Inserts a new row payload into the table tree.
    ///
    /// Returns [`crate::error::ConstraintError::DuplicateKey`] if `row_id`
    /// already exists.
    pub fn insert(&mut self, row_id: RowId, payload: &[u8]) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (row_id, payload);
        todo!("table-tree insert is not implemented yet")
    }

    /// Replaces the payload stored for an existing `row_id`.
    pub fn update(&mut self, row_id: RowId, payload: &[u8]) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (row_id, payload);
        todo!("table-tree update is not implemented yet")
    }

    /// Deletes the row identified by `row_id`.
    pub fn delete(&mut self, row_id: RowId) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("table-tree delete is not implemented yet")
    }

    /// Positions the cursor on the smallest row id in the table tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let _ = &self.page_cache;
        todo!("table-tree cursor positioning is not implemented yet")
    }

    /// Positions the cursor on `row_id` if it exists.
    pub fn seek_to_row_id(&mut self, row_id: RowId) -> StorageResult<bool> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("table-tree cursor positioning is not implemented yet")
    }

    /// Reads the currently selected row, if any.
    pub fn current(&self) -> StorageResult<Option<TableRecord>> {
        let _ = &self.page_cache;
        todo!("table-tree cursor reads are not implemented yet")
    }

    /// Advances to the next row in sorted row-id order.
    pub fn next_row(&mut self) -> StorageResult<Option<TableRecord>> {
        let _ = &self.page_cache;
        todo!("table-tree cursor iteration is not implemented yet")
    }

    /// Moves to the previous row in sorted row-id order.
    pub fn prev_row(&mut self) -> StorageResult<Option<TableRecord>> {
        let _ = &self.page_cache;
        todo!("table-tree cursor iteration is not implemented yet")
    }
}

impl IndexCursor {
    /// Positions the cursor on the first entry whose key is greater than or
    /// equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let _ = &self.page_cache;
        let _ = key;
        todo!("index-tree cursor positioning is not implemented yet")
    }

    /// Positions the cursor on one exact `(key, row_id)` pair.
    pub fn seek_to_entry(&mut self, key: &[u8], row_id: RowId) -> StorageResult<bool> {
        let _ = &self.page_cache;
        let _ = (key, row_id);
        todo!("index-tree cursor positioning is not implemented yet")
    }

    /// Positions the cursor on the smallest `(key, row_id)` entry in the tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let _ = &self.page_cache;
        todo!("index-tree cursor positioning is not implemented yet")
    }

    /// Reads the currently selected index entry, if any.
    pub fn current(&self) -> StorageResult<Option<IndexEntry>> {
        let _ = &self.page_cache;
        todo!("index-tree cursor reads are not implemented yet")
    }

    /// Advances to the next `(key, row_id)` pair in key order.
    pub fn next_row(&mut self) -> StorageResult<Option<IndexEntry>> {
        let _ = &self.page_cache;
        todo!("index-tree cursor iteration is not implemented yet")
    }

    /// Moves to the previous `(key, row_id)` pair in key order.
    pub fn prev_row(&mut self) -> StorageResult<Option<IndexEntry>> {
        let _ = &self.page_cache;
        todo!("index-tree cursor iteration is not implemented yet")
    }

    /// Inserts a new `(key, row_id)` pair into the index tree.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (key, row_id);
        todo!("index-tree insert is not implemented yet")
    }

    /// Replaces one exact `(key, row_id)` entry with a new one.
    ///
    /// This models the "update" operation for secondary indexes without
    /// assuming key uniqueness.
    pub fn update(
        &mut self,
        key: &[u8],
        old_row_id: RowId,
        new_key: &[u8],
        new_row_id: RowId,
    ) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (key, old_row_id, new_key, new_row_id);
        todo!("index-tree update is not implemented yet")
    }

    /// Deletes one exact `(key, row_id)` entry from the index tree.
    pub fn delete(&mut self, key: &[u8], row_id: RowId) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (key, row_id);
        todo!("index-tree delete is not implemented yet")
    }
}

pub(crate) fn initialize_empty_root<K>(page_cache: &PageCache) -> StorageResult<PageId>
where
    K: TreeKindExt,
{
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = Page::<Write<'_>, page::Leaf, K::PageTree>::initialize(page.page_mut());
    Ok(page_id)
}

pub(crate) fn validate_root_page<K>(
    page_cache: &PageCache,
    root_page_id: PageId,
) -> StorageResult<()>
where
    K: TreeKindExt,
{
    let pin = page_cache.fetch_page(root_page_id)?;
    let page = pin.read()?;
    let raw_kind = page.page()[KIND_OFFSET];
    let Some(page_kind) = PageKind::from_raw(raw_kind) else {
        return Err(StorageError::from(page::PageError::UnknownPageKind { actual: raw_kind }));
    };

    if !K::matches_page_kind(page_kind) {
        return Err(StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::Page,
            page_id: Some(root_page_id),
            kind: CorruptionKind::InvalidPageKind { expected: K::ROOT_KIND_NAME, actual: raw_kind },
        }));
    }

    let _ = page.open_any()?;
    Ok(())
}
