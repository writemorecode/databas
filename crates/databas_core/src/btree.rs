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
        self, Page, PageError, Write,
        format::{KIND_OFFSET, PageKind},
    },
    page_cache::{PageCache, PinGuard},
};

mod sealed {
    pub trait Sealed {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafSeek {
    Positioned(u16),
    Advance(PageId),
    Exhausted,
}

/// Direction selector for shared cursor scans over linked leaf pages.
///
/// This keeps the cursor state machine generic while centralizing the small
/// forward/backward differences in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanDirection {
    /// Move toward larger keys or row ids.
    Forward,
    /// Move toward smaller keys or row ids.
    Backward,
}

impl ScanDirection {
    /// Descends from `start_page_id` to the leaf at the edge implied by `self`.
    fn descend_to_edge_leaf<K>(
        self,
        cursor: &TreeCursor<K>,
        start_page_id: PageId,
    ) -> StorageResult<PageId>
    where
        K: TreeKindExt,
    {
        match self {
            Self::Forward => cursor.descend_to_first_leaf_from(start_page_id),
            Self::Backward => cursor.descend_to_last_leaf_from(start_page_id),
        }
    }

    /// Chooses the first or last slot in `leaf`, or advances to the next leaf
    /// in the scan direction when the page is empty.
    fn edge_seek<T>(self, leaf: &Page<page::Read<'_>, page::Leaf, T>) -> LeafSeek
    where
        T: page::TreeMarker,
    {
        match self {
            Self::Forward => {
                if leaf.slot_count() > 0 {
                    LeafSeek::Positioned(0)
                } else if let Some(next_page_id) = leaf.next_page_id() {
                    LeafSeek::Advance(next_page_id)
                } else {
                    LeafSeek::Exhausted
                }
            }
            Self::Backward => {
                if leaf.slot_count() > 0 {
                    LeafSeek::Positioned(leaf.slot_count() - 1)
                } else if let Some(prev_page_id) = leaf.prev_page_id() {
                    LeafSeek::Advance(prev_page_id)
                } else {
                    LeafSeek::Exhausted
                }
            }
        }
    }

    /// Chooses the adjacent slot relative to `slot_index`, or advances to the
    /// neighboring leaf when the cursor is already at the page boundary.
    fn adjacent_seek<T>(
        self,
        leaf: &Page<page::Read<'_>, page::Leaf, T>,
        slot_index: u16,
    ) -> LeafSeek
    where
        T: page::TreeMarker,
    {
        match self {
            Self::Forward => {
                if slot_index + 1 < leaf.slot_count() {
                    LeafSeek::Positioned(slot_index + 1)
                } else if let Some(next_page_id) = leaf.next_page_id() {
                    LeafSeek::Advance(next_page_id)
                } else {
                    LeafSeek::Exhausted
                }
            }
            Self::Backward => {
                if slot_index > 0 {
                    LeafSeek::Positioned(slot_index - 1)
                } else if let Some(prev_page_id) = leaf.prev_page_id() {
                    LeafSeek::Advance(prev_page_id)
                } else {
                    LeafSeek::Exhausted
                }
            }
        }
    }
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
    const PAGE_KIND_NAME: &'static str;
    /// Human-readable name of the leaf page kind for corruption diagnostics.
    const LEAF_KIND_NAME: &'static str;

    fn matches_page_kind(kind: PageKind) -> bool {
        kind.tree_kind() == <Self::PageTree as page::TreeMarker>::KIND
    }

    fn first_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId>;

    fn last_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId>;
}

impl TreeKindExt for Table {
    type PageTree = page::Table;

    const ROOT_KIND_NAME: &'static str = "table root page";
    const PAGE_KIND_NAME: &'static str = "table page";
    const LEAF_KIND_NAME: &'static str = "table leaf";

    fn first_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId> {
        if interior.slot_count() == 0 {
            Ok(interior.rightmost_child())
        } else {
            Ok(interior.cell(0)?.left_child()?)
        }
    }

    fn last_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId> {
        Ok(interior.rightmost_child())
    }
}

impl TreeKindExt for Index {
    type PageTree = page::Index;

    const ROOT_KIND_NAME: &'static str = "index root page";
    const PAGE_KIND_NAME: &'static str = "index page";
    const LEAF_KIND_NAME: &'static str = "index leaf";

    fn first_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId> {
        if interior.slot_count() == 0 {
            Ok(interior.rightmost_child())
        } else {
            Ok(interior.cell(0)?.left_child()?)
        }
    }

    fn last_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId> {
        Ok(interior.rightmost_child())
    }
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

    fn set_page_state(&mut self, page_id: PageId) {
        self.state = CursorState::Page { page_id };
    }

    fn set_positioned_state(&mut self, page_id: PageId, slot_index: u16) {
        self.state = CursorState::Positioned { page_id, slot_index };
    }

    fn set_exhausted_state(&mut self) {
        self.state = CursorState::Exhausted;
    }

    fn descend_to_first_leaf_from(&self, start_page_id: PageId) -> StorageResult<PageId>
    where
        K: TreeKindExt,
    {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                let raw_kind = page.page()[KIND_OFFSET];
                let Some(page_kind) = PageKind::from_raw(raw_kind) else {
                    return Err(StorageError::from(page::PageError::UnknownPageKind {
                        actual: raw_kind,
                    }));
                };

                if !K::matches_page_kind(page_kind) {
                    return Err(StorageError::Corruption(CorruptionError {
                        component: CorruptionComponent::Page,
                        page_id: Some(page_id),
                        kind: CorruptionKind::InvalidPageKind {
                            expected: K::PAGE_KIND_NAME,
                            actual: raw_kind,
                        },
                    }));
                }

                match page_kind.node_kind() {
                    page::format::NodeKind::Leaf => {
                        let _ = page.open_typed::<page::Leaf, K::PageTree>()?;
                        return Ok(page_id);
                    }
                    page::format::NodeKind::Interior => {
                        let interior = page.open_typed::<page::Interior, K::PageTree>()?;
                        K::first_descend_child(&interior)?
                    }
                }
            };
            page_id = next;
        }
    }

    fn descend_to_last_leaf_from(&self, start_page_id: PageId) -> StorageResult<PageId>
    where
        K: TreeKindExt,
    {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                let raw_kind = page.page()[KIND_OFFSET];
                let Some(page_kind) = PageKind::from_raw(raw_kind) else {
                    return Err(StorageError::from(page::PageError::UnknownPageKind {
                        actual: raw_kind,
                    }));
                };

                if !K::matches_page_kind(page_kind) {
                    return Err(StorageError::Corruption(CorruptionError {
                        component: CorruptionComponent::Page,
                        page_id: Some(page_id),
                        kind: CorruptionKind::InvalidPageKind {
                            expected: K::PAGE_KIND_NAME,
                            actual: raw_kind,
                        },
                    }));
                }

                match page_kind.node_kind() {
                    page::format::NodeKind::Leaf => {
                        let _ = page.open_typed::<page::Leaf, K::PageTree>()?;
                        return Ok(page_id);
                    }
                    page::format::NodeKind::Interior => {
                        let interior = page.open_typed::<page::Interior, K::PageTree>()?;
                        K::last_descend_child(&interior)?
                    }
                }
            };
            page_id = next;
        }
    }
}

impl<K> TreeCursor<K>
where
    K: TreeKind,
{
    /// Reads the first reachable item from `start_page_id` in `direction`,
    /// skipping over empty leaf pages until a slot is found or the scan ends.
    fn edge_item_from_leaf<T, F>(
        &mut self,
        start_page_id: PageId,
        direction: ScanDirection,
        item_at: F,
    ) -> StorageResult<Option<T>>
    where
        K: TreeKindExt,
        F: Fn(&Self, PageId, u16) -> StorageResult<T> + Copy,
    {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let seek = {
                let page = pin.read()?;
                let raw_kind = page.page()[KIND_OFFSET];
                let Some(page_kind) = PageKind::from_raw(raw_kind) else {
                    return Err(StorageError::from(page::PageError::UnknownPageKind {
                        actual: raw_kind,
                    }));
                };

                if !K::matches_page_kind(page_kind)
                    || page_kind.node_kind() != page::format::NodeKind::Leaf
                {
                    return Err(StorageError::Corruption(CorruptionError {
                        component: CorruptionComponent::Page,
                        page_id: Some(page_id),
                        kind: CorruptionKind::InvalidPageKind {
                            expected: K::LEAF_KIND_NAME,
                            actual: raw_kind,
                        },
                    }));
                }

                let leaf = page.open_typed::<page::Leaf, K::PageTree>()?;
                direction.edge_seek(&leaf)
            };

            match seek {
                LeafSeek::Positioned(slot_index) => {
                    self.set_positioned_state(page_id, slot_index);
                    return item_at(self, page_id, slot_index).map(Some);
                }
                LeafSeek::Advance(next_page_id) => page_id = next_page_id,
                LeafSeek::Exhausted => {
                    self.set_exhausted_state();
                    return Ok(None);
                }
            }
        }
    }

    /// Advances or rewinds the cursor by one logical row according to
    /// `direction`, reusing the same state machine for both table and index
    /// cursors.
    fn step_row<T, F>(&mut self, direction: ScanDirection, item_at: F) -> StorageResult<Option<T>>
    where
        K: TreeKindExt,
        F: Fn(&Self, PageId, u16) -> StorageResult<T> + Copy,
    {
        match self.state {
            CursorState::Exhausted => Ok(None),
            CursorState::Page { page_id } => {
                let leaf_page_id = direction.descend_to_edge_leaf(self, page_id)?;
                self.edge_item_from_leaf(leaf_page_id, direction, item_at)
            }
            CursorState::Positioned { page_id, slot_index } => {
                let pin = self.page_cache.fetch_page(page_id)?;
                let seek = {
                    let page = pin.read()?;
                    let leaf = page.open_typed::<page::Leaf, K::PageTree>()?;
                    direction.adjacent_seek(&leaf, slot_index)
                };

                match seek {
                    LeafSeek::Positioned(next_slot) => {
                        self.set_positioned_state(page_id, next_slot);
                        item_at(self, page_id, next_slot).map(Some)
                    }
                    LeafSeek::Advance(next_page_id) => {
                        self.edge_item_from_leaf(next_page_id, direction, item_at)
                    }
                    LeafSeek::Exhausted => {
                        self.set_exhausted_state();
                        Ok(None)
                    }
                }
            }
        }
    }
}

impl TableCursor {
    fn record_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<TableRecord> {
        let pin = self.page_cache.fetch_page(page_id)?;
        TableRecord::new(pin, slot_index)
    }

    fn leaf_page_for_row_id(&self, row_id: RowId) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::TableLeaf(_) => return Ok(page_id),
                    page::AnyPage::TableInterior(interior) => interior.child_for(row_id)?,
                    _ => {
                        return Err(StorageError::Corruption(CorruptionError {
                            component: CorruptionComponent::Page,
                            page_id: Some(page_id),
                            kind: CorruptionKind::InvalidPageKind {
                                expected: "table page",
                                actual: page.page()[KIND_OFFSET],
                            },
                        }));
                    }
                }
            };
            page_id = next;
        }
    }

    /// Searches the table tree for `row_id`.
    ///
    /// The cursor is expected to end on the matching row when found, or on the
    /// leaf page where `row_id` would be inserted when absent.
    pub fn get(&mut self, row_id: RowId) -> StorageResult<Option<TableRecord>> {
        let page_id = self.leaf_page_for_row_id(row_id)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let slot_index = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Table>()?;
            leaf.lookup(row_id)?.map(|cell| cell.slot_index())
        };

        match slot_index {
            Some(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                TableRecord::new(pin, slot_index).map(Some)
            }
            None => {
                self.set_page_state(page_id);
                Ok(None)
            }
        }
    }

    /// Inserts a new row payload into the table tree.
    ///
    /// Returns [`crate::error::ConstraintError::DuplicateKey`] if `row_id`
    /// already exists.
    pub fn insert(&mut self, row_id: RowId, payload: &[u8]) -> StorageResult<()> {
        let page_id = self.leaf_page_for_row_id(row_id)?;
        let pin_guard = self.page_cache.fetch_page(page_id)?;
        let mut write_guard = pin_guard.write()?;
        let mut page = write_guard.open_typed_mut::<page::Leaf, page::Table>()?;

        let insert_result = page.insert(row_id, payload);
        match insert_result {
            Ok(_) => Ok(()),
            Err(PageError::CellTooLarge { .. }) => {
                panic!("Cell too large!");
            }
            Err(PageError::PageFull { .. }) => {
                panic!("Page full!")
            }
            Err(err) => Err(err.into()),
        }
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
        let leaf_page_id = self.descend_to_first_leaf_from(self.root_page_id)?;
        self.edge_item_from_leaf(leaf_page_id, ScanDirection::Forward, Self::record_at)
            .map(|record| record.is_some())
    }

    /// Positions the cursor on `row_id` if it exists.
    pub fn seek_to_row_id(&mut self, row_id: RowId) -> StorageResult<bool> {
        Ok(self.get(row_id)?.is_some())
    }

    /// Reads the currently selected row, if any.
    pub fn current(&self) -> StorageResult<Option<TableRecord>> {
        match self.state {
            CursorState::Positioned { page_id, slot_index } => {
                self.record_at(page_id, slot_index).map(Some)
            }
            CursorState::Page { .. } | CursorState::Exhausted => Ok(None),
        }
    }

    /// Advances to the next row in sorted row-id order.
    pub fn next_row(&mut self) -> StorageResult<Option<TableRecord>> {
        self.step_row(ScanDirection::Forward, Self::record_at)
    }

    /// Moves to the previous row in sorted row-id order.
    pub fn prev_row(&mut self) -> StorageResult<Option<TableRecord>> {
        self.step_row(ScanDirection::Backward, Self::record_at)
    }
}

impl IndexCursor {
    fn entry_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<IndexEntry> {
        let pin = self.page_cache.fetch_page(page_id)?;
        IndexEntry::new(pin, slot_index)
    }

    fn leaf_page_for_key(&self, key: &[u8]) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::IndexLeaf(_) => return Ok(page_id),
                    page::AnyPage::IndexInterior(interior) => interior.child_for(key)?,
                    _ => {
                        return Err(StorageError::Corruption(CorruptionError {
                            component: CorruptionComponent::Page,
                            page_id: Some(page_id),
                            kind: CorruptionKind::InvalidPageKind {
                                expected: "index page",
                                actual: page.page()[KIND_OFFSET],
                            },
                        }));
                    }
                }
            };
            page_id = next;
        }
    }

    fn lower_bound_slot(
        leaf: &Page<page::Read<'_>, page::Leaf, page::Index>,
        key: &[u8],
    ) -> StorageResult<Option<u16>> {
        for slot_index in 0..leaf.slot_count() {
            let cell = leaf.cell(slot_index)?;
            if cell.key()? >= key {
                return Ok(Some(slot_index));
            }
        }

        Ok(None)
    }

    fn entry_slot(
        leaf: &Page<page::Read<'_>, page::Leaf, page::Index>,
        key: &[u8],
        row_id: RowId,
    ) -> StorageResult<Option<u16>> {
        for slot_index in 0..leaf.slot_count() {
            let cell = leaf.cell(slot_index)?;
            match cell.key()?.cmp(key) {
                core::cmp::Ordering::Less => continue,
                core::cmp::Ordering::Greater => return Ok(None),
                core::cmp::Ordering::Equal => {
                    let cell_row_id = cell.row_id()?;
                    if cell_row_id == row_id {
                        return Ok(Some(slot_index));
                    }
                    if cell_row_id > row_id {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Positions the cursor on the first entry whose key is greater than or
    /// equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let seek = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Index>()?;
            match Self::lower_bound_slot(&leaf, key)? {
                Some(slot_index) => LeafSeek::Positioned(slot_index),
                None => match leaf.next_page_id() {
                    Some(next_page_id) => LeafSeek::Advance(next_page_id),
                    None => LeafSeek::Exhausted,
                },
            }
        };

        match seek {
            LeafSeek::Positioned(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                Ok(true)
            }
            LeafSeek::Advance(next_page_id) => self
                .edge_item_from_leaf(next_page_id, ScanDirection::Forward, Self::entry_at)
                .map(|entry| entry.is_some()),
            LeafSeek::Exhausted => {
                self.set_exhausted_state();
                Ok(false)
            }
        }
    }

    /// Positions the cursor on one exact `(key, row_id)` pair.
    pub fn seek_to_entry(&mut self, key: &[u8], row_id: RowId) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let slot_index = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Index>()?;
            Self::entry_slot(&leaf, key, row_id)?
        };

        match slot_index {
            Some(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                Ok(true)
            }
            None => {
                self.set_page_state(page_id);
                Ok(false)
            }
        }
    }

    /// Positions the cursor on the smallest `(key, row_id)` entry in the tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let leaf_page_id = self.descend_to_first_leaf_from(self.root_page_id)?;
        self.edge_item_from_leaf(leaf_page_id, ScanDirection::Forward, Self::entry_at)
            .map(|entry| entry.is_some())
    }

    /// Reads the currently selected index entry, if any.
    pub fn current(&self) -> StorageResult<Option<IndexEntry>> {
        match self.state {
            CursorState::Positioned { page_id, slot_index } => {
                self.entry_at(page_id, slot_index).map(Some)
            }
            CursorState::Page { .. } | CursorState::Exhausted => Ok(None),
        }
    }

    /// Advances to the next `(key, row_id)` pair in key order.
    pub fn next_row(&mut self) -> StorageResult<Option<IndexEntry>> {
        self.step_row(ScanDirection::Forward, Self::entry_at)
    }

    /// Moves to the previous `(key, row_id)` pair in key order.
    pub fn prev_row(&mut self) -> StorageResult<Option<IndexEntry>> {
        self.step_row(ScanDirection::Backward, Self::entry_at)
    }

    /// Inserts a new `(key, row_id)` pair into the index tree.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> StorageResult<()> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin_guard = self.page_cache.fetch_page(page_id)?;
        let mut write_guard = pin_guard.write()?;
        let mut page = write_guard.open_typed_mut::<page::Leaf, page::Index>()?;

        let insert_result = page.insert(key, row_id);
        match insert_result {
            Ok(_) => Ok(()),
            Err(PageError::CellTooLarge { .. }) => {
                panic!("Cell too large!");
            }
            Err(PageError::PageFull { .. }) => {
                panic!("Page full!")
            }
            Err(err) => Err(err.into()),
        }
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
