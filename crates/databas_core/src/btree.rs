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
use std::mem::size_of;
use std::ops::Range;

use crate::{
    PAGE_SIZE, PageId, RowId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    page::{
        self, Page, PageError, SearchResult, Write,
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
        Ok(f(cell.payload()?))
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

#[derive(Debug, Clone)]
enum LeafSplitCell {
    Snapshot { row_id: RowId, payload_range: Range<usize> },
    Incoming { row_id: RowId, payload_len: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InteriorChildRef {
    Separator(RowId),
    Rightmost,
}

impl LeafSplitCell {
    fn row_id(&self) -> RowId {
        match self {
            Self::Snapshot { row_id, .. } | Self::Incoming { row_id, .. } => *row_id,
        }
    }

    fn payload_len(&self) -> usize {
        match self {
            Self::Snapshot { payload_range, .. } => payload_range.len(),
            Self::Incoming { payload_len, .. } => *payload_len,
        }
    }

    fn encoded_size(&self) -> usize {
        size_of::<u16>() + size_of::<RowId>() + self.payload_len()
    }

    fn payload<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE], incoming: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Snapshot { payload_range, .. } => &snapshot[payload_range.clone()],
            Self::Incoming { payload_len, .. } => {
                debug_assert_eq!(*payload_len, incoming.len());
                incoming
            }
        }
    }
}

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
    fn find_interior_child_ref(
        interior_page: &Page<page::Write<'_>, page::Interior, page::Table>,
        child_page_id: PageId,
    ) -> StorageResult<InteriorChildRef> {
        for slot_index in 0..interior_page.slot_count() {
            let cell = interior_page.cell(slot_index)?;
            if cell.left_child()? == child_page_id {
                return Ok(InteriorChildRef::Separator(cell.row_id()?));
            }
        }

        if interior_page.rightmost_child() == child_page_id {
            Ok(InteriorChildRef::Rightmost)
        } else {
            Err(PageError::KeyNotFound.into())
        }
    }

    fn update_interior_child_ref(
        interior_page: &mut Page<page::Write<'_>, page::Interior, page::Table>,
        child_ref: InteriorChildRef,
        child_page_id: PageId,
    ) -> StorageResult<()> {
        match child_ref {
            InteriorChildRef::Separator(separator_row_id) => {
                interior_page.update(separator_row_id, child_page_id)?;
            }
            InteriorChildRef::Rightmost => {
                interior_page.set_rightmost_child(child_page_id);
            }
        }
        Ok(())
    }

    fn table_insert_into_parent(
        &mut self,
        ancestor_path: &[PageId],
        interior_page_id: PageId,
        separator_row_id: RowId,
        left_child: PageId,
        right_child: PageId,
    ) -> StorageResult<()> {
        let interior_page_guard = self.page_cache.fetch_page(interior_page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::open(interior_guard.page_mut())?;
        let child_ref = Self::find_interior_child_ref(&interior_page, left_child)?;

        match interior_page.insert(separator_row_id, left_child) {
            Ok(_) => Self::update_interior_child_ref(&mut interior_page, child_ref, right_child),
            Err(PageError::PageFull { .. }) => {
                drop(interior_page);
                drop(interior_guard);
                drop(interior_page_guard);
                self.table_insert_with_interior_page_split(
                    ancestor_path,
                    interior_page_id,
                    separator_row_id,
                    left_child,
                    right_child,
                )
            }
            Err(err) => Err(err.into()),
        }
    }

    fn leaf_cells_fit(cells: &[LeafSplitCell]) -> bool {
        let used_bytes = page::format::PageKind::TableLeaf.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    fn choose_leaf_split_index(cells: &[LeafSplitCell]) -> StorageResult<usize> {
        debug_assert!(cells.len() >= 2, "leaf splits need at least two cells");

        let total_cell_len = cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let mut left_cell_len = 0;
        let mut best = None;

        for split_index in 1..cells.len() {
            left_cell_len += cells[split_index - 1].encoded_size();
            if !Self::leaf_cells_fit(&cells[..split_index])
                || !Self::leaf_cells_fit(&cells[split_index..])
            {
                continue;
            }

            let right_cell_len = total_cell_len - left_cell_len;
            let imbalance = left_cell_len.abs_diff(right_cell_len);
            let is_better = match best {
                Some((best_imbalance, best_left_cell_len, _)) => {
                    imbalance < best_imbalance
                        || (imbalance == best_imbalance && left_cell_len > best_left_cell_len)
                }
                None => true,
            };

            if is_better {
                best = Some((imbalance, left_cell_len, split_index));
            }
        }

        match best {
            Some((_, _, split_index)) => Ok(split_index),
            None => Err(PageError::PageFull {
                needed: total_cell_len,
                available: total_cell_len.saturating_sub(1),
            }
            .into()),
        }
    }

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

    fn leaf_page_path_for_row_id(&self, row_id: RowId) -> StorageResult<(PageId, Vec<PageId>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::TableLeaf(_) => return Ok((page_id, path)),
                    page::AnyPage::TableInterior(interior) => {
                        path.push(page_id);
                        interior.child_for(row_id)?
                    }
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
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_row_id(row_id)?;
        let insert_result = {
            let pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut write_guard = pin_guard.write()?;
            let mut page = write_guard.open_typed_mut::<page::Leaf, page::Table>()?;
            page.insert(row_id, payload)
        };
        match insert_result {
            Ok(_) => Ok(()),
            Err(PageError::CellTooLarge { .. }) => {
                panic!("Cell too large!");
            }
            Err(PageError::PageFull { .. }) => {
                self.table_insert_with_leaf_page_split(leaf_page_id, &tree_path, row_id, payload)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn table_insert_with_leaf_page_split(
        &mut self,
        leaf_page_id: PageId,
        tree_path: &[PageId],
        row_id: RowId,
        payload: &[u8],
    ) -> StorageResult<()> {
        // Fetch leaf page from page cache
        let leaf_page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_guard = leaf_page_guard.write()?;
        let leaf_snapshot_bytes = *leaf_guard.page();
        let leaf_snapshot =
            Page::<page::Read<'_>, page::Leaf, page::Table>::open(&leaf_snapshot_bytes)?;

        let prev_page_id = leaf_snapshot.prev_page_id();
        let next_page_id = leaf_snapshot.next_page_id();
        let mut cells = Vec::with_capacity(leaf_snapshot.slot_count() as usize + 1);
        for slot_index in 0..leaf_snapshot.slot_count() {
            let cell = leaf_snapshot.cell(slot_index)?;
            cells.push(LeafSplitCell::Snapshot {
                row_id: cell.row_id()?,
                payload_range: cell.payload()?.as_ptr_range().start as usize
                    - leaf_snapshot_bytes.as_ptr() as usize
                    ..cell.payload()?.as_ptr_range().end as usize
                        - leaf_snapshot_bytes.as_ptr() as usize,
            });
        }

        match cells.binary_search_by_key(&row_id, LeafSplitCell::row_id) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => cells.insert(
                insert_index,
                LeafSplitCell::Incoming { row_id, payload_len: payload.len() },
            ),
        }

        // Allocate new right sibling leaf page
        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page =
            Page::<page::Write<'_>, page::Leaf, page::Table>::initialize(right_guard.page_mut());

        let split_index = Self::choose_leaf_split_index(&cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);

        let mut leaf_page =
            Page::<page::Write<'_>, page::Leaf, page::Table>::initialize(leaf_guard.page_mut());
        leaf_page.set_prev_page_id(prev_page_id);
        leaf_page.set_next_page_id(Some(right_page_id));
        right_page.set_prev_page_id(Some(leaf_page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            leaf_page.insert(cell.row_id(), cell.payload(&leaf_snapshot_bytes, payload))?;
        }
        for cell in right_cells {
            right_page.insert(cell.row_id(), cell.payload(&leaf_snapshot_bytes, payload))?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = next_guard.open_typed_mut::<page::Leaf, page::Table>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator_row_id =
            left_cells.last().expect("leaf split must leave a non-empty left page").row_id();

        if let Some((&interior_page_id, ancestor_path)) = tree_path.split_last() {
            self.table_insert_into_parent(
                ancestor_path,
                interior_page_id,
                separator_row_id,
                leaf_page_id,
                right_page_id,
            )?;
        } else {
            let (root_page_id, root_page_guard) = self.page_cache.new_page()?;
            let mut root_guard = root_page_guard.write()?;
            let mut root_page =
                Page::<page::Write<'_>, page::Interior, page::Table>::initialize_with_rightmost(
                    root_guard.page_mut(),
                    right_page_id,
                );
            root_page.insert(separator_row_id, leaf_page_id)?;
            self.root_page_id = root_page_id;
        };

        let target_page_id = if row_id <= separator_row_id { leaf_page_id } else { right_page_id };
        let target_slot_index = match if row_id <= separator_row_id {
            leaf_page.search(row_id)?
        } else {
            right_page.search(row_id)?
        } {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => unreachable!("split insert must place the new row"),
        };
        self.set_positioned_state(target_page_id, target_slot_index);

        Ok(())
    }

    fn table_insert_with_interior_page_split(
        &mut self,
        ancestor_path: &[PageId],
        interior_page_id: PageId,
        row_id: RowId,
        left_child: PageId,
        right_child: PageId,
    ) -> StorageResult<()> {
        // Fetch interior page from page cache
        let interior_page_guard = self.page_cache.fetch_page(interior_page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::open(interior_guard.page_mut())?;

        let prev_page_id = interior_page.prev_page_id();
        let next_page_id = interior_page.next_page_id();
        let child_ref = Self::find_interior_child_ref(&interior_page, left_child)?;
        let mut old_rightmost_child = interior_page.rightmost_child();
        let mut cells: Vec<(RowId, PageId)> =
            Vec::with_capacity(interior_page.slot_count() as usize + 1);
        for slot_index in 0..interior_page.slot_count() {
            let cell = interior_page.cell(slot_index)?;
            cells.push((cell.row_id()?, cell.left_child()?));
        }

        match child_ref {
            InteriorChildRef::Separator(separator_row_id) => {
                let existing_index = cells
                    .binary_search_by_key(&separator_row_id, |cell| cell.0)
                    .expect("existing separator for child must be present");
                cells[existing_index].1 = right_child;
            }
            InteriorChildRef::Rightmost => {
                old_rightmost_child = right_child;
            }
        }

        match cells.binary_search_by_key(&row_id, |cell| cell.0) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => {
                cells.insert(insert_index, (row_id, left_child));
            }
        }

        // Allocate new right sibling interior page
        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page = Page::<page::Write<'_>, page::Interior, page::Table>::initialize(
            right_guard.page_mut(),
        );

        let split_index = cells.len() / 2;
        let (left_cells, right_cells) = cells.split_at(split_index);
        let left_rightmost_child =
            right_cells.first().map(|cell| cell.1).unwrap_or(old_rightmost_child);

        drop(interior_page);
        interior_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::initialize_with_rightmost(
                interior_guard.page_mut(),
                left_rightmost_child,
            );
        interior_page.set_prev_page_id(prev_page_id);
        interior_page.set_next_page_id(Some(right_page_id));
        right_page.set_rightmost_child(old_rightmost_child);
        right_page.set_prev_page_id(Some(interior_page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            interior_page.insert(cell.0, cell.1)?;
        }
        for cell in right_cells {
            right_page.insert(cell.0, cell.1)?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = next_guard.open_typed_mut::<page::Interior, page::Table>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator_row_id =
            left_cells.last().expect("interior split must leave a non-empty left page").0;

        if let Some((&parent_page_id, grandparent_path)) = ancestor_path.split_last() {
            self.table_insert_into_parent(
                grandparent_path,
                parent_page_id,
                separator_row_id,
                interior_page_id,
                right_page_id,
            )
        } else {
            let (root_page_id, root_page_guard) = self.page_cache.new_page()?;
            let mut root_guard = root_page_guard.write()?;
            let mut root_page =
                Page::<page::Write<'_>, page::Interior, page::Table>::initialize_with_rightmost(
                    root_guard.page_mut(),
                    right_page_id,
                );
            root_page.insert(separator_row_id, interior_page_id)?;
            self.root_page_id = root_page_id;
            Ok(())
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
            if cell.payload()? >= key {
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
            match cell.payload()?.cmp(key) {
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        mem::size_of,
    };

    use tempfile::NamedTempFile;

    use super::*;
    use crate::{
        Pager, PagerOptions,
        error::ConstraintError,
        page::format::{PageKind, SLOT_ENTRY_SIZE, USABLE_SPACE_END},
    };

    fn generate_table_cells(target_bytes: usize) -> Vec<(RowId, Box<[u8]>)> {
        let mut rng = fastrand::Rng::with_seed(0x5EED_u64);
        let mut seen_row_ids = BTreeSet::new();
        let mut cells = Vec::new();
        let mut approximate_bytes = 0;
        let max_payload_len = USABLE_SPACE_END
            - PageKind::TableLeaf.header_size()
            - size_of::<u16>()
            - size_of::<RowId>();
        let bounded_payload_len = max_payload_len.min(512);

        while approximate_bytes <= target_bytes {
            let row_id = loop {
                let candidate = rng.u64(..);
                if seen_row_ids.insert(candidate) {
                    break candidate;
                }
            };

            let payload_len = rng.usize(1..=bounded_payload_len);
            let mut payload = vec![0_u8; payload_len];
            for byte in &mut payload {
                *byte = rng.u8(..);
            }

            approximate_bytes +=
                size_of::<u16>() + size_of::<RowId>() + payload.len() + SLOT_ENTRY_SIZE;
            cells.push((row_id, payload.into_boxed_slice()));
        }

        cells
    }

    #[test]
    fn tree_insert_and_get_stress_test() {
        let page_count = 500;
        let cache_frames = page_count / 4;

        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open_with_options(file.path(), PagerOptions { cache_frames }).unwrap();
        let mut cursor = pager.create_table().unwrap();
        let target_bytes = page_count * USABLE_SPACE_END;
        let cells = generate_table_cells(target_bytes);
        let expected: BTreeMap<RowId, &[u8]> =
            cells.iter().map(|(row_id, payload)| (*row_id, payload.as_ref())).collect();

        assert!(!cells.is_empty());
        assert_eq!(cells.len(), expected.len(), "generated row ids must be unique");

        for (row_id, payload) in &cells {
            cursor.insert(*row_id, payload).unwrap();
        }

        for (&row_id, &expected_payload) in &expected {
            let record = cursor.get(row_id).unwrap().expect("Record not found");
            assert_eq!(record.row_id, row_id);
            record.with_payload(|payload| assert_eq!(payload, expected_payload)).unwrap();
        }

        let mut rng = fastrand::Rng::with_seed(0xBAD5EED_u64);
        let mut missing_row_ids = Vec::new();
        while missing_row_ids.len() < 8 {
            let candidate = rng.u64(..);
            if !expected.contains_key(&candidate) {
                missing_row_ids.push(candidate);
            }
        }

        for row_id in missing_row_ids {
            assert_eq!(expected.get(&row_id).copied(), None);
            assert!(cursor.get(row_id).unwrap().is_none());
        }
    }
}
