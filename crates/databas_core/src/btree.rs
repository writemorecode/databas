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
use std::{cell::Cell, fmt, mem::size_of, ops::Range, rc::Rc};

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
    /// Prevents external code from implementing [`super::TreeKind`].
    pub trait Sealed {}
}

/// Outcome of trying to position a scan within or beyond one leaf page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafSeek {
    /// A concrete slot was found in the current leaf page.
    Positioned(u16),
    /// The scan must continue from an adjacent leaf page.
    Advance(PageId),
    /// No more leaf pages remain in the scan direction.
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

/// Internal tree-kind hooks shared by table and index cursor code.
pub(crate) trait TreeKindExt: TreeKind {
    /// Page marker associated with the tree flavor.
    type PageTree: page::TreeMarker;

    /// Human-readable name of the root page kind for corruption diagnostics.
    const ROOT_KIND_NAME: &'static str;
    /// Human-readable name of the general page kind for corruption diagnostics.
    const PAGE_KIND_NAME: &'static str;
    /// Human-readable name of the leaf page kind for corruption diagnostics.
    const LEAF_KIND_NAME: &'static str;

    /// Returns whether `kind` belongs to this tree flavor.
    fn matches_page_kind(kind: PageKind) -> bool {
        kind.tree_kind() == <Self::PageTree as page::TreeMarker>::KIND
    }

    /// Returns the leftmost child to descend into from an interior page.
    fn first_descend_child(
        interior: &Page<page::Read<'_>, page::Interior, Self::PageTree>,
    ) -> StorageResult<PageId>;

    /// Returns the rightmost child to descend into from an interior page.
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
    /// Builds a table record view from one pinned leaf-page slot.
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
    /// Builds an index entry view from one pinned leaf-page slot.
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
    root_page_id: Rc<Cell<PageId>>,
    state: CursorState,
    _marker: PhantomData<K>,
}

/// Typed alias for a table-tree cursor.
pub type TableCursor = TreeCursor<Table>;
/// Typed alias for an index-tree cursor.
pub type IndexCursor = TreeCursor<Index>;

/// Temporary description of one table leaf cell while rebuilding split pages.
#[derive(Debug, Clone)]
enum LeafSplitCell {
    /// An existing cell borrowed from the pre-split page snapshot.
    Snapshot { row_id: RowId, payload_range: Range<usize> },
    /// The newly inserted cell that triggered the split.
    Incoming { row_id: RowId, payload_len: usize },
}

/// Temporary description of one index leaf cell while rebuilding split pages.
#[derive(Debug, Clone)]
enum IndexLeafSplitCell {
    /// An existing cell borrowed from the pre-split page snapshot.
    Snapshot { row_id: RowId, key_range: Range<usize> },
    /// The newly inserted cell that triggered the split.
    Incoming { row_id: RowId, key_len: usize },
}

/// Identifies which child pointer of an interior page led to a descended path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildSlotRef {
    /// The child pointer stored in one interior cell.
    Slot(u16),
    /// The dedicated rightmost child pointer of the page.
    Rightmost,
}

/// One step of the path from the root to a target leaf page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathFrame {
    /// Interior page traversed on the way down.
    page_id: PageId,
    /// Child reference followed from that interior page.
    child_ref: ChildSlotRef,
}

/// Split result that still needs to be inserted into an ancestor page.
#[derive(Debug, Clone)]
struct PendingSplit<S> {
    /// Separator key promoted out of the split page.
    separator: S,
    /// Left child page after the split.
    left_page_id: PageId,
    /// Right child page after the split.
    right_page_id: PageId,
}

/// Separator payload promoted from an index split.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexSeparator {
    /// Separator key bytes.
    key: Vec<u8>,
    /// Row id used to break ties between duplicate keys.
    row_id: RowId,
}

/// Temporary description of one index interior cell while rebuilding split pages.
#[derive(Debug, Clone)]
enum IndexInteriorSplitCell {
    /// An existing cell borrowed from the pre-split page snapshot.
    Snapshot { left_child: PageId, row_id: RowId, key_range: Range<usize> },
    /// The newly inserted separator cell that triggered the split.
    Incoming { left_child: PageId, row_id: RowId, key: Box<[u8]> },
}

impl LeafSplitCell {
    /// Returns the row id used to sort this split cell.
    fn row_id(&self) -> RowId {
        match self {
            Self::Snapshot { row_id, .. } | Self::Incoming { row_id, .. } => *row_id,
        }
    }

    /// Returns the payload length this cell will occupy after the split.
    fn payload_len(&self) -> usize {
        match self {
            Self::Snapshot { payload_range, .. } => payload_range.len(),
            Self::Incoming { payload_len, .. } => *payload_len,
        }
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        size_of::<u16>() + size_of::<RowId>() + self.payload_len()
    }

    /// Returns the payload bytes from either the page snapshot or the incoming value.
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

impl IndexLeafSplitCell {
    /// Returns the row id used to order duplicate index keys.
    fn row_id(&self) -> RowId {
        match self {
            Self::Snapshot { row_id, .. } | Self::Incoming { row_id, .. } => *row_id,
        }
    }

    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        match self {
            Self::Snapshot { key_range, .. } => key_range.len(),
            Self::Incoming { key_len, .. } => *key_len,
        }
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        size_of::<u16>() + size_of::<RowId>() + self.key_len()
    }

    /// Returns the key bytes from either the page snapshot or the incoming value.
    fn key<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE], incoming: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Snapshot { key_range, .. } => &snapshot[key_range.clone()],
            Self::Incoming { key_len, .. } => {
                debug_assert_eq!(*key_len, incoming.len());
                incoming
            }
        }
    }
}

impl IndexInteriorSplitCell {
    /// Returns the left child page referenced by this interior cell.
    fn left_child(&self) -> PageId {
        match self {
            Self::Snapshot { left_child, .. } | Self::Incoming { left_child, .. } => *left_child,
        }
    }

    /// Returns the row id stored alongside the separator key.
    fn row_id(&self) -> RowId {
        match self {
            Self::Snapshot { row_id, .. } | Self::Incoming { row_id, .. } => *row_id,
        }
    }

    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        match self {
            Self::Snapshot { key_range, .. } => key_range.len(),
            Self::Incoming { key, .. } => key.len(),
        }
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        size_of::<u16>() + size_of::<PageId>() + self.key_len() + size_of::<RowId>()
    }

    /// Returns the separator key bytes from either the snapshot or owned storage.
    fn key<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        match self {
            Self::Snapshot { key_range, .. } => &snapshot[key_range.clone()],
            Self::Incoming { key, .. } => key,
        }
    }
}

impl<K> TreeCursor<K>
where
    K: TreeKind,
{
    /// Creates a cursor anchored at `root_page_id` in page-level state.
    pub(crate) fn new(page_cache: PageCache, root_page_id: PageId) -> Self {
        Self {
            page_cache,
            root_page_id: Rc::new(Cell::new(root_page_id)),
            state: CursorState::Page { page_id: root_page_id },
            _marker: PhantomData,
        }
    }

    /// Returns the root page id that anchors this tree.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id.get()
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
        self.state = CursorState::Page { page_id: self.root_page_id() };
    }

    /// Switches the cursor to a page-anchored but not slot-anchored state.
    fn set_page_state(&mut self, page_id: PageId) {
        self.state = CursorState::Page { page_id };
    }

    /// Switches the cursor to one concrete slot inside a leaf page.
    fn set_positioned_state(&mut self, page_id: PageId, slot_index: u16) {
        self.state = CursorState::Positioned { page_id, slot_index };
    }

    /// Marks the cursor as having moved past the scan range.
    fn set_exhausted_state(&mut self) {
        self.state = CursorState::Exhausted;
    }

    /// Bubbles one pending split up the recorded tree path until it lands.
    fn propagate_split<S, FParent, FRoot>(
        &mut self,
        tree_path: &[PathFrame],
        mut pending: PendingSplit<S>,
        mut insert_parent: FParent,
        mut install_root: FRoot,
    ) -> StorageResult<()>
    where
        FParent:
            FnMut(&mut Self, PathFrame, PendingSplit<S>) -> StorageResult<Option<PendingSplit<S>>>,
        FRoot: FnMut(&mut Self, PendingSplit<S>) -> StorageResult<()>,
    {
        for &parent_frame in tree_path.iter().rev() {
            match insert_parent(self, parent_frame, pending)? {
                Some(next_pending) => pending = next_pending,
                None => return Ok(()),
            }
        }

        install_root(self, pending)
    }

    /// Follows leftmost children from `start_page_id` until reaching a leaf.
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

    /// Follows rightmost children from `start_page_id` until reaching a leaf.
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
    /// Re-points the parent-side child reference after inserting a separator.
    fn update_interior_child_ref(
        interior_page: &mut Page<page::Write<'_>, page::Interior, page::Table>,
        child_ref: ChildSlotRef,
        inserted_slot_index: u16,
        child_page_id: PageId,
    ) -> StorageResult<()> {
        match child_ref {
            ChildSlotRef::Slot(slot_index) => {
                let updated_slot_index =
                    if inserted_slot_index <= slot_index { slot_index + 1 } else { slot_index };
                let mut cell = interior_page.cell_mut(updated_slot_index)?;
                cell.set_left_child(child_page_id)?;
            }
            ChildSlotRef::Rightmost => {
                interior_page.set_rightmost_child(child_page_id);
            }
        }
        Ok(())
    }

    /// Inserts one promoted separator into a table interior page or reports another split.
    fn table_insert_into_parent(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit<RowId>,
    ) -> StorageResult<Option<PendingSplit<RowId>>> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::open(interior_guard.page_mut())?;

        match interior_page.insert(pending.separator, pending.left_page_id) {
            Ok(inserted_slot_index) => Self::update_interior_child_ref(
                &mut interior_page,
                parent_frame.child_ref,
                inserted_slot_index,
                pending.right_page_id,
            )
            .map(|()| None),
            Err(PageError::PageFull { .. }) => {
                drop(interior_page);
                drop(interior_guard);
                drop(interior_page_guard);
                self.table_insert_with_interior_page_split(parent_frame, pending).map(Some)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Returns whether the provided table leaf cells fit into one leaf page.
    fn leaf_cells_fit(cells: &[LeafSplitCell]) -> bool {
        let used_bytes = page::format::PageKind::TableLeaf.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Chooses the table leaf split point with the smallest byte imbalance.
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

    /// Materializes one table record from a positioned leaf slot.
    fn record_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<TableRecord> {
        let pin = self.page_cache.fetch_page(page_id)?;
        TableRecord::new(pin, slot_index)
    }

    /// Descends to the leaf page that contains or would contain `row_id`.
    fn leaf_page_for_row_id(&self, row_id: RowId) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id();

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

    /// Descends to the target leaf and records the interior path taken to reach it.
    fn leaf_page_path_for_row_id(&self, row_id: RowId) -> StorageResult<(PageId, Vec<PathFrame>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::TableLeaf(_) => return Ok((page_id, path)),
                    page::AnyPage::TableInterior(interior) => {
                        match interior.lower_bound(row_id)? {
                            page::BoundResult::At(slot_index) => {
                                let child_page_id = interior.cell(slot_index)?.left_child()?;
                                path.push(PathFrame {
                                    page_id,
                                    child_ref: ChildSlotRef::Slot(slot_index),
                                });
                                child_page_id
                            }
                            page::BoundResult::PastEnd => {
                                path.push(PathFrame {
                                    page_id,
                                    child_ref: ChildSlotRef::Rightmost,
                                });
                                interior.rightmost_child()
                            }
                        }
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
            Ok(slot_index) => {
                self.set_positioned_state(leaf_page_id, slot_index);
                Ok(())
            }
            Err(PageError::CellTooLarge { .. }) => {
                panic!("Cell too large!");
            }
            Err(PageError::PageFull { .. }) => {
                let pending =
                    self.table_insert_with_leaf_page_split(leaf_page_id, row_id, payload)?;
                self.propagate_split(
                    &tree_path,
                    pending,
                    |cursor, parent_frame, pending| {
                        cursor.table_insert_into_parent(parent_frame, pending)
                    },
                    |cursor, pending| cursor.table_install_new_root(pending),
                )
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Splits a full table leaf page and returns the separator to propagate upward.
    fn table_insert_with_leaf_page_split(
        &mut self,
        leaf_page_id: PageId,
        row_id: RowId,
        payload: &[u8],
    ) -> StorageResult<PendingSplit<RowId>> {
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

        Ok(PendingSplit { separator: separator_row_id, left_page_id: leaf_page_id, right_page_id })
    }

    /// Splits a full table interior page while inserting a propagated separator.
    fn table_insert_with_interior_page_split(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit<RowId>,
    ) -> StorageResult<PendingSplit<RowId>> {
        // Fetch interior page from page cache
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::open(interior_guard.page_mut())?;

        let prev_page_id = interior_page.prev_page_id();
        let next_page_id = interior_page.next_page_id();
        let mut old_rightmost_child = interior_page.rightmost_child();
        let mut cells: Vec<(RowId, PageId)> =
            Vec::with_capacity(interior_page.slot_count() as usize + 1);
        for slot_index in 0..interior_page.slot_count() {
            let cell = interior_page.cell(slot_index)?;
            cells.push((cell.row_id()?, cell.left_child()?));
        }

        match parent_frame.child_ref {
            ChildSlotRef::Slot(slot_index) => {
                cells[slot_index as usize].1 = pending.right_page_id;
            }
            ChildSlotRef::Rightmost => {
                old_rightmost_child = pending.right_page_id;
            }
        }

        match cells.binary_search_by_key(&pending.separator, |cell| cell.0) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => {
                cells.insert(insert_index, (pending.separator, pending.left_page_id));
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
        right_page.set_prev_page_id(Some(parent_frame.page_id));
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
        Ok(PendingSplit {
            separator: separator_row_id,
            left_page_id: parent_frame.page_id,
            right_page_id,
        })
    }

    /// Creates a fresh table root page after the old root split.
    fn table_install_new_root(&mut self, pending: PendingSplit<RowId>) -> StorageResult<()> {
        let (root_page_id, root_page_guard) = self.page_cache.new_page()?;
        let mut root_guard = root_page_guard.write()?;
        let mut root_page =
            Page::<page::Write<'_>, page::Interior, page::Table>::initialize_with_rightmost(
                root_guard.page_mut(),
                pending.right_page_id,
            );
        root_page.insert(pending.separator, pending.left_page_id)?;
        self.root_page_id.set(root_page_id);
        Ok(())
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
        let leaf_page_id = self.descend_to_first_leaf_from(self.root_page_id())?;
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
    /// Re-points the parent-side child reference after inserting a separator.
    fn update_interior_child_ref(
        interior_page: &mut Page<page::Write<'_>, page::Interior, page::Index>,
        child_ref: ChildSlotRef,
        inserted_slot_index: u16,
        child_page_id: PageId,
    ) -> StorageResult<()> {
        match child_ref {
            ChildSlotRef::Slot(slot_index) => {
                let updated_slot_index =
                    if inserted_slot_index <= slot_index { slot_index + 1 } else { slot_index };
                let mut cell = interior_page.cell_mut(updated_slot_index)?;
                cell.set_left_child(child_page_id)?;
            }
            ChildSlotRef::Rightmost => {
                interior_page.set_rightmost_child(child_page_id);
            }
        }
        Ok(())
    }

    /// Inserts one promoted separator into an index interior page or reports another split.
    fn index_insert_into_parent(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit<IndexSeparator>,
    ) -> StorageResult<Option<PendingSplit<IndexSeparator>>> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Index>::open(interior_guard.page_mut())?;

        match interior_page.insert(
            &pending.separator.key,
            pending.separator.row_id,
            pending.left_page_id,
        ) {
            Ok(inserted_slot_index) => Self::update_interior_child_ref(
                &mut interior_page,
                parent_frame.child_ref,
                inserted_slot_index,
                pending.right_page_id,
            )
            .map(|()| None),
            Err(PageError::PageFull { .. }) => {
                drop(interior_page);
                drop(interior_guard);
                drop(interior_page_guard);
                self.index_insert_with_interior_page_split(parent_frame, pending).map(Some)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Returns whether the provided index leaf cells fit into one leaf page.
    fn leaf_cells_fit(cells: &[IndexLeafSplitCell]) -> bool {
        let used_bytes = page::format::PageKind::IndexLeaf.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(IndexLeafSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Chooses the index leaf split point with the smallest byte imbalance.
    fn choose_leaf_split_index(cells: &[IndexLeafSplitCell]) -> StorageResult<usize> {
        debug_assert!(cells.len() >= 2, "leaf splits need at least two cells");

        let total_cell_len = cells.iter().map(IndexLeafSplitCell::encoded_size).sum::<usize>();
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

    /// Returns whether the provided index interior cells fit into one interior page.
    fn interior_cells_fit(cells: &[IndexInteriorSplitCell]) -> bool {
        let used_bytes = page::format::PageKind::IndexInterior.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(IndexInteriorSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Chooses the index interior split point with the smallest byte imbalance.
    fn choose_interior_split_index(cells: &[IndexInteriorSplitCell]) -> StorageResult<usize> {
        debug_assert!(cells.len() >= 2, "interior splits need at least two cells");

        let total_cell_len = cells.iter().map(IndexInteriorSplitCell::encoded_size).sum::<usize>();
        let mut left_cell_len = 0;
        let mut best = None;

        for split_index in 1..cells.len() {
            left_cell_len += cells[split_index - 1].encoded_size();
            if !Self::interior_cells_fit(&cells[..split_index])
                || !Self::interior_cells_fit(&cells[split_index..])
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

    /// Materializes one index entry from a positioned leaf slot.
    fn entry_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<IndexEntry> {
        let pin = self.page_cache.fetch_page(page_id)?;
        IndexEntry::new(pin, slot_index)
    }

    /// Descends to the leaf page that contains or would contain `(key, row_id)`.
    fn leaf_page_for_key_and_row_id(&self, key: &[u8], row_id: RowId) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::IndexLeaf(_) => return Ok(page_id),
                    page::AnyPage::IndexInterior(interior) => {
                        interior.child_for_entry(key, row_id)?
                    }
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

    /// Descends to the leftmost leaf page that could contain `key`.
    fn leaf_page_for_key(&self, key: &[u8]) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id();

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

    /// Descends to the target leaf and records the interior path taken to reach it.
    fn leaf_page_path_for_key_and_row_id(
        &self,
        key: &[u8],
        row_id: RowId,
    ) -> StorageResult<(PageId, Vec<PathFrame>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match page.open_any()? {
                    page::AnyPage::IndexLeaf(_) => return Ok((page_id, path)),
                    page::AnyPage::IndexInterior(interior) => match interior
                        .lower_bound_entry(key, row_id)?
                    {
                        page::BoundResult::At(slot_index) => {
                            let child_page_id = interior.cell(slot_index)?.left_child()?;
                            path.push(PathFrame {
                                page_id,
                                child_ref: ChildSlotRef::Slot(slot_index),
                            });
                            child_page_id
                        }
                        page::BoundResult::PastEnd => {
                            path.push(PathFrame { page_id, child_ref: ChildSlotRef::Rightmost });
                            interior.rightmost_child()
                        }
                    },
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

    /// Returns the first slot whose key is greater than or equal to `key`.
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

    /// Returns the slot of one exact `(key, row_id)` match within a leaf page.
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
        Ok(self.get(key, row_id)?.is_some())
    }

    /// Positions the cursor on the smallest `(key, row_id)` entry in the tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let leaf_page_id = self.descend_to_first_leaf_from(self.root_page_id())?;
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
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key_and_row_id(key, row_id)?;
        let insert_result = {
            let pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut write_guard = pin_guard.write()?;
            let mut page = write_guard.open_typed_mut::<page::Leaf, page::Index>()?;
            page.insert(key, row_id)
        };
        match insert_result {
            Ok(slot_index) => {
                self.set_positioned_state(leaf_page_id, slot_index);
                Ok(())
            }
            Err(PageError::CellTooLarge { .. }) => {
                panic!("Cell too large!");
            }
            Err(PageError::PageFull { .. }) => {
                let pending = self.index_insert_with_leaf_page_split(leaf_page_id, key, row_id)?;
                self.propagate_split(
                    &tree_path,
                    pending,
                    |cursor, parent_frame, pending| {
                        cursor.index_insert_into_parent(parent_frame, pending)
                    },
                    |cursor, pending| cursor.index_install_new_root(pending),
                )
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Searches the index tree for one exact `(key, row_id)` entry.
    ///
    /// The cursor is expected to end on the matching entry when found, or on
    /// the leaf page where `key` would be inserted when absent.
    pub fn get(&mut self, key: &[u8], row_id: RowId) -> StorageResult<Option<IndexEntry>> {
        let page_id = self.leaf_page_for_key_and_row_id(key, row_id)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let slot_index = {
            let page = pin.read()?;
            let leaf = page.open_typed::<page::Leaf, page::Index>()?;
            Self::entry_slot(&leaf, key, row_id)?
        };

        match slot_index {
            Some(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                IndexEntry::new(pin, slot_index).map(Some)
            }
            None => {
                self.set_page_state(page_id);
                Ok(None)
            }
        }
    }

    /// Splits a full index leaf page and returns the separator to propagate upward.
    fn index_insert_with_leaf_page_split(
        &mut self,
        leaf_page_id: PageId,
        key: &[u8],
        row_id: RowId,
    ) -> StorageResult<PendingSplit<IndexSeparator>> {
        let leaf_page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_guard = leaf_page_guard.write()?;
        let leaf_snapshot_bytes = *leaf_guard.page();
        let leaf_snapshot =
            Page::<page::Read<'_>, page::Leaf, page::Index>::open(&leaf_snapshot_bytes)?;

        let prev_page_id = leaf_snapshot.prev_page_id();
        let next_page_id = leaf_snapshot.next_page_id();
        let mut cells = Vec::with_capacity(leaf_snapshot.slot_count() as usize + 1);
        for slot_index in 0..leaf_snapshot.slot_count() {
            let cell = leaf_snapshot.cell(slot_index)?;
            cells.push(IndexLeafSplitCell::Snapshot {
                row_id: cell.row_id()?,
                key_range: cell.payload()?.as_ptr_range().start as usize
                    - leaf_snapshot_bytes.as_ptr() as usize
                    ..cell.payload()?.as_ptr_range().end as usize
                        - leaf_snapshot_bytes.as_ptr() as usize,
            });
        }

        match cells.binary_search_by(|cell| {
            let ordering = cell.key(&leaf_snapshot_bytes, key).cmp(key);
            if ordering == core::cmp::Ordering::Equal {
                cell.row_id().cmp(&row_id)
            } else {
                ordering
            }
        }) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => cells
                .insert(insert_index, IndexLeafSplitCell::Incoming { row_id, key_len: key.len() }),
        }

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page =
            Page::<page::Write<'_>, page::Leaf, page::Index>::initialize(right_guard.page_mut());

        let split_index = Self::choose_leaf_split_index(&cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);

        let mut leaf_page =
            Page::<page::Write<'_>, page::Leaf, page::Index>::initialize(leaf_guard.page_mut());
        leaf_page.set_prev_page_id(prev_page_id);
        leaf_page.set_next_page_id(Some(right_page_id));
        right_page.set_prev_page_id(Some(leaf_page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            leaf_page.insert(cell.key(&leaf_snapshot_bytes, key), cell.row_id())?;
        }
        for cell in right_cells {
            right_page.insert(cell.key(&leaf_snapshot_bytes, key), cell.row_id())?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = next_guard.open_typed_mut::<page::Leaf, page::Index>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator = {
            let separator_cell =
                left_cells.last().expect("leaf split must leave a non-empty left page");
            IndexSeparator {
                key: separator_cell.key(&leaf_snapshot_bytes, key).to_vec(),
                row_id: separator_cell.row_id(),
            }
        };

        let target_page_id = if left_cells
            .last()
            .expect("leaf split must leave a non-empty left page")
            .key(&leaf_snapshot_bytes, key)
            .cmp(key)
            .then_with(|| {
                left_cells
                    .last()
                    .expect("leaf split must leave a non-empty left page")
                    .row_id()
                    .cmp(&row_id)
            })
            != core::cmp::Ordering::Less
        {
            leaf_page_id
        } else {
            right_page_id
        };
        let target_slot_index = match if target_page_id == leaf_page_id {
            leaf_page.equal_range(key)?.find(|&slot_index| {
                leaf_page
                    .cell(slot_index)
                    .and_then(|cell| cell.row_id())
                    .map(|cell_row_id| cell_row_id == row_id)
                    .unwrap_or(false)
            })
        } else {
            right_page.equal_range(key)?.find(|&slot_index| {
                right_page
                    .cell(slot_index)
                    .and_then(|cell| cell.row_id())
                    .map(|cell_row_id| cell_row_id == row_id)
                    .unwrap_or(false)
            })
        } {
            Some(slot_index) => slot_index,
            None => unreachable!("split insert must place the new entry"),
        };
        self.set_positioned_state(target_page_id, target_slot_index);

        Ok(PendingSplit { separator, left_page_id: leaf_page_id, right_page_id })
    }

    /// Splits a full index interior page while inserting a propagated separator.
    fn index_insert_with_interior_page_split(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit<IndexSeparator>,
    ) -> StorageResult<PendingSplit<IndexSeparator>> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let interior_snapshot_bytes = *interior_guard.page();
        let mut interior_page =
            Page::<page::Write<'_>, page::Interior, page::Index>::open(interior_guard.page_mut())?;
        let interior_snapshot =
            Page::<page::Read<'_>, page::Interior, page::Index>::open(&interior_snapshot_bytes)?;
        let prev_page_id = interior_snapshot.prev_page_id();
        let next_page_id = interior_snapshot.next_page_id();
        let mut old_rightmost_child = interior_snapshot.rightmost_child();
        let mut cells = Vec::with_capacity(interior_snapshot.slot_count() as usize + 1);
        for slot_index in 0..interior_snapshot.slot_count() {
            let cell = interior_snapshot.cell(slot_index)?;
            cells.push(IndexInteriorSplitCell::Snapshot {
                left_child: cell.left_child()?,
                row_id: cell.row_id()?,
                key_range: cell.payload()?.as_ptr_range().start as usize
                    - interior_snapshot_bytes.as_ptr() as usize
                    ..cell.payload()?.as_ptr_range().end as usize
                        - interior_snapshot_bytes.as_ptr() as usize,
            });
        }

        match parent_frame.child_ref {
            ChildSlotRef::Slot(slot_index) => {
                let key_range = match &cells[slot_index as usize] {
                    IndexInteriorSplitCell::Snapshot { key_range, .. } => key_range.clone(),
                    IndexInteriorSplitCell::Incoming { .. } => {
                        unreachable!("interior split snapshots are collected before inserts")
                    }
                };
                cells[slot_index as usize] = IndexInteriorSplitCell::Snapshot {
                    left_child: pending.right_page_id,
                    row_id: cells[slot_index as usize].row_id(),
                    key_range,
                };
            }
            ChildSlotRef::Rightmost => {
                old_rightmost_child = pending.right_page_id;
            }
        }

        let insert_index = cells.partition_point(|cell| {
            cell.key(&interior_snapshot_bytes)
                .cmp(&pending.separator.key)
                .then_with(|| cell.row_id().cmp(&pending.separator.row_id))
                != core::cmp::Ordering::Greater
        });
        cells.insert(
            insert_index,
            IndexInteriorSplitCell::Incoming {
                left_child: pending.left_page_id,
                row_id: pending.separator.row_id,
                key: pending.separator.key.into_boxed_slice(),
            },
        );

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page = Page::<page::Write<'_>, page::Interior, page::Index>::initialize(
            right_guard.page_mut(),
        );

        let split_index = Self::choose_interior_split_index(&cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);
        let left_rightmost_child = right_cells
            .first()
            .map(IndexInteriorSplitCell::left_child)
            .unwrap_or(old_rightmost_child);

        drop(interior_page);
        interior_page =
            Page::<page::Write<'_>, page::Interior, page::Index>::initialize_with_rightmost(
                interior_guard.page_mut(),
                left_rightmost_child,
            );
        interior_page.set_prev_page_id(prev_page_id);
        interior_page.set_next_page_id(Some(right_page_id));
        right_page.set_rightmost_child(old_rightmost_child);
        right_page.set_prev_page_id(Some(parent_frame.page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            interior_page.insert(
                cell.key(&interior_snapshot_bytes),
                cell.row_id(),
                cell.left_child(),
            )?;
        }
        for cell in right_cells {
            right_page.insert(
                cell.key(&interior_snapshot_bytes),
                cell.row_id(),
                cell.left_child(),
            )?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = next_guard.open_typed_mut::<page::Interior, page::Index>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator = {
            let separator_cell =
                left_cells.last().expect("interior split must leave a non-empty left page");
            IndexSeparator {
                key: separator_cell.key(&interior_snapshot_bytes).to_vec(),
                row_id: separator_cell.row_id(),
            }
        };
        Ok(PendingSplit { separator, left_page_id: parent_frame.page_id, right_page_id })
    }

    /// Creates a fresh index root page after the old root split.
    fn index_install_new_root(
        &mut self,
        pending: PendingSplit<IndexSeparator>,
    ) -> StorageResult<()> {
        let (root_page_id, root_page_guard) = self.page_cache.new_page()?;
        let mut root_guard = root_page_guard.write()?;
        let mut root_page =
            Page::<page::Write<'_>, page::Interior, page::Index>::initialize_with_rightmost(
                root_guard.page_mut(),
                pending.right_page_id,
            );
        root_page.insert(&pending.separator.key, pending.separator.row_id, pending.left_page_id)?;
        self.root_page_id.set(root_page_id);
        Ok(())
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

/// Allocates and initializes a brand-new empty root leaf page for one tree kind.
pub(crate) fn initialize_empty_root<K>(page_cache: &PageCache) -> StorageResult<PageId>
where
    K: TreeKindExt,
{
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = Page::<Write<'_>, page::Leaf, K::PageTree>::initialize(page.page_mut());
    Ok(page_id)
}

/// Verifies that `root_page_id` names a page compatible with tree kind `K`.
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

    fn create_stress_test_fixture(
        page_count: usize,
    ) -> (NamedTempFile, Pager, Vec<(RowId, Box<[u8]>)>) {
        let cache_frames = page_count / 4;
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open_with_options(file.path(), PagerOptions { cache_frames }).unwrap();
        let cells = generate_table_cells(page_count * USABLE_SPACE_END);
        (file, pager, cells)
    }

    fn expected_table_records<'a>(cells: &'a [(RowId, Box<[u8]>)]) -> BTreeMap<RowId, &'a [u8]> {
        cells.iter().map(|(row_id, payload)| (*row_id, payload.as_ref())).collect()
    }

    fn expected_index_entries(cells: &[(RowId, Box<[u8]>)]) -> BTreeMap<(Box<[u8]>, RowId), ()> {
        cells.iter().map(|(row_id, payload)| ((payload.clone(), *row_id), ())).collect()
    }

    fn split_test_table_payload(row_id: RowId) -> Box<[u8]> {
        vec![(row_id & 0xFF) as u8; 768].into_boxed_slice()
    }

    fn split_test_index_key(ordinal: u64) -> Box<[u8]> {
        let mut key = vec![0_u8; 768];
        key[..size_of::<u64>()].copy_from_slice(&ordinal.to_be_bytes());
        key[size_of::<u64>()..].fill((ordinal & 0xFF) as u8);
        key.into_boxed_slice()
    }

    fn duplicate_split_test_index_key() -> Box<[u8]> {
        vec![0xAB; 768].into_boxed_slice()
    }

    fn force_table_root_split(cursor: &mut TableCursor) -> RowId {
        let initial_root_page_id = cursor.root_page_id();
        for row_id in 1..=1_024 {
            let payload = split_test_table_payload(row_id);
            cursor.insert(row_id, &payload).unwrap();
            if cursor.root_page_id() != initial_root_page_id {
                return row_id + 1;
            }
        }
        panic!("table root did not split");
    }

    fn force_index_root_split(cursor: &mut IndexCursor) -> RowId {
        let initial_root_page_id = cursor.root_page_id();
        for ordinal in 1..=1_024 {
            let key = split_test_index_key(ordinal);
            cursor.insert(&key, ordinal).unwrap();
            if cursor.root_page_id() != initial_root_page_id {
                return ordinal + 1;
            }
        }
        panic!("index root did not split");
    }

    fn force_duplicate_index_root_split(cursor: &mut IndexCursor, key: &[u8]) -> (RowId, RowId) {
        let initial_root_page_id = cursor.root_page_id();
        for row_id in 1..=1_024 {
            cursor.insert(key, row_id).unwrap();
            if cursor.root_page_id() != initial_root_page_id {
                return (row_id, row_id + 1);
            }
        }
        panic!("duplicate-key index root did not split");
    }

    #[test]
    fn tree_insert_and_get_stress_test() {
        let page_count = 500;
        let (_file, pager, cells) = create_stress_test_fixture(page_count);
        let mut cursor = pager.create_table().unwrap();
        let expected = expected_table_records(&cells);

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

    #[test]
    fn index_tree_insert_and_get_stress_test() {
        let page_count = 500;
        let (_file, pager, cells) = create_stress_test_fixture(page_count);
        let mut cursor = pager.create_index().unwrap();
        let expected = expected_index_entries(&cells);

        assert!(!cells.is_empty());
        assert_eq!(cells.len(), expected.len(), "generated (key, row_id) pairs must be unique");

        for (row_id, payload) in &cells {
            cursor.insert(payload, *row_id).unwrap();
        }

        for ((expected_key, row_id), ()) in &expected {
            let entry = cursor.get(expected_key, *row_id).unwrap().expect("Entry not found");
            assert_eq!(entry.row_id, *row_id);
            entry.with_key(|key| assert_eq!(key, expected_key.as_ref())).unwrap();
        }

        let mut rng = fastrand::Rng::with_seed(0x1D3E5_u64);
        let mut missing_entries = Vec::new();
        while missing_entries.len() < 8 {
            let row_id = rng.u64(..);
            let key_len = rng.usize(1..=64);
            let mut key = vec![0_u8; key_len];
            for byte in &mut key {
                *byte = rng.u8(..);
            }

            let candidate = (key.into_boxed_slice(), row_id);
            if !expected.contains_key(&candidate) {
                missing_entries.push(candidate);
            }
        }

        for (key, row_id) in missing_entries {
            assert_eq!(expected.get(&(key.clone(), row_id)).copied(), None);
            assert!(cursor.get(&key, row_id).unwrap().is_none());
        }
    }

    #[test]
    fn index_get_finds_duplicate_key_entry_after_root_split() {
        let file = NamedTempFile::new().unwrap();
        let pager =
            Pager::open_with_options(file.path(), PagerOptions { cache_frames: 16 }).unwrap();
        let mut cursor = pager.create_index().unwrap();
        let key = duplicate_split_test_index_key();

        let (split_row_id, _) = force_duplicate_index_root_split(&mut cursor, &key);

        let entry = cursor
            .get(&key, split_row_id)
            .unwrap()
            .expect("duplicate-key entry on right sibling should remain reachable");
        assert_eq!(entry.row_id, split_row_id);
        entry.with_key(|entry_key| assert_eq!(entry_key, key.as_ref())).unwrap();
    }

    #[test]
    fn index_insert_preserves_global_order_after_duplicate_key_root_split() {
        let file = NamedTempFile::new().unwrap();
        let pager =
            Pager::open_with_options(file.path(), PagerOptions { cache_frames: 16 }).unwrap();
        let mut cursor = pager.create_index().unwrap();
        let key = duplicate_split_test_index_key();

        let (_, next_row_id) = force_duplicate_index_root_split(&mut cursor, &key);
        cursor.insert(&key, next_row_id).unwrap();

        let mut scanned_row_ids = Vec::new();
        let mut scan = pager.open_index(cursor.root_page_id()).unwrap();
        let mut current = scan.seek_to_first().unwrap().then(|| scan.current().unwrap().unwrap());
        while let Some(entry) = current {
            scanned_row_ids.push(entry.row_id);
            current = scan.next_row().unwrap();
        }

        assert_eq!(scanned_row_ids, (1..=next_row_id).collect::<Vec<_>>());
    }

    #[test]
    fn table_cursor_clones_observe_root_splits() {
        let file = NamedTempFile::new().unwrap();
        let pager =
            Pager::open_with_options(file.path(), PagerOptions { cache_frames: 16 }).unwrap();
        let mut cursor = pager.create_table().unwrap();
        let mut stale_clone = cursor.clone();
        let initial_root_page_id = cursor.root_page_id();

        let target_row_id = force_table_root_split(&mut cursor);
        let target_payload = split_test_table_payload(target_row_id);
        cursor.insert(target_row_id, &target_payload).unwrap();

        assert_ne!(cursor.root_page_id(), initial_root_page_id);
        assert_eq!(stale_clone.root_page_id(), cursor.root_page_id());

        let record =
            stale_clone.get(target_row_id).unwrap().expect("record should remain reachable");
        assert_eq!(record.row_id, target_row_id);
        record.with_payload(|payload| assert_eq!(payload, target_payload.as_ref())).unwrap();
    }

    #[test]
    fn index_cursor_clones_observe_root_splits() {
        let file = NamedTempFile::new().unwrap();
        let pager =
            Pager::open_with_options(file.path(), PagerOptions { cache_frames: 16 }).unwrap();
        let mut cursor = pager.create_index().unwrap();
        let mut stale_clone = cursor.clone();
        let initial_root_page_id = cursor.root_page_id();

        let target_row_id = force_index_root_split(&mut cursor);
        let target_key = split_test_index_key(target_row_id);
        cursor.insert(&target_key, target_row_id).unwrap();

        assert_ne!(cursor.root_page_id(), initial_root_page_id);
        assert_eq!(stale_clone.root_page_id(), cursor.root_page_id());

        let entry = stale_clone
            .get(&target_key, target_row_id)
            .unwrap()
            .expect("entry should remain reachable");
        assert_eq!(entry.row_id, target_row_id);
        entry.with_key(|key| assert_eq!(key, target_key.as_ref())).unwrap();
    }
}
