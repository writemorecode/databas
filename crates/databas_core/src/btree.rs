//! Foundational byte-oriented B+-tree cursor.
//!
//! This module is intentionally below table and index interpretation. It works
//! only with raw byte keys and raw byte values stored in `RawLeaf` pages
//! and separator byte keys stored in `RawInterior` pages.

use std::{cell::Cell, fmt, ops::Range, rc::Rc};

use crate::{
    PAGE_SIZE, PageId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    page::{
        self, BoundResult, PageError, RawInterior, RawLeaf, Read, SearchResult, Write,
        format::{KIND_OFFSET, PageKind},
    },
    page_cache::{PageCache, PageWriteGuard, PinGuard},
};

const LEAF_CELL_PREFIX_SIZE: usize = 2 + 2 + 2;
const INTERIOR_CELL_PREFIX_SIZE: usize = 2 + 8 + 2;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanDirection {
    /// Move toward larger keys.
    Forward,
    /// Move toward smaller keys.
    Backward,
}

impl ScanDirection {
    /// Descends from `start_page_id` to the leaf at the edge implied by `self`.
    fn descend_to_edge_leaf(
        self,
        cursor: &TreeCursor,
        start_page_id: PageId,
    ) -> StorageResult<PageId> {
        match self {
            Self::Forward => cursor.descend_to_first_leaf_from(start_page_id),
            Self::Backward => cursor.descend_to_last_leaf_from(start_page_id),
        }
    }

    /// Chooses the first or last slot in `leaf`, or advances to the next leaf
    /// in the scan direction when the page is empty.
    fn edge_seek(self, leaf: &RawLeaf<Read<'_>>) -> LeafSeek {
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
    fn adjacent_seek(self, leaf: &RawLeaf<Read<'_>>, slot_index: u16) -> LeafSeek {
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

/// Guard-backed raw record view returned by tree reads and cursor iteration.
pub struct Record {
    pin: PinGuard,
    slot_index: u16,
}

impl Record {
    /// Builds a record view from one pinned raw leaf-page slot.
    pub(crate) fn new(pin: PinGuard, slot_index: u16) -> StorageResult<Self> {
        {
            let page = pin.read()?;
            let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
            let _ = leaf.cell(slot_index)?;
        }
        Ok(Self { pin, slot_index })
    }

    /// Returns the slot index that this record refers to within its leaf page.
    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }

    /// Executes `f` with a borrowed view of the record key while the backing
    /// page remains pinned and immutably borrowed.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let page = self.pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        let cell = leaf.cell(self.slot_index)?;
        Ok(f(cell.key()?))
    }

    /// Executes `f` with a borrowed view of the record value while the backing
    /// page remains pinned and immutably borrowed.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let page = self.pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        let cell = leaf.cell(self.slot_index)?;
        Ok(f(cell.value()?))
    }

    /// Executes `f` with borrowed views of the key and value while the backing
    /// page remains pinned and immutably borrowed.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> StorageResult<R> {
        let page = self.pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        let cell = leaf.cell(self.slot_index)?;
        Ok(f(cell.key()?, cell.value()?))
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("page_id", &self.pin.page_id())
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

/// Public handle to a single raw B+-tree rooted at `root_page_id`.
#[derive(Clone)]
pub struct TreeCursor {
    page_cache: PageCache,
    root_page_id: Rc<Cell<PageId>>,
    state: CursorState,
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
struct PendingSplit {
    /// Separator key promoted out of the split page.
    separator: Vec<u8>,
    /// Left child page after the split.
    left_page_id: PageId,
    /// Right child page after the split.
    right_page_id: PageId,
}

/// Temporary description of one leaf cell while rebuilding split pages.
#[derive(Debug, Clone)]
enum LeafSplitCell<'c> {
    /// An existing cell borrowed from the pre-split page snapshot.
    Snapshot { key_range: Range<usize>, value_range: Range<usize> },
    /// The newly inserted cell that triggered the split.
    Incoming { key: &'c [u8], value: &'c [u8] },
}

/// Temporary description of one interior cell while rebuilding split pages.
#[derive(Debug, Clone)]
enum InteriorSplitCell<'c> {
    /// An existing cell borrowed from the pre-split page snapshot.
    Snapshot { left_child: PageId, key_range: Range<usize> },
    /// The newly inserted separator cell that triggered the split.
    Incoming { left_child: PageId, key: &'c [u8] },
}

impl<'c> LeafSplitCell<'c> {
    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        match self {
            Self::Snapshot { key_range, .. } => key_range.len(),
            Self::Incoming { key, .. } => key.len(),
        }
    }

    /// Returns the value length this cell will occupy after the split.
    fn value_len(&self) -> usize {
        match self {
            Self::Snapshot { value_range, .. } => value_range.len(),
            Self::Incoming { value, .. } => value.len(),
        }
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        LEAF_CELL_PREFIX_SIZE + self.key_len() + self.value_len()
    }

    /// Returns the key bytes from either the page snapshot or owned storage.
    fn key<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        match self {
            Self::Snapshot { key_range, .. } => &snapshot[key_range.clone()],
            Self::Incoming { key, .. } => key,
        }
    }

    /// Returns the value bytes from either the page snapshot or owned storage.
    fn value<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        match self {
            Self::Snapshot { value_range, .. } => &snapshot[value_range.clone()],
            Self::Incoming { value, .. } => value,
        }
    }
}

impl<'c> InteriorSplitCell<'c> {
    /// Returns the left child page referenced by this interior cell.
    fn left_child(&self) -> PageId {
        match self {
            Self::Snapshot { left_child, .. } | Self::Incoming { left_child, .. } => *left_child,
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
        INTERIOR_CELL_PREFIX_SIZE + self.key_len()
    }

    /// Returns the separator key bytes from either the page snapshot or incoming storage.
    fn key<'a>(&'a self, snapshot: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        match self {
            Self::Snapshot { key_range, .. } => &snapshot[key_range.clone()],
            Self::Incoming { key, .. } => key,
        }
    }

    /// Replaces the left-child pointer while preserving the cell key storage.
    fn with_left_child(&self, left_child: PageId) -> Self {
        match self {
            Self::Snapshot { key_range, .. } => {
                Self::Snapshot { left_child, key_range: key_range.clone() }
            }
            Self::Incoming { key, .. } => Self::Incoming { left_child, key },
        }
    }
}

impl TreeCursor {
    /// Creates a cursor anchored at `root_page_id` in page-level state.
    pub(crate) fn new(page_cache: PageCache, root_page_id: PageId) -> Self {
        Self {
            page_cache,
            root_page_id: Rc::new(Cell::new(root_page_id)),
            state: CursorState::Page { page_id: root_page_id },
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

    /// Materializes one record from a positioned raw leaf slot.
    fn record_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<Record> {
        let pin = self.page_cache.fetch_page(page_id)?;
        Record::new(pin, slot_index)
    }

    /// Follows leftmost children from `start_page_id` until reaching a leaf.
    fn descend_to_first_leaf_from(&self, start_page_id: PageId) -> StorageResult<PageId> {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                        return Ok(page_id);
                    }
                    PageKind::RawInterior => {
                        let interior = RawInterior::<Read<'_>>::open(page.page())?;
                        if interior.slot_count() == 0 {
                            interior.rightmost_child()
                        } else {
                            interior.cell(0)?.left_child()?
                        }
                    }
                }
            };
            page_id = next;
        }
    }

    /// Follows rightmost children from `start_page_id` until reaching a leaf.
    fn descend_to_last_leaf_from(&self, start_page_id: PageId) -> StorageResult<PageId> {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                        return Ok(page_id);
                    }
                    PageKind::RawInterior => {
                        let interior = RawInterior::<Read<'_>>::open(page.page())?;
                        interior.rightmost_child()
                    }
                }
            };
            page_id = next;
        }
    }

    /// Descends to the leaf page that contains or would contain `key`.
    fn leaf_page_for_key(&self, key: &[u8]) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                        return Ok(page_id);
                    }
                    PageKind::RawInterior => {
                        let interior = RawInterior::<Read<'_>>::open(page.page())?;
                        interior.child_for(key)?
                    }
                }
            };
            page_id = next;
        }
    }

    /// Descends to the target leaf and records the interior path taken to reach it.
    fn leaf_page_path_for_key(&self, key: &[u8]) -> StorageResult<(PageId, Vec<PathFrame>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                        return Ok((page_id, path));
                    }
                    PageKind::RawInterior => {
                        let interior = RawInterior::<Read<'_>>::open(page.page())?;
                        match interior.lower_bound(key)? {
                            BoundResult::At(slot_index) => {
                                let child_page_id = interior.cell(slot_index)?.left_child()?;
                                path.push(PathFrame {
                                    page_id,
                                    child_ref: ChildSlotRef::Slot(slot_index),
                                });
                                child_page_id
                            }
                            BoundResult::PastEnd => {
                                path.push(PathFrame {
                                    page_id,
                                    child_ref: ChildSlotRef::Rightmost,
                                });
                                interior.rightmost_child()
                            }
                        }
                    }
                }
            };
            page_id = next;
        }
    }

    /// Reads the first reachable record from `start_page_id` in `direction`,
    /// skipping over empty leaf pages until a slot is found or the scan ends.
    fn edge_record_from_leaf(
        &mut self,
        start_page_id: PageId,
        direction: ScanDirection,
    ) -> StorageResult<Option<Record>> {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let seek = {
                let page = pin.read()?;
                expect_page_kind(page.page(), page_id, PageKind::RawLeaf, "raw leaf")?;
                let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
                direction.edge_seek(&leaf)
            };

            match seek {
                LeafSeek::Positioned(slot_index) => {
                    self.set_positioned_state(page_id, slot_index);
                    return Record::new(pin, slot_index).map(Some);
                }
                LeafSeek::Advance(next_page_id) => page_id = next_page_id,
                LeafSeek::Exhausted => {
                    self.set_exhausted_state();
                    return Ok(None);
                }
            }
        }
    }

    /// Advances or rewinds the cursor by one logical record.
    fn step_record(&mut self, direction: ScanDirection) -> StorageResult<Option<Record>> {
        match self.state {
            CursorState::Exhausted => Ok(None),
            CursorState::Page { page_id } => {
                let leaf_page_id = direction.descend_to_edge_leaf(self, page_id)?;
                self.edge_record_from_leaf(leaf_page_id, direction)
            }
            CursorState::Positioned { page_id, slot_index } => {
                let pin = self.page_cache.fetch_page(page_id)?;
                let seek = {
                    let page = pin.read()?;
                    expect_page_kind(page.page(), page_id, PageKind::RawLeaf, "raw leaf")?;
                    let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
                    direction.adjacent_seek(&leaf, slot_index)
                };

                match seek {
                    LeafSeek::Positioned(next_slot) => {
                        self.set_positioned_state(page_id, next_slot);
                        Record::new(pin, next_slot).map(Some)
                    }
                    LeafSeek::Advance(next_page_id) => {
                        self.edge_record_from_leaf(next_page_id, direction)
                    }
                    LeafSeek::Exhausted => {
                        self.set_exhausted_state();
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Searches the raw tree for `key`.
    ///
    /// The cursor ends on the matching record when found, or on the leaf page
    /// where `key` would be inserted when absent.
    pub fn get(&mut self, key: &[u8]) -> StorageResult<Option<Record>> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let slot_index = {
            let page = pin.read()?;
            let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
            leaf.lookup(key)?.map(|cell| cell.slot_index())
        };

        match slot_index {
            Some(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                Record::new(pin, slot_index).map(Some)
            }
            None => {
                self.set_page_state(page_id);
                Ok(None)
            }
        }
    }

    /// Inserts a new raw key/value record into the tree.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_guard = leaf_pin_guard.write()?;
        let insert_result = {
            let mut page = RawLeaf::<Write<'_>>::open(leaf_guard.page_mut())?;
            page.insert(key, value)
        };

        match insert_result {
            Ok(slot_index) => {
                self.set_positioned_state(leaf_page_id, slot_index);
                Ok(())
            }
            Err(PageError::PageFull { .. }) => {
                let pending =
                    self.insert_with_leaf_page_split(leaf_page_id, &mut leaf_guard, key, value)?;
                drop(leaf_guard);
                drop(leaf_pin_guard);
                self.propagate_split(&tree_path, pending)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Replaces the value stored for an existing `key`.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> StorageResult<Record> {
        let _ = &self.page_cache;
        let _ = (key, value);
        todo!("tree update is not implemented yet")
    }

    /// Deletes the record identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = key;
        todo!("tree delete is not implemented yet")
    }

    /// Positions the cursor on the first record whose key is greater than or
    /// equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let seek = {
            let page = pin.read()?;
            let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
            match leaf.lower_bound(key)? {
                BoundResult::At(slot_index) => LeafSeek::Positioned(slot_index),
                BoundResult::PastEnd => match leaf.next_page_id() {
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
                .edge_record_from_leaf(next_page_id, ScanDirection::Forward)
                .map(|record| record.is_some()),
            LeafSeek::Exhausted => {
                self.set_exhausted_state();
                Ok(false)
            }
        }
    }

    /// Positions the cursor on the smallest key in the tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let leaf_page_id = self.descend_to_first_leaf_from(self.root_page_id())?;
        self.edge_record_from_leaf(leaf_page_id, ScanDirection::Forward)
            .map(|record| record.is_some())
    }

    /// Reads the currently selected record, if any.
    pub fn current(&self) -> StorageResult<Option<Record>> {
        match self.state {
            CursorState::Positioned { page_id, slot_index } => {
                self.record_at(page_id, slot_index).map(Some)
            }
            CursorState::Page { .. } | CursorState::Exhausted => Ok(None),
        }
    }

    /// Advances to the next record in sorted key order.
    pub fn next_record(&mut self) -> StorageResult<Option<Record>> {
        self.step_record(ScanDirection::Forward)
    }

    /// Moves to the previous record in sorted key order.
    pub fn prev_record(&mut self) -> StorageResult<Option<Record>> {
        self.step_record(ScanDirection::Backward)
    }

    /// Bubbles one pending split up the recorded tree path until it lands.
    fn propagate_split(
        &mut self,
        tree_path: &[PathFrame],
        mut pending: PendingSplit,
    ) -> StorageResult<()> {
        for &parent_frame in tree_path.iter().rev() {
            match self.insert_into_parent(parent_frame, pending)? {
                Some(next_pending) => pending = next_pending,
                None => return Ok(()),
            }
        }

        self.install_new_root(pending)
    }

    /// Re-points the parent-side child reference after inserting a separator.
    fn update_interior_child_ref(
        interior_page: &mut RawInterior<Write<'_>>,
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

    /// Inserts one promoted separator into an interior page or reports another split.
    fn insert_into_parent(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit,
    ) -> StorageResult<Option<PendingSplit>> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let mut interior_page = RawInterior::<Write<'_>>::open(interior_guard.page_mut())?;

        match interior_page.insert(&pending.separator, pending.left_page_id) {
            Ok(inserted_slot_index) => Self::update_interior_child_ref(
                &mut interior_page,
                parent_frame.child_ref,
                inserted_slot_index,
                pending.right_page_id,
            )
            .map(|()| None),
            Err(PageError::PageFull { .. }) => {
                drop(interior_guard);
                drop(interior_page_guard);
                self.insert_with_interior_page_split(parent_frame, pending).map(Some)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Returns whether the provided leaf cells fit into one leaf page.
    fn leaf_cells_fit(cells: &[LeafSplitCell]) -> bool {
        let used_bytes = PageKind::RawLeaf.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Chooses the leaf split point with the smallest byte imbalance.
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

    /// Splits a full leaf page and returns the separator to propagate upward.
    fn insert_with_leaf_page_split(
        &mut self,
        leaf_page_id: PageId,
        leaf_guard: &mut PageWriteGuard<'_>,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<PendingSplit> {
        let leaf_snapshot_bytes = *leaf_guard.page();
        let leaf_snapshot = RawLeaf::<Read<'_>>::open(&leaf_snapshot_bytes)?;

        let prev_page_id = leaf_snapshot.prev_page_id();
        let next_page_id = leaf_snapshot.next_page_id();
        let mut cells = Vec::with_capacity(leaf_snapshot.slot_count() as usize + 1);
        for slot_index in 0..leaf_snapshot.slot_count() {
            let (key_range, value_range) = leaf_snapshot.cell_key_value_ranges(slot_index)?;
            cells.push(LeafSplitCell::Snapshot { key_range, value_range });
        }

        let idx = match cells.binary_search_by(|cell| cell.key(&leaf_snapshot_bytes).cmp(key)) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => insert_index,
        };
        cells.insert(idx, LeafSplitCell::Incoming { key, value });

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page = RawLeaf::<Write<'_>>::initialize(right_guard.page_mut());

        let split_index = Self::choose_leaf_split_index(&cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);

        let mut leaf_page = RawLeaf::<Write<'_>>::initialize(leaf_guard.page_mut());
        leaf_page.set_prev_page_id(prev_page_id);
        leaf_page.set_next_page_id(Some(right_page_id));
        right_page.set_prev_page_id(Some(leaf_page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            leaf_page.insert(cell.key(&leaf_snapshot_bytes), cell.value(&leaf_snapshot_bytes))?;
        }
        for cell in right_cells {
            right_page.insert(cell.key(&leaf_snapshot_bytes), cell.value(&leaf_snapshot_bytes))?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = RawLeaf::<Write<'_>>::open(next_guard.page_mut())?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator = left_cells
            .last()
            .expect("leaf split must leave a non-empty left page")
            .key(&leaf_snapshot_bytes)
            .to_vec();

        let target_page_id = if key <= separator.as_slice() { leaf_page_id } else { right_page_id };
        let target_slot_index = match if target_page_id == leaf_page_id {
            leaf_page.search(key)?
        } else {
            right_page.search(key)?
        } {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => unreachable!("split insert must place the new key"),
        };
        self.set_positioned_state(target_page_id, target_slot_index);

        Ok(PendingSplit { separator, left_page_id: leaf_page_id, right_page_id })
    }

    /// Returns whether the provided interior cells fit into one interior page.
    fn interior_cells_fit(cells: &[InteriorSplitCell]) -> bool {
        let used_bytes = PageKind::RawInterior.header_size()
            + cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(InteriorSplitCell::encoded_size).sum::<usize>();
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Chooses the interior split point with the smallest byte imbalance.
    fn choose_interior_split_index(cells: &[InteriorSplitCell]) -> StorageResult<usize> {
        debug_assert!(cells.len() >= 2, "interior splits need at least two cells");

        let total_cell_len = cells.iter().map(InteriorSplitCell::encoded_size).sum::<usize>();
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

    /// Splits a full interior page while inserting a propagated separator.
    fn insert_with_interior_page_split(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit,
    ) -> StorageResult<PendingSplit> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let interior_snapshot_bytes = *interior_guard.page();
        let interior_snapshot = RawInterior::<Read<'_>>::open(&interior_snapshot_bytes)?;

        let prev_page_id = interior_snapshot.prev_page_id();
        let next_page_id = interior_snapshot.next_page_id();
        let mut old_rightmost_child = interior_snapshot.rightmost_child();
        let mut cells = Vec::with_capacity(interior_snapshot.slot_count() as usize + 1);
        for slot_index in 0..interior_snapshot.slot_count() {
            let (left_child, key_range) =
                interior_snapshot.cell_left_child_key_range(slot_index)?;
            cells.push(InteriorSplitCell::Snapshot { left_child, key_range });
        }

        match parent_frame.child_ref {
            ChildSlotRef::Slot(slot_index) => {
                let cell = cells[slot_index as usize].with_left_child(pending.right_page_id);
                cells[slot_index as usize] = cell;
            }
            ChildSlotRef::Rightmost => {
                old_rightmost_child = pending.right_page_id;
            }
        }

        let idx = match cells
            .binary_search_by(|cell| cell.key(&interior_snapshot_bytes).cmp(&pending.separator))
        {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => insert_index,
        };
        cells.insert(
            idx,
            InteriorSplitCell::Incoming {
                left_child: pending.left_page_id,
                key: &pending.separator,
            },
        );

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page = RawInterior::<Write<'_>>::initialize(right_guard.page_mut());

        let split_index = Self::choose_interior_split_index(&cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);
        let left_rightmost_child =
            right_cells.first().map(InteriorSplitCell::left_child).unwrap_or(old_rightmost_child);

        let mut interior_page = RawInterior::<Write<'_>>::initialize_with_rightmost(
            interior_guard.page_mut(),
            left_rightmost_child,
        );
        interior_page.set_prev_page_id(prev_page_id);
        interior_page.set_next_page_id(Some(right_page_id));
        right_page.set_rightmost_child(old_rightmost_child);
        right_page.set_prev_page_id(Some(parent_frame.page_id));
        right_page.set_next_page_id(next_page_id);

        for cell in left_cells {
            interior_page.insert(cell.key(&interior_snapshot_bytes), cell.left_child())?;
        }
        for cell in right_cells {
            right_page.insert(cell.key(&interior_snapshot_bytes), cell.left_child())?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = RawInterior::<Write<'_>>::open(next_guard.page_mut())?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator = left_cells
            .last()
            .expect("interior split must leave a non-empty left page")
            .key(&interior_snapshot_bytes)
            .to_vec();

        Ok(PendingSplit { separator, left_page_id: parent_frame.page_id, right_page_id })
    }

    /// Creates a fresh root page after the old root split.
    fn install_new_root(&mut self, pending: PendingSplit) -> StorageResult<()> {
        let (root_page_id, root_page_guard) = self.page_cache.new_page()?;
        let mut root_guard = root_page_guard.write()?;
        let mut root_page = RawInterior::<Write<'_>>::initialize_with_rightmost(
            root_guard.page_mut(),
            pending.right_page_id,
        );
        root_page.insert(&pending.separator, pending.left_page_id)?;
        self.root_page_id.set(root_page_id);
        Ok(())
    }
}

/// Allocates and initializes a brand-new empty raw root leaf page.
pub(crate) fn initialize_empty_root(page_cache: &PageCache) -> StorageResult<PageId> {
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = RawLeaf::<Write<'_>>::initialize(page.page_mut());
    Ok(page_id)
}

/// Verifies that `root_page_id` names a raw leaf or raw interior page.
pub(crate) fn validate_root_page(
    page_cache: &PageCache,
    root_page_id: PageId,
) -> StorageResult<()> {
    let pin = page_cache.fetch_page(root_page_id)?;
    let page = pin.read()?;
    match read_page_kind(page.page(), root_page_id)? {
        PageKind::RawLeaf => {
            let _ = RawLeaf::<Read<'_>>::open(page.page())?;
        }
        PageKind::RawInterior => {
            let _ = RawInterior::<Read<'_>>::open(page.page())?;
        }
    }
    Ok(())
}

fn read_page_kind(bytes: &[u8; PAGE_SIZE], page_id: PageId) -> StorageResult<PageKind> {
    let raw_kind = bytes[KIND_OFFSET];
    PageKind::from_raw(raw_kind).ok_or({
        StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::Page,
            page_id: Some(page_id),
            kind: CorruptionKind::UnknownPageKind { actual: raw_kind },
        })
    })
}

fn expect_page_kind(
    bytes: &[u8; PAGE_SIZE],
    page_id: PageId,
    expected: PageKind,
    expected_name: &'static str,
) -> StorageResult<()> {
    let raw_kind = bytes[KIND_OFFSET];
    let actual = read_page_kind(bytes, page_id)?;
    if actual == expected {
        Ok(())
    } else {
        Err(StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::Page,
            page_id: Some(page_id),
            kind: CorruptionKind::InvalidPageKind { expected: expected_name, actual: raw_kind },
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fastrand::Rng;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::disk_manager::DiskManager;
    use crate::page::format::USABLE_SPACE_END;

    const KEY_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=192;
    const VALUE_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=512;
    const TARGET_HEIGHT: usize = 4;
    const MAX_RECORDS: usize = 50_000;

    fn random_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        rng.fill(&mut bytes);
        bytes
    }

    fn random_unique_cell(
        rng: &mut Rng,
        expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> (Vec<u8>, Vec<u8>) {
        loop {
            let key_len = rng.usize(KEY_LEN_RANGE);
            let value_len = rng.usize(VALUE_LEN_RANGE);
            let key = random_bytes(rng, key_len);
            if expected.contains_key(&key) {
                continue;
            }

            let value = random_bytes(rng, value_len);
            return (key, value);
        }
    }

    fn assert_inline_cell(key: &[u8], value: &[u8]) {
        let leaf_cell_len = LEAF_CELL_PREFIX_SIZE + key.len() + value.len();
        let leaf_max_cell_len = USABLE_SPACE_END - PageKind::RawLeaf.header_size();
        assert!(
            leaf_cell_len <= leaf_max_cell_len,
            "leaf cell should fit inline: len={leaf_cell_len}, max={leaf_max_cell_len}"
        );

        let interior_cell_len = INTERIOR_CELL_PREFIX_SIZE + key.len();
        let interior_max_cell_len = USABLE_SPACE_END - PageKind::RawInterior.header_size();
        assert!(
            interior_cell_len <= interior_max_cell_len,
            "interior separator should fit inline: len={interior_cell_len}, \
             max={interior_max_cell_len}"
        );
    }

    fn tree_height(cursor: &TreeCursor) -> StorageResult<usize> {
        let mut height = 1;
        let mut page_id = cursor.root_page_id();

        loop {
            let pin = cursor.page_cache.fetch_page(page_id)?;
            let next_page_id = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                        return Ok(height);
                    }
                    PageKind::RawInterior => {
                        let interior = RawInterior::<Read<'_>>::open(page.page())?;
                        if interior.slot_count() == 0 {
                            interior.rightmost_child()
                        } else {
                            interior.cell(0)?.left_child()?
                        }
                    }
                }
            };

            height += 1;
            page_id = next_page_id;
        }
    }

    #[test]
    // Builds a four-level raw B+ tree from deterministic random inline cells,
    // proving leaf splits, repeated interior split propagation, exact-key
    // lookups, and forward/backward sorted cursor scans.
    fn random_insert_get_simulation_reaches_four_levels() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 256).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let mut rng = Rng::with_seed(0xd47a_ba5e_b7ee_2026);
        let mut expected = BTreeMap::new();
        let mut cells = Vec::new();
        let mut previous_height = tree_height(&cursor).unwrap();
        let mut saw_leaf_root_split = false;
        let mut saw_first_interior_root_split = false;
        let mut saw_repeated_interior_split_propagation = false;

        assert_eq!(previous_height, 1);

        for _ in 0..MAX_RECORDS {
            let (key, value) = random_unique_cell(&mut rng, &expected);
            assert_inline_cell(&key, &value);

            cursor.insert(&key, &value).unwrap();
            assert!(expected.insert(key.clone(), value.clone()).is_none());
            cells.push((key, value));

            let height = tree_height(&cursor).unwrap();
            saw_leaf_root_split |= previous_height == 1 && height == 2;
            saw_first_interior_root_split |= previous_height == 2 && height == 3;
            saw_repeated_interior_split_propagation |= previous_height == 3 && height == 4;
            previous_height = height;

            if height == TARGET_HEIGHT {
                break;
            }
        }

        assert!(saw_leaf_root_split, "tree should split the root leaf");
        assert!(saw_first_interior_root_split, "tree should split an interior root");
        assert!(
            saw_repeated_interior_split_propagation,
            "tree should propagate an interior split through an existing interior level"
        );
        assert_eq!(
            previous_height, TARGET_HEIGHT,
            "simulation should reach {TARGET_HEIGHT} tree levels within {MAX_RECORDS} inserts"
        );

        for (key, value) in &cells {
            assert_eq!(expected.get(key).map(Vec::as_slice), Some(value.as_slice()));

            let record = cursor.get(&key).unwrap().expect("inserted tree key should be present");
            assert_record_matches(&record, key, value);
        }

        assert_forward_scan_matches(&mut cursor, &expected);
        assert_reverse_scan_matches(&mut cursor, &expected);
        assert_eq!(expected.len(), cells.len());
    }

    fn assert_record_matches(record: &Record, expected_key: &[u8], expected_value: &[u8]) {
        record
            .with_key_value(|actual_key, actual_value| {
                assert_eq!(actual_key, expected_key);
                assert_eq!(actual_value, expected_value);
            })
            .unwrap();
    }

    fn assert_forward_scan_matches(cursor: &mut TreeCursor, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
        assert!(cursor.seek_to_first().unwrap(), "tree should not be empty");

        let mut expected_entries = expected.iter();
        let (first_key, first_value) = expected_entries.next().unwrap();
        let first_record = cursor.current().unwrap().expect("seek_to_first should position cursor");
        assert_record_matches(&first_record, first_key, first_value);

        let mut scanned = 1;
        for (key, value) in expected_entries {
            let record = cursor.next_record().unwrap().expect("forward scan ended early");
            assert_record_matches(&record, key, value);
            scanned += 1;
        }

        assert!(cursor.next_record().unwrap().is_none());
        assert_eq!(scanned, expected.len());
    }

    fn assert_reverse_scan_matches(cursor: &mut TreeCursor, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let (last_key, _) = expected.iter().next_back().unwrap();
        let last_record = cursor.get(last_key).unwrap().expect("last key should be present");

        let mut expected_entries = expected.iter().rev();
        let (key, value) = expected_entries.next().unwrap();
        assert_record_matches(&last_record, key, value);

        let mut scanned = 1;
        for (key, value) in expected_entries {
            let record = cursor.prev_record().unwrap().expect("reverse scan ended early");
            assert_record_matches(&record, key, value);
            scanned += 1;
        }

        assert!(cursor.prev_record().unwrap().is_none());
        assert_eq!(scanned, expected.len());
    }
}
