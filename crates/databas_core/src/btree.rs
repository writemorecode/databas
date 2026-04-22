//! Foundational byte-oriented B+-tree cursor.
//!
//! This module is intentionally below table and index interpretation. It works
//! only with raw byte keys and raw byte values stored in `RawLeaf` pages
//! and separator byte keys stored in `RawInterior` pages.

use std::{cell::Cell, fmt, rc::Rc};

use crate::{
    PAGE_SIZE, PageId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    overflow,
    page::{
        self, BoundResult, PageError, RawInterior, RawLeaf, Read, SearchResult, Write,
        format::{KIND_OFFSET, MAX_INLINE_OVERFLOW_PAYLOAD_BYTES, PageKind},
    },
    page_cache::{PageCache, PageWriteGuard},
};

const LEAF_CELL_PREFIX_SIZE: usize = 2 + 8 + 2 + 2;
const INTERIOR_CELL_PREFIX_SIZE: usize = 2 + 8 + 8 + 2;

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

/// Materialized raw record view returned by tree reads and cursor iteration.
pub struct Record {
    page_id: PageId,
    slot_index: u16,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Record {
    /// Builds a record view from one raw leaf-page slot.
    pub(crate) fn new(
        page_cache: &PageCache,
        page_id: PageId,
        slot_index: u16,
    ) -> StorageResult<Self> {
        let (key, value) = read_leaf_cell(page_cache, page_id, slot_index)?;
        Ok(Self { page_id, slot_index, key, value })
    }

    /// Returns the slot index that this record refers to within its leaf page.
    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }

    /// Executes `f` with a borrowed view of the record key.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        Ok(f(&self.key))
    }

    /// Executes `f` with a borrowed view of the record value.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        Ok(f(&self.value))
    }

    /// Executes `f` with borrowed views of the key and value.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> StorageResult<R> {
        Ok(f(&self.key, &self.value))
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("page_id", &self.page_id)
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
struct LeafSplitCell {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Temporary description of one interior cell while rebuilding split pages.
#[derive(Debug, Clone)]
struct InteriorSplitCell {
    left_child: PageId,
    key: Vec<u8>,
}

impl LeafSplitCell {
    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        self.key.len()
    }

    /// Returns the value length this cell will occupy after the split.
    fn value_len(&self) -> usize {
        self.value.len()
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        LEAF_CELL_PREFIX_SIZE + local_payload_len(self.key_len() + self.value_len())
    }

    /// Returns the key bytes from either the page snapshot or owned storage.
    fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value bytes from either the page snapshot or owned storage.
    fn value(&self) -> &[u8] {
        &self.value
    }
}

impl InteriorSplitCell {
    /// Returns the left child page referenced by this interior cell.
    fn left_child(&self) -> PageId {
        self.left_child
    }

    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        self.key.len()
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        INTERIOR_CELL_PREFIX_SIZE + local_payload_len(self.key_len())
    }

    /// Returns the separator key bytes from either the page snapshot or incoming storage.
    fn key(&self) -> &[u8] {
        &self.key
    }

    /// Replaces the left-child pointer while preserving the cell key storage.
    fn with_left_child(&self, left_child: PageId) -> Self {
        Self { left_child, key: self.key.clone() }
    }
}

fn payload_uses_overflow(payload_len: usize) -> bool {
    payload_len > MAX_INLINE_OVERFLOW_PAYLOAD_BYTES
}

fn local_payload_len(payload_len: usize) -> usize {
    if payload_uses_overflow(payload_len) { MAX_INLINE_OVERFLOW_PAYLOAD_BYTES } else { payload_len }
}

fn cell_corruption(page_id: PageId, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Cell,
        page_id: Some(page_id),
        kind,
    })
}

fn materialize_payload(
    page_cache: &PageCache,
    page_id: PageId,
    inline_payload: Vec<u8>,
    first_overflow_page_id: Option<PageId>,
    payload_len: usize,
) -> StorageResult<Vec<u8>> {
    if inline_payload.len() > payload_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    let mut payload = inline_payload;
    match first_overflow_page_id {
        Some(first_overflow_page_id) => {
            let remaining = payload_len - payload.len();
            payload.extend(overflow::read_chain(page_cache, first_overflow_page_id, remaining)?);
        }
        None if payload.len() != payload_len => {
            return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
        }
        None => {}
    }

    if payload.len() != payload_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }
    Ok(payload)
}

fn read_leaf_cell(
    page_cache: &PageCache,
    page_id: PageId,
    slot_index: u16,
) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let pin = page_cache.fetch_page(page_id)?;
    let (key_len, value_len, first_overflow_page_id, inline_payload) = {
        let page = pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        let (key_len, value_len, first_overflow_page_id, inline_range) =
            leaf.cell_payload_parts(slot_index)?;
        (key_len, value_len, first_overflow_page_id, page.page()[inline_range].to_vec())
    };
    drop(pin);

    let payload = materialize_payload(
        page_cache,
        page_id,
        inline_payload,
        first_overflow_page_id,
        key_len + value_len,
    )?;
    if payload.len() < key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }
    let key = payload[..key_len].to_vec();
    let value = payload[key_len..].to_vec();
    Ok((key, value))
}

fn read_interior_cell(
    page_cache: &PageCache,
    page_id: PageId,
    slot_index: u16,
) -> StorageResult<(PageId, Vec<u8>)> {
    let pin = page_cache.fetch_page(page_id)?;
    let (left_child, key_len, first_overflow_page_id, inline_payload) = {
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        let (left_child, key_len, first_overflow_page_id, inline_range) =
            interior.cell_payload_parts(slot_index)?;
        (left_child, key_len, first_overflow_page_id, page.page()[inline_range].to_vec())
    };
    drop(pin);

    let key =
        materialize_payload(page_cache, page_id, inline_payload, first_overflow_page_id, key_len)?;
    Ok((left_child, key))
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
        Record::new(&self.page_cache, page_id, slot_index)
    }

    fn raw_leaf_slot_count(&self, page_id: PageId) -> StorageResult<u16> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        Ok(leaf.slot_count())
    }

    fn raw_interior_slot_count(&self, page_id: PageId) -> StorageResult<u16> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        Ok(interior.slot_count())
    }

    fn search_leaf_slot(&self, page_id: PageId, key: &[u8]) -> StorageResult<SearchResult> {
        let mut low: u16 = 0;
        let mut high = self.raw_leaf_slot_count(page_id)?;

        while low < high {
            let mid = low + (high - low) / 2;
            let (cell_key, _) = read_leaf_cell(&self.page_cache, page_id, mid)?;
            match cell_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(SearchResult::Found(mid)),
            }
        }

        Ok(SearchResult::InsertAt(low))
    }

    fn lower_bound_leaf_slot(&self, page_id: PageId, key: &[u8]) -> StorageResult<BoundResult> {
        let mut low: u16 = 0;
        let slot_count = self.raw_leaf_slot_count(page_id)?;
        let mut high = slot_count;

        while low < high {
            let mid = low + (high - low) / 2;
            let (cell_key, _) = read_leaf_cell(&self.page_cache, page_id, mid)?;
            if cell_key.as_slice() < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    fn lower_bound_interior_slot(&self, page_id: PageId, key: &[u8]) -> StorageResult<BoundResult> {
        let mut low: u16 = 0;
        let slot_count = self.raw_interior_slot_count(page_id)?;
        let mut high = slot_count;

        while low < high {
            let mid = low + (high - low) / 2;
            let (_, cell_key) = read_interior_cell(&self.page_cache, page_id, mid)?;
            if cell_key.as_slice() < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    fn interior_child_for_key(&self, page_id: PageId, key: &[u8]) -> StorageResult<PageId> {
        match self.lower_bound_interior_slot(page_id, key)? {
            BoundResult::At(slot_index) => {
                let (left_child, _) = read_interior_cell(&self.page_cache, page_id, slot_index)?;
                Ok(left_child)
            }
            BoundResult::PastEnd => {
                let pin = self.page_cache.fetch_page(page_id)?;
                let page = pin.read()?;
                let interior = RawInterior::<Read<'_>>::open(page.page())?;
                Ok(interior.rightmost_child())
            }
        }
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
                        drop(page);
                        drop(pin);
                        self.interior_child_for_key(page_id, key)?
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
                        drop(page);
                        drop(pin);
                        match self.lower_bound_interior_slot(page_id, key)? {
                            BoundResult::At(slot_index) => {
                                let (child_page_id, _) =
                                    read_interior_cell(&self.page_cache, page_id, slot_index)?;
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
                                let pin = self.page_cache.fetch_page(page_id)?;
                                let page = pin.read()?;
                                let interior = RawInterior::<Read<'_>>::open(page.page())?;
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
                    return self.record_at(page_id, slot_index).map(Some);
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
                        self.record_at(page_id, next_slot).map(Some)
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
        let slot_index = match self.search_leaf_slot(page_id, key)? {
            SearchResult::Found(slot_index) => Some(slot_index),
            SearchResult::InsertAt(_) => None,
        };

        match slot_index {
            Some(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                self.record_at(page_id, slot_index).map(Some)
            }
            None => {
                self.set_page_state(page_id);
                Ok(None)
            }
        }
    }

    fn checked_payload_len(&self, payload_len: usize) -> StorageResult<()> {
        if payload_len > u16::MAX as usize {
            return Err(PageError::CellTooLarge { len: payload_len, max: u16::MAX as usize }.into());
        }
        Ok(())
    }

    fn leaf_cell_local_size(&self, key: &[u8], value: &[u8]) -> StorageResult<usize> {
        let payload_len = key.len() + value.len();
        self.checked_payload_len(payload_len)?;
        Ok(LEAF_CELL_PREFIX_SIZE + local_payload_len(payload_len))
    }

    fn interior_cell_local_size(&self, key: &[u8]) -> StorageResult<usize> {
        self.checked_payload_len(key.len())?;
        Ok(INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len()))
    }

    fn payload_storage_parts(&self, payload: &[u8]) -> StorageResult<(Option<PageId>, Vec<u8>)> {
        self.checked_payload_len(payload.len())?;
        if !payload_uses_overflow(payload.len()) {
            return Ok((None, payload.to_vec()));
        }

        let inline_payload = payload[..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES].to_vec();
        let first_overflow_page_id =
            overflow::write_chain(&self.page_cache, &payload[MAX_INLINE_OVERFLOW_PAYLOAD_BYTES..])?
                .ok_or_else(|| {
                    cell_corruption(self.root_page_id(), CorruptionKind::CellLengthOutOfBounds)
                })?;
        Ok((Some(first_overflow_page_id), inline_payload))
    }

    fn insert_leaf_payload_at(
        &self,
        leaf: &mut RawLeaf<Write<'_>>,
        slot_index: u16,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<u16> {
        let mut payload = Vec::with_capacity(key.len() + value.len());
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
        let (first_overflow_page_id, inline_payload) = self.payload_storage_parts(&payload)?;
        Ok(leaf.insert_payload_at(
            slot_index,
            key.len(),
            value.len(),
            first_overflow_page_id,
            &inline_payload,
        )?)
    }

    fn insert_interior_payload_at(
        &self,
        interior: &mut RawInterior<Write<'_>>,
        slot_index: u16,
        left_child: PageId,
        key: &[u8],
    ) -> StorageResult<u16> {
        let (first_overflow_page_id, inline_payload) = self.payload_storage_parts(key)?;
        Ok(interior.insert_payload_at(
            slot_index,
            left_child,
            key.len(),
            first_overflow_page_id,
            &inline_payload,
        )?)
    }

    /// Inserts a new raw key/value record into the tree.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        let slot_index = match self.search_leaf_slot(leaf_page_id, key)? {
            SearchResult::Found(_) => return Err(PageError::DuplicateKey.into()),
            SearchResult::InsertAt(slot_index) => slot_index,
        };
        let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_guard = leaf_pin_guard.write()?;
        let has_capacity = {
            let page = RawLeaf::<Write<'_>>::open(leaf_guard.page_mut())?;
            let needed = self.leaf_cell_local_size(key, value)? + page::format::SLOT_ENTRY_SIZE;
            page.total_reclaimable_space()? >= needed
        };

        if has_capacity {
            let mut page = RawLeaf::<Write<'_>>::open(leaf_guard.page_mut())?;
            let slot_index = self.insert_leaf_payload_at(&mut page, slot_index, key, value)?;
            self.set_positioned_state(leaf_page_id, slot_index);
            return Ok(());
        }

        let pending =
            self.insert_with_leaf_page_split(leaf_page_id, &mut leaf_guard, key, value)?;
        drop(leaf_guard);
        drop(leaf_pin_guard);
        self.propagate_split(&tree_path, pending)
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
            let bound = self.lower_bound_leaf_slot(page_id, key)?;
            match bound {
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
        let insert_slot_index =
            match self.lower_bound_interior_slot(parent_frame.page_id, &pending.separator)? {
                BoundResult::At(slot_index) => {
                    let (_, existing_key) =
                        read_interior_cell(&self.page_cache, parent_frame.page_id, slot_index)?;
                    if existing_key == pending.separator {
                        return Err(PageError::DuplicateKey.into());
                    }
                    slot_index
                }
                BoundResult::PastEnd => self.raw_interior_slot_count(parent_frame.page_id)?,
            };
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let mut interior_guard = interior_page_guard.write()?;
        let has_capacity = {
            let page = RawInterior::<Write<'_>>::open(interior_guard.page_mut())?;
            let needed =
                self.interior_cell_local_size(&pending.separator)? + page::format::SLOT_ENTRY_SIZE;
            page.total_reclaimable_space()? >= needed
        };

        if has_capacity {
            let mut interior_page = RawInterior::<Write<'_>>::open(interior_guard.page_mut())?;
            let inserted_slot_index = self.insert_interior_payload_at(
                &mut interior_page,
                insert_slot_index,
                pending.left_page_id,
                &pending.separator,
            )?;
            Self::update_interior_child_ref(
                &mut interior_page,
                parent_frame.child_ref,
                inserted_slot_index,
                pending.right_page_id,
            )
            .map(|()| None)
        } else {
            drop(interior_guard);
            drop(interior_page_guard);
            self.insert_with_interior_page_split(parent_frame, pending).map(Some)
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
            let (key_len, value_len, first_overflow_page_id, inline_range) =
                leaf_snapshot.cell_payload_parts(slot_index)?;
            let payload = materialize_payload(
                &self.page_cache,
                leaf_page_id,
                leaf_snapshot_bytes[inline_range].to_vec(),
                first_overflow_page_id,
                key_len + value_len,
            )?;
            cells.push(LeafSplitCell {
                key: payload[..key_len].to_vec(),
                value: payload[key_len..].to_vec(),
            });
        }

        let idx = match cells.binary_search_by(|cell| cell.key().cmp(key)) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => insert_index,
        };
        cells.insert(idx, LeafSplitCell { key: key.to_vec(), value: value.to_vec() });

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
            let slot_index = leaf_page.slot_count();
            self.insert_leaf_payload_at(&mut leaf_page, slot_index, cell.key(), cell.value())?;
        }
        for cell in right_cells {
            let slot_index = right_page.slot_count();
            self.insert_leaf_payload_at(&mut right_page, slot_index, cell.key(), cell.value())?;
        }

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = RawLeaf::<Write<'_>>::open(next_guard.page_mut())?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator =
            left_cells.last().expect("leaf split must leave a non-empty left page").key().to_vec();

        let target_page_id = if key <= separator.as_slice() { leaf_page_id } else { right_page_id };
        let target_cells = if target_page_id == leaf_page_id { left_cells } else { right_cells };
        let target_slot_index = target_cells
            .iter()
            .position(|cell| cell.key() == key)
            .expect("split insert must place the new key") as u16;
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
            let (left_child, key_len, first_overflow_page_id, inline_range) =
                interior_snapshot.cell_payload_parts(slot_index)?;
            let key = materialize_payload(
                &self.page_cache,
                parent_frame.page_id,
                interior_snapshot_bytes[inline_range].to_vec(),
                first_overflow_page_id,
                key_len,
            )?;
            cells.push(InteriorSplitCell { left_child, key });
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

        let idx = match cells.binary_search_by(|cell| cell.key().cmp(&pending.separator)) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => insert_index,
        };
        cells.insert(
            idx,
            InteriorSplitCell { left_child: pending.left_page_id, key: pending.separator.clone() },
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
            let slot_index = interior_page.slot_count();
            self.insert_interior_payload_at(
                &mut interior_page,
                slot_index,
                cell.left_child(),
                cell.key(),
            )?;
        }
        for cell in right_cells {
            let slot_index = right_page.slot_count();
            self.insert_interior_payload_at(
                &mut right_page,
                slot_index,
                cell.left_child(),
                cell.key(),
            )?;
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
            .key()
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
        self.insert_interior_payload_at(
            &mut root_page,
            0,
            pending.left_page_id,
            &pending.separator,
        )?;
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

    const KEY_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=192;
    const VALUE_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=PAGE_SIZE * 3;
    const INLINE_VALUE_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=512;
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
            let value_len = if rng.u8(0..32) == 0 {
                rng.usize(VALUE_LEN_RANGE)
            } else {
                rng.usize(INLINE_VALUE_LEN_RANGE)
            };
            let key = random_bytes(rng, key_len);
            if expected.contains_key(&key) {
                continue;
            }

            let value = random_bytes(rng, value_len);
            return (key, value);
        }
    }

    fn assert_supported_cell(key: &[u8], value: &[u8]) {
        assert!(
            key.len() + value.len() <= u16::MAX as usize,
            "leaf payload should fit the current u16 payload-length field"
        );
        assert!(
            key.len() <= u16::MAX as usize,
            "interior separator should fit the current u16 payload-length field"
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
    fn random_insert_get_simulation_with_oversized_values_reaches_four_levels() {
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
            assert_supported_cell(&key, &value);

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

    fn oversized_key(index: u16) -> Vec<u8> {
        let mut key = vec![0; PAGE_SIZE + 256];
        key[..2].copy_from_slice(&index.to_be_bytes());
        for byte in &mut key[2..] {
            *byte = (index % 251) as u8;
        }
        key
    }

    #[test]
    fn insert_get_supports_oversized_keys_promoted_to_interior_pages() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 256).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let mut expected = BTreeMap::new();

        for index in 0..48 {
            let key = oversized_key(index);
            let value = format!("value-{index}").into_bytes();
            assert_supported_cell(&key, &value);
            cursor.insert(&key, &value).unwrap();
            expected.insert(key, value);
        }

        assert!(tree_height(&cursor).unwrap() >= 2, "large keys should force a root split");
        for (key, value) in &expected {
            let record = cursor.get(key).unwrap().expect("inserted oversized key should exist");
            assert_record_matches(&record, key, value);
        }
        assert_forward_scan_matches(&mut cursor, &expected);
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
