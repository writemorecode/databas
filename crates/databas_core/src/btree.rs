//! Foundational byte-oriented B+-tree cursor.
//!
//! This module is intentionally below table and index interpretation. It works
//! only with raw byte keys and raw byte values stored in `RawLeaf` pages
//! and separator byte keys stored in `RawInterior` pages.

use std::{cell::Cell, cmp::Ordering, fmt, ops::Range, rc::Rc};

use crate::{
    PAGE_SIZE, PageId,
    disk_manager::DiskManager,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    overflow,
    page::{
        self, BoundResult, PageError, RawInterior, RawLeaf, Read, SearchResult, Write,
        format::{KIND_OFFSET, MAX_INLINE_OVERFLOW_PAYLOAD_BYTES, PageKind},
    },
    page_cache::{PageCache, PageWriteGuard, PinGuard},
    page_store::PageStore,
};

const LEAF_CELL_PREFIX_SIZE: usize = 2 + 8 + 2 + 2;
const INTERIOR_CELL_PREFIX_SIZE: usize = 2 + 8 + 8 + 2;

type MaterializedLeafCell = (Box<[u8]>, Box<[u8]>);

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
    fn descend_to_edge_leaf<S: PageStore>(
        self,
        cursor: &TreeCursor<S>,
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

enum RecordStorage<S: PageStore = DiskManager> {
    PageResident { pin: PinGuard<S>, key_range: Range<usize>, value_range: Range<usize> },
    Materialized { key: Box<[u8]>, value: Box<[u8]> },
}

/// Raw record returned by tree reads and cursor iteration.
///
/// Records for cells that fit entirely in their leaf page borrow the page
/// through an internal pin and only expose byte slices during accessor
/// callbacks. Records for cells with overflow payload are materialized into
/// fixed-size heap allocations. Use [`OwnedRecord`] when a stable snapshot is
/// needed across later tree mutations.
pub struct Record<S: PageStore = DiskManager> {
    page_id: PageId,
    slot_index: u16,
    storage: RecordStorage<S>,
}

/// Stable, owned raw record snapshot.
pub struct OwnedRecord {
    key: Box<[u8]>,
    value: Box<[u8]>,
}

/// Borrowed record view valid only for the callback that receives it.
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> RecordView<'a> {
    fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key, value }
    }

    /// Returns the record key bytes.
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    /// Returns the record value bytes.
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Returns the record key and value bytes.
    pub fn key_value(&self) -> (&'a [u8], &'a [u8]) {
        (self.key, self.value)
    }
}

impl<S: PageStore> Record<S> {
    /// Builds a record view from one raw leaf-page slot.
    pub(crate) fn new(
        page_cache: &PageCache<S>,
        page_id: PageId,
        slot_index: u16,
    ) -> StorageResult<Self> {
        let pin = page_cache.fetch_page(page_id)?;
        let (key_len, value_len, first_overflow_page_id, inline_range) = {
            let page = pin.read()?;
            let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
            leaf.cell_payload_parts(slot_index)?
        };

        let storage = match first_overflow_page_id {
            None => {
                if inline_range.len() != key_len + value_len {
                    return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
                }

                let key_start = inline_range.start;
                let value_start = key_start + key_len;
                RecordStorage::PageResident {
                    pin,
                    key_range: key_start..value_start,
                    value_range: value_start..inline_range.end,
                }
            }
            Some(first_overflow_page_id) => {
                let inline_payload = {
                    let page = pin.read()?;
                    page.page()[inline_range].to_vec()
                };
                drop(pin);

                let (key, value) = materialize_leaf_cell(
                    page_cache,
                    page_id,
                    inline_payload,
                    first_overflow_page_id,
                    key_len,
                    value_len,
                )?;
                RecordStorage::Materialized { key, value }
            }
        };

        Ok(Self { page_id, slot_index, storage })
    }

    /// Returns the slot index that this record refers to within its leaf page.
    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }

    /// Executes `f` with a borrowed view of this record.
    pub fn with_view<R>(&self, f: impl FnOnce(RecordView<'_>) -> R) -> StorageResult<R> {
        match &self.storage {
            RecordStorage::PageResident { pin, key_range, value_range } => {
                let page = pin.read()?;
                let key = &page.page()[key_range.clone()];
                let value = &page.page()[value_range.clone()];
                Ok(f(RecordView::new(key, value)))
            }
            RecordStorage::Materialized { key, value } => {
                Ok(f(RecordView::new(key.as_ref(), value.as_ref())))
            }
        }
    }

    /// Returns a stable, owned snapshot of this record.
    pub fn to_owned_record(&self) -> StorageResult<OwnedRecord> {
        self.with_key_value(|key, value| OwnedRecord::new(key.into(), value.into()))
    }

    /// Executes `f` with a borrowed view of the record key.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| f(record.key()))
    }

    /// Executes `f` with a borrowed view of the record value.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| f(record.value()))
    }

    /// Executes `f` with borrowed views of the key and value.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| {
            let (key, value) = record.key_value();
            f(key, value)
        })
    }
}

impl OwnedRecord {
    fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        Self { key, value }
    }

    /// Executes `f` with a borrowed view of the record key.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.key)
    }

    /// Executes `f` with a borrowed view of the record value.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.value)
    }

    /// Executes `f` with borrowed views of the key and value.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> R {
        f(&self.key, &self.value)
    }
}

impl fmt::Debug for OwnedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedRecord")
            .field("key_len", &self.key.len())
            .field("value_len", &self.value.len())
            .finish()
    }
}

impl<S: PageStore> fmt::Debug for Record<S> {
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
pub struct TreeCursor<S: PageStore = DiskManager> {
    page_cache: PageCache<S>,
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

/// Child pointer plus the maximum key reachable through that child.
#[derive(Debug, Clone)]
struct ChildEntry {
    page_id: PageId,
    max_key: Option<Vec<u8>>,
}

/// Fully prepared interior cell payload for an atomic page rewrite.
#[derive(Debug, Clone)]
struct PreparedInteriorCell {
    left_child: PageId,
    key_len: usize,
    first_overflow_page_id: Option<PageId>,
    inline_payload: Vec<u8>,
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

fn materialize_payload<S: PageStore>(
    page_cache: &PageCache<S>,
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

fn materialize_leaf_cell<S: PageStore>(
    page_cache: &PageCache<S>,
    page_id: PageId,
    inline_payload: Vec<u8>,
    first_overflow_page_id: PageId,
    key_len: usize,
    value_len: usize,
) -> StorageResult<MaterializedLeafCell> {
    let mut payload = materialize_payload(
        page_cache,
        page_id,
        inline_payload,
        Some(first_overflow_page_id),
        key_len + value_len,
    )?;
    if payload.len() < key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    let value = payload.split_off(key_len);
    Ok((payload.into_boxed_slice(), value.into_boxed_slice()))
}

fn read_leaf_cell<S: PageStore>(
    page_cache: &PageCache<S>,
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

fn read_interior_cell<S: PageStore>(
    page_cache: &PageCache<S>,
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

fn compare_key_prefix(
    page_id: PageId,
    inline_key: &[u8],
    key_len: usize,
    key: &[u8],
) -> StorageResult<Option<Ordering>> {
    if inline_key.len() > key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    if key.len() <= inline_key.len() {
        let ordering = inline_key[..key.len()].cmp(key);
        if ordering != Ordering::Equal {
            return Ok(Some(ordering));
        }
        return Ok(Some(key_len.cmp(&key.len())));
    }

    let ordering = inline_key.cmp(&key[..inline_key.len()]);
    if ordering != Ordering::Equal {
        return Ok(Some(ordering));
    }

    if inline_key.len() == key_len {
        return Ok(Some(Ordering::Less));
    }

    Ok(None)
}

fn materialize_overflow_key<S: PageStore>(
    page_cache: &PageCache<S>,
    page_id: PageId,
    mut inline_key: Vec<u8>,
    first_overflow_page_id: Option<PageId>,
    key_len: usize,
) -> StorageResult<Vec<u8>> {
    let first_overflow_page_id = first_overflow_page_id
        .ok_or_else(|| cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds))?;
    inline_key.extend(overflow::read_chain_prefix(
        page_cache,
        first_overflow_page_id,
        key_len - inline_key.len(),
    )?);
    if inline_key.len() != key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }
    Ok(inline_key)
}

impl<S: PageStore> TreeCursor<S> {
    /// Creates a cursor anchored at `root_page_id` in page-level state.
    pub(crate) fn new(page_cache: PageCache<S>, root_page_id: PageId) -> Self {
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
    fn record_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<Record<S>> {
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

    fn compare_leaf_key(
        &self,
        page_id: PageId,
        slot_index: u16,
        key: &[u8],
    ) -> StorageResult<Ordering> {
        let (inline_key, first_overflow_page_id, key_len) = {
            let pin = self.page_cache.fetch_page(page_id)?;
            let page = pin.read()?;
            let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
            let (key_len, _, first_overflow_page_id, inline_range) =
                leaf.cell_payload_parts(slot_index)?;
            let inline_key_len = key_len.min(inline_range.len());
            let inline_key = &page.page()[inline_range.start..inline_range.start + inline_key_len];
            if let Some(ordering) = compare_key_prefix(page_id, inline_key, key_len, key)? {
                return Ok(ordering);
            }
            (inline_key.to_vec(), first_overflow_page_id, key_len)
        };

        let materialized_key = materialize_overflow_key(
            &self.page_cache,
            page_id,
            inline_key,
            first_overflow_page_id,
            key_len,
        )?;
        Ok(materialized_key.as_slice().cmp(key))
    }

    fn compare_interior_key(
        &self,
        page_id: PageId,
        slot_index: u16,
        key: &[u8],
    ) -> StorageResult<Ordering> {
        let (inline_key, first_overflow_page_id, key_len) = {
            let pin = self.page_cache.fetch_page(page_id)?;
            let page = pin.read()?;
            let interior = RawInterior::<Read<'_>>::open(page.page())?;
            let (_, key_len, first_overflow_page_id, inline_range) =
                interior.cell_payload_parts(slot_index)?;
            let inline_key_len = key_len.min(inline_range.len());
            let inline_key = &page.page()[inline_range.start..inline_range.start + inline_key_len];
            if let Some(ordering) = compare_key_prefix(page_id, inline_key, key_len, key)? {
                return Ok(ordering);
            }
            (inline_key.to_vec(), first_overflow_page_id, key_len)
        };

        let materialized_key = materialize_overflow_key(
            &self.page_cache,
            page_id,
            inline_key,
            first_overflow_page_id,
            key_len,
        )?;
        Ok(materialized_key.as_slice().cmp(key))
    }

    fn read_interior_left_child(&self, page_id: PageId, slot_index: u16) -> StorageResult<PageId> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
        Ok(left_child)
    }

    fn search_leaf_slot(&self, page_id: PageId, key: &[u8]) -> StorageResult<SearchResult> {
        let mut low: u16 = 0;
        let mut high = self.raw_leaf_slot_count(page_id)?;

        while low < high {
            let mid = low + (high - low) / 2;
            match self.compare_leaf_key(page_id, mid, key)? {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Ok(SearchResult::Found(mid)),
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
            if self.compare_leaf_key(page_id, mid, key)? == Ordering::Less {
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
            if self.compare_interior_key(page_id, mid, key)? == Ordering::Less {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    fn interior_child_for_key(&self, page_id: PageId, key: &[u8]) -> StorageResult<PageId> {
        match self.lower_bound_interior_slot(page_id, key)? {
            BoundResult::At(slot_index) => self.read_interior_left_child(page_id, slot_index),
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
                                let child_page_id =
                                    self.read_interior_left_child(page_id, slot_index)?;
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
    ) -> StorageResult<Option<Record<S>>> {
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
    fn step_record(&mut self, direction: ScanDirection) -> StorageResult<Option<Record<S>>> {
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
    pub fn get(&mut self, key: &[u8]) -> StorageResult<Option<Record<S>>> {
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

    /// Searches the raw tree for `key` and returns a stable owned record snapshot.
    pub fn get_owned(&mut self, key: &[u8]) -> StorageResult<Option<OwnedRecord>> {
        self.get(key)?.map(|record| record.to_owned_record()).transpose()
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

    fn missing_child_max_key_error(page_id: PageId) -> StorageError {
        StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::InteriorPage,
            page_id: Some(page_id),
            kind: CorruptionKind::CellLengthOutOfBounds,
        })
    }

    /// Returns the previous and next sibling pointers for a leaf page.
    fn read_leaf_page_links(
        &self,
        page_id: PageId,
    ) -> StorageResult<(Option<PageId>, Option<PageId>)> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        Ok((leaf.prev_page_id(), leaf.next_page_id()))
    }

    /// Returns the previous and next sibling pointers for an interior page.
    fn read_interior_page_links(
        &self,
        page_id: PageId,
    ) -> StorageResult<(Option<PageId>, Option<PageId>)> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        Ok((interior.prev_page_id(), interior.next_page_id()))
    }

    /// Materializes all leaf cells in slot order for rebalance planning.
    fn read_leaf_cells(&self, page_id: PageId) -> StorageResult<Vec<LeafSplitCell>> {
        let slot_count = self.raw_leaf_slot_count(page_id)?;
        let mut cells = Vec::with_capacity(slot_count as usize);
        for slot_index in 0..slot_count {
            let (key, value) = read_leaf_cell(&self.page_cache, page_id, slot_index)?;
            cells.push(LeafSplitCell { key, value });
        }
        Ok(cells)
    }

    /// Returns the largest key in a leaf page, or `None` when the page is empty.
    fn read_leaf_max_key(&self, page_id: PageId) -> StorageResult<Option<Vec<u8>>> {
        let slot_count = self.raw_leaf_slot_count(page_id)?;
        if slot_count == 0 {
            return Ok(None);
        }

        read_leaf_cell(&self.page_cache, page_id, slot_count - 1).map(|(key, _)| Some(key))
    }

    /// Reads child page ids from an interior page in logical left-to-right order.
    fn read_interior_child_page_ids(&self, page_id: PageId) -> StorageResult<Vec<PageId>> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        let mut children = Vec::with_capacity(interior.slot_count() as usize + 1);
        for slot_index in 0..interior.slot_count() {
            let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
            children.push(left_child);
        }
        children.push(interior.rightmost_child());
        Ok(children)
    }

    /// Returns whether `child_page_id` is still linked from `parent_page_id`.
    fn interior_page_has_child(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> StorageResult<bool> {
        Ok(self.read_interior_child_page_ids(parent_page_id)?.contains(&child_page_id))
    }

    /// Returns the largest key reachable from the subtree rooted at `page_id`.
    fn subtree_max_key(&self, page_id: PageId) -> StorageResult<Option<Vec<u8>>> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let next = {
            let page = pin.read()?;
            match read_page_kind(page.page(), page_id)? {
                PageKind::RawLeaf => {
                    drop(page);
                    drop(pin);
                    return self.read_leaf_max_key(page_id);
                }
                PageKind::RawInterior => {
                    let interior = RawInterior::<Read<'_>>::open(page.page())?;
                    interior.rightmost_child()
                }
            }
        };
        drop(pin);
        self.subtree_max_key(next)
    }

    /// Collects ordered child entries for an interior page with refreshed max keys.
    fn read_interior_child_entries(&self, page_id: PageId) -> StorageResult<Vec<ChildEntry>> {
        let child_page_ids = self.read_interior_child_page_ids(page_id)?;
        let mut children = Vec::with_capacity(child_page_ids.len());
        for child_page_id in child_page_ids {
            children.push(ChildEntry {
                page_id: child_page_id,
                max_key: self.subtree_max_key(child_page_id)?,
            });
        }
        Ok(children)
    }

    /// Locates a child page within its parent's ordered child list.
    fn child_index_in_parent(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> StorageResult<usize> {
        self.read_interior_child_page_ids(parent_page_id)?
            .iter()
            .position(|&candidate| candidate == child_page_id)
            .ok_or({
                StorageError::Corruption(CorruptionError {
                    component: CorruptionComponent::InteriorPage,
                    page_id: Some(parent_page_id),
                    kind: CorruptionKind::CellLengthOutOfBounds,
                })
            })
    }

    /// Reinitializes a leaf page with `cells` and updated sibling links.
    fn rewrite_leaf_page(
        &self,
        page_id: PageId,
        cells: &[LeafSplitCell],
        prev_page_id: Option<PageId>,
        next_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        let mut leaf = RawLeaf::<Write<'_>>::initialize(guard.page_mut());
        leaf.set_prev_page_id(prev_page_id);
        leaf.set_next_page_id(next_page_id);
        for cell in cells {
            let slot_index = leaf.slot_count();
            self.insert_leaf_payload_at(&mut leaf, slot_index, cell.key(), cell.value())?;
        }
        Ok(())
    }

    /// Reinitializes an interior page from ordered `children` and sibling links.
    fn rewrite_interior_page(
        &self,
        page_id: PageId,
        children: &[ChildEntry],
        prev_page_id: Option<PageId>,
        next_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let rightmost_child = children.last().ok_or({
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::InteriorPage,
                page_id: Some(page_id),
                kind: CorruptionKind::CellLengthOutOfBounds,
            })
        })?;
        let mut used_bytes = PageKind::RawInterior.header_size()
            + (children.len() - 1) * page::format::SLOT_ENTRY_SIZE;
        for child in &children[..children.len() - 1] {
            let key = child
                .max_key
                .as_deref()
                .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;
            used_bytes += self.interior_cell_local_size(key)?;
        }
        if used_bytes > page::format::USABLE_SPACE_END {
            return Err(PageError::PageFull {
                needed: used_bytes,
                available: page::format::USABLE_SPACE_END,
            }
            .into());
        }

        let mut prepared_cells = Vec::with_capacity(children.len() - 1);
        for child in &children[..children.len() - 1] {
            let key = child
                .max_key
                .as_deref()
                .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;
            let (first_overflow_page_id, inline_payload) = self.payload_storage_parts(key)?;
            prepared_cells.push(PreparedInteriorCell {
                left_child: child.page_id,
                key_len: key.len(),
                first_overflow_page_id,
                inline_payload,
            });
        }

        let mut page_image = [0; PAGE_SIZE];
        {
            let mut interior = RawInterior::<Write<'_>>::initialize_with_rightmost(
                &mut page_image,
                rightmost_child.page_id,
            );
            interior.set_prev_page_id(prev_page_id);
            interior.set_next_page_id(next_page_id);
            for cell in &prepared_cells {
                let slot_index = interior.slot_count();
                interior.insert_payload_at(
                    slot_index,
                    cell.left_child,
                    cell.key_len,
                    cell.first_overflow_page_id,
                    &cell.inline_payload,
                )?;
            }
        }

        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        *guard.page_mut() = page_image;
        Ok(())
    }

    /// Returns whether an interior page already matches refreshed child maxima.
    fn interior_page_matches_children(
        &self,
        page_id: PageId,
        children: &[ChildEntry],
    ) -> StorageResult<bool> {
        let current_children = self.read_interior_child_page_ids(page_id)?;
        if current_children.len() != children.len()
            || current_children
                .iter()
                .zip(children)
                .any(|(&current, desired)| current != desired.page_id)
        {
            return Ok(false);
        }

        for (slot_index, child) in children[..children.len() - 1].iter().enumerate() {
            let expected_key = child
                .max_key
                .as_deref()
                .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;
            let (_, actual_key) = read_interior_cell(&self.page_cache, page_id, slot_index as u16)?;
            if actual_key != expected_key {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Refreshes one interior page only when one of its separators changed.
    fn refresh_interior_page_separators(&self, page_id: PageId) -> StorageResult<()> {
        let children = self.read_interior_child_entries(page_id)?;
        if self.interior_page_matches_children(page_id, &children)? {
            return Ok(());
        }

        let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
        self.rewrite_interior_page(page_id, &children, prev_page_id, next_page_id)
    }

    /// Returns whether a leaf page is below the minimum occupancy target.
    fn leaf_page_underoccupied(&self, page_id: PageId) -> StorageResult<bool> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = RawLeaf::<Read<'_>>::open(page.page())?;
        Ok(leaf.is_underoccupied()?)
    }

    /// Returns whether an interior page is below the minimum occupancy target.
    fn interior_page_underoccupied(&self, page_id: PageId) -> StorageResult<bool> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = RawInterior::<Read<'_>>::open(page.page())?;
        Ok(interior.is_underoccupied()?)
    }

    /// Returns whether a leaf rebuilt from `cells` would be underoccupied.
    fn leaf_cells_underoccupied(cells: &[LeafSplitCell]) -> bool {
        let occupied_variable_bytes = cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let usable_variable_bytes =
            page::format::USABLE_SPACE_END - PageKind::RawLeaf.header_size();
        occupied_variable_bytes * 2 < usable_variable_bytes
    }

    /// Chooses a split index that keeps both leaf siblings fit and occupied.
    fn choose_leaf_rebalance_split(cells: &[LeafSplitCell]) -> Option<usize> {
        let total_cell_len = cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let mut left_cell_len = 0;
        let mut best = None;

        for split_index in 1..cells.len() {
            left_cell_len += cells[split_index - 1].encoded_size();
            if !Self::leaf_cells_fit(&cells[..split_index])
                || !Self::leaf_cells_fit(&cells[split_index..])
                || Self::leaf_cells_underoccupied(&cells[..split_index])
                || Self::leaf_cells_underoccupied(&cells[split_index..])
            {
                continue;
            }

            let right_cell_len = total_cell_len - left_cell_len;
            let imbalance = left_cell_len.abs_diff(right_cell_len);
            if best.is_none_or(|(best_imbalance, _)| imbalance < best_imbalance) {
                best = Some((imbalance, split_index));
            }
        }

        best.map(|(_, split_index)| split_index)
    }

    /// Chooses a split index that keeps both leaf siblings within page capacity.
    fn choose_leaf_fitting_split(cells: &[LeafSplitCell]) -> Option<usize> {
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
            if best.is_none_or(|(best_imbalance, _)| imbalance < best_imbalance) {
                best = Some((imbalance, split_index));
            }
        }

        best.map(|(_, split_index)| split_index)
    }

    /// Returns whether `children` can be encoded in one interior page.
    fn interior_children_fit(children: &[ChildEntry]) -> bool {
        if children.is_empty() {
            return false;
        }
        let mut cell_bytes = 0;
        for child in &children[..children.len() - 1] {
            let Some(key) = child.max_key.as_ref() else {
                return false;
            };
            cell_bytes += INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len());
        }
        let used_bytes = PageKind::RawInterior.header_size()
            + (children.len() - 1) * page::format::SLOT_ENTRY_SIZE
            + cell_bytes;
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Returns whether an interior page rebuilt from `children` would be underoccupied.
    fn interior_children_underoccupied(children: &[ChildEntry]) -> bool {
        let mut cell_bytes = 0;
        for child in &children[..children.len().saturating_sub(1)] {
            let Some(key) = child.max_key.as_ref() else {
                return true;
            };
            cell_bytes += INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len());
        }
        let occupied_variable_bytes =
            children.len().saturating_sub(1) * page::format::SLOT_ENTRY_SIZE + cell_bytes;
        let usable_variable_bytes =
            page::format::USABLE_SPACE_END - PageKind::RawInterior.header_size();
        occupied_variable_bytes * 2 < usable_variable_bytes
    }

    /// Chooses a split index that keeps both interior siblings fit and occupied.
    fn choose_interior_rebalance_split(children: &[ChildEntry]) -> Option<usize> {
        let mut best = None;
        for split_index in 1..children.len() {
            let left = &children[..split_index];
            let right = &children[split_index..];
            if !Self::interior_children_fit(left)
                || !Self::interior_children_fit(right)
                || Self::interior_children_underoccupied(left)
                || Self::interior_children_underoccupied(right)
            {
                continue;
            }

            let imbalance = split_index.abs_diff(children.len() - split_index);
            if best.is_none_or(|(best_imbalance, _)| imbalance < best_imbalance) {
                best = Some((imbalance, split_index));
            }
        }
        best.map(|(_, split_index)| split_index)
    }

    /// Chooses a split index that keeps both interior siblings within page capacity.
    fn choose_interior_fitting_split(children: &[ChildEntry]) -> Option<usize> {
        let mut best = None;
        for split_index in 1..children.len() {
            let left = &children[..split_index];
            let right = &children[split_index..];
            if !Self::interior_children_fit(left) || !Self::interior_children_fit(right) {
                continue;
            }

            let imbalance = split_index.abs_diff(children.len() - split_index);
            if best.is_none_or(|(best_imbalance, _)| imbalance < best_imbalance) {
                best = Some((imbalance, split_index));
            }
        }
        best.map(|(_, split_index)| split_index)
    }

    /// Updates the previous-sibling link for a leaf page.
    fn set_leaf_prev_page_id(
        &self,
        page_id: PageId,
        prev_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        let mut leaf = RawLeaf::<Write<'_>>::open(guard.page_mut())?;
        leaf.set_prev_page_id(prev_page_id);
        Ok(())
    }

    /// Updates the previous-sibling link for an interior page.
    fn set_interior_prev_page_id(
        &self,
        page_id: PageId,
        prev_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        let mut interior = RawInterior::<Write<'_>>::open(guard.page_mut())?;
        interior.set_prev_page_id(prev_page_id);
        Ok(())
    }

    /// Removes `child_page_id` from `parent_page_id` and rewrites the parent.
    fn remove_child_from_parent(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> StorageResult<()> {
        let mut children = self.read_interior_child_entries(parent_page_id)?;
        let child_index =
            children.iter().position(|child| child.page_id == child_page_id).ok_or({
                StorageError::Corruption(CorruptionError {
                    component: CorruptionComponent::InteriorPage,
                    page_id: Some(parent_page_id),
                    kind: CorruptionKind::CellLengthOutOfBounds,
                })
            })?;
        children.remove(child_index);
        let (prev_page_id, next_page_id) = self.read_interior_page_links(parent_page_id)?;
        self.rewrite_interior_page(parent_page_id, &children, prev_page_id, next_page_id)
    }

    /// Rewrites adjacent leaf siblings after redistributing their combined cells.
    fn redistribute_leaf_pair(
        &self,
        left_page_id: PageId,
        right_page_id: PageId,
        cells: &[LeafSplitCell],
        split_index: usize,
    ) -> StorageResult<()> {
        let (left_prev_page_id, _) = self.read_leaf_page_links(left_page_id)?;
        let (_, right_next_page_id) = self.read_leaf_page_links(right_page_id)?;
        self.rewrite_leaf_page(
            left_page_id,
            &cells[..split_index],
            left_prev_page_id,
            Some(right_page_id),
        )?;
        self.rewrite_leaf_page(
            right_page_id,
            &cells[split_index..],
            Some(left_page_id),
            right_next_page_id,
        )
    }

    /// Merges two adjacent leaf pages into `survivor_page_id`.
    fn merge_leaf_pages(
        &self,
        survivor_page_id: PageId,
        removed_page_id: PageId,
        cells: &[LeafSplitCell],
    ) -> StorageResult<()> {
        let (survivor_prev_page_id, _) = self.read_leaf_page_links(survivor_page_id)?;
        let (_, removed_next_page_id) = self.read_leaf_page_links(removed_page_id)?;
        self.rewrite_leaf_page(
            survivor_page_id,
            cells,
            survivor_prev_page_id,
            removed_next_page_id,
        )?;
        if let Some(next_page_id) = removed_next_page_id {
            self.set_leaf_prev_page_id(next_page_id, Some(survivor_page_id))?;
        }
        Ok(())
    }

    /// Rebalances an underoccupied leaf against siblings.
    ///
    /// Returns `true` when a merge removed one child from the parent page.
    fn rebalance_leaf_page(
        &mut self,
        leaf_page_id: PageId,
        parent_page_id: PageId,
    ) -> StorageResult<bool> {
        let child_index = self.child_index_in_parent(parent_page_id, leaf_page_id)?;
        let parent_children = self.read_interior_child_page_ids(parent_page_id)?;

        if child_index > 0 {
            let left_page_id = parent_children[child_index - 1];
            let mut cells = self.read_leaf_cells(left_page_id)?;
            cells.extend(self.read_leaf_cells(leaf_page_id)?);
            if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
                self.redistribute_leaf_pair(left_page_id, leaf_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        if child_index + 1 < parent_children.len() {
            let right_page_id = parent_children[child_index + 1];
            let mut cells = self.read_leaf_cells(leaf_page_id)?;
            cells.extend(self.read_leaf_cells(right_page_id)?);
            if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
                self.redistribute_leaf_pair(leaf_page_id, right_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        if child_index > 0 {
            let left_page_id = parent_children[child_index - 1];
            let mut cells = self.read_leaf_cells(left_page_id)?;
            cells.extend(self.read_leaf_cells(leaf_page_id)?);
            if Self::leaf_cells_fit(&cells) {
                self.merge_leaf_pages(left_page_id, leaf_page_id, &cells)?;
                self.remove_child_from_parent(parent_page_id, leaf_page_id)?;
                self.set_page_state(left_page_id);
                return Ok(true);
            }
            if let Some(split_index) = Self::choose_leaf_fitting_split(&cells) {
                self.redistribute_leaf_pair(left_page_id, leaf_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        if child_index + 1 < parent_children.len() {
            let right_page_id = parent_children[child_index + 1];
            let mut cells = self.read_leaf_cells(leaf_page_id)?;
            cells.extend(self.read_leaf_cells(right_page_id)?);
            if Self::leaf_cells_fit(&cells) {
                self.merge_leaf_pages(leaf_page_id, right_page_id, &cells)?;
                self.remove_child_from_parent(parent_page_id, right_page_id)?;
                self.set_page_state(leaf_page_id);
                return Ok(true);
            }
            if let Some(split_index) = Self::choose_leaf_fitting_split(&cells) {
                self.redistribute_leaf_pair(leaf_page_id, right_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        Ok(false)
    }

    /// Rewrites adjacent interior siblings after redistributing their children.
    fn redistribute_interior_pair(
        &self,
        left_page_id: PageId,
        right_page_id: PageId,
        children: &[ChildEntry],
        split_index: usize,
    ) -> StorageResult<()> {
        let (left_prev_page_id, _) = self.read_interior_page_links(left_page_id)?;
        let (_, right_next_page_id) = self.read_interior_page_links(right_page_id)?;
        self.rewrite_interior_page(
            left_page_id,
            &children[..split_index],
            left_prev_page_id,
            Some(right_page_id),
        )?;
        self.rewrite_interior_page(
            right_page_id,
            &children[split_index..],
            Some(left_page_id),
            right_next_page_id,
        )
    }

    /// Merges two adjacent interior pages into `survivor_page_id`.
    fn merge_interior_pages(
        &self,
        survivor_page_id: PageId,
        removed_page_id: PageId,
        children: &[ChildEntry],
    ) -> StorageResult<()> {
        let (survivor_prev_page_id, _) = self.read_interior_page_links(survivor_page_id)?;
        let (_, removed_next_page_id) = self.read_interior_page_links(removed_page_id)?;
        self.rewrite_interior_page(
            survivor_page_id,
            children,
            survivor_prev_page_id,
            removed_next_page_id,
        )?;
        if let Some(next_page_id) = removed_next_page_id {
            self.set_interior_prev_page_id(next_page_id, Some(survivor_page_id))?;
        }
        Ok(())
    }

    /// Rebalances an underoccupied interior page against siblings.
    ///
    /// Returns `true` when a merge removed one child from the parent page.
    fn rebalance_interior_page(
        &self,
        interior_page_id: PageId,
        parent_page_id: PageId,
    ) -> StorageResult<bool> {
        let child_index = self.child_index_in_parent(parent_page_id, interior_page_id)?;
        let parent_children = self.read_interior_child_page_ids(parent_page_id)?;

        if child_index > 0 {
            let left_page_id = parent_children[child_index - 1];
            let mut children = self.read_interior_child_entries(left_page_id)?;
            children.extend(self.read_interior_child_entries(interior_page_id)?);
            if let Some(split_index) = Self::choose_interior_rebalance_split(&children) {
                self.redistribute_interior_pair(
                    left_page_id,
                    interior_page_id,
                    &children,
                    split_index,
                )?;
                return Ok(false);
            }
        }

        if child_index + 1 < parent_children.len() {
            let right_page_id = parent_children[child_index + 1];
            let mut children = self.read_interior_child_entries(interior_page_id)?;
            children.extend(self.read_interior_child_entries(right_page_id)?);
            if let Some(split_index) = Self::choose_interior_rebalance_split(&children) {
                self.redistribute_interior_pair(
                    interior_page_id,
                    right_page_id,
                    &children,
                    split_index,
                )?;
                return Ok(false);
            }
        }

        if child_index > 0 {
            let left_page_id = parent_children[child_index - 1];
            let mut children = self.read_interior_child_entries(left_page_id)?;
            children.extend(self.read_interior_child_entries(interior_page_id)?);
            if Self::interior_children_fit(&children) {
                self.merge_interior_pages(left_page_id, interior_page_id, &children)?;
                self.remove_child_from_parent(parent_page_id, interior_page_id)?;
                return Ok(true);
            }
            if let Some(split_index) = Self::choose_interior_fitting_split(&children) {
                self.redistribute_interior_pair(
                    left_page_id,
                    interior_page_id,
                    &children,
                    split_index,
                )?;
                return Ok(false);
            }
        }

        if child_index + 1 < parent_children.len() {
            let right_page_id = parent_children[child_index + 1];
            let mut children = self.read_interior_child_entries(interior_page_id)?;
            children.extend(self.read_interior_child_entries(right_page_id)?);
            if Self::interior_children_fit(&children) {
                self.merge_interior_pages(interior_page_id, right_page_id, &children)?;
                self.remove_child_from_parent(parent_page_id, right_page_id)?;
                return Ok(true);
            }
            if let Some(split_index) = Self::choose_interior_fitting_split(&children) {
                self.redistribute_interior_pair(
                    interior_page_id,
                    right_page_id,
                    &children,
                    split_index,
                )?;
                return Ok(false);
            }
        }

        Ok(false)
    }

    /// Replaces an empty interior root with its sole child.
    fn shrink_root_if_empty(&mut self) -> StorageResult<()> {
        let root_page_id = self.root_page_id();
        let pin = self.page_cache.fetch_page(root_page_id)?;
        let child_page_id = {
            let page = pin.read()?;
            match read_page_kind(page.page(), root_page_id)? {
                PageKind::RawLeaf => return Ok(()),
                PageKind::RawInterior => {
                    let interior = RawInterior::<Read<'_>>::open(page.page())?;
                    if interior.slot_count() > 0 {
                        return Ok(());
                    }
                    interior.rightmost_child()
                }
            }
        };
        self.root_page_id.set(child_page_id);
        self.set_page_state(child_page_id);
        Ok(())
    }

    /// Refreshes separators along the still-reachable delete path.
    fn refresh_path_separators(&self, tree_path: &[PathFrame]) -> StorageResult<()> {
        if tree_path.is_empty() {
            return Ok(());
        }

        let mut reachable = Vec::with_capacity(tree_path.len());
        for (depth, frame) in tree_path.iter().enumerate() {
            let is_reachable = if depth == 0 {
                frame.page_id == self.root_page_id()
            } else {
                reachable[depth - 1]
                    && self.interior_page_has_child(tree_path[depth - 1].page_id, frame.page_id)?
            };
            reachable.push(is_reachable);
        }

        for (frame, is_reachable) in tree_path.iter().zip(reachable).rev() {
            if is_reachable {
                self.refresh_interior_page_separators(frame.page_id)?;
            }
        }

        Ok(())
    }

    /// Runs post-delete rebalancing from the modified leaf toward the root.
    fn rebalance_after_leaf_delete(
        &mut self,
        leaf_page_id: PageId,
        tree_path: &[PathFrame],
    ) -> StorageResult<()> {
        if tree_path.is_empty() {
            return Ok(());
        }
        if !self.leaf_page_underoccupied(leaf_page_id)? {
            return Ok(());
        }

        let mut depth = tree_path.len() - 1;
        let parent_page_id = tree_path[depth].page_id;
        if !self.rebalance_leaf_page(leaf_page_id, parent_page_id)? {
            return Ok(());
        }

        loop {
            let page_id = tree_path[depth].page_id;
            if page_id == self.root_page_id() {
                self.shrink_root_if_empty()?;
                return Ok(());
            }
            if !self.interior_page_underoccupied(page_id)? {
                return Ok(());
            }
            if depth == 0 {
                return Ok(());
            }

            let parent_page_id = tree_path[depth - 1].page_id;
            if !self.rebalance_interior_page(page_id, parent_page_id)? {
                return Ok(());
            }
            depth -= 1;
        }
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
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> StorageResult<Record<S>> {
        let _ = &self.page_cache;
        let _ = (key, value);
        todo!("tree update is not implemented yet")
    }

    /// Deletes the record identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        {
            let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_guard = leaf_pin_guard.write()?;
            let mut page = RawLeaf::<Write<'_>>::open(leaf_guard.page_mut())?;
            page.delete(key)?;
        }

        self.set_page_state(leaf_page_id);
        self.rebalance_after_leaf_delete(leaf_page_id, &tree_path)?;
        self.refresh_path_separators(&tree_path)?;
        self.shrink_root_if_empty()?;
        Ok(())
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
    pub fn current(&self) -> StorageResult<Option<Record<S>>> {
        match self.state {
            CursorState::Positioned { page_id, slot_index } => {
                self.record_at(page_id, slot_index).map(Some)
            }
            CursorState::Page { .. } | CursorState::Exhausted => Ok(None),
        }
    }

    /// Reads the currently selected record as a stable owned snapshot, if any.
    pub fn current_owned(&self) -> StorageResult<Option<OwnedRecord>> {
        self.current()?.map(|record| record.to_owned_record()).transpose()
    }

    /// Advances to the next record in sorted key order.
    pub fn next_record(&mut self) -> StorageResult<Option<Record<S>>> {
        self.step_record(ScanDirection::Forward)
    }

    /// Advances to the next record and returns a stable owned snapshot.
    pub fn next_owned_record(&mut self) -> StorageResult<Option<OwnedRecord>> {
        self.next_record()?.map(|record| record.to_owned_record()).transpose()
    }

    /// Moves to the previous record in sorted key order.
    pub fn prev_record(&mut self) -> StorageResult<Option<Record<S>>> {
        self.step_record(ScanDirection::Backward)
    }

    /// Moves to the previous record and returns a stable owned snapshot.
    pub fn prev_owned_record(&mut self) -> StorageResult<Option<OwnedRecord>> {
        self.prev_record()?.map(|record| record.to_owned_record()).transpose()
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
                    if self.compare_interior_key(
                        parent_frame.page_id,
                        slot_index,
                        &pending.separator,
                    )? == Ordering::Equal
                    {
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
pub(crate) fn initialize_empty_root<S: PageStore>(
    page_cache: &PageCache<S>,
) -> StorageResult<PageId> {
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = RawLeaf::<Write<'_>>::initialize(page.page_mut());
    Ok(page_id)
}

/// Verifies that `root_page_id` names a raw leaf or raw interior page.
pub(crate) fn validate_root_page(
    page_cache: &PageCache<impl PageStore>,
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
    use crate::error::LimitExceededError;

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

    #[test]
    fn random_insert_delete_simulation_empties_tree_after_random_delete_order() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 256).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let mut rng = Rng::with_seed(0x9dd0_c312_741f_2026);
        let mut expected = BTreeMap::new();

        const INSERT_COUNT: usize = 200;
        for _ in 0..INSERT_COUNT {
            let (key, value) = random_unique_cell(&mut rng, &expected);
            assert_supported_cell(&key, &value);
            cursor.insert(&key, &value).unwrap();
            expected.insert(key, value);
        }
        assert!(!expected.is_empty(), "simulation should create at least one record");

        let mut delete_order: Vec<Vec<u8>> = expected.keys().cloned().collect();
        let sorted_order = delete_order.clone();
        rng.shuffle(&mut delete_order);
        while delete_order == sorted_order {
            rng.shuffle(&mut delete_order);
        }

        for key in &delete_order {
            cursor.delete(key).unwrap();
        }

        assert!(!cursor.seek_to_first().unwrap(), "tree should be empty after deleting all keys");
        assert!(cursor.current().unwrap().is_none(), "empty tree cursor should have no current");
        for key in expected.keys() {
            assert!(cursor.get(key).unwrap().is_none(), "deleted key should not be found");
        }
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

    #[test]
    fn failed_interior_rewrite_leaves_page_unchanged() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 16).unwrap();
        let (page_id, pin) = page_cache.new_page().unwrap();
        {
            let mut guard = pin.write().unwrap();
            let mut interior =
                RawInterior::<Write<'_>>::initialize_with_rightmost(guard.page_mut(), 2);
            interior.insert(b"stable", 0).unwrap();
        }
        drop(pin);

        let cursor = TreeCursor::new(page_cache.clone(), page_id);
        let original_page = {
            let pin = page_cache.fetch_page(page_id).unwrap();
            let page = pin.read().unwrap();
            *page.page()
        };
        let children: Vec<_> = (0..16)
            .map(|index| ChildEntry {
                page_id: 100 + index,
                max_key: Some(vec![index as u8; PAGE_SIZE]),
            })
            .collect();

        let result = cursor.rewrite_interior_page(page_id, &children, None, None);

        assert!(matches!(
            result,
            Err(StorageError::LimitExceeded(LimitExceededError::PageFull { .. }))
        ));
        let rewritten_page = {
            let pin = page_cache.fetch_page(page_id).unwrap();
            let page = pin.read().unwrap();
            *page.page()
        };
        assert_eq!(rewritten_page, original_page);
    }

    #[test]
    fn unchanged_path_separator_refresh_does_not_grow_file() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 256).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let mut expected = BTreeMap::new();

        for index in 0..96 {
            let key = oversized_key(index);
            let value = format!("value-{index}").into_bytes();
            cursor.insert(&key, &value).unwrap();
            expected.insert(key, value);
        }

        let key = expected.keys().next().expect("test setup should create records");
        let (_, tree_path) = cursor.leaf_page_path_for_key(key).unwrap();
        assert!(!tree_path.is_empty(), "large keys should force interior separators");
        cursor.refresh_path_separators(&tree_path).unwrap();
        let file_len_before = file.path().metadata().unwrap().len();

        cursor.refresh_path_separators(&tree_path).unwrap();

        let file_len_after = file.path().metadata().unwrap().len();
        assert_eq!(file_len_after, file_len_before);
    }

    #[test]
    fn inline_record_is_page_resident() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 4).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);

        cursor.insert(b"alpha", b"value").unwrap();

        let record = cursor.get(b"alpha").unwrap().expect("inline record should exist");
        assert!(matches!(record.storage, RecordStorage::PageResident { .. }));
        assert_record_matches(&record, b"alpha", b"value");
    }

    #[test]
    fn overflow_record_is_materialized() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 4).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let value = vec![42; PAGE_SIZE];

        cursor.insert(b"alpha", &value).unwrap();

        let record = cursor.get(b"alpha").unwrap().expect("overflow record should exist");
        assert!(matches!(record.storage, RecordStorage::Materialized { .. }));
        assert_record_matches(&record, b"alpha", &value);
    }

    #[test]
    fn inline_record_converts_to_owned_snapshot() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 4).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);

        cursor.insert(b"alpha", b"value").unwrap();

        let owned = cursor
            .get(b"alpha")
            .unwrap()
            .expect("inline record should exist")
            .to_owned_record()
            .unwrap();
        assert_owned_record_matches(&owned, b"alpha", b"value");
    }

    #[test]
    fn binary_search_supports_inline_key_with_overflow_value() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 8).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let value = vec![7; PAGE_SIZE];

        cursor.insert(b"alpha", b"small").unwrap();
        cursor.insert(b"bravo", &value).unwrap();
        cursor.insert(b"charlie", b"small").unwrap();

        let record = cursor.get(b"bravo").unwrap().expect("overflow value key should exist");
        assert_record_matches(&record, b"bravo", &value);
        assert!(cursor.get(b"between").unwrap().is_none());
    }

    #[test]
    fn binary_search_supports_oversized_key_with_overflow_value() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 16).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let key = oversized_key(7);
        let value = vec![11; PAGE_SIZE];

        cursor.insert(&key, &value).unwrap();

        let record = cursor.get(&key).unwrap().expect("oversized key should exist");
        assert_record_matches(&record, &key, &value);
    }

    #[test]
    fn binary_search_supports_oversized_interior_separator_keys() {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let page_cache = PageCache::new(disk_manager, 256).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        let mut cursor = TreeCursor::new(page_cache, root_page_id);
        let mut expected = BTreeMap::new();

        for index in 0..48 {
            let key = oversized_key(index);
            let value = format!("value-{index}").into_bytes();
            cursor.insert(&key, &value).unwrap();
            expected.insert(key, value);
        }

        assert!(tree_height(&cursor).unwrap() >= 2, "large keys should force interior routing");
        for (key, value) in &expected {
            assert!(cursor.seek_to_key(key).unwrap(), "seek_to_key should find oversized key");
            let record = cursor.current().unwrap().expect("seek_to_key should position cursor");
            assert_record_matches(&record, key, value);
        }
    }

    fn assert_record_matches(record: &Record, expected_key: &[u8], expected_value: &[u8]) {
        record
            .with_key_value(|actual_key, actual_value| {
                assert_eq!(actual_key, expected_key);
                assert_eq!(actual_value, expected_value);
            })
            .unwrap();
    }

    fn assert_owned_record_matches(
        record: &OwnedRecord,
        expected_key: &[u8],
        expected_value: &[u8],
    ) {
        record.with_key_value(|actual_key, actual_value| {
            assert_eq!(actual_key, expected_key);
            assert_eq!(actual_value, expected_value);
        });
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
