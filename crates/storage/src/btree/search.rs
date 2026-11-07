use super::payload::{compare_key_prefix, compare_overflow_key};
use super::root::{expect_page_kind, read_page_kind};
use super::{
    Arc, AtomicOrdering, AtomicU64, BoundResult, ChildSlotRef, CursorState, Interior, Leaf,
    Ordering, PAGE_SIZE, PageCache, PageId, PageKind, PathFrame, PinGuard, RawInterior, RawLeaf,
    Read, Record, SearchResult, StorageResult, TreeCursor, TxnId,
};

pub fn advance_mutation_epoch(epoch: &AtomicU64) {
    epoch.fetch_add(1, AtomicOrdering::Release);
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

#[derive(Clone, Copy)]
enum Direction {
    Forward,
    Reverse,
}

impl TreeCursor {
    /// Creates a cursor anchored at `root_page_id` in page-level state.
    pub(crate) fn new(page_cache: PageCache, root_page_id: PageId) -> StorageResult<Self> {
        let mutation_epoch = page_cache.tree_mutation_epoch(root_page_id)?;
        Ok(Self {
            page_cache,
            root_page_id: Arc::new(AtomicU64::new(root_page_id)),
            mutation_epoch,
            txn_id: None,
            state: CursorState::Page { page_id: root_page_id },
        })
    }

    /// Associates subsequent mutations with an explicit transaction.
    pub(crate) fn for_transaction(mut self, txn_id: TxnId) -> Self {
        self.txn_id = Some(txn_id);
        self
    }

    /// Returns the root page id that anchors this tree.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id.load(AtomicOrdering::Acquire)
    }

    /// Returns the current mutation epoch shared by cursors over this tree.
    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch.load(AtomicOrdering::Acquire)
    }

    /// Invalidates physical positions held by other cursors over this tree.
    pub(super) fn mark_tree_mutated(&self) {
        advance_mutation_epoch(&self.mutation_epoch);
    }

    /// Returns the cursor's current logical state.
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Switches the cursor to a page-anchored but not slot-anchored state.
    pub(super) fn set_page_state(&mut self, page_id: PageId) {
        self.state = CursorState::Page { page_id };
    }

    /// Switches the cursor to one concrete slot inside a leaf page.
    pub(super) fn set_positioned_state(&mut self, page_id: PageId, slot_index: u16) {
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

    pub(super) fn raw_interior_slot_count(&self, page_id: PageId) -> StorageResult<u16> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        Ok(interior.slot_count())
    }

    fn compare_leaf_key_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        leaf: &RawLeaf<Read<'_>>,
        slot_index: u16,
        key: &[u8],
    ) -> StorageResult<Ordering> {
        let (key_len, _, first_overflow_page_id, inline_range) =
            leaf.cell_payload_parts(slot_index)?;
        let inline_key_len = key_len.min(inline_range.len());
        let inline_key = &page_bytes[inline_range.start..inline_range.start + inline_key_len];
        if let Some(ordering) = compare_key_prefix(page_id, inline_key, key_len, key)? {
            return Ok(ordering);
        }

        compare_overflow_key(
            &self.page_cache,
            page_id,
            key,
            inline_key_len,
            first_overflow_page_id,
            key_len,
        )
    }

    pub(super) fn compare_interior_key(
        &self,
        page_id: PageId,
        slot_index: u16,
        key: &[u8],
    ) -> StorageResult<Ordering> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        self.compare_interior_key_in_page(page_id, page.page(), &interior, slot_index, key)
    }

    fn compare_interior_key_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        interior: &RawInterior<Read<'_>>,
        slot_index: u16,
        key: &[u8],
    ) -> StorageResult<Ordering> {
        let (_, key_len, first_overflow_page_id, inline_range) =
            interior.cell_payload_parts(slot_index)?;
        let inline_key_len = key_len.min(inline_range.len());
        let inline_key = &page_bytes[inline_range.start..inline_range.start + inline_key_len];
        if let Some(ordering) = compare_key_prefix(page_id, inline_key, key_len, key)? {
            return Ok(ordering);
        }

        compare_overflow_key(
            &self.page_cache,
            page_id,
            key,
            inline_key_len,
            first_overflow_page_id,
            key_len,
        )
    }

    pub(super) fn search_leaf_slot(
        &self,
        page_id: PageId,
        key: &[u8],
    ) -> StorageResult<SearchResult> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = page.open::<Leaf>()?;
        self.search_leaf_slot_in_page(page_id, page.page(), &leaf, key)
    }

    pub(super) fn search_leaf_slot_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        leaf: &RawLeaf<Read<'_>>,
        key: &[u8],
    ) -> StorageResult<SearchResult> {
        leaf.search_slots_by(|leaf, slot| {
            self.compare_leaf_key_in_page(page_id, page_bytes, leaf, slot, key)
        })
    }

    fn lower_bound_leaf_slot_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        leaf: &RawLeaf<Read<'_>>,
        key: &[u8],
    ) -> StorageResult<BoundResult> {
        let mut low: u16 = 0;
        let slot_count = leaf.slot_count();
        let mut high = slot_count;

        while low < high {
            let mid = low + (high - low) / 2;
            if self.compare_leaf_key_in_page(page_id, page_bytes, leaf, mid, key)? == Ordering::Less
            {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    fn upper_bound_leaf_slot_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        leaf: &RawLeaf<Read<'_>>,
        key: &[u8],
    ) -> StorageResult<BoundResult> {
        let mut low: u16 = 0;
        let slot_count = leaf.slot_count();
        let mut high = slot_count;

        while low < high {
            let mid = low + (high - low) / 2;
            match self.compare_leaf_key_in_page(page_id, page_bytes, leaf, mid, key)? {
                Ordering::Less | Ordering::Equal => low = mid + 1,
                Ordering::Greater => high = mid,
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    fn lower_bound_interior_slot_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        interior: &RawInterior<Read<'_>>,
        key: &[u8],
    ) -> StorageResult<BoundResult> {
        let mut low: u16 = 0;
        let slot_count = interior.slot_count();
        let mut high = slot_count;

        while low < high {
            let mid = low + (high - low) / 2;
            if self.compare_interior_key_in_page(page_id, page_bytes, interior, mid, key)?
                == Ordering::Less
            {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == slot_count { Ok(BoundResult::PastEnd) } else { Ok(BoundResult::At(low)) }
    }

    /// Chooses the child pointer to follow for `key` from an already-open interior page.
    fn interior_child_for_key_in_page(
        &self,
        page_id: PageId,
        page_bytes: &[u8; PAGE_SIZE],
        interior: &RawInterior<Read<'_>>,
        key: &[u8],
    ) -> StorageResult<(PageId, ChildSlotRef)> {
        match self.lower_bound_interior_slot_in_page(page_id, page_bytes, interior, key)? {
            BoundResult::At(slot_index) => {
                let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
                Ok((left_child, ChildSlotRef::Slot(slot_index)))
            }
            BoundResult::PastEnd => Ok((interior.rightmost_child(), ChildSlotRef::Rightmost)),
        }
    }

    /// Follows the outermost children in the scan direction until reaching a leaf.
    fn descend_to_edge_leaf(
        &self,
        start_page_id: PageId,
        direction: Direction,
    ) -> StorageResult<PageId> {
        let mut page_id = start_page_id;

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = page.open::<Leaf>()?;
                        return Ok(page_id);
                    }
                    PageKind::RawInterior => {
                        let interior = page.open::<Interior>()?;
                        match direction {
                            Direction::Forward if interior.slot_count() != 0 => {
                                interior.cell(0)?.left_child()
                            }
                            _ => interior.rightmost_child(),
                        }
                    }
                }
            };
            page_id = next;
        }
    }

    /// Descends to the leaf page that contains or would contain `key`.
    pub(super) fn leaf_page_for_key(&self, key: &[u8]) -> StorageResult<PageId> {
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = page.open::<Leaf>()?;
                        return Ok(page_id);
                    }
                    PageKind::RawInterior => {
                        let interior = page.open::<Interior>()?;
                        let (child_page_id, _) = self.interior_child_for_key_in_page(
                            page_id,
                            page.page(),
                            &interior,
                            key,
                        )?;
                        child_page_id
                    }
                }
            };
            page_id = next;
        }
    }

    /// Descends to the target leaf and records the interior path taken to reach it.
    pub(super) fn leaf_page_path_for_key(
        &self,
        key: &[u8],
    ) -> StorageResult<(PageId, Vec<PathFrame>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = page.open::<Leaf>()?;
                        return Ok((page_id, path));
                    }
                    PageKind::RawInterior => {
                        let interior = page.open::<Interior>()?;
                        let (child_page_id, child_ref) = self.interior_child_for_key_in_page(
                            page_id,
                            page.page(),
                            &interior,
                            key,
                        )?;
                        path.push(PathFrame { page_id, child_ref });
                        child_page_id
                    }
                }
            };
            page_id = next;
        }
    }

    /// Descends to the target leaf, records the interior path, and keeps the leaf pinned.
    pub(super) fn leaf_page_pin_path_for_key(
        &self,
        key: &[u8],
    ) -> StorageResult<(PageId, PinGuard, Vec<PathFrame>)> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id();

        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let next = {
                let page = pin.read()?;
                match read_page_kind(page.page(), page_id)? {
                    PageKind::RawLeaf => {
                        let _ = page.open::<Leaf>()?;
                        None
                    }
                    PageKind::RawInterior => {
                        let interior = page.open::<Interior>()?;
                        let (child_page_id, child_ref) = self.interior_child_for_key_in_page(
                            page_id,
                            page.page(),
                            &interior,
                            key,
                        )?;
                        path.push(PathFrame { page_id, child_ref });
                        Some(child_page_id)
                    }
                }
            };

            match next {
                Some(next_page_id) => page_id = next_page_id,
                None => return Ok((page_id, pin, path)),
            }
        }
    }

    /// Positions at the first slot reachable in the scan direction.
    fn seek_leaf_edge(&mut self, mut page_id: PageId, direction: Direction) -> StorageResult<bool> {
        loop {
            let pin = self.page_cache.fetch_page(page_id)?;
            let page = pin.read()?;
            expect_page_kind(page.page(), page_id, PageKind::RawLeaf, "raw leaf")?;
            let leaf = page.open::<Leaf>()?;
            if leaf.slot_count() > 0 {
                let slot = match direction {
                    Direction::Forward => 0,
                    Direction::Reverse => leaf.slot_count() - 1,
                };
                self.set_positioned_state(page_id, slot);
                return Ok(true);
            }

            match leaf_sibling(&leaf, direction) {
                Some(sibling) => page_id = sibling,
                None => {
                    self.set_exhausted_state();
                    return Ok(false);
                }
            }
        }
    }

    fn apply_seek(
        &mut self,
        page_id: PageId,
        seek: LeafSeek,
        direction: Direction,
    ) -> StorageResult<bool> {
        match seek {
            LeafSeek::Positioned(slot_index) => {
                self.set_positioned_state(page_id, slot_index);
                Ok(true)
            }
            LeafSeek::Advance(sibling) => self.seek_leaf_edge(sibling, direction),
            LeafSeek::Exhausted => {
                self.set_exhausted_state();
                Ok(false)
            }
        }
    }

    /// Advances the cursor by one logical record in the scan direction.
    fn step_record(&mut self, direction: Direction) -> StorageResult<Option<Record>> {
        match self.state {
            CursorState::Exhausted => return Ok(None),
            CursorState::Page { page_id } => {
                let leaf_page_id = self.descend_to_edge_leaf(page_id, direction)?;
                self.seek_leaf_edge(leaf_page_id, direction)?;
            }
            CursorState::Positioned { page_id, slot_index } => {
                let seek = {
                    let pin = self.page_cache.fetch_page(page_id)?;
                    let page = pin.read()?;
                    expect_page_kind(page.page(), page_id, PageKind::RawLeaf, "raw leaf")?;
                    let leaf = page.open::<Leaf>()?;
                    let slot = match direction {
                        Direction::Forward => {
                            let next_slot = slot_index + 1;
                            (next_slot < leaf.slot_count()).then_some(next_slot)
                        }
                        Direction::Reverse => slot_index.checked_sub(1),
                    };
                    leaf_seek(&leaf, slot, direction)
                };
                self.apply_seek(page_id, seek, direction)?;
            }
        }
        self.current()
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

    /// Positions the cursor on the first record whose key is greater than or
    /// equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let seek = {
            let page = pin.read()?;
            let leaf = page.open::<Leaf>()?;
            let bound = self.lower_bound_leaf_slot_in_page(page_id, page.page(), &leaf, key)?;
            match bound {
                BoundResult::At(slot_index) => LeafSeek::Positioned(slot_index),
                BoundResult::PastEnd => match leaf.next_page_id() {
                    Some(next_page_id) => LeafSeek::Advance(next_page_id),
                    None => LeafSeek::Exhausted,
                },
            }
        };

        self.apply_seek(page_id, seek, Direction::Forward)
    }

    /// Positions the cursor on the first record whose key is strictly greater
    /// than `key`.
    pub fn seek_after_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let seek = {
            let page = pin.read()?;
            let leaf = page.open::<Leaf>()?;
            match self.upper_bound_leaf_slot_in_page(page_id, page.page(), &leaf, key)? {
                BoundResult::At(slot_index) => LeafSeek::Positioned(slot_index),
                BoundResult::PastEnd => match leaf.next_page_id() {
                    Some(next_page_id) => LeafSeek::Advance(next_page_id),
                    None => LeafSeek::Exhausted,
                },
            }
        };

        self.apply_seek(page_id, seek, Direction::Forward)
    }

    /// Positions on the greatest key less than or equal to `key`.
    pub fn seek_to_key_at_most(&mut self, key: &[u8]) -> StorageResult<bool> {
        self.seek_reverse_bound(key, true)
    }

    /// Positions on the greatest key strictly less than `key`.
    pub fn seek_before_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        self.seek_reverse_bound(key, false)
    }

    fn seek_reverse_bound(&mut self, key: &[u8], inclusive: bool) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let seek = {
            let pin = self.page_cache.fetch_page(page_id)?;
            let page = pin.read()?;
            let leaf = page.open::<Leaf>()?;
            let slot = match self.search_leaf_slot_in_page(page_id, page.page(), &leaf, key)? {
                SearchResult::Found(slot) if inclusive => Some(slot),
                SearchResult::Found(slot) | SearchResult::InsertAt(slot) => slot.checked_sub(1),
            };
            leaf_seek(&leaf, slot, Direction::Reverse)
        };
        self.apply_seek(page_id, seek, Direction::Reverse)
    }

    /// Positions the cursor on the smallest key in the tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let leaf = self.descend_to_edge_leaf(self.root_page_id(), Direction::Forward)?;
        self.seek_leaf_edge(leaf, Direction::Forward)
    }

    /// Positions the cursor on the greatest key in the tree.
    pub fn seek_to_last(&mut self) -> StorageResult<bool> {
        let leaf = self.descend_to_edge_leaf(self.root_page_id(), Direction::Reverse)?;
        self.seek_leaf_edge(leaf, Direction::Reverse)
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
        self.step_record(Direction::Forward)
    }

    /// Advances to the previous record in sorted key order.
    pub fn previous_record(&mut self) -> StorageResult<Option<Record>> {
        self.step_record(Direction::Reverse)
    }
}

fn leaf_sibling(leaf: &RawLeaf<Read<'_>>, direction: Direction) -> Option<PageId> {
    match direction {
        Direction::Forward => leaf.next_page_id(),
        Direction::Reverse => leaf.prev_page_id(),
    }
}

fn leaf_seek(leaf: &RawLeaf<Read<'_>>, slot: Option<u16>, direction: Direction) -> LeafSeek {
    match slot {
        Some(slot) => LeafSeek::Positioned(slot),
        None => leaf_sibling(leaf, direction).map_or(LeafSeek::Exhausted, LeafSeek::Advance),
    }
}
