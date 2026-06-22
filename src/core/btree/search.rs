use super::payload::{compare_key_prefix, compare_overflow_key};
use super::root::{expect_page_kind, read_page_kind};
use super::*;

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
    pub(super) fn set_page_state(&mut self, page_id: PageId) {
        self.state = CursorState::Page { page_id };
    }

    /// Switches the cursor to one concrete slot inside a leaf page.
    pub(super) fn set_positioned_state(&mut self, page_id: PageId, slot_index: u16) {
        self.state = CursorState::Positioned { page_id, slot_index };
    }

    /// Marks the cursor as having moved past the scan range.
    pub(super) fn set_exhausted_state(&mut self) {
        self.state = CursorState::Exhausted;
    }

    /// Materializes one record from a positioned raw leaf slot.
    pub(super) fn record_at(&self, page_id: PageId, slot_index: u16) -> StorageResult<Record> {
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
        let mut low: u16 = 0;
        let mut high = leaf.slot_count();

        while low < high {
            let mid = low + (high - low) / 2;
            match self.compare_leaf_key_in_page(page_id, page_bytes, leaf, mid, key)? {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Ok(SearchResult::Found(mid)),
            }
        }

        Ok(SearchResult::InsertAt(low))
    }

    pub(super) fn lower_bound_leaf_slot(
        &self,
        page_id: PageId,
        key: &[u8],
    ) -> StorageResult<BoundResult> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = page.open::<Leaf>()?;
        self.lower_bound_leaf_slot_in_page(page_id, page.page(), &leaf, key)
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

    /// Follows leftmost children from `start_page_id` until reaching a leaf.
    pub(super) fn descend_to_first_leaf_from(
        &self,
        start_page_id: PageId,
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
    pub(super) fn descend_to_last_leaf_from(&self, start_page_id: PageId) -> StorageResult<PageId> {
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
                        interior.rightmost_child()
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
                let leaf = page.open::<Leaf>()?;
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
                    let leaf = page.open::<Leaf>()?;
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

    /// Searches the raw tree for `key` and returns a stable owned record snapshot.
    pub fn get_owned(&mut self, key: &[u8]) -> StorageResult<Option<OwnedRecord>> {
        self.get(key)?.map(|record| record.to_owned_record()).transpose()
    }

    /// Positions the cursor on the first record whose key is greater than or
    /// equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        let page_id = self.leaf_page_for_key(key)?;
        let pin = self.page_cache.fetch_page(page_id)?;
        let seek = {
            let page = pin.read()?;
            let leaf = page.open::<Leaf>()?;
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

    /// Reads the currently selected record as a stable owned snapshot, if any.
    pub fn current_owned(&self) -> StorageResult<Option<OwnedRecord>> {
        self.current()?.map(|record| record.to_owned_record()).transpose()
    }

    /// Advances to the next record in sorted key order.
    pub fn next_record(&mut self) -> StorageResult<Option<Record>> {
        self.step_record(ScanDirection::Forward)
    }

    /// Advances to the next record and returns a stable owned snapshot.
    pub fn next_owned_record(&mut self) -> StorageResult<Option<OwnedRecord>> {
        self.next_record()?.map(|record| record.to_owned_record()).transpose()
    }

    /// Moves to the previous record in sorted key order.
    pub fn prev_record(&mut self) -> StorageResult<Option<Record>> {
        self.step_record(ScanDirection::Backward)
    }

    /// Moves to the previous record and returns a stable owned snapshot.
    pub fn prev_owned_record(&mut self) -> StorageResult<Option<OwnedRecord>> {
        self.prev_record()?.map(|record| record.to_owned_record()).transpose()
    }
}
