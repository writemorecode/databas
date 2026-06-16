use std::borrow::Cow;

use super::payload::{cell_corruption, materialize_payload};
use super::root::read_page_kind;
use super::*;

impl TreeCursor {
    /// Bubbles one pending split up the recorded tree path until it lands.
    pub(super) fn propagate_split(
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
    pub(super) fn update_interior_child_ref(
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
    pub(super) fn insert_into_parent(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit,
    ) -> StorageResult<Option<PendingSplit>> {
        let interior_page_guard = self.page_cache.fetch_page(parent_frame.page_id)?;
        let (insert_slot_index, has_capacity) = {
            let interior_read_guard = interior_page_guard.read()?;
            let page = interior_read_guard.open::<Interior>()?;
            let insert_slot_index = match parent_frame.child_ref {
                ChildSlotRef::Slot(slot_index) => slot_index,
                ChildSlotRef::Rightmost => page.slot_count(),
            };
            let needed =
                self.interior_cell_local_size(&pending.separator)? + page::format::SLOT_ENTRY_SIZE;
            (insert_slot_index, page.total_reclaimable_space()? >= needed)
        };

        if has_capacity {
            let mut interior_guard = interior_page_guard.write()?;
            let mut interior_page = interior_guard.open_mut::<Interior>()?;
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
            drop(interior_page_guard);
            self.insert_with_interior_page_split(parent_frame, pending).map(Some)
        }
    }

    /// Returns whether the provided leaf cells fit into one leaf page.
    pub(super) fn leaf_cells_fit(cells: &[LeafSplitCell<'_>]) -> bool {
        let cell_bytes = cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        Self::leaf_cell_bytes_fit(cells.len(), cell_bytes)
    }

    pub(super) fn leaf_cell_bytes_fit(cell_count: usize, cell_bytes: usize) -> bool {
        let used_bytes = PageKind::RawLeaf.header_size()
            + cell_count * page::format::SLOT_ENTRY_SIZE
            + cell_bytes;
        used_bytes <= page::format::USABLE_SPACE_END
    }

    pub(super) fn leaf_cell_bytes_underoccupied(cell_count: usize, cell_bytes: usize) -> bool {
        let occupied_variable_bytes = cell_count * page::format::SLOT_ENTRY_SIZE + cell_bytes;
        let usable_variable_bytes =
            page::format::USABLE_SPACE_END - PageKind::RawLeaf.header_size();
        occupied_variable_bytes * 2 < usable_variable_bytes
    }

    /// Chooses the leaf split point with the smallest byte imbalance.
    pub(super) fn choose_leaf_split_index(cells: &[LeafSplitCell<'_>]) -> StorageResult<usize> {
        debug_assert!(cells.len() >= 2, "leaf splits need at least two cells");

        let total_cell_len = cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let mut left_cell_len = 0;
        let mut best = None;

        for split_index in 1..cells.len() {
            left_cell_len += cells[split_index - 1].encoded_size();
            let right_cell_len = total_cell_len - left_cell_len;
            if !Self::leaf_cell_bytes_fit(split_index, left_cell_len)
                || !Self::leaf_cell_bytes_fit(cells.len() - split_index, right_cell_len)
            {
                continue;
            }

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

    /// Reads all leaf cells from a stable page snapshot.
    pub(super) fn read_leaf_cells_from_snapshot<'a>(
        &self,
        leaf_page_id: PageId,
        leaf_snapshot_bytes: &'a [u8; PAGE_SIZE],
        leaf_snapshot: &RawLeaf<Read<'_>>,
    ) -> StorageResult<Vec<LeafSplitCell<'a>>> {
        let mut cells = Vec::with_capacity(leaf_snapshot.slot_count() as usize);
        for slot_index in 0..leaf_snapshot.slot_count() {
            let (key_len, value_len, first_overflow_page_id, inline_range) =
                leaf_snapshot.cell_payload_parts(slot_index)?;
            let inline_payload = &leaf_snapshot_bytes[inline_range];
            let cell = match first_overflow_page_id {
                None => {
                    if inline_payload.len() != key_len + value_len {
                        return Err(cell_corruption(
                            leaf_page_id,
                            CorruptionKind::CellLengthOutOfBounds,
                        ));
                    }
                    let (key, value) = inline_payload.split_at(key_len);
                    LeafSplitCell::borrowed(key, value)
                }
                Some(_) => {
                    let mut payload = materialize_payload(
                        &self.page_cache,
                        leaf_page_id,
                        inline_payload,
                        first_overflow_page_id,
                        key_len + value_len,
                    )?;
                    if payload.len() < key_len {
                        return Err(cell_corruption(
                            leaf_page_id,
                            CorruptionKind::CellLengthOutOfBounds,
                        ));
                    }
                    let value = payload.split_off(key_len);
                    LeafSplitCell::owned(payload, value)
                }
            };
            cells.push(cell);
        }
        Ok(cells)
    }

    pub(super) fn snapshot_leaf_cells<'a>(
        &self,
        page_id: PageId,
        snapshot_bytes: &'a mut [u8; PAGE_SIZE],
    ) -> StorageResult<Vec<LeafSplitCell<'a>>> {
        let pin = self.page_cache.fetch_page(page_id)?;
        {
            let page = pin.read()?;
            *snapshot_bytes = *page.page();
        }
        drop(pin);

        let snapshot = RawLeaf::<Read<'_>>::open(snapshot_bytes)?;
        self.read_leaf_cells_from_snapshot(page_id, snapshot_bytes, &snapshot)
    }

    pub(super) fn snapshot_leaf_pair_cells<'a>(
        &self,
        first_page_id: PageId,
        first_snapshot_bytes: &'a mut [u8; PAGE_SIZE],
        second_page_id: PageId,
        second_snapshot_bytes: &'a mut [u8; PAGE_SIZE],
    ) -> StorageResult<Vec<LeafSplitCell<'a>>> {
        let mut cells = self.snapshot_leaf_cells(first_page_id, first_snapshot_bytes)?;
        cells.extend(self.snapshot_leaf_cells(second_page_id, second_snapshot_bytes)?);
        Ok(cells)
    }

    pub(super) fn snapshot_leaf_pair_cells_sorted<'a>(
        &self,
        first_page_id: PageId,
        first_snapshot_bytes: &'a mut [u8; PAGE_SIZE],
        second_page_id: PageId,
        second_snapshot_bytes: &'a mut [u8; PAGE_SIZE],
    ) -> StorageResult<Vec<LeafSplitCell<'a>>> {
        let mut cells = self.snapshot_leaf_pair_cells(
            first_page_id,
            first_snapshot_bytes,
            second_page_id,
            second_snapshot_bytes,
        )?;
        Self::sort_leaf_cells(&mut cells);
        Ok(cells)
    }

    #[cfg(test)]
    pub(super) fn leaf_cell_storage_is_borrowed_for_test(cell: &LeafSplitCell<'_>) -> (bool, bool) {
        (matches!(cell.key, Cow::Borrowed(_)), matches!(cell.value, Cow::Borrowed(_)))
    }

    #[cfg(test)]
    pub(super) fn leaf_cell_storage_is_owned_for_test(cell: &LeafSplitCell<'_>) -> (bool, bool) {
        (matches!(cell.key, Cow::Owned(_)), matches!(cell.value, Cow::Owned(_)))
    }

    /// Rebuilds a split leaf pair from ordered materialized cells.
    pub(super) fn split_leaf_cells(
        &mut self,
        leaf_page_id: PageId,
        leaf_guard: &mut PageWriteGuard<'_>,
        prev_page_id: Option<PageId>,
        next_page_id: Option<PageId>,
        cells: &[LeafSplitCell<'_>],
        target_key: &[u8],
    ) -> StorageResult<PendingSplit> {
        let split_index = Self::choose_leaf_split_index(cells)?;
        let (left_cells, right_cells) = cells.split_at(split_index);

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        let mut right_guard = right_page_guard.write()?;
        let mut right_page = RawLeaf::<Write<'_>>::initialize(right_guard.page_mut());

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
            let mut next_page = next_guard.open_mut::<Leaf>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        let separator =
            left_cells.last().expect("leaf split must leave a non-empty left page").key().to_vec();

        let target_page_id =
            if target_key <= separator.as_slice() { leaf_page_id } else { right_page_id };
        let target_cells = if target_page_id == leaf_page_id { left_cells } else { right_cells };
        let target_slot_index = target_cells
            .iter()
            .position(|cell| cell.key() == target_key)
            .expect("leaf split must retain the target key") as u16;
        self.set_positioned_state(target_page_id, target_slot_index);

        Ok(PendingSplit { separator, left_page_id: leaf_page_id, right_page_id })
    }

    /// Splits a full leaf page while inserting a new key/value cell.
    pub(super) fn insert_with_leaf_page_split(
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
        let mut cells =
            self.read_leaf_cells_from_snapshot(leaf_page_id, &leaf_snapshot_bytes, &leaf_snapshot)?;

        let idx = match cells.binary_search_by(|cell| cell.key().cmp(key)) {
            Ok(_) => return Err(PageError::DuplicateKey.into()),
            Err(insert_index) => insert_index,
        };
        cells.insert(idx, LeafSplitCell::owned(key.to_vec(), value.to_vec()));

        self.split_leaf_cells(leaf_page_id, leaf_guard, prev_page_id, next_page_id, &cells, key)
    }

    /// Splits a full leaf page while replacing one existing cell value.
    pub(super) fn update_with_leaf_page_split(
        &mut self,
        leaf_page_id: PageId,
        leaf_guard: &mut PageWriteGuard<'_>,
        slot_index: u16,
        value: &[u8],
    ) -> StorageResult<PendingSplit> {
        let leaf_snapshot_bytes = *leaf_guard.page();
        let leaf_snapshot = RawLeaf::<Read<'_>>::open(&leaf_snapshot_bytes)?;

        let prev_page_id = leaf_snapshot.prev_page_id();
        let next_page_id = leaf_snapshot.next_page_id();
        let mut cells =
            self.read_leaf_cells_from_snapshot(leaf_page_id, &leaf_snapshot_bytes, &leaf_snapshot)?;
        let target = cells
            .get_mut(slot_index as usize)
            .ok_or_else(|| cell_corruption(leaf_page_id, CorruptionKind::CellLengthOutOfBounds))?;
        target.value = Cow::Owned(value.to_vec());
        let target_key = target.key().to_vec();

        self.split_leaf_cells(
            leaf_page_id,
            leaf_guard,
            prev_page_id,
            next_page_id,
            &cells,
            &target_key,
        )
    }

    /// Splits a full interior page while inserting a propagated separator.
    pub(super) fn insert_with_interior_page_split(
        &mut self,
        parent_frame: PathFrame,
        pending: PendingSplit,
    ) -> StorageResult<PendingSplit> {
        let PendingSplit { separator, left_page_id, right_page_id: incoming_right_page_id } =
            pending;
        let (prev_page_id, next_page_id) = self.read_interior_page_links(parent_frame.page_id)?;
        let mut children = self.read_interior_child_entries_from_page(parent_frame.page_id)?;
        let child_index = match parent_frame.child_ref {
            ChildSlotRef::Slot(slot_index) => slot_index as usize,
            ChildSlotRef::Rightmost => children.len() - 1,
        };
        let original_max_key = children[child_index].max_key.take();
        children[child_index] = ChildEntry { page_id: left_page_id, max_key: Some(separator) };
        children.insert(
            child_index + 1,
            ChildEntry { page_id: incoming_right_page_id, max_key: original_max_key },
        );

        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        drop(right_page_guard);

        let split_index = Self::choose_interior_fitting_split(&children)
            .ok_or(PageError::PageFull { needed: PAGE_SIZE + 1, available: PAGE_SIZE })?;
        let (left_children, right_children) = children.split_at(split_index);

        let propagated_separator = left_children
            .last()
            .and_then(|child| child.max_key.clone())
            .ok_or_else(|| Self::missing_child_max_key_error(parent_frame.page_id))?;

        self.rewrite_interior_page(
            parent_frame.page_id,
            left_children,
            prev_page_id,
            Some(right_page_id),
        )?;
        self.rewrite_interior_page(
            right_page_id,
            right_children,
            Some(parent_frame.page_id),
            next_page_id,
        )?;

        if let Some(next_page_id) = next_page_id {
            let next_page_guard = self.page_cache.fetch_page(next_page_id)?;
            let mut next_guard = next_page_guard.write()?;
            let mut next_page = next_guard.open_mut::<Interior>()?;
            next_page.set_prev_page_id(Some(right_page_id));
        }

        Ok(PendingSplit {
            separator: propagated_separator,
            left_page_id: parent_frame.page_id,
            right_page_id,
        })
    }

    /// Installs a new interior root in the existing root page after the old root split.
    pub(super) fn install_new_root(&mut self, pending: PendingSplit) -> StorageResult<()> {
        let root_page_id = self.root_page_id();
        debug_assert_eq!(
            pending.left_page_id, root_page_id,
            "root split must report the old root page as its left page"
        );

        let root_pin = self.page_cache.fetch_page(root_page_id)?;
        let root_snapshot = {
            let root_guard = root_pin.read()?;
            *root_guard.page()
        };

        let (left_page_id, left_page_pin) = self.page_cache.new_page()?;
        {
            let mut left_guard = left_page_pin.write()?;
            *left_guard.page_mut() = root_snapshot;
        }
        self.relink_copied_root_left_child(left_page_id, pending.right_page_id)?;

        let mut root_guard = root_pin.write()?;
        let mut root_page = RawInterior::<Write<'_>>::initialize_with_rightmost(
            root_guard.page_mut(),
            pending.right_page_id,
        );
        self.insert_interior_payload_at(&mut root_page, 0, left_page_id, &pending.separator)?;
        self.remap_positioned_page(root_page_id, left_page_id);
        Ok(())
    }

    pub(super) fn relink_copied_root_left_child(
        &self,
        left_page_id: PageId,
        right_page_id: PageId,
    ) -> StorageResult<()> {
        let left_pin = self.page_cache.fetch_page(left_page_id)?;
        let mut left_guard = left_pin.write()?;
        match read_page_kind(left_guard.page(), left_page_id)? {
            PageKind::RawLeaf => {
                {
                    let mut leaf = left_guard.open_mut::<Leaf>()?;
                    leaf.set_prev_page_id(None);
                    leaf.set_next_page_id(Some(right_page_id));
                }
                self.set_leaf_prev_page_id(right_page_id, Some(left_page_id))?;
            }
            PageKind::RawInterior => {
                {
                    let mut interior = left_guard.open_mut::<Interior>()?;
                    interior.set_prev_page_id(None);
                    interior.set_next_page_id(Some(right_page_id));
                }
                self.set_interior_prev_page_id(right_page_id, Some(left_page_id))?;
            }
        }
        Ok(())
    }

    pub(super) fn remap_positioned_page(&mut self, from_page_id: PageId, to_page_id: PageId) {
        if let CursorState::Positioned { page_id, slot_index } = self.state
            && page_id == from_page_id
        {
            self.set_positioned_state(to_page_id, slot_index);
        }
    }
}
