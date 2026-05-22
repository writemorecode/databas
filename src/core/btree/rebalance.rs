use super::payload::{read_interior_cell, read_leaf_cell};
use super::root::read_page_kind;
use super::*;

impl<S: PageStore> TreeCursor<S> {
    /// Returns the previous and next sibling pointers for a leaf page.
    pub(super) fn read_leaf_page_links(
        &self,
        page_id: PageId,
    ) -> StorageResult<(Option<PageId>, Option<PageId>)> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = page.open::<Leaf>()?;
        Ok((leaf.prev_page_id(), leaf.next_page_id()))
    }

    /// Returns the previous and next sibling pointers for an interior page.
    pub(super) fn read_interior_page_links(
        &self,
        page_id: PageId,
    ) -> StorageResult<(Option<PageId>, Option<PageId>)> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        Ok((interior.prev_page_id(), interior.next_page_id()))
    }

    /// Materializes all leaf cells in slot order for rebalance planning.
    pub(super) fn read_leaf_cells(&self, page_id: PageId) -> StorageResult<Vec<LeafSplitCell>> {
        let slot_count = self.raw_leaf_slot_count(page_id)?;
        let mut cells = Vec::with_capacity(slot_count as usize);
        for slot_index in 0..slot_count {
            let (key, value) = read_leaf_cell(&self.page_cache, page_id, slot_index)?;
            cells.push(LeafSplitCell { key, value });
        }
        Ok(cells)
    }

    /// Returns the largest key in a leaf page, or `None` when the page is empty.
    pub(super) fn read_leaf_max_key(&self, page_id: PageId) -> StorageResult<Option<Vec<u8>>> {
        let slot_count = self.raw_leaf_slot_count(page_id)?;
        if slot_count == 0 {
            return Ok(None);
        }

        read_leaf_cell(&self.page_cache, page_id, slot_count - 1).map(|(key, _)| Some(key))
    }

    /// Reads child page ids from an interior page in logical left-to-right order.
    pub(super) fn read_interior_child_page_ids(
        &self,
        page_id: PageId,
    ) -> StorageResult<Vec<PageId>> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        let mut children = Vec::with_capacity(interior.slot_count() as usize + 1);
        for slot_index in 0..interior.slot_count() {
            let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
            children.push(left_child);
        }
        children.push(interior.rightmost_child());
        Ok(children)
    }

    /// Returns whether `child_page_id` is still linked from `parent_page_id`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn interior_page_has_child(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> StorageResult<bool> {
        let pin = self.page_cache.fetch_page(parent_page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        for slot_index in 0..interior.slot_count() {
            let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
            if left_child == child_page_id {
                return Ok(true);
            }
        }
        Ok(interior.rightmost_child() == child_page_id)
    }

    /// Returns the largest key reachable from the subtree rooted at `page_id`.
    pub(super) fn subtree_max_key(&self, page_id: PageId) -> StorageResult<Option<Vec<u8>>> {
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
                    let interior = page.open::<Interior>()?;
                    interior.rightmost_child()
                }
            }
        };
        drop(pin);
        self.subtree_max_key(next)
    }

    /// Collects ordered child entries for an interior page with refreshed max keys.
    pub(super) fn read_interior_child_entries(
        &self,
        page_id: PageId,
    ) -> StorageResult<Vec<ChildEntry>> {
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
    pub(super) fn child_index_in_parent(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> StorageResult<usize> {
        let pin = self.page_cache.fetch_page(parent_page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        for slot_index in 0..interior.slot_count() {
            let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
            if left_child == child_page_id {
                return Ok(slot_index as usize);
            }
        }
        if interior.rightmost_child() == child_page_id {
            return Ok(interior.slot_count() as usize);
        }
        Err(StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::InteriorPage,
            page_id: Some(parent_page_id),
            kind: CorruptionKind::CellLengthOutOfBounds,
        }))
    }

    /// Reinitializes a leaf page with `cells` and updated sibling links.
    pub(super) fn rewrite_leaf_page(
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
    pub(super) fn rewrite_interior_page(
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

        let mut page_image = [0; PAGE_SIZE];
        {
            let mut interior = RawInterior::<Write<'_>>::initialize_with_rightmost(
                &mut page_image,
                rightmost_child.page_id,
            );
            interior.set_prev_page_id(prev_page_id);
            interior.set_next_page_id(next_page_id);
            for child in &children[..children.len() - 1] {
                let key = child
                    .max_key
                    .as_deref()
                    .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;
                let slot_index = interior.slot_count();
                self.insert_interior_payload_at(&mut interior, slot_index, child.page_id, key)?;
            }
        }

        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        *guard.page_mut() = page_image;
        Ok(())
    }

    /// Returns whether an interior page already matches refreshed child maxima.
    pub(super) fn interior_page_matches_children(
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
    pub(super) fn refresh_interior_page_separators(&self, page_id: PageId) -> StorageResult<()> {
        let children = self.read_interior_child_entries(page_id)?;
        if self.interior_page_matches_children(page_id, &children)? {
            return Ok(());
        }

        let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
        self.rewrite_interior_page(page_id, &children, prev_page_id, next_page_id)
    }

    /// Returns whether a leaf page is below the minimum occupancy target.
    pub(super) fn leaf_page_underoccupied(&self, page_id: PageId) -> StorageResult<bool> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let leaf = page.open::<Leaf>()?;
        Ok(leaf.is_underoccupied()?)
    }

    /// Returns whether an interior page is below the minimum occupancy target.
    pub(super) fn interior_page_underoccupied(&self, page_id: PageId) -> StorageResult<bool> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        Ok(interior.is_underoccupied()?)
    }

    /// Returns whether a leaf rebuilt from `cells` would be underoccupied.
    pub(super) fn leaf_cells_underoccupied(cells: &[LeafSplitCell]) -> bool {
        let occupied_variable_bytes = cells.len() * page::format::SLOT_ENTRY_SIZE
            + cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let usable_variable_bytes =
            page::format::USABLE_SPACE_END - PageKind::RawLeaf.header_size();
        occupied_variable_bytes * 2 < usable_variable_bytes
    }

    /// Chooses a split index that keeps both leaf siblings fit and occupied.
    pub(super) fn choose_leaf_rebalance_split(cells: &[LeafSplitCell]) -> Option<usize> {
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
    pub(super) fn choose_leaf_fitting_split(cells: &[LeafSplitCell]) -> Option<usize> {
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
    pub(super) fn interior_children_fit(children: &[ChildEntry]) -> bool {
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
    pub(super) fn interior_children_underoccupied(children: &[ChildEntry]) -> bool {
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
    pub(super) fn choose_interior_rebalance_split(children: &[ChildEntry]) -> Option<usize> {
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
    pub(super) fn choose_interior_fitting_split(children: &[ChildEntry]) -> Option<usize> {
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
    pub(super) fn set_leaf_prev_page_id(
        &self,
        page_id: PageId,
        prev_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        let mut leaf = guard.open_mut::<Leaf>()?;
        leaf.set_prev_page_id(prev_page_id);
        Ok(())
    }

    /// Updates the previous-sibling link for an interior page.
    pub(super) fn set_interior_prev_page_id(
        &self,
        page_id: PageId,
        prev_page_id: Option<PageId>,
    ) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let mut guard = pin.write()?;
        let mut interior = guard.open_mut::<Interior>()?;
        interior.set_prev_page_id(prev_page_id);
        Ok(())
    }

    /// Removes `child_page_id` from `parent_page_id` and rewrites the parent.
    pub(super) fn remove_child_from_parent(
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
    pub(super) fn redistribute_leaf_pair(
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
    pub(super) fn merge_leaf_pages(
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

    pub(super) fn sort_leaf_cells(cells: &mut [LeafSplitCell]) {
        cells.sort_by(|left, right| left.key().cmp(right.key()));
    }

    /// Rebalances an underoccupied leaf against siblings.
    ///
    /// Returns `true` when a merge removed one child from the parent page.
    pub(super) fn rebalance_leaf_page(
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
            Self::sort_leaf_cells(&mut cells);
            if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
                self.redistribute_leaf_pair(left_page_id, leaf_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        if child_index + 1 < parent_children.len() {
            let right_page_id = parent_children[child_index + 1];
            let mut cells = self.read_leaf_cells(leaf_page_id)?;
            cells.extend(self.read_leaf_cells(right_page_id)?);
            Self::sort_leaf_cells(&mut cells);
            if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
                self.redistribute_leaf_pair(leaf_page_id, right_page_id, &cells, split_index)?;
                return Ok(false);
            }
        }

        if child_index > 0 {
            let left_page_id = parent_children[child_index - 1];
            let mut cells = self.read_leaf_cells(left_page_id)?;
            cells.extend(self.read_leaf_cells(leaf_page_id)?);
            Self::sort_leaf_cells(&mut cells);
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
            Self::sort_leaf_cells(&mut cells);
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
    pub(super) fn redistribute_interior_pair(
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
    pub(super) fn merge_interior_pages(
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

    /// Splits an existing interior page after refreshed separators no longer fit.
    pub(super) fn split_existing_interior_page(
        &mut self,
        page_id: PageId,
        children: &[ChildEntry],
    ) -> StorageResult<PendingSplit> {
        let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
        let (right_page_id, right_page_guard) = self.page_cache.new_page()?;
        drop(right_page_guard);

        let split_index = Self::choose_interior_fitting_split(children)
            .ok_or(PageError::PageFull { needed: PAGE_SIZE + 1, available: PAGE_SIZE })?;
        let (left_children, right_children) = children.split_at(split_index);

        let propagated_separator = left_children
            .last()
            .and_then(|child| child.max_key.clone())
            .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;

        self.rewrite_interior_page(page_id, left_children, prev_page_id, Some(right_page_id))?;
        self.rewrite_interior_page(right_page_id, right_children, Some(page_id), next_page_id)?;

        if let Some(next_page_id) = next_page_id {
            self.set_interior_prev_page_id(next_page_id, Some(right_page_id))?;
        }

        Ok(PendingSplit { separator: propagated_separator, left_page_id: page_id, right_page_id })
    }

    /// Rebalances an underoccupied interior page against siblings.
    ///
    /// Returns `true` when a merge removed one child from the parent page.
    pub(super) fn rebalance_interior_page(
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

    /// Replaces an empty interior root with its sole child while preserving the root page id.
    pub(super) fn shrink_root_if_empty(&mut self) -> StorageResult<()> {
        let root_page_id = self.root_page_id();
        let pin = self.page_cache.fetch_page(root_page_id)?;
        let child_page_id = {
            let page = pin.read()?;
            match read_page_kind(page.page(), root_page_id)? {
                PageKind::RawLeaf => return Ok(()),
                PageKind::RawInterior => {
                    let interior = page.open::<Interior>()?;
                    if interior.slot_count() > 0 {
                        return Ok(());
                    }
                    interior.rightmost_child()
                }
            }
        };

        let child_pin = self.page_cache.fetch_page(child_page_id)?;
        let child_snapshot = {
            let child_page = child_pin.read()?;
            *child_page.page()
        };

        let mut root_guard = pin.write()?;
        *root_guard.page_mut() = child_snapshot;
        drop(root_guard);

        self.clear_root_sibling_links(root_page_id)?;
        self.set_page_state(root_page_id);
        Ok(())
    }

    pub(super) fn clear_root_sibling_links(&self, root_page_id: PageId) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(root_page_id)?;
        let mut guard = pin.write()?;
        match read_page_kind(guard.page(), root_page_id)? {
            PageKind::RawLeaf => {
                let mut leaf = guard.open_mut::<Leaf>()?;
                leaf.set_prev_page_id(None);
                leaf.set_next_page_id(None);
            }
            PageKind::RawInterior => {
                let mut interior = guard.open_mut::<Interior>()?;
                interior.set_prev_page_id(None);
                interior.set_next_page_id(None);
            }
        }
        Ok(())
    }

    /// Refreshes separators along the still-reachable delete path.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn refresh_path_separators(&self, tree_path: &[PathFrame]) -> StorageResult<()> {
        if tree_path.is_empty() {
            return Ok(());
        }

        let mut reachable_depth = 0;
        for (depth, frame) in tree_path.iter().enumerate() {
            let is_reachable = if depth == 0 {
                frame.page_id == self.root_page_id()
            } else {
                self.interior_page_has_child(tree_path[depth - 1].page_id, frame.page_id)?
            };
            if !is_reachable {
                break;
            }
            reachable_depth += 1;
        }

        for frame in tree_path[..reachable_depth].iter().rev() {
            self.refresh_interior_page_separators(frame.page_id)?;
        }

        Ok(())
    }

    /// Refreshes separators throughout the reachable subtree rooted at the current root.
    pub(super) fn refresh_subtree_separators(&mut self) -> StorageResult<()> {
        loop {
            let root_page_id = self.root_page_id();
            let Some(pending) = self.refresh_subtree_separators_once(root_page_id)? else {
                return Ok(());
            };
            self.install_new_root(pending)?;
        }
    }

    /// Refreshes one subtree and returns a split that must be inserted by its parent.
    pub(super) fn refresh_subtree_separators_once(
        &mut self,
        page_id: PageId,
    ) -> StorageResult<Option<PendingSplit>> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let is_interior = {
            let page = pin.read()?;
            match read_page_kind(page.page(), page_id)? {
                PageKind::RawLeaf => false,
                PageKind::RawInterior => true,
            }
        };
        drop(pin);
        if !is_interior {
            return Ok(None);
        }

        let mut child_index = 0;
        loop {
            let child_page_ids = self.read_interior_child_page_ids(page_id)?;
            let Some(&child_page_id) = child_page_ids.get(child_index) else {
                break;
            };

            if let Some(pending) = self.refresh_subtree_separators_once(child_page_id)? {
                let child_ref = if child_index + 1 == child_page_ids.len() {
                    ChildSlotRef::Rightmost
                } else {
                    ChildSlotRef::Slot(child_index as u16)
                };
                let parent_frame = PathFrame { page_id, child_ref };
                if let Some(parent_pending) = self.insert_into_parent(parent_frame, pending)? {
                    return Ok(Some(parent_pending));
                }
                child_index += 2;
            } else {
                child_index += 1;
            }
        }

        let children = self.read_interior_child_entries(page_id)?;
        if self.interior_page_matches_children(page_id, &children)? {
            return Ok(None);
        }

        if Self::interior_children_fit(&children) {
            let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
            self.rewrite_interior_page(page_id, &children, prev_page_id, next_page_id)?;
            return Ok(None);
        }

        self.split_existing_interior_page(page_id, &children).map(Some)
    }

    /// Runs post-delete rebalancing from the modified leaf toward the root.
    pub(super) fn rebalance_after_leaf_delete(
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
}
