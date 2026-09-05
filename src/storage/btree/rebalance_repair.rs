//! Root repair and separator maintenance after structural B+-tree changes.

use super::root::read_page_kind;
use super::*;

impl TreeCursor {
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

        let mut root_guard = pin.write(self.txn_id)?;
        *root_guard.page_mut() = child_snapshot;
        drop(root_guard);

        self.clear_root_sibling_links(root_page_id)?;
        self.set_page_state(root_page_id);
        Ok(())
    }

    fn clear_root_sibling_links(&self, root_page_id: PageId) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(root_page_id)?;
        let mut guard = pin.write(self.txn_id)?;
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
    fn refresh_subtree_separators_once(
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
            let slot_count = self.raw_interior_slot_count(page_id)?;
            let child_count = usize::from(slot_count) + 1;
            if child_index >= child_count {
                break;
            }
            let child_page_id = self.read_interior_child_page_id(page_id, child_index)?;

            if let Some(pending) = self.refresh_subtree_separators_once(child_page_id)? {
                let child_ref = if child_index + 1 == child_count {
                    ChildSlotRef::Rightmost
                } else {
                    let slot_index = u16::try_from(child_index).map_err(|_out_of_range| {
                        PageError::InvalidSlotIndex { slot_index: u16::MAX, slot_count }
                    })?;
                    ChildSlotRef::Slot(slot_index)
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

        if self.interior_page_matches_child_max_keys(page_id)? {
            return Ok(None);
        }

        let children = self.read_interior_child_entries(page_id)?;
        if Self::interior_children_fit(&children) {
            let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
            self.rewrite_interior_page(page_id, &children, prev_page_id, next_page_id)?;
            return Ok(None);
        }

        self.split_existing_interior_page(page_id, &children).map(Some)
    }
}
