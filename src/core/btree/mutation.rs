use super::payload::{cell_corruption, write_overflow_chain_from_slices};
use super::*;

impl TreeCursor {
    pub(super) fn checked_payload_len(&self, payload_len: usize) -> StorageResult<()> {
        if payload_len > u16::MAX as usize {
            return Err(PageError::CellTooLarge { len: payload_len, max: u16::MAX as usize }.into());
        }
        Ok(())
    }

    pub(super) fn leaf_cell_local_size(&self, key: &[u8], value: &[u8]) -> StorageResult<usize> {
        let payload_len = key.len() + value.len();
        self.checked_payload_len(payload_len)?;
        Ok(LEAF_CELL_PREFIX_SIZE + local_payload_len(payload_len))
    }

    pub(super) fn interior_cell_local_size(&self, key: &[u8]) -> StorageResult<usize> {
        self.checked_payload_len(key.len())?;
        Ok(INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len()))
    }

    /// Builds the local leaf payload bytes and optional overflow chain for one key/value cell.
    pub(super) fn prepare_leaf_payload(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<(Option<PageId>, Vec<u8>)> {
        let payload_len = key.len() + value.len();
        self.checked_payload_len(payload_len)?;
        let mut inline_payload = [0; MAX_INLINE_OVERFLOW_PAYLOAD_BYTES];

        let first_overflow_page_id = if payload_uses_overflow(payload_len) {
            if key.len() >= MAX_INLINE_OVERFLOW_PAYLOAD_BYTES {
                inline_payload.copy_from_slice(&key[..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES]);
                Some(
                    write_overflow_chain_from_slices(
                        &self.page_cache,
                        &key[MAX_INLINE_OVERFLOW_PAYLOAD_BYTES..],
                        value,
                    )?
                    .ok_or_else(|| {
                        cell_corruption(self.root_page_id(), CorruptionKind::CellLengthOutOfBounds)
                    })?,
                )
            } else {
                inline_payload[..key.len()].copy_from_slice(key);
                let value_prefix_len = MAX_INLINE_OVERFLOW_PAYLOAD_BYTES - key.len();
                inline_payload[key.len()..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES]
                    .copy_from_slice(&value[..value_prefix_len]);
                Some(
                    overflow::write_chain(&self.page_cache, &value[value_prefix_len..])?
                        .ok_or_else(|| {
                            cell_corruption(
                                self.root_page_id(),
                                CorruptionKind::CellLengthOutOfBounds,
                            )
                        })?,
                )
            }
        } else {
            inline_payload[..key.len()].copy_from_slice(key);
            inline_payload[key.len()..payload_len].copy_from_slice(value);
            None
        };
        let inline_payload = inline_payload[..local_payload_len(payload_len)].to_vec();
        Ok((first_overflow_page_id, inline_payload))
    }

    pub(super) fn insert_leaf_payload_at(
        &self,
        leaf: &mut RawLeaf<Write<'_>>,
        slot_index: u16,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<u16> {
        let (first_overflow_page_id, inline_payload) = self.prepare_leaf_payload(key, value)?;
        Ok(leaf.insert_payload_at(
            slot_index,
            key.len(),
            value.len(),
            first_overflow_page_id,
            &inline_payload,
        )?)
    }

    /// Rewrites one existing leaf slot using the same overflow layout as tree inserts.
    pub(super) fn update_leaf_payload_at(
        &self,
        leaf: &mut RawLeaf<Write<'_>>,
        slot_index: u16,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<u16> {
        let (first_overflow_page_id, inline_payload) = self.prepare_leaf_payload(key, value)?;
        Ok(leaf.update_payload_at(
            slot_index,
            key.len(),
            value.len(),
            first_overflow_page_id,
            &inline_payload,
        )?)
    }

    pub(super) fn insert_interior_payload_at(
        &self,
        interior: &mut RawInterior<Write<'_>>,
        slot_index: u16,
        left_child: PageId,
        key: &[u8],
    ) -> StorageResult<u16> {
        self.checked_payload_len(key.len())?;
        let (first_overflow_page_id, inline_payload): (Option<PageId>, &[u8]) =
            if payload_uses_overflow(key.len()) {
                (
                    Some(
                        overflow::write_chain(
                            &self.page_cache,
                            &key[MAX_INLINE_OVERFLOW_PAYLOAD_BYTES..],
                        )?
                        .ok_or_else(|| {
                            cell_corruption(
                                self.root_page_id(),
                                CorruptionKind::CellLengthOutOfBounds,
                            )
                        })?,
                    ),
                    &key[..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES],
                )
            } else {
                (None, key)
            };
        Ok(interior.insert_payload_at(
            slot_index,
            left_child,
            key.len(),
            first_overflow_page_id,
            inline_payload,
        )?)
    }

    pub(super) fn missing_child_max_key_error(page_id: PageId) -> StorageError {
        StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::InteriorPage,
            page_id: Some(page_id),
            kind: CorruptionKind::CellLengthOutOfBounds,
        })
    }

    /// Inserts a new raw key/value record into the tree.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, leaf_pin_guard, tree_path) = self.leaf_page_pin_path_for_key(key)?;
        let (slot_index, has_capacity, old_slot_count) = {
            let leaf_read_guard = leaf_pin_guard.read()?;
            let page = leaf_read_guard.open::<Leaf>()?;
            let slot_index = match self.search_leaf_slot_in_page(
                leaf_page_id,
                leaf_read_guard.page(),
                &page,
                key,
            )? {
                SearchResult::Found(_) => return Err(PageError::DuplicateKey.into()),
                SearchResult::InsertAt(slot_index) => slot_index,
            };
            let needed = self.leaf_cell_local_size(key, value)? + page::format::SLOT_ENTRY_SIZE;
            (slot_index, page.total_reclaimable_space()? >= needed, page.slot_count())
        };
        let mut leaf_guard = leaf_pin_guard.write()?;

        if has_capacity {
            let inserted_new_leaf_max;
            {
                let mut page = leaf_guard.open_mut::<Leaf>()?;
                let slot_index = self.insert_leaf_payload_at(&mut page, slot_index, key, value)?;
                self.set_positioned_state(leaf_page_id, slot_index);
                inserted_new_leaf_max = slot_index == old_slot_count;
            }
            drop(leaf_guard);
            drop(leaf_pin_guard);
            if inserted_new_leaf_max {
                self.refresh_insert_path_after_leaf_max_change(&tree_path, key)?;
            }
            return Ok(());
        }

        let pending =
            self.insert_with_leaf_page_split(leaf_page_id, &mut leaf_guard, key, value)?;
        drop(leaf_guard);
        drop(leaf_pin_guard);
        self.propagate_split(&tree_path, pending)?;
        self.refresh_path_separators(&tree_path)
    }

    /// Replaces the value stored for an existing `key`.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        let slot_index = match self.search_leaf_slot(leaf_page_id, key)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound.into()),
        };
        let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let has_capacity = {
            let leaf_read_guard = leaf_pin_guard.read()?;
            let page = leaf_read_guard.open::<Leaf>()?;
            let old_len = page.cell_len(slot_index)?;
            let needed = self.leaf_cell_local_size(key, value)?;
            page.total_reclaimable_space()? + old_len >= needed
        };
        let mut leaf_guard = leaf_pin_guard.write()?;

        if has_capacity {
            {
                let mut page = leaf_guard.open_mut::<Leaf>()?;
                let slot_index = self.update_leaf_payload_at(&mut page, slot_index, key, value)?;
                self.set_positioned_state(leaf_page_id, slot_index);
            }
            drop(leaf_guard);
            drop(leaf_pin_guard);
            self.refresh_path_separators(&tree_path)?;
            return Ok(());
        }

        let pending =
            self.update_with_leaf_page_split(leaf_page_id, &mut leaf_guard, slot_index, value)?;
        drop(leaf_guard);
        drop(leaf_pin_guard);
        self.propagate_split(&tree_path, pending)?;
        self.refresh_path_separators(&tree_path)
    }

    /// Deletes the record identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        {
            let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_guard = leaf_pin_guard.write()?;
            let mut page = leaf_guard.open_mut::<Leaf>()?;
            page.delete(key)?;
        }

        self.set_page_state(leaf_page_id);
        self.rebalance_after_leaf_delete(leaf_page_id, &tree_path)?;
        self.shrink_root_if_empty()?;
        self.refresh_subtree_separators()?;
        Ok(())
    }
}
