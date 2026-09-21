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
    ) -> StorageResult<(Option<PageId>, [u8; MAX_INLINE_OVERFLOW_PAYLOAD_BYTES], usize)> {
        let payload_len = key.len() + value.len();
        self.checked_payload_len(payload_len)?;
        let mut inline_payload = [0; MAX_INLINE_OVERFLOW_PAYLOAD_BYTES];

        let first_overflow_page_id = if payload_uses_overflow(payload_len) {
            if key.len() >= MAX_INLINE_OVERFLOW_PAYLOAD_BYTES {
                inline_payload.copy_from_slice(&key[..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES]);
                Some(
                    write_overflow_chain_from_slices(
                        &self.page_cache,
                        self.txn_id,
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
                    overflow::write_chain(
                        &self.page_cache,
                        self.txn_id,
                        &value[value_prefix_len..],
                    )?
                    .ok_or_else(|| {
                        cell_corruption(self.root_page_id(), CorruptionKind::CellLengthOutOfBounds)
                    })?,
                )
            }
        } else {
            inline_payload[..key.len()].copy_from_slice(key);
            inline_payload[key.len()..payload_len].copy_from_slice(value);
            None
        };
        Ok((first_overflow_page_id, inline_payload, local_payload_len(payload_len)))
    }

    pub(super) fn insert_leaf_payload_at(
        &self,
        leaf: &mut RawLeaf<Write<'_>>,
        slot_index: u16,
        key: &[u8],
        value: &[u8],
    ) -> StorageResult<u16> {
        let (first_overflow_page_id, inline_payload, inline_payload_len) =
            self.prepare_leaf_payload(key, value)?;
        Ok(leaf.insert_payload_at(
            slot_index,
            key.len(),
            value.len(),
            first_overflow_page_id,
            &inline_payload[..inline_payload_len],
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
        let (first_overflow_page_id, inline_payload, inline_payload_len) =
            self.prepare_leaf_payload(key, value)?;
        Ok(leaf.update_payload_at(
            slot_index,
            key.len(),
            value.len(),
            first_overflow_page_id,
            &inline_payload[..inline_payload_len],
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
                            self.txn_id,
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
        if has_capacity {
            let inserted_new_leaf_max;
            {
                let mut leaf_guard = leaf_pin_guard.write(self.txn_id)?;
                let mut page = leaf_guard.open_mut::<Leaf>()?;
                let slot_index = self.insert_leaf_payload_at(&mut page, slot_index, key, value)?;
                self.mark_tree_mutated();
                self.set_positioned_state(leaf_page_id, slot_index);
                inserted_new_leaf_max = slot_index == old_slot_count;
            }
            drop(leaf_pin_guard);
            if inserted_new_leaf_max {
                self.refresh_insert_path_after_leaf_max_change(&tree_path, key)?;
            }
            return Ok(());
        }

        let pending =
            self.insert_with_leaf_page_split(leaf_page_id, &leaf_pin_guard, key, value)?;
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

        if has_capacity {
            {
                let mut leaf_guard = leaf_pin_guard.write(self.txn_id)?;
                let mut page = leaf_guard.open_mut::<Leaf>()?;
                let slot_index = self.update_leaf_payload_at(&mut page, slot_index, key, value)?;
                self.mark_tree_mutated();
                self.set_positioned_state(leaf_page_id, slot_index);
            }
            drop(leaf_pin_guard);
            self.refresh_path_separators(&tree_path)?;
            return Ok(());
        }

        let pending =
            self.update_with_leaf_page_split(leaf_page_id, &leaf_pin_guard, slot_index, value)?;
        drop(leaf_pin_guard);
        self.propagate_split(&tree_path, pending)?;
        self.refresh_path_separators(&tree_path)
    }

    /// Deletes the record identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        {
            let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_guard = leaf_pin_guard.write(self.txn_id)?;
            let mut page = leaf_guard.open_mut::<Leaf>()?;
            page.delete(key)?;
            self.mark_tree_mutated();
        }

        self.set_page_state(leaf_page_id);
        self.rebalance_after_leaf_delete(leaf_page_id, &tree_path)?;
        self.shrink_root_if_empty()?;
        self.refresh_subtree_separators()?;
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::collections::BTreeMap;

    use fastrand::Rng;

    use super::*;
    use crate::storage::btree::test_support::{
        assert_matches, cursor, random_bytes, random_key, random_record, record_bytes,
    };

    #[test]
    fn mutations_advance_the_shared_epoch_only_when_successful() {
        let mut cursor = cursor(32);
        let observer = TreeCursor::new(cursor.page_cache.clone(), cursor.root_page_id()).unwrap();
        let oversized = vec![0; u16::MAX as usize];

        for mutation in [
            cursor.update(b"missing", b"value"),
            cursor.delete(b"missing"),
            cursor.insert(b"oversized", &oversized),
        ] {
            assert!(mutation.is_err());
            assert_eq!(observer.mutation_epoch(), 0);
        }

        cursor.insert(b"key", b"value").unwrap();
        assert_eq!(observer.mutation_epoch(), 1);
        for mutation in [cursor.insert(b"key", b"duplicate"), cursor.update(b"key", &oversized)] {
            assert!(mutation.is_err());
            assert_eq!(observer.mutation_epoch(), 1);
        }

        cursor.update(b"key", b"updated").unwrap();
        assert_eq!(observer.mutation_epoch(), 2);
        cursor.delete(b"key").unwrap();
        assert_eq!(observer.mutation_epoch(), 3);
    }

    #[ignore = "slow because of fsync"]
    #[test]
    fn random_updates_replace_inline_and_overflow_values() {
        let mut cursor = cursor(256);
        let mut rng = Rng::with_seed(0xa11d_47e5_2026_0425);
        let mut model = BTreeMap::new();

        for index in 0..200 {
            let (key, mut value) = random_record(&mut rng, &model);
            if index % 9 == 0 {
                let len = PAGE_SIZE + rng.usize(1..=PAGE_SIZE);
                value = random_bytes(&mut rng, len);
            }
            cursor.insert(&key, &value).unwrap();
            model.insert(key, value);
        }

        let mut keys = model.keys().cloned().collect::<Vec<_>>();
        rng.shuffle(&mut keys);
        for (index, key) in keys.into_iter().enumerate() {
            let len = if index % 9 == 0 {
                PAGE_SIZE + rng.usize(1..=PAGE_SIZE)
            } else {
                rng.usize(8..=512)
            };
            let old_value = &model[&key];
            let value = loop {
                let candidate = random_bytes(&mut rng, len);
                if candidate != *old_value {
                    break candidate;
                }
            };
            cursor.update(&key, &value).unwrap();
            model.insert(key, value);
        }

        assert!(model.values().any(|value| value.len() > PAGE_SIZE));
        assert_matches(&mut cursor, &model);
    }

    #[ignore = "slow because of fsync"]
    #[test]
    fn random_mixed_operations_match_a_btreemap_model() {
        let mut cursor = cursor(256);
        let mut rng = Rng::with_seed(0x741e_5afe_2026_0429);
        let mut model = BTreeMap::new();

        for step in 0..3_000 {
            match if model.len() < 128 { 0 } else { rng.u8(0..100) } {
                0..=34 => {
                    let (key, mut value) = random_record(&mut rng, &model);
                    if step % 31 == 0 {
                        let len = PAGE_SIZE + rng.usize(1..=PAGE_SIZE);
                        value = random_bytes(&mut rng, len);
                    }
                    cursor.insert(&key, &value).unwrap();
                    model.insert(key, value);
                }
                35..=64 if !model.is_empty() && !rng.bool() => {
                    let key = random_key(&mut rng, &model);
                    let record = cursor.get(key).unwrap().unwrap();
                    assert_eq!(record_bytes(&record).1, model[key]);
                }
                35..=64 => {
                    let (key, _) = random_record(&mut rng, &model);
                    assert!(cursor.get(&key).unwrap().is_none());
                }
                65..=84 => {
                    let key = random_key(&mut rng, &model).clone();
                    let len = if step % 31 == 0 {
                        PAGE_SIZE + rng.usize(1..=PAGE_SIZE)
                    } else {
                        rng.usize(8..=512)
                    };
                    let old_value = &model[&key];
                    let value = loop {
                        let candidate = random_bytes(&mut rng, len);
                        if candidate != *old_value {
                            break candidate;
                        }
                    };
                    cursor.update(&key, &value).unwrap();
                    model.insert(key, value);
                }
                _ => {
                    let key = random_key(&mut rng, &model).clone();
                    cursor.delete(&key).unwrap();
                    model.remove(&key);
                }
            }

            if step % 50 == 0 {
                assert_matches(&mut cursor, &model);
            }
        }
        assert_matches(&mut cursor, &model);
    }
}
