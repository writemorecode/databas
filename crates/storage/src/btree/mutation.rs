use crate::TxnId;
use crate::logical_undo::{Inverse, LogicalUndo};
use crate::page_cache::PageCache;
use crate::page_operation::{LeafOperation, LeafPayload};
use crate::page_recovery::PageRecovery;

use super::payload::cell_corruption;
use super::{
    CorruptionComponent, CorruptionError, CorruptionKind, INTERIOR_CELL_PREFIX_SIZE,
    LEAF_CELL_PREFIX_SIZE, Leaf, MAX_INLINE_OVERFLOW_PAYLOAD_BYTES, PageError, PageId, PathFrame,
    RawInterior, RawLeaf, SearchResult, StorageError, StorageResult, TreeCursor, Write,
    local_payload_len, overflow, page, payload_uses_overflow,
};

#[derive(Clone, Copy)]
enum Edit<'a> {
    Insert(&'a [u8]),
    Replace(&'a [u8]),
    Delete,
}

struct Deletion {
    leaf: PageId,
    path: Vec<PathFrame>,
    empty: bool,
}

impl TreeCursor {
    fn mutate(&mut self, key: &[u8], edit: Edit<'_>) -> StorageResult<()> {
        if let Some(txn_id) = self.txn_id {
            return self.logged_edit(txn_id, key, edit);
        }

        let state = self.state;
        let result = self.page_cache.clone().autocommit(|txn_id| {
            self.txn_id = Some(txn_id);
            let result = self.logged_edit(txn_id, key, edit);
            self.txn_id = None;
            result
        });
        if result.is_err() {
            self.state = state;
            self.mark_tree_mutated();
        }
        result
    }

    fn logged_edit(&mut self, txn_id: TxnId, key: &[u8], edit: Edit<'_>) -> StorageResult<()> {
        self.page_cache.validate_transaction_write(Some(txn_id))?;
        let root = self.root_page_id();
        let identity = self.root_recovery()?;
        if !self.page_cache.can_retain_structure(txn_id, identity.incarnation)? {
            self.apply_physical_edit(key, edit)?;
            return self.page_cache.validate_transaction_write(Some(txn_id));
        }

        let action = match edit {
            Edit::Insert(_) => Inverse::Delete,
            Edit::Replace(_) | Edit::Delete => {
                let value = self
                    .get(key)?
                    .ok_or(PageError::KeyNotFound)?
                    .with_value(|bytes| Box::<[u8]>::from(bytes))?;
                if matches!(edit, Edit::Delete) {
                    Inverse::Insert(value)
                } else {
                    Inverse::Replace(value)
                }
            }
        };
        if let Edit::Insert(value) | Edit::Replace(value) = edit {
            self.retain_leaf_split_if_needed(txn_id, key, value, matches!(edit, Edit::Replace(_)))?;
        }
        let boundary = self.page_cache.begin_action(txn_id)?;
        let leaf_to_rebalance = match edit {
            Edit::Delete => self.delete_preserving_searchability(key)?,
            _ => {
                self.apply_physical_edit(key, edit)?;
                None
            }
        };
        self.page_cache.complete_action(
            txn_id,
            boundary,
            Some(LogicalUndo { root, incarnation: identity.incarnation, key: key.into(), action }),
        )?;
        if let Some(leaf) = leaf_to_rebalance {
            self.retain_leaf_rebalance_if_needed(txn_id, leaf)?;
        }
        Ok(())
    }

    fn retain_leaf_split_if_needed(
        &mut self,
        txn_id: TxnId,
        key: &[u8],
        value: &[u8],
        replacing: bool,
    ) -> StorageResult<()> {
        let needed = self.leaf_cell_local_size(key, value)?;
        let (page_id, pin, path) = self.leaf_page_pin_path_for_key(key)?;
        {
            let guard = pin.read()?;
            let leaf = guard.open::<Leaf>()?;
            let space = leaf.total_reclaimable_space()?;
            let fits = match (
                replacing,
                self.search_leaf_slot_in_page(page_id, guard.page(), &leaf, key)?,
            ) {
                (true, SearchResult::Found(slot)) => needed <= space + leaf.cell_len(slot)?,
                (false, SearchResult::InsertAt(_)) => {
                    needed + page::format::SLOT_ENTRY_SIZE <= space
                }
                (true, SearchResult::InsertAt(_)) => return Err(PageError::KeyNotFound.into()),
                (false, SearchResult::Found(_)) => return Err(PageError::DuplicateKey.into()),
            };
            if fits {
                return Ok(());
            }
        }
        let boundary = self.page_cache.begin_action(txn_id)?;
        let pending = self.split_existing_leaf(page_id, &pin)?;
        drop(pin);
        self.propagate_split(&path, pending)?;
        self.refresh_path_separators(&path)?;
        self.page_cache.complete_action(txn_id, boundary, None)
    }

    fn retain_leaf_rebalance_if_needed(
        &mut self,
        txn_id: TxnId,
        leaf: PageId,
    ) -> StorageResult<()> {
        if !self.leaf_page_underoccupied(leaf)? {
            return Ok(());
        }
        let anchor =
            self.read_leaf_max_key(leaf)?.ok_or_else(|| Self::missing_child_max_key_error(leaf))?;
        let (found, path) = self.leaf_page_path_for_key(&anchor)?;
        if found != leaf {
            return Err(Self::missing_child_max_key_error(leaf));
        }
        let boundary = self.page_cache.begin_action(txn_id)?;
        self.repair_after_delete(leaf, &path)?;
        self.page_cache.complete_action(txn_id, boundary, None)
    }

    pub(crate) fn apply_inverse(
        cache: PageCache,
        txn_id: TxnId,
        inverse: &LogicalUndo,
    ) -> StorageResult<()> {
        let mut cursor = Self::new(cache, inverse.root)?.for_transaction(txn_id);
        cursor.root_recovery()?.require_incarnation(inverse.root, inverse.incarnation)?;
        match &inverse.action {
            Inverse::Delete => cursor.delete_preserving_searchability(&inverse.key).map(|_| ()),
            Inverse::Insert(value) => cursor.insert_inner(&inverse.key, value),
            Inverse::Replace(value) => cursor.update_inner(&inverse.key, value),
        }
    }

    fn root_recovery(&self) -> StorageResult<PageRecovery> {
        let pin = self.page_cache.fetch_page(self.root_page_id())?;
        Ok(PageRecovery::read(pin.read()?.page()))
    }

    fn apply_physical_edit(&mut self, key: &[u8], edit: Edit<'_>) -> StorageResult<()> {
        match edit {
            Edit::Insert(value) => self.insert_inner(key, value),
            Edit::Replace(value) => self.update_inner(key, value),
            Edit::Delete => self.delete_inner(key),
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

    pub(super) fn interior_cell_local_size(&self, key: &[u8]) -> StorageResult<usize> {
        self.checked_payload_len(key.len())?;
        Ok(INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len()))
    }

    /// Builds the local leaf payload bytes and optional overflow chain for one key/value cell.
    fn prepare_leaf_payload(
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
                    overflow::write_chain_from_slices(
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

    fn prepare_logged_leaf_payload(&self, key: &[u8], value: &[u8]) -> StorageResult<LeafPayload> {
        let (overflow, inline, len) = self.prepare_leaf_payload(key, value)?;
        Ok(LeafPayload::new(key.len(), value.len(), overflow, &inline[..len])?)
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
        self.mutate(key, Edit::Insert(value))
    }

    fn insert_inner(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
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
                let cell = self.prepare_logged_leaf_payload(key, value)?;
                leaf_guard.apply_leaf(LeafOperation::Insert { slot: slot_index, cell })?;
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
        self.mutate(key, Edit::Replace(value))
    }

    fn update_inner(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
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
                let old_overflow = leaf_guard.open_mut::<Leaf>()?.cell_payload_parts(slot_index)?.2;
                let cell = self.prepare_logged_leaf_payload(key, value)?;
                leaf_guard.apply_leaf(LeafOperation::Replace { slot: slot_index, cell })?;
                super::reclaim::reclaim_chains(
                    &self.page_cache,
                    self.txn_id,
                    old_overflow.into_iter().collect(),
                )?;
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
        self.mutate(key, Edit::Delete)
    }

    fn delete_inner(&mut self, key: &[u8]) -> StorageResult<()> {
        let deleted = self.delete_row(key)?;
        self.repair_after_delete(deleted.leaf, &deleted.path)
    }

    fn delete_preserving_searchability(&mut self, key: &[u8]) -> StorageResult<Option<PageId>> {
        let Deletion { leaf, path, empty } = self.delete_row(key)?;
        if empty && !path.is_empty() {
            self.repair_after_delete(leaf, &path)?;
            return Ok(None);
        }
        self.refresh_subtree_separators()?;
        Ok((!path.is_empty()).then_some(leaf))
    }

    fn delete_row(&mut self, key: &[u8]) -> StorageResult<Deletion> {
        let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
        let slot_index = match self.search_leaf_slot(leaf_page_id, key)? {
            SearchResult::Found(slot_index) => slot_index,
            SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound.into()),
        };
        let (old_overflow, empty) = {
            let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_guard = leaf_pin_guard.write(self.txn_id)?;
            let leaf = leaf_guard.open_mut::<Leaf>()?;
            let old_overflow = leaf.cell_payload_parts(slot_index)?.2;
            let empty = leaf.slot_count() == 1;
            leaf_guard.apply_leaf(LeafOperation::Delete { slot: slot_index })?;
            self.mark_tree_mutated();
            (old_overflow, empty)
        };
        super::reclaim::reclaim_chains(
            &self.page_cache,
            self.txn_id,
            old_overflow.into_iter().collect(),
        )?;

        self.set_page_state(leaf_page_id);
        Ok(Deletion { leaf: leaf_page_id, path: tree_path, empty })
    }

    fn repair_after_delete(&mut self, leaf: PageId, path: &[PathFrame]) -> StorageResult<()> {
        self.rebalance_after_leaf_delete(leaf, path)?;
        self.shrink_root_if_empty()?;
        self.refresh_subtree_separators()
    }
}
