use std::path::Path;

use crate::{
    disk_manager::DiskManager,
    error::{LimitExceededError, StorageError},
    page_cache::{PageCache, PinGuard},
    table_page::{
        TableInteriorPageMut, TableLeafPageMut, TableLeafPageRef, TablePageCorruptionKind,
        TablePageError, TablePageRef,
    },
    types::PAGE_SIZE,
};

pub type PageId = u64;
pub type RowId = u64;

const DEFAULT_PAGE_CACHE_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BTree {
    root_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordLocation {
    page_id: PageId,
    slot_id: u16,
    key: RowId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BTreePathEntry {
    page_id: PageId,
    child_index: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeafEntry {
    row_id: RowId,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildSplitEvent {
    separator_key: RowId,
    left_child_page_id: PageId,
    right_child_page_id: PageId,
}

pub struct Engine {
    pub(crate) page_cache: PageCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef<'a> {
    pub key: RowId,
    pub value: &'a [u8],
}

pub struct RecordGuard<'tree> {
    guard: PinGuard<'tree>,
    slot_id: u16,
}

impl<'tree> RecordGuard<'tree> {
    pub fn key(&self) -> Result<RowId, StorageError> {
        Ok(TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?.row_id)
    }

    pub fn value(&self) -> Result<&[u8], StorageError> {
        Ok(TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?.payload)
    }

    pub fn record(&self) -> Result<RecordRef<'_>, StorageError> {
        let cell = TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?;
        Ok(RecordRef { key: cell.row_id, value: cell.payload })
    }
}

pub struct BTreeHandle<'engine> {
    engine: &'engine mut Engine,
    tree: BTree,
}

pub struct BTreeCursor<'tree> {
    engine: &'tree mut Engine,
    tree: BTree,
    position: Option<RecordLocation>,
}

impl Engine {
    pub fn new(file: &Path) -> Result<Self, StorageError> {
        let disk_manager = DiskManager::new(file)?;
        let page_cache = PageCache::new(disk_manager, DEFAULT_PAGE_CACHE_SIZE)?;
        Ok(Self { page_cache })
    }

    pub fn open_btree(&mut self, root_page_id: PageId) -> Result<BTreeHandle<'_>, StorageError> {
        let tree = BTree { root_page_id };
        Ok(BTreeHandle { engine: self, tree })
    }

    pub fn create_btree(&mut self) -> Result<BTreeHandle<'_>, StorageError> {
        let root_page_id = {
            let (page_id, mut page_guard) = self.page_cache.new_page()?;
            TableLeafPageMut::init_empty(page_guard.page_mut())?;
            page_id
        };

        self.open_btree(root_page_id)
    }

    fn btree_find_leaf_page_for_row_id(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<PageId, StorageError> {
        let (leaf_page_id, _) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, row_id)?;
        Ok(leaf_page_id)
    }

    fn btree_find_leaf_page_and_path_for_row_id(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<(PageId, Vec<BTreePathEntry>), StorageError> {
        let mut current_page_id = root_page_id;
        let mut path = Vec::new();

        let leaf_page_id = loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => break current_page_id,
                TablePageRef::Interior(interior) => {
                    let child_index = interior.child_index_for_row_id(row_id)?;
                    let child_page_id = interior.child_at(child_index)?;
                    path.push(BTreePathEntry { page_id: current_page_id, child_index });
                    current_page_id = child_page_id;
                }
            }
        };

        Ok((leaf_page_id, path))
    }

    fn read_leaf_entries_from_page(
        &mut self,
        leaf_page_id: PageId,
    ) -> Result<Vec<LeafEntry>, StorageError> {
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
        let mut out = Vec::with_capacity(usize::from(leaf_page.cell_count()));

        for slot_id in 0..leaf_page.cell_count() {
            let cell = leaf_page.cell_at_slot(slot_id)?;
            out.push(LeafEntry { row_id: cell.row_id, payload: cell.payload.to_vec() });
        }

        Ok(out)
    }

    fn insert_entry_into_leaf_entries(
        entries: &mut Vec<LeafEntry>,
        row_id: RowId,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
            Ok(_) => Err(TablePageError::DuplicateRowId { row_id }.into()),
            Err(insert_index) => {
                entries.insert(insert_index, LeafEntry { row_id, payload: payload.to_vec() });
                Ok(())
            }
        }
    }

    fn leaf_entries_fit_in_single_page(entries: &[LeafEntry]) -> Result<bool, StorageError> {
        let mut page = [0u8; PAGE_SIZE];
        let mut leaf_page = TableLeafPageMut::init_empty(&mut page)?;
        for entry in entries {
            match leaf_page.insert(entry.row_id, &entry.payload) {
                Ok(()) => {}
                Err(TablePageError::PageFull { .. }) => return Ok(false),
                Err(err) => return Err(err.into()),
            }
        }
        Ok(true)
    }

    fn choose_leaf_split_index(entries: &[LeafEntry]) -> Result<Option<usize>, StorageError> {
        if entries.len() < 2 {
            return Ok(None);
        }

        let midpoint = entries.len() / 2;
        let mut candidate_indices: Vec<usize> = (1..entries.len()).collect();
        candidate_indices.sort_by_key(|index| {
            let distance = if *index >= midpoint { *index - midpoint } else { midpoint - *index };
            (distance, *index)
        });

        for split_index in candidate_indices {
            let left_fits = Self::leaf_entries_fit_in_single_page(&entries[..split_index])?;
            if !left_fits {
                continue;
            }
            let right_fits = Self::leaf_entries_fit_in_single_page(&entries[split_index..])?;
            if right_fits {
                return Ok(Some(split_index));
            }
        }

        Ok(None)
    }

    fn write_leaf_entries_to_page_bytes(
        page: &mut [u8; PAGE_SIZE],
        entries: &[LeafEntry],
    ) -> Result<(), StorageError> {
        let mut leaf_page = TableLeafPageMut::init_empty(page)?;
        for entry in entries {
            leaf_page.insert(entry.row_id, &entry.payload)?;
        }
        Ok(())
    }

    fn apply_child_split_to_parent_no_split(
        parent_page: &mut TableInteriorPageMut<'_>,
        child_index: u16,
        split_event: ChildSplitEvent,
    ) -> Result<(), StorageError> {
        let cell_count = parent_page.cell_count();
        if child_index > cell_count {
            return Err(
                TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds).into()
            );
        }

        if child_index < cell_count {
            let existing_separator = parent_page.cell_at_slot(child_index)?.row_id;
            parent_page.insert(split_event.separator_key, split_event.left_child_page_id)?;
            parent_page.update(existing_separator, split_event.right_child_page_id)?;
            return Ok(());
        }

        parent_page.insert(split_event.separator_key, split_event.left_child_page_id)?;
        parent_page.set_rightmost_child(split_event.right_child_page_id)?;
        Ok(())
    }

    fn preflight_parent_insert_for_child_split(
        &mut self,
        parent_page_id: PageId,
        child_index: u16,
        split_event: ChildSplitEvent,
    ) -> Result<(), StorageError> {
        let mut parent_copy = {
            let parent_guard = self.page_cache.fetch_page(parent_page_id)?;
            *parent_guard.page()
        };

        let mut parent_page = TableInteriorPageMut::from_bytes(&mut parent_copy)?;
        Self::apply_child_split_to_parent_no_split(&mut parent_page, child_index, split_event)
    }

    fn btree_insert_with_leaf_split_step1(
        &mut self,
        root_page_id: PageId,
        leaf_page_id: PageId,
        path: Vec<BTreePathEntry>,
        row_id: RowId,
        value: &[u8],
        original_page_full_error: TablePageError,
    ) -> Result<(), StorageError> {
        let mut entries = self.read_leaf_entries_from_page(leaf_page_id)?;
        Self::insert_entry_into_leaf_entries(&mut entries, row_id, value)?;

        let split_index = match Self::choose_leaf_split_index(&entries)? {
            Some(split_index) => split_index,
            None => return Err(original_page_full_error.into()),
        };

        let left_entries = &entries[..split_index];
        let right_entries = &entries[split_index..];
        let split_key = right_entries[0].row_id;

        if path.is_empty() {
            let left_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_leaf_entries_to_page_bytes(page_guard.page_mut(), left_entries)?;
                page_id
            };

            let right_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_leaf_entries_to_page_bytes(page_guard.page_mut(), right_entries)?;
                page_id
            };

            let mut root_page_guard = self.page_cache.fetch_page(root_page_id)?;
            let mut root_page =
                TableInteriorPageMut::init_empty(root_page_guard.page_mut(), right_page_id)?;
            root_page.insert(split_key, left_page_id)?;
            return Ok(());
        }

        let parent = path[path.len() - 1];
        let preflight_split_event = ChildSplitEvent {
            separator_key: split_key,
            left_child_page_id: leaf_page_id,
            right_child_page_id: 0,
        };
        match self.preflight_parent_insert_for_child_split(
            parent.page_id,
            parent.child_index,
            preflight_split_event,
        ) {
            Ok(()) => {}
            Err(StorageError::LimitExceeded(LimitExceededError::PageFull {
                needed,
                available,
            })) => {
                return Err(StorageError::LimitExceeded(LimitExceededError::PageFull {
                    needed,
                    available,
                }));
            }
            Err(err) => return Err(err),
        }

        let right_page_id = {
            let (page_id, mut page_guard) = self.page_cache.new_page()?;
            Self::write_leaf_entries_to_page_bytes(page_guard.page_mut(), right_entries)?;
            page_id
        };

        {
            let mut left_page_guard = self.page_cache.fetch_page(leaf_page_id)?;
            Self::write_leaf_entries_to_page_bytes(left_page_guard.page_mut(), left_entries)?;
        }

        let split_event = ChildSplitEvent {
            separator_key: split_key,
            left_child_page_id: leaf_page_id,
            right_child_page_id: right_page_id,
        };
        let mut parent_page_guard = self.page_cache.fetch_page(parent.page_id)?;
        let mut parent_page = TableInteriorPageMut::from_bytes(parent_page_guard.page_mut())?;
        Self::apply_child_split_to_parent_no_split(
            &mut parent_page,
            parent.child_index,
            split_event,
        )
    }

    fn btree_search<'a>(
        &'a mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<Option<RecordGuard<'a>>, StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let page = TableLeafPageRef::from_bytes(page_guard.page())?;
        match page.search_for_slot_id(row_id)? {
            Some(slot_id) => Ok(Some(RecordGuard { guard: page_guard, slot_id })),
            None => Ok(None),
        }
    }

    fn btree_insert(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let (leaf_page_id, path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, row_id)?;
        let insert_result = {
            let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
            leaf_page.insert(row_id, value)
        };

        match insert_result {
            Ok(()) => Ok(()),
            Err(err @ TablePageError::PageFull { .. }) => self.btree_insert_with_leaf_split_step1(
                root_page_id,
                leaf_page_id,
                path,
                row_id,
                value,
                err,
            ),
            Err(err) => Err(err.into()),
        }
    }

    fn btree_update(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
        leaf_page.update(row_id, value)?;
        Ok(())
    }

    fn btree_delete(&mut self, root_page_id: PageId, row_id: RowId) -> Result<bool, StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
        match leaf_page.delete(row_id) {
            Ok(()) => Ok(true),
            Err(TablePageError::RowIdNotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn btree_collect_locations(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Vec<RecordLocation>, StorageError> {
        let mut out = Vec::new();
        self.btree_collect_locations_in_order(root_page_id, &mut out)?;
        Ok(out)
    }

    fn btree_collect_locations_in_order(
        &mut self,
        page_id: PageId,
        out: &mut Vec<RecordLocation>,
    ) -> Result<(), StorageError> {
        let child_page_ids = {
            let page_guard = self.page_cache.fetch_page(page_id)?;
            let page = TablePageRef::from_bytes(page_guard.page())?;

            match page {
                TablePageRef::Leaf(leaf_page) => {
                    for slot_id in 0..leaf_page.cell_count() {
                        let cell = leaf_page.cell_at_slot(slot_id)?;
                        out.push(RecordLocation { page_id, slot_id, key: cell.row_id });
                    }
                    return Ok(());
                }
                TablePageRef::Interior(interior_page) => {
                    let mut page_ids = Vec::with_capacity(usize::from(interior_page.child_count()));
                    for child_index in 0..interior_page.child_count() {
                        page_ids.push(interior_page.child_at(child_index)?);
                    }
                    page_ids
                }
            }
        };

        for child_page_id in child_page_ids {
            self.btree_collect_locations_in_order(child_page_id, out)?;
        }

        Ok(())
    }

    fn btree_seek_first_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        Ok(self.btree_collect_locations(root_page_id)?.into_iter().next())
    }

    fn btree_seek_last_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        Ok(self.btree_collect_locations(root_page_id)?.into_iter().last())
    }

    fn btree_seek_ge_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let locations = self.btree_collect_locations(root_page_id)?;
        let index = match locations.binary_search_by_key(&key, |location| location.key) {
            Ok(index) | Err(index) => index,
        };
        Ok(locations.get(index).copied())
    }

    fn btree_seek_gt_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let locations = self.btree_collect_locations(root_page_id)?;
        let mut index = match locations.binary_search_by_key(&key, |location| location.key) {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        while index < locations.len() && locations[index].key <= key {
            index += 1;
        }
        Ok(locations.get(index).copied())
    }

    fn btree_seek_le_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let locations = self.btree_collect_locations(root_page_id)?;
        let index = match locations.binary_search_by_key(&key, |location| location.key) {
            Ok(index) => Some(index),
            Err(0) => None,
            Err(index) => Some(index - 1),
        };
        Ok(index.and_then(|index| locations.get(index)).copied())
    }

    fn btree_seek_lt_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let locations = self.btree_collect_locations(root_page_id)?;
        let index = match locations.binary_search_by_key(&key, |location| location.key) {
            Ok(0) | Err(0) => None,
            Ok(index) | Err(index) => Some(index - 1),
        };
        Ok(index.and_then(|index| locations.get(index)).copied())
    }
}

impl<'engine> BTreeHandle<'engine> {
    pub fn root_page_id(&self) -> PageId {
        self.tree.root_page_id
    }

    pub fn get(&mut self, key: RowId) -> Result<Option<RecordGuard<'_>>, StorageError> {
        self.engine.btree_search(self.tree.root_page_id, key)
    }

    pub fn contains(&mut self, key: RowId) -> Result<bool, StorageError> {
        Ok(self.engine.btree_search(self.tree.root_page_id, key)?.is_some())
    }

    pub fn insert(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        self.engine.btree_insert(self.tree.root_page_id, key, value)
    }

    pub fn update(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        self.engine.btree_update(self.tree.root_page_id, key, value)
    }

    pub fn upsert(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        if self.contains(key)? { self.update(key, value) } else { self.insert(key, value) }
    }

    pub fn delete(&mut self, key: RowId) -> Result<bool, StorageError> {
        self.engine.btree_delete(self.tree.root_page_id, key)
    }

    pub fn cursor(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        Ok(BTreeCursor::new(self.engine, self.tree))
    }

    pub fn seek_first(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_first()?;
        Ok(cursor)
    }

    pub fn seek_last(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_last()?;
        Ok(cursor)
    }

    pub fn seek(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek(key)?;
        Ok(cursor)
    }

    pub fn seek_ge(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_ge(key)?;
        Ok(cursor)
    }

    pub fn seek_gt(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_gt(key)?;
        Ok(cursor)
    }

    pub fn seek_le(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_le(key)?;
        Ok(cursor)
    }

    pub fn seek_lt(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_lt(key)?;
        Ok(cursor)
    }
}

impl<'tree> BTreeCursor<'tree> {
    fn new(engine: &'tree mut Engine, tree: BTree) -> Self {
        Self { engine, tree, position: None }
    }

    pub fn is_valid(&self) -> bool {
        self.position.is_some()
    }

    pub fn key(&self) -> Result<Option<RowId>, StorageError> {
        Ok(self.position.map(|position| position.key))
    }

    pub fn record(&mut self) -> Result<Option<RecordGuard<'_>>, StorageError> {
        let Some(position) = self.position else {
            return Ok(None);
        };

        let page_guard = self.engine.page_cache.fetch_page(position.page_id)?;
        Ok(Some(RecordGuard { guard: page_guard, slot_id: position.slot_id }))
    }

    pub fn seek_first(&mut self) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_first_location(self.tree.root_page_id)?;
        Ok(())
    }

    pub fn seek_last(&mut self) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_last_location(self.tree.root_page_id)?;
        Ok(())
    }

    pub fn seek(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, key)?;
        if self.position.map(|position| position.key) != Some(key) {
            self.position = None;
        }
        Ok(())
    }

    pub fn seek_ge(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_gt(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_gt_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_le(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_le_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_lt(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_lt_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn next_position(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.position = self.engine.btree_seek_gt_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn prev_position(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.position = self.engine.btree_seek_lt_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn delete_current(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        let _deleted = self.engine.btree_delete(self.tree.root_page_id, position.key)?;
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn update_current(&mut self, value: &[u8]) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.engine.btree_update(self.tree.root_page_id, position.key, value)?;
        self.position = self.engine.btree_seek(position.key, self.tree.root_page_id)?;
        Ok(())
    }
}

impl Engine {
    fn btree_seek(
        &mut self,
        key: RowId,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let location = self.btree_seek_ge_location(root_page_id, key)?;
        Ok(location.filter(|location| location.key == key))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use tempfile::NamedTempFile;

    use crate::{
        error::{InvalidArgumentError, LimitExceededError, StorageError},
        table_page::TableInteriorPageMut,
        types::PAGE_SIZE,
    };

    use super::*;

    fn get_temp_engine() -> Engine {
        let file = NamedTempFile::new().expect("temp file");
        let disk_manager = DiskManager::new(file.path()).expect("disk manager");
        let page_cache = PageCache::new(disk_manager, 8).expect("page cache");
        Engine { page_cache }
    }

    fn expect_payload(record: Option<RecordGuard<'_>>, expected: Option<&[u8]>) {
        match (record, expected) {
            (Some(record), Some(expected)) => assert_eq!(record.value().unwrap(), expected),
            (None, None) => {}
            _ => panic!("record/payload mismatch"),
        }
    }

    fn fixed_payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    fn fill_leaf_until_page_full(
        engine: &mut Engine,
        page_id: PageId,
        start_key: RowId,
        payload: &[u8],
    ) -> RowId {
        let mut page_guard = engine.page_cache.fetch_page(page_id).unwrap();
        let mut leaf = TableLeafPageMut::from_bytes(page_guard.page_mut()).unwrap();
        let mut next_key = start_key;
        loop {
            match leaf.insert(next_key, payload) {
                Ok(()) => next_key += 1,
                Err(TablePageError::PageFull { .. }) => return next_key,
                Err(err) => panic!("unexpected leaf insert error while filling page: {err:?}"),
            }
        }
    }

    #[test]
    fn point_ops_match_expected_semantics() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();

        assert!(!tree.contains(10).unwrap());
        assert!(!tree.delete(10).unwrap());

        let missing_update = tree.update(10, &[1]).unwrap_err();
        assert!(matches!(
            missing_update,
            StorageError::InvalidArgument(InvalidArgumentError::RowIdNotFound { row_id: 10 })
        ));

        tree.insert(10, &[1, 2]).unwrap();
        assert!(tree.contains(10).unwrap());
        expect_payload(tree.get(10).unwrap(), Some(&[1, 2]));

        let duplicate_insert = tree.insert(10, &[3]).unwrap_err();
        assert!(matches!(duplicate_insert, StorageError::Constraint(..)));

        tree.update(10, &[9, 9, 9]).unwrap();
        expect_payload(tree.get(10).unwrap(), Some(&[9, 9, 9]));

        tree.upsert(10, &[4]).unwrap();
        tree.upsert(11, &[5, 6]).unwrap();
        expect_payload(tree.get(10).unwrap(), Some(&[4]));
        expect_payload(tree.get(11).unwrap(), Some(&[5, 6]));

        assert!(tree.delete(10).unwrap());
        assert!(!tree.delete(10).unwrap());
        expect_payload(tree.get(10).unwrap(), None);
    }

    #[test]
    fn cursor_seek_and_movement_behave_as_expected() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        for key in [10, 20, 30] {
            tree.insert(key, &[key as u8]).unwrap();
        }

        let mut cursor = tree.seek_first().unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), Some(10));
        assert_eq!(cursor.record().unwrap().unwrap().value().unwrap(), &[10]);

        cursor.next_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.next_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.next_position().unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), None);

        cursor.seek_last().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.prev_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.prev_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));
        cursor.prev_position().unwrap();
        assert!(!cursor.is_valid());

        cursor.seek_ge(25).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.seek_gt(30).unwrap();
        assert!(!cursor.is_valid());
        cursor.seek_le(20).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.seek_lt(20).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));
        cursor.seek(999).unwrap();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn empty_tree_cursor_is_invalid() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();

        let mut cursor = tree.seek_first().unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), None);
        assert!(cursor.record().unwrap().is_none());

        cursor.next_position().unwrap();
        cursor.prev_position().unwrap();
        cursor.delete_current().unwrap();
        cursor.update_current(&[1]).unwrap();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn cursor_delete_and_update_current() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        tree.insert(1, &[1]).unwrap();
        tree.insert(2, &[2]).unwrap();
        tree.insert(3, &[3]).unwrap();

        let mut cursor = tree.seek_ge(2).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(2));

        cursor.update_current(&[8, 8]).unwrap();
        assert_eq!(cursor.record().unwrap().unwrap().value().unwrap(), &[8, 8]);

        cursor.delete_current().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(3));
    }

    #[test]
    fn traversal_and_lookup_work_across_multiple_leaf_pages() {
        let mut engine = get_temp_engine();

        let left_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(1, &[1]).unwrap();
            leaf.insert(2, &[2]).unwrap();
            page_id
        };

        let right_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(10, &[10]).unwrap();
            leaf.insert(20, &[20]).unwrap();
            page_id
        };

        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut interior =
                TableInteriorPageMut::init_empty(page_guard.page_mut(), right_leaf).unwrap();
            interior.insert(10, left_leaf).unwrap();
            page_id
        };

        let mut tree = engine.open_btree(root_page_id).unwrap();
        expect_payload(tree.get(1).unwrap(), Some(&[1]));
        expect_payload(tree.get(10).unwrap(), Some(&[10]));
        expect_payload(tree.get(20).unwrap(), Some(&[20]));
        expect_payload(tree.get(99).unwrap(), None);

        let mut cursor = tree.seek_first().unwrap();
        let mut keys = Vec::new();
        while cursor.is_valid() {
            keys.push(cursor.key().unwrap().unwrap());
            cursor.next_position().unwrap();
        }
        assert_eq!(keys, vec![1, 2, 10, 20]);

        tree.insert(15, &[15]).unwrap();
        expect_payload(tree.get(15).unwrap(), Some(&[15]));
    }

    #[test]
    fn insert_splits_root_leaf_into_interior_page() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        let root_page_id = tree.root_page_id();

        for key in 0u64..14 {
            let payload = fixed_payload((key % 255) as u8, 512);
            tree.insert(key, &payload).unwrap();
        }

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        let root_page = TablePageRef::from_bytes(root_guard.page()).unwrap();
        match root_page {
            TablePageRef::Leaf(_) => panic!("expected root to become an interior page"),
            TablePageRef::Interior(interior) => assert!(interior.cell_count() > 0),
        }
        drop(root_guard);

        for key in 0u64..14 {
            let expected = fixed_payload((key % 255) as u8, 512);
            expect_payload(tree.get(key).unwrap(), Some(&expected));
        }
    }

    #[test]
    fn repeated_inserts_split_multiple_leaves_while_parent_has_room() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        let root_page_id = tree.root_page_id();

        for key in 0u64..160 {
            let payload = fixed_payload((key % 251) as u8, 160);
            tree.insert(key, &payload).unwrap();
        }

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        let root_page = TablePageRef::from_bytes(root_guard.page()).unwrap();
        let child_count = match root_page {
            TablePageRef::Leaf(_) => panic!("expected interior root after repeated splits"),
            TablePageRef::Interior(interior) => interior.child_count(),
        };
        assert!(child_count > 2);
        drop(root_guard);

        let mut cursor = tree.seek_first().unwrap();
        let mut seen = Vec::new();
        while cursor.is_valid() {
            seen.push(cursor.key().unwrap().unwrap());
            cursor.next_position().unwrap();
        }
        let expected: Vec<RowId> = (0u64..160).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn insert_returns_page_full_when_leaf_split_needs_full_parent() {
        let mut engine = get_temp_engine();

        let rightmost_leaf_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            page_id
        };

        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableInteriorPageMut::init_empty(page_guard.page_mut(), rightmost_leaf_page_id)
                .unwrap();
            page_id
        };

        let mut next_separator = 10u64;
        loop {
            let left_child_page_id = {
                let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
                TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
                page_id
            };

            let insert_result = {
                let mut root_guard = engine.page_cache.fetch_page(root_page_id).unwrap();
                let mut root = TableInteriorPageMut::from_bytes(root_guard.page_mut()).unwrap();
                root.insert(next_separator, left_child_page_id)
            };

            match insert_result {
                Ok(()) => next_separator += 10,
                Err(TablePageError::PageFull { .. }) => break,
                Err(err) => panic!("unexpected root fill error: {err:?}"),
            }
        }

        let big_payload = fixed_payload(7, 256);
        let first_failing_key = fill_leaf_until_page_full(
            &mut engine,
            rightmost_leaf_page_id,
            next_separator + 100,
            &big_payload,
        );

        let mut tree = engine.open_btree(root_page_id).unwrap();
        let err = tree.insert(first_failing_key, &big_payload).unwrap_err();
        assert!(matches!(err, StorageError::LimitExceeded(LimitExceededError::PageFull { .. })));
        assert!(!tree.contains(first_failing_key).unwrap());
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u8, Vec<u8>),
        Update(u8, Vec<u8>),
        Upsert(u8, Vec<u8>),
        Delete(u8),
        Get(u8),
        Contains(u8),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        const MAX_TEST_PAYLOAD_SIZE: usize = PAGE_SIZE * 3;
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX_TEST_PAYLOAD_SIZE);
        prop_oneof![
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Insert(key, value)),
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Update(key, value)),
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Upsert(key, value)),
            any::<u8>().prop_map(Op::Delete),
            any::<u8>().prop_map(Op::Get),
            any::<u8>().prop_map(Op::Contains),
        ]
    }

    proptest! {
        #[test]
        fn prop_point_ops_match_btreemap_model(ops in prop::collection::vec(op_strategy(), 0..128)) {
            let mut engine = get_temp_engine();
            let mut tree = engine.create_btree().unwrap();
            let mut oracle = BTreeMap::<RowId, Vec<u8>>::new();

            for op in ops {
                match op {
                    Op::Insert(key, value) => {
                        let key = RowId::from(key);
                        let expected_ok = !oracle.contains_key(&key);
                        let actual_ok = tree.insert(key, &value).is_ok();
                        if expected_ok {
                            oracle.insert(key, value);
                        }
                        prop_assert_eq!(actual_ok, expected_ok);
                    }
                    Op::Update(key, value) => {
                        let key = RowId::from(key);
                        let expected_ok = oracle.contains_key(&key);
                        let actual_ok = tree.update(key, &value).is_ok();
                        if expected_ok {
                            oracle.insert(key, value);
                        }
                        prop_assert_eq!(actual_ok, expected_ok);
                    }
                    Op::Upsert(key, value) => {
                        let key = RowId::from(key);
                        tree.upsert(key, &value).unwrap();
                        oracle.insert(key, value);
                    }
                    Op::Delete(key) => {
                        let key = RowId::from(key);
                        let expected = oracle.remove(&key).is_some();
                        let actual = tree.delete(key).unwrap();
                        prop_assert_eq!(actual, expected);
                    }
                    Op::Get(key) => {
                        let key = RowId::from(key);
                        let actual = tree.get(key).unwrap().map(|record| record.value().unwrap().to_vec());
                        let expected = oracle.get(&key).cloned();
                        prop_assert_eq!(actual, expected);
                    }
                    Op::Contains(key) => {
                        let key = RowId::from(key);
                        prop_assert_eq!(tree.contains(key).unwrap(), oracle.contains_key(&key));
                    }
                }
            }
        }

        #[test]
        fn prop_cursor_next_prev_are_monotonic(keys in prop::collection::vec(any::<u8>(), 0..64), payload_byte in any::<u8>()) {
            let mut engine = get_temp_engine();
            let mut tree = engine.create_btree().unwrap();
            let key_set: BTreeSet<RowId> = keys.into_iter().map(RowId::from).collect();

            for key in &key_set {
                tree.insert(*key, &[payload_byte]).unwrap();
            }

            let expected_forward: Vec<RowId> = key_set.iter().copied().collect();
            let expected_reverse: Vec<RowId> = key_set.iter().rev().copied().collect();

            let mut cursor = tree.seek_first().unwrap();
            let mut forward = Vec::new();
            while cursor.is_valid() {
                forward.push(cursor.key().unwrap().unwrap());
                cursor.next_position().unwrap();
            }
            prop_assert_eq!(forward, expected_forward);

            let mut cursor = tree.seek_last().unwrap();
            let mut reverse = Vec::new();
            while cursor.is_valid() {
                reverse.push(cursor.key().unwrap().unwrap());
                cursor.prev_position().unwrap();
            }
            prop_assert_eq!(reverse, expected_reverse);
        }
    }
}
