use std::path::Path;

use crate::{
    disk_manager::DiskManager,
    error::StorageError,
    page_cache::{PageCache, PinGuard},
    table_page::{TableLeafPageMut, TableLeafPageRef, TablePageError, TablePageRef},
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
        let mut current_page_id = root_page_id;
        let leaf_page_id = loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => break current_page_id,
                TablePageRef::Interior(interior) => {
                    current_page_id = interior.child_for_row_id(row_id)?;
                }
            }
        };
        Ok(leaf_page_id)
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
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
        leaf_page.insert(row_id, value)?;
        Ok(())
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
        error::{InvalidArgumentError, StorageError},
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
