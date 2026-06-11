use std::{collections::BTreeMap, rc::Rc};

use fastrand::Rng;
use tempfile::NamedTempFile;

use super::*;
use crate::core::disk_manager::DiskManager;
use crate::core::error::LimitExceededError;
use crate::core::storage_runtime::StorageRuntime;

const KEY_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=192;
const VALUE_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=PAGE_SIZE * 3;
const INLINE_VALUE_LEN_RANGE: std::ops::RangeInclusive<usize> = 8..=512;
const TARGET_HEIGHT: usize = 4;
const MAX_RECORDS: usize = 50_000;

fn random_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    rng.fill(&mut bytes);
    bytes
}

fn random_unique_cell(rng: &mut Rng, expected: &BTreeMap<Vec<u8>, Vec<u8>>) -> (Vec<u8>, Vec<u8>) {
    loop {
        let key_len = rng.usize(KEY_LEN_RANGE);
        let value_len = if rng.u8(0..32) == 0 {
            rng.usize(VALUE_LEN_RANGE)
        } else {
            rng.usize(INLINE_VALUE_LEN_RANGE)
        };
        let key = random_bytes(rng, key_len);
        if expected.contains_key(&key) {
            continue;
        }

        let value = random_bytes(rng, value_len);
        return (key, value);
    }
}

fn assert_supported_cell(key: &[u8], value: &[u8]) {
    assert!(
        key.len() + value.len() <= u16::MAX as usize,
        "leaf payload should fit the current u16 payload-length field"
    );
    assert!(
        key.len() <= u16::MAX as usize,
        "interior separator should fit the current u16 payload-length field"
    );
}

fn temp_page_cache(cache_frames: usize) -> PageCache {
    let file = NamedTempFile::new().unwrap();
    let disk_manager = DiskManager::new(file.path()).unwrap();
    let runtime = Rc::new(StorageRuntime::new(file.path().to_path_buf(), disk_manager).unwrap());
    PageCache::new(runtime, cache_frames).unwrap()
}

fn temp_tree_cursor(cache_frames: usize) -> TreeCursor {
    let page_cache = temp_page_cache(cache_frames);
    let root_page_id = initialize_empty_root(&page_cache).unwrap();
    TreeCursor::new(page_cache, root_page_id)
}

fn tree_height(cursor: &TreeCursor) -> StorageResult<usize> {
    let mut height = 1;
    let mut page_id = cursor.root_page_id();

    loop {
        let pin = cursor.page_cache.fetch_page(page_id)?;
        let next_page_id = {
            let page = pin.read()?;
            match read_page_kind(page.page(), page_id)? {
                PageKind::RawLeaf => {
                    let _ = page.open::<Leaf>()?;
                    return Ok(height);
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

        height += 1;
        page_id = next_page_id;
    }
}

#[test]
fn root_page_id_stays_stable_after_root_splits() {
    let mut cursor = temp_tree_cursor(256);
    let root_page_id = cursor.root_page_id();
    let mut expected = BTreeMap::new();

    for index in 0..256_u16 {
        let key = oversized_key(index);
        let value = format!("value-{index}").into_bytes();
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
        assert_eq!(cursor.root_page_id(), root_page_id);

        if tree_height(&cursor).unwrap() >= 3 {
            break;
        }
    }

    assert!(tree_height(&cursor).unwrap() >= 3, "test setup should split an interior root");
    for (key, value) in &expected {
        let record = cursor.get(key).unwrap().expect("inserted key should be readable");
        assert_record_matches(&record, key, value);
    }
}

#[test]
fn root_page_id_stays_stable_after_root_shrink() {
    let mut cursor = temp_tree_cursor(256);
    let root_page_id = cursor.root_page_id();
    let mut keys = Vec::new();

    for index in 0..512_u32 {
        let key = index.to_be_bytes().to_vec();
        cursor.insert(&key, b"value").unwrap();
        keys.push(key);
    }
    assert!(tree_height(&cursor).unwrap() >= 2, "test setup should split the root");

    for key in keys {
        cursor.delete(&key).unwrap();
        assert_eq!(cursor.root_page_id(), root_page_id);
    }

    assert_eq!(cursor.root_page_id(), root_page_id);
    assert_eq!(tree_height(&cursor).unwrap(), 1);
    assert!(!cursor.seek_to_first().unwrap());
}

#[ignore = "slow because of fsync"]
#[test]
// Builds a four-level raw B+ tree from deterministic random inline cells,
// proving leaf splits, repeated interior split propagation, exact-key
// lookups, and forward/backward sorted cursor scans.
fn random_insert_get_simulation_with_oversized_values_reaches_four_levels() {
    let mut cursor = temp_tree_cursor(256);
    let mut rng = Rng::with_seed(0xd47a_ba5e_b7ee_2026);
    let mut expected = BTreeMap::new();
    let mut cells = Vec::new();
    let mut previous_height = tree_height(&cursor).unwrap();
    let mut saw_leaf_root_split = false;
    let mut saw_first_interior_root_split = false;
    let mut saw_repeated_interior_split_propagation = false;

    assert_eq!(previous_height, 1);

    for _ in 0..MAX_RECORDS {
        let (key, value) = random_unique_cell(&mut rng, &expected);
        assert_supported_cell(&key, &value);

        cursor.insert(&key, &value).unwrap();
        assert!(expected.insert(key.clone(), value.clone()).is_none());
        cells.push((key, value));

        let height = tree_height(&cursor).unwrap();
        saw_leaf_root_split |= previous_height == 1 && height == 2;
        saw_first_interior_root_split |= previous_height == 2 && height == 3;
        saw_repeated_interior_split_propagation |= previous_height == 3 && height == 4;
        previous_height = height;

        if height == TARGET_HEIGHT {
            break;
        }
    }

    assert!(saw_leaf_root_split, "tree should split the root leaf");
    assert!(saw_first_interior_root_split, "tree should split an interior root");
    assert!(
        saw_repeated_interior_split_propagation,
        "tree should propagate an interior split through an existing interior level"
    );
    assert_eq!(
        previous_height, TARGET_HEIGHT,
        "simulation should reach {TARGET_HEIGHT} tree levels within {MAX_RECORDS} inserts"
    );

    for (key, value) in &cells {
        assert_eq!(expected.get(key).map(Vec::as_slice), Some(value.as_slice()));

        let record = cursor.get(key).unwrap().expect("inserted tree key should be present");
        assert_record_matches(&record, key, value);
    }

    assert_forward_scan_matches(&mut cursor, &expected);
    assert_reverse_scan_matches(&mut cursor, &expected);
    assert_eq!(expected.len(), cells.len());
}

#[ignore = "slow because of fsync"]
#[test]
fn random_insert_delete_simulation_empties_tree_after_random_delete_order() {
    let mut cursor = temp_tree_cursor(256);
    let mut rng = Rng::with_seed(0x9dd0_c312_741f_2026);
    let mut expected = BTreeMap::new();

    const INSERT_COUNT: usize = 200;
    for _ in 0..INSERT_COUNT {
        let (key, value) = random_unique_cell(&mut rng, &expected);
        assert_supported_cell(&key, &value);
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }
    assert!(!expected.is_empty(), "simulation should create at least one record");

    let mut delete_order: Vec<Vec<u8>> = expected.keys().cloned().collect();
    let sorted_order = delete_order.clone();
    rng.shuffle(&mut delete_order);
    while delete_order == sorted_order {
        rng.shuffle(&mut delete_order);
    }

    for key in &delete_order {
        cursor.delete(key).unwrap();
    }

    assert!(!cursor.seek_to_first().unwrap(), "tree should be empty after deleting all keys");
    assert!(cursor.current().unwrap().is_none(), "empty tree cursor should have no current");
    for key in expected.keys() {
        assert!(cursor.get(key).unwrap().is_none(), "deleted key should not be found");
    }
}

#[ignore = "slow because of fsync"]
#[test]
fn random_insert_update_simulation_replaces_values_for_all_keys() {
    let mut cursor = temp_tree_cursor(256);
    let mut rng = Rng::with_seed(0xa11d_47e5_2026_0425);
    let mut expected = BTreeMap::new();

    const INSERT_COUNT: usize = 200;
    const OVERSIZED_EVERY_NTH: usize = 9;

    for index in 0..INSERT_COUNT {
        let (key, mut value) = random_unique_cell(&mut rng, &expected);
        if index % OVERSIZED_EVERY_NTH == 0 {
            let oversized_len = PAGE_SIZE + rng.usize(1..=PAGE_SIZE);
            value = random_bytes(&mut rng, oversized_len);
        }

        assert_supported_cell(&key, &value);
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }

    assert!(
        expected.values().any(|value| value.len() > PAGE_SIZE),
        "simulation setup should include oversized values"
    );

    let mut keys: Vec<Vec<u8>> = expected.keys().cloned().collect();
    rng.shuffle(&mut keys);
    let mut updated = BTreeMap::new();
    for (index, key) in keys.iter().enumerate() {
        let old_value = expected.get(key).unwrap();
        let new_value = loop {
            let value_len = if index % OVERSIZED_EVERY_NTH == 0 {
                PAGE_SIZE + rng.usize(1..=PAGE_SIZE)
            } else {
                rng.usize(INLINE_VALUE_LEN_RANGE)
            };
            let candidate = random_bytes(&mut rng, value_len);
            if candidate != *old_value {
                break candidate;
            }
        };

        assert_supported_cell(key, &new_value);
        cursor.update(key, &new_value).unwrap();
        updated.insert(key.clone(), new_value);
    }

    assert_eq!(updated.len(), expected.len());
    assert!(
        updated.values().any(|value| value.len() > PAGE_SIZE),
        "updated values should include oversized entries"
    );

    for (key, value) in &updated {
        let record = cursor.get(key).unwrap().expect("updated key should still exist");
        assert_record_matches(&record, key, value);
    }

    assert_forward_scan_matches(&mut cursor, &updated);
    assert_reverse_scan_matches(&mut cursor, &updated);
}

#[ignore = "slow because of fsync"]
#[test]
fn random_mixed_operation_simulation_matches_btreemap_model() {
    let mut cursor = temp_tree_cursor(256);
    let mut rng = Rng::with_seed(0x741e_5afe_2026_0429);
    let mut expected = BTreeMap::new();

    const STEPS: usize = 3_000;
    const MIN_LIVE_KEYS: usize = 128;
    const VERIFY_EVERY_NTH: usize = 50;
    const OVERSIZED_EVERY_NTH: usize = 31;

    for step in 0..STEPS {
        let operation = if expected.len() < MIN_LIVE_KEYS { 0 } else { rng.u8(0..100) };
        match operation {
            0..=34 => {
                let (key, mut value) = random_unique_cell(&mut rng, &expected);
                if step % OVERSIZED_EVERY_NTH == 0 {
                    let oversized_len = PAGE_SIZE + rng.usize(1..=PAGE_SIZE);
                    value = random_bytes(&mut rng, oversized_len);
                }

                assert_supported_cell(&key, &value);
                cursor.insert(&key, &value).unwrap();
                assert!(expected.insert(key.clone(), value.clone()).is_none());
                let record = cursor.get(&key).unwrap().expect("inserted key should be found");
                assert_record_matches(&record, &key, &value);
            }
            35..=64 => {
                if expected.is_empty() || rng.bool() {
                    let (key, _) = random_unique_cell(&mut rng, &expected);
                    assert!(cursor.get(&key).unwrap().is_none());
                } else {
                    let key = random_existing_key(&mut rng, &expected);
                    let expected_value = expected.get(key).unwrap();
                    let record = cursor
                        .get(key)
                        .unwrap()
                        .unwrap_or_else(|| panic!("model key should exist at step {step}"));
                    assert_record_matches(&record, key, expected_value);
                }
            }
            65..=84 => {
                let key = random_existing_key(&mut rng, &expected).to_vec();
                let old_value = expected.get(&key).unwrap();
                let new_value = loop {
                    let value_len = if step % OVERSIZED_EVERY_NTH == 0 {
                        PAGE_SIZE + rng.usize(1..=PAGE_SIZE)
                    } else {
                        rng.usize(INLINE_VALUE_LEN_RANGE)
                    };
                    let candidate = random_bytes(&mut rng, value_len);
                    if candidate != *old_value {
                        break candidate;
                    }
                };

                assert_supported_cell(&key, &new_value);
                cursor.update(&key, &new_value).unwrap();
                expected.insert(key.clone(), new_value.clone());
                let record = cursor.get(&key).unwrap().expect("updated key should still exist");
                assert_record_matches(&record, &key, &new_value);
            }
            _ => {
                let key = random_existing_key(&mut rng, &expected).to_vec();
                cursor.delete(&key).unwrap();
                assert!(expected.remove(&key).is_some());
                assert!(cursor.get(&key).unwrap().is_none(), "deleted key should not be found");
            }
        }

        if step % VERIFY_EVERY_NTH == 0 {
            assert_tree_gets_match_model(&mut cursor, &expected);
        }
    }

    assert_tree_gets_match_model(&mut cursor, &expected);
}

fn oversized_key(index: u16) -> Vec<u8> {
    let mut key = vec![0; PAGE_SIZE + 256];
    key[..2].copy_from_slice(&index.to_be_bytes());
    for byte in &mut key[2..] {
        *byte = (index % 251) as u8;
    }
    key
}

#[test]
fn insert_get_supports_oversized_keys_promoted_to_interior_pages() {
    let mut cursor = temp_tree_cursor(256);
    let mut expected = BTreeMap::new();

    for index in 0..48 {
        let key = oversized_key(index);
        let value = format!("value-{index}").into_bytes();
        assert_supported_cell(&key, &value);
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }

    assert!(tree_height(&cursor).unwrap() >= 2, "large keys should force a root split");
    for (key, value) in &expected {
        let record = cursor.get(key).unwrap().expect("inserted oversized key should exist");
        assert_record_matches(&record, key, value);
    }
    assert_forward_scan_matches(&mut cursor, &expected);
}

#[test]
fn failed_interior_rewrite_leaves_page_unchanged() {
    let page_cache = temp_page_cache(16);
    let (page_id, pin) = page_cache.new_page().unwrap();
    {
        let mut guard = pin.write().unwrap();
        let mut interior = RawInterior::<Write<'_>>::initialize_with_rightmost(guard.page_mut(), 2);
        interior.insert_payload_at(0, 0, b"stable".len(), None, b"stable").unwrap();
    }
    drop(pin);

    let cursor = TreeCursor::new(page_cache.clone(), page_id);
    let original_page = {
        let pin = page_cache.fetch_page(page_id).unwrap();
        let page = pin.read().unwrap();
        *page.page()
    };
    let children: Vec<_> = (0..16)
        .map(|index| ChildEntry {
            page_id: 100 + index,
            max_key: Some(vec![index as u8; PAGE_SIZE]),
        })
        .collect();

    let result = cursor.rewrite_interior_page(page_id, &children, None, None);

    assert!(matches!(
        result,
        Err(StorageError::LimitExceeded(LimitExceededError::PageFull { .. }))
    ));
    let rewritten_page = {
        let pin = page_cache.fetch_page(page_id).unwrap();
        let page = pin.read().unwrap();
        *page.page()
    };
    assert_eq!(rewritten_page, original_page);
}

#[test]
fn unchanged_path_separator_refresh_does_not_grow_file() {
    let file = NamedTempFile::new().unwrap();
    let disk_manager = DiskManager::new(file.path()).unwrap();
    let runtime = Rc::new(StorageRuntime::new(file.path().to_path_buf(), disk_manager).unwrap());
    let page_cache = PageCache::new(runtime, 256).unwrap();
    let root_page_id = initialize_empty_root(&page_cache).unwrap();
    let mut cursor = TreeCursor::new(page_cache, root_page_id);
    let mut expected = BTreeMap::new();

    for index in 0..96 {
        let key = oversized_key(index);
        let value = format!("value-{index}").into_bytes();
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }

    let key = expected.keys().next().expect("test setup should create records");
    let (_, tree_path) = cursor.leaf_page_path_for_key(key).unwrap();
    assert!(!tree_path.is_empty(), "large keys should force interior separators");
    cursor.refresh_path_separators(&tree_path).unwrap();
    let file_len_before = file.path().metadata().unwrap().len();

    cursor.refresh_path_separators(&tree_path).unwrap();

    let file_len_after = file.path().metadata().unwrap().len();
    assert_eq!(file_len_after, file_len_before);
}

#[test]
fn inline_record_is_page_resident() {
    let mut cursor = temp_tree_cursor(4);

    cursor.insert(b"alpha", b"value").unwrap();

    let record = cursor.get(b"alpha").unwrap().expect("inline record should exist");
    assert!(matches!(record.storage, RecordStorage::PageResident { .. }));
    assert_record_matches(&record, b"alpha", b"value");
}

#[test]
fn overflow_record_is_materialized() {
    let mut cursor = temp_tree_cursor(4);
    let value = vec![42; PAGE_SIZE];

    cursor.insert(b"alpha", &value).unwrap();

    let record = cursor.get(b"alpha").unwrap().expect("overflow record should exist");
    assert!(matches!(record.storage, RecordStorage::Materialized { .. }));
    assert_record_matches(&record, b"alpha", &value);
}

#[test]
fn inline_record_converts_to_owned_snapshot() {
    let mut cursor = temp_tree_cursor(4);

    cursor.insert(b"alpha", b"value").unwrap();

    let owned = cursor
        .get(b"alpha")
        .unwrap()
        .expect("inline record should exist")
        .to_owned_record()
        .unwrap();
    assert_owned_record_matches(&owned, b"alpha", b"value");
}

#[test]
fn binary_search_supports_inline_key_with_overflow_value() {
    let mut cursor = temp_tree_cursor(8);
    let value = vec![7; PAGE_SIZE];

    cursor.insert(b"alpha", b"small").unwrap();
    cursor.insert(b"bravo", &value).unwrap();
    cursor.insert(b"charlie", b"small").unwrap();

    let record = cursor.get(b"bravo").unwrap().expect("overflow value key should exist");
    assert_record_matches(&record, b"bravo", &value);
    assert!(cursor.get(b"between").unwrap().is_none());
}

#[test]
fn binary_search_supports_oversized_key_with_overflow_value() {
    let mut cursor = temp_tree_cursor(16);
    let key = oversized_key(7);
    let value = vec![11; PAGE_SIZE];

    cursor.insert(&key, &value).unwrap();

    let record = cursor.get(&key).unwrap().expect("oversized key should exist");
    assert_record_matches(&record, &key, &value);
}

#[test]
fn binary_search_supports_oversized_interior_separator_keys() {
    let mut cursor = temp_tree_cursor(256);
    let mut expected = BTreeMap::new();

    for index in 0..48 {
        let key = oversized_key(index);
        let value = format!("value-{index}").into_bytes();
        cursor.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }

    assert!(tree_height(&cursor).unwrap() >= 2, "large keys should force interior routing");
    for (key, value) in &expected {
        assert!(cursor.seek_to_key(key).unwrap(), "seek_to_key should find oversized key");
        let record = cursor.current().unwrap().expect("seek_to_key should position cursor");
        assert_record_matches(&record, key, value);
    }
}

fn assert_record_matches(record: &Record, expected_key: &[u8], expected_value: &[u8]) {
    record
        .with_key_value(|actual_key, actual_value| {
            assert_eq!(actual_key, expected_key);
            assert_eq!(actual_value, expected_value);
        })
        .unwrap();
}

fn assert_owned_record_matches(record: &OwnedRecord, expected_key: &[u8], expected_value: &[u8]) {
    record.with_key_value(|actual_key, actual_value| {
        assert_eq!(actual_key, expected_key);
        assert_eq!(actual_value, expected_value);
    });
}

fn random_existing_key<'a>(rng: &mut Rng, expected: &'a BTreeMap<Vec<u8>, Vec<u8>>) -> &'a Vec<u8> {
    let index = rng.usize(0..expected.len());
    expected.keys().nth(index).expect("model should contain keys")
}

fn assert_tree_gets_match_model(cursor: &mut TreeCursor, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    if expected.is_empty() {
        assert!(!cursor.seek_to_first().unwrap(), "empty model should match empty tree");
        assert!(cursor.current().unwrap().is_none(), "empty tree cursor should have no current");
        return;
    }

    for (key, value) in expected {
        let record = cursor.get(key).unwrap().expect("model key should exist in tree");
        assert_record_matches(&record, key, value);
    }
}

fn assert_forward_scan_matches(cursor: &mut TreeCursor, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert!(cursor.seek_to_first().unwrap(), "tree should not be empty");

    let mut expected_entries = expected.iter();
    let (first_key, first_value) = expected_entries.next().unwrap();
    let first_record = cursor.current().unwrap().expect("seek_to_first should position cursor");
    assert_record_matches(&first_record, first_key, first_value);

    let mut scanned = 1;
    for (key, value) in expected_entries {
        let record = cursor.next_record().unwrap().expect("forward scan ended early");
        assert_record_matches(&record, key, value);
        scanned += 1;
    }

    assert!(cursor.next_record().unwrap().is_none());
    assert_eq!(scanned, expected.len());
}

fn assert_reverse_scan_matches(cursor: &mut TreeCursor, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let (last_key, _) = expected.iter().next_back().unwrap();
    let last_record = cursor.get(last_key).unwrap().expect("last key should be present");

    let mut expected_entries = expected.iter().rev();
    let (key, value) = expected_entries.next().unwrap();
    assert_record_matches(&last_record, key, value);

    let mut scanned = 1;
    for (key, value) in expected_entries {
        let record = cursor.prev_record().unwrap().expect("reverse scan ended early");
        assert_record_matches(&record, key, value);
        scanned += 1;
    }

    assert!(cursor.prev_record().unwrap().is_none());
    assert_eq!(scanned, expected.len());
}
