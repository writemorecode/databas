//! Public-API properties. The oracle contains only ordered byte records; it knows
//! nothing about tree height, split policy, page identifiers, or cell layouts.

use std::collections::BTreeMap;

use databas_storage::{
    Record, Storage, StorageOptions, TreeCursor,
    error::{ConstraintError, InvalidArgumentError, StorageError, StorageResult},
};
use proptest::{prelude::*, test_runner::TestCaseResult};
use tempfile::TempDir;

type Pair = (Vec<u8>, Vec<u8>);
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[path = "btree/cursors.rs"]
mod cursors;
#[path = "btree/growth.rs"]
mod growth;
#[path = "btree/limits.rs"]
mod limits;
#[path = "btree/long_keys.rs"]
mod long_keys;

fn config() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/proptest-regressions/btree.txt"),
        ))),
        ..ProptestConfig::default()
    }
}

fn new_storage(cache_frames: usize) -> Result<(TempDir, Storage), TestCaseError> {
    let directory = tempfile::tempdir()?;
    let storage = Storage::open_or_create_with_options(
        directory.path().join("tree.db"),
        StorageOptions { cache_frames },
    )?;
    Ok((directory, storage))
}

fn owned(record: Option<Record>) -> StorageResult<Option<Pair>> {
    record.map(|r| r.with_key_value(|key, value| (key.to_vec(), value.to_vec()))).transpose()
}

fn pair((key, value): (&Vec<u8>, &Vec<u8>)) -> Pair {
    (key.clone(), value.clone())
}

fn check_tree(cursor: &mut TreeCursor, model: &Model) -> TestCaseResult {
    prop_assert_eq!(cursor.seek_to_first()?, !model.is_empty());
    let mut actual = owned(cursor.current()?)?;
    // Bound the scan by the oracle size so extra/duplicated records cannot make
    // the test's scan loop run forever.
    for expected in model.iter().map(pair) {
        prop_assert_eq!(&actual, &Some(expected.clone()));
        prop_assert_eq!(owned(cursor.current()?)?, Some(expected));
        actual = owned(cursor.next_record()?)?;
    }
    prop_assert_eq!(actual, None);
    prop_assert_eq!(owned(cursor.current()?)?, None);
    prop_assert_eq!(owned(cursor.next_record()?)?, None);
    prop_assert_eq!(owned(cursor.next_record()?)?, None);
    for (key, value) in model {
        prop_assert_eq!(owned(cursor.get(key)?)?, Some((key.clone(), value.clone())));
    }
    Ok(())
}

fn check_probe(cursor: &mut TreeCursor, model: &Model, probe: &[u8]) -> TestCaseResult {
    let expected = model.get_key_value(probe).map(pair);
    prop_assert_eq!(owned(cursor.get(probe)?)?, expected.clone());
    prop_assert_eq!(owned(cursor.current()?)?, expected);
    for strict in [false, true] {
        let mut tail = model.iter().filter(|(key, _)| {
            if strict { key.as_slice() > probe } else { key.as_slice() >= probe }
        });
        let expected = tail.next().map(pair);
        let found =
            if strict { cursor.seek_after_key(probe)? } else { cursor.seek_to_key(probe)? };
        prop_assert_eq!(found, expected.is_some());
        prop_assert_eq!(owned(cursor.current()?)?, expected);
        prop_assert_eq!(owned(cursor.next_record()?)?, tail.next().map(pair));
    }
    Ok(())
}

fn keys() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::btree_set(
        prop_oneof![
            Just(Vec::new()),
            Just(vec![0]),
            Just(vec![0, 0]),
            Just(vec![255]),
            prop::collection::vec(any::<u8>(), 1..=8),
        ],
        1..=24,
    )
    .prop_map(|keys| keys.into_iter().collect())
}

fn value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        6 => prop::collection::vec(any::<u8>(), 0..=64),
        2 => (prop::sample::select(vec![510usize, 511, 512, 513, 4087, 4088, 4089, 8191, 8192]))
            .prop_flat_map(|len| prop::collection::vec(any::<u8>(), len)),
    ]
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u8, Vec<u8>),
    Update(u8, Vec<u8>),
    Delete(u8),
}

fn operation() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any::<u8>(), value()).prop_map(|(key, value)| Op::Insert(key, value)),
        4 => (any::<u8>(), value()).prop_map(|(key, value)| Op::Update(key, value)),
        3 => any::<u8>().prop_map(Op::Delete),
    ]
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn mutations_and_seeks_match_an_ordered_map(
        keys in keys(),
        prefix in prop::collection::vec(any::<u8>(), 0..=4),
        ops in prop::collection::vec(operation(), 1..=64),
        probe in prop::collection::vec(any::<u8>(), 0..=10),
        cache_frames in prop::sample::select(vec![8usize, 16, 64]),
    ) {
        let (_directory, storage) = new_storage(cache_frames)?;
        let mut cursor = storage.create_tree()?;
        let mut model = Model::new();
        check_tree(&mut cursor, &model)?;
        check_probe(&mut cursor, &model, &probe)?;
        for op in ops {
            let index = match &op {
                Op::Insert(index, _) | Op::Update(index, _) | Op::Delete(index) => *index,
            };
            let key = [prefix.as_slice(), &keys[usize::from(index) % keys.len()]].concat();
            match op {
                Op::Insert(_, value) => {
                    let result = cursor.insert(&key, &value);
                    if model.contains_key(&key) {
                        prop_assert!(matches!(result, Err(StorageError::Constraint(ConstraintError::DuplicateKey))),
                            "expected duplicate rejection, got {:?}", result);
                    } else {
                        result?;
                        model.insert(key.clone(), value);
                    }
                }
                Op::Update(_, value) => {
                    let result = cursor.update(&key, &value);
                    if let Some(previous) = model.get_mut(&key) {
                        result?;
                        *previous = value;
                    } else {
                        prop_assert!(matches!(result, Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))),
                            "expected missing-key rejection, got {:?}", result);
                    }
                }
                Op::Delete(_) => {
                    let result = cursor.delete(&key);
                    if model.remove(&key).is_some() {
                        result?;
                    } else {
                        prop_assert!(matches!(result, Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))),
                            "expected missing-key rejection, got {:?}", result);
                    }
                }
            }
            // Also observe through an independently constructed cursor, including
            // after rejected operations. No stale physical positions are reused.
            let mut observer = storage.tree_cursor(cursor.root_page_id())?;
            check_tree(&mut observer, &model)?;
            check_probe(&mut observer, &model, &key)?;
            check_probe(&mut observer, &model, &probe)?;
        }
    }
}
