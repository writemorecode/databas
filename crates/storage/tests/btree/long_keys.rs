use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{Model, check_probe, check_tree, new_storage};

#[test]
fn deleting_long_keys_preserves_neighbors_with_the_same_inline_prefix() -> TestCaseResult {
    let (_directory, storage) = new_storage(8)?;
    let mut cursor = storage.create_tree()?;
    let mut model = Model::new();
    let keys: Vec<_> = (0u8..3).map(|id| [vec![0; 512], vec![id]].concat()).collect();
    for (id, key) in keys.iter().enumerate() {
        let value = id.to_be_bytes().to_vec();
        cursor.insert(key, &value)?;
        model.insert(key.clone(), value);
    }
    for index in [1, 0, 2] {
        let key = &keys[index];
        prop_assert!(cursor.get(key)?.is_some());
        cursor.delete(key)?;
        model.remove(key);
        check_tree(&mut cursor, &model)?;
        check_probe(&mut cursor, &model, key)?;
    }
    Ok(())
}
