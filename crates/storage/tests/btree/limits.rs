use databas_storage::error::{LimitExceededError, StorageError};
use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{Model, check_probe, check_tree, new_storage};

#[test]
fn record_size_limits_preserve_existing_contents() -> TestCaseResult {
    let (_directory, storage) = new_storage(8)?;
    let mut cursor = storage.create_tree()?;
    // The format encodes total key/value length in 16 bits. Exercise the limit
    // with both a many-page value and a many-page key, then exceed it by one.
    let long_key = vec![0; 65535];
    let long_value: Vec<_> = (0usize..65534).map(|index| index.to_le_bytes()[1]).collect();
    let mut model: Model = [(vec![1], long_value), (long_key.clone(), Vec::new())].into();
    for (key, value) in &model {
        cursor.insert(key, value)?;
    }
    check_tree(&mut cursor, &model)?;
    for (update, key, value) in [
        (false, vec![2], vec![0; 65535]),
        (true, vec![1], vec![0; 65535]),
        (false, vec![0; 65536], Vec::new()),
        (true, long_key.clone(), vec![0]),
    ] {
        let result = if update { cursor.update(&key, &value) } else { cursor.insert(&key, &value) };
        prop_assert!(
            matches!(
                result,
                Err(StorageError::LimitExceeded(LimitExceededError::CellTooLarge { .. }))
            ),
            "expected an encoding-limit error, got {:?}",
            result
        );
        check_tree(&mut cursor, &model)?;
        check_probe(&mut cursor, &model, &key)?;
    }
    cursor.delete(&long_key)?;
    model.remove(&long_key);
    check_tree(&mut cursor, &model)?;
    Ok(())
}
