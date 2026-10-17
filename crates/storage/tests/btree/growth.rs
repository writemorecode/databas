use databas_storage::{Storage, StorageOptions, TreeCursor};
use proptest::prelude::*;

use super::{Model, check_probe, check_tree, new_storage};

fn patterned(len: usize, seed: u8) -> Vec<u8> {
    // Different overflow chunks carry different bytes, while the counterexample
    // remains a small length/seed pair rather than thousands of random bytes.
    (0..len).map(|index| seed.wrapping_add((index / 251).to_le_bytes()[0])).collect()
}

proptest! {
    #![proptest_config(super::config())]

    #[test]
    fn growth_draining_and_reopening_preserve_records(
        priorities in prop::collection::vec(any::<u16>(), 1..=64),
        (prefix_len, prefix_byte) in (prop::sample::select(vec![0usize, 511, 512, 513, 4600]), any::<u8>()),
        seed in prop::collection::vec(any::<u8>(), 0..=64),
        (expanded_len, expanded_seed) in (4087usize..=8192, any::<u8>()),
        reverse in any::<bool>(),
        rotation in any::<u16>(),
        cache_frames in prop::sample::select(vec![8usize, 32]),
    ) {
        let prefix = vec![prefix_byte; prefix_len];
        let expanded = patterned(expanded_len, expanded_seed);
        let (_directory, storage) = new_storage(cache_frames)?;
        let mut cursors = [storage.create_tree()?, storage.create_tree()?];
        // Persisted roots must remain usable through growth, draining, and refill.
        let roots = cursors.each_ref().map(TreeCursor::root_page_id);
        let mut records = Vec::new();
        for index in 0..priorities.len() {
            let id = u16::try_from(index)?;
            let key = [prefix.as_slice(), &(id * 2).to_be_bytes()].concat();
            let value = [&id.to_be_bytes()[..], &seed].concat();
            records.push((key, value));
        }
        let original: Model = records.iter().cloned().collect();
        let mut order: Vec<_> = (0..records.len()).collect();
        order.sort_by_key(|&index| priorities[index]);
        if reverse {
            order.reverse();
        }
        for &index in &order {
            let (key, value) = &records[index];
            cursors[0].insert(key, value)?;
        }
        // Metamorphic oracle: the same records inserted in another order have
        // the same observable contents, irrespective of their physical layouts.
        for (key, value) in records.iter().rev() {
            cursors[1].insert(key, value)?;
        }
        for cursor in &mut cursors {
            check_tree(cursor, &original)?;
        }
        let mut model = original.clone();
        for &index in &order {
            let key = &records[index].0;
            let value = [&u16::try_from(index)?.to_be_bytes()[..], &expanded].concat();
            cursors[0].update(key, &value)
                .map_err(|err| TestCaseError::fail(format!("expanding record {index}: {err}")))?;
            model.insert(key.clone(), value);
        }
        check_tree(&mut cursors[0], &model)?;
        for &index in order.iter().rev() {
            let key = &records[index].0;
            cursors[0].update(key, b"")
                .map_err(|err| TestCaseError::fail(format!("shrinking record {index}: {err}")))?;
            model.insert(key.clone(), Vec::new());
        }
        check_tree(&mut cursors[0], &model)?;
        for index in [0, records.len() / 2, records.len() - 1] {
            let key = &records[index].0;
            check_probe(&mut cursors[0], &model, key)?;
            check_probe(&mut cursors[0], &model, &[key.as_slice(), &[0]].concat())?;
        }
        check_probe(&mut cursors[0], &model, &prefix)?;
        check_probe(&mut cursors[0], &model, &[prefix.as_slice(), &[255; 3]].concat())?;
        let rotate = usize::from(rotation) % order.len();
        order.rotate_left(rotate);
        for (step, &index) in order.iter().enumerate() {
            let key = &records[index].0;
            cursors[0].delete(key)
                .map_err(|err| TestCaseError::fail(format!("deleting record {index} at step {step}: {err}")))?;
            model.remove(key);
            if step % 8 == 0 {
                check_tree(&mut cursors[0], &model)?;
                check_probe(&mut cursors[0], &model, key)?;
            }
        }
        check_tree(&mut cursors[0], &Model::new())?;
        check_tree(&mut cursors[1], &original)?;
        for &index in order.iter().rev() {
            let (key, value) = &records[index];
            cursors[0].insert(key, value)?;
        }
        for root in roots {
            check_tree(&mut storage.tree_cursor(root)?, &original)?;
            storage.validate_tree_page_formats(root)?;
        }

        // This checks explicit flush/reopen behavior of raw initialization writes;
        // transaction durability and crash recovery are separate test workloads.
        drop(cursors);
        storage.flush()?;
        let path = storage.path().to_path_buf();
        drop(storage);
        let storage = Storage::open_or_create_with_options(path, StorageOptions { cache_frames })?;
        for root in roots {
            check_tree(&mut storage.tree_cursor(root)?, &original)?;
            storage.validate_tree_page_formats(root)?;
        }
    }
}
