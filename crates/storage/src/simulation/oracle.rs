use crate::{PageId, Storage, TreeCursor};

use super::{
    RunError, RunResult,
    workload::{Changes, Contents, KEYS},
};

pub fn apply(model: &mut [Contents], changes: Changes) {
    for ((tree, key), value) in changes {
        match value {
            Some(value) => {
                model[tree].insert(key, value);
            }
            None => {
                model[tree].remove(&key);
            }
        }
    }
}

/// Compare the whole database against both permitted outcomes of an
/// interrupted commit. Choose the outcome once across all its trees.
pub fn reconcile(
    storage: &Storage,
    roots: &[PageId],
    model: &mut Vec<Contents>,
    pending_commit: Option<Changes>,
) -> RunResult<bool> {
    let alternate = pending_commit.map(|changes| {
        let mut alternate = model.clone();
        apply(&mut alternate, changes);
        alternate
    });
    let mut matches_base = true;
    let mut matches_alternate = alternate.is_some();
    let mut first_difference = None;
    for (tree, root) in roots.iter().enumerate() {
        storage.validate_tree_page_formats(*root)?;
        let actual = scan(&mut storage.tree_cursor(*root)?)?;
        if actual != model[tree] {
            matches_base = false;
            first_difference.get_or_insert_with(|| difference(tree, &model[tree], &actual));
        }
        if let Some(alternate) = &alternate {
            matches_alternate &= actual == alternate[tree];
        }
    }
    if matches_base {
        return Ok(false);
    }
    if matches_alternate && let Some(alternate) = alternate {
        *model = alternate;
        return Ok(true);
    }
    Err(RunError::Mismatch(format!(
        "recovered database matches neither permitted outcome. {}",
        first_difference.unwrap_or_default(),
    )))
}

fn scan(cursor: &mut TreeCursor) -> RunResult<Contents> {
    let mut actual = Contents::new();
    if cursor.seek_to_first()? {
        let mut record = cursor.current()?;
        while let Some(row) = record {
            let (key, value) = row.with_key_value(|key, value| (key.to_vec(), value.to_vec()))?;
            drop(row);
            if actual.last_key_value().is_some_and(|(last, _)| last >= &key) {
                return Err(RunError::Mismatch(
                    "ordered scan returned a repeated or descending key".into(),
                ));
            }
            actual.insert(key, value);
            if actual.len() as u64 > KEYS {
                return Err(RunError::Mismatch(
                    "ordered scan exceeded the workload keyspace".into(),
                ));
            }
            record = cursor.next_record()?;
        }
    }
    Ok(actual)
}

fn difference(tree: usize, expected: &Contents, actual: &Contents) -> String {
    let changed = expected.iter().find(|(key, value)| actual.get(*key) != Some(*value));
    let extra = actual.keys().find(|key| !expected.contains_key(*key));
    let key = changed.map(|(key, _)| key).or(extra);
    let number = key
        .and_then(|key| key.get(..8))
        .and_then(|prefix| <[u8; 8]>::try_from(prefix).ok())
        .map(u64::from_be_bytes);
    format!(
        "tree {tree}, key {number:?}, expected length {:?}, recovered length {:?} (tree has {} records, expected {})",
        key.and_then(|key| expected.get(key)).map(Vec::len),
        key.and_then(|key| actual.get(key)).map(Vec::len),
        actual.len(),
        expected.len(),
    )
}
