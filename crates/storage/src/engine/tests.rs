use std::{fs, path::Path};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{Storage, StorageOptions};
use crate::{
    PAGE_SIZE,
    error::{CorruptionKind, StorageError},
};

mod faults;

const OPTIONS: StorageOptions = StorageOptions { cache_frames: 8 };

// Independent fixture for the documented database header, not its encoder.
fn header() -> Vec<u8> {
    let mut bytes = vec![0; PAGE_SIZE];
    bytes[..12].copy_from_slice(b"DATABAS\0\x02\0\0\x10");
    bytes
}

fn open(path: &Path, mode: usize) -> crate::error::StorageResult<Storage> {
    match mode {
        0 => Storage::create_with_options(path, OPTIONS),
        1 => Storage::open_with_options(path, OPTIONS),
        _ => Storage::open_or_create_with_options(path, OPTIONS),
    }
}

fn check_usable(storage: Storage, path: &Path) -> TestCaseResult {
    let mut cursor = storage.create_tree()?;
    let root = cursor.root_page_id();
    cursor.insert(b"key", b"value")?;
    storage.flush()?;
    drop(cursor);
    drop(storage);
    let storage = open(path, 1)?;
    let mut cursor = storage.tree_cursor(root)?;
    prop_assert_eq!(
        cursor
            .get(b"key")?
            .expect("persisted record")
            .with_key_value(|k, v| (k.to_vec(), v.to_vec()))?,
        (b"key".to_vec(), b"value".to_vec())
    );
    Ok(())
}

#[test]
fn opening_modes_distinguish_missing_empty_and_initialized_files() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    for mode in 0..3 {
        for state in 0..3 {
            let path = dir.path().join(format!("db-{mode}-{state}"));
            if state != 0 {
                fs::write(&path, if state == 1 { vec![] } else { header() })?;
            }
            let result = open(&path, mode);
            let succeeds = match mode {
                0 => state == 0,
                1 => state == 2,
                _ => true,
            };
            if succeeds {
                let storage = result?;
                prop_assert_eq!(storage.opened_page_count(), 1);
                prop_assert_eq!(fs::read(&path)?, header());
                check_usable(storage, &path)?;
            } else {
                let err = result.err().expect("open must fail");
                match (mode, state) {
                    (0, _) => prop_assert!(
                        matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::AlreadyExists)
                    ),
                    (1, 0) => prop_assert!(
                        matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::NotFound)
                    ),
                    _ => prop_assert!(
                        matches!(err, StorageError::Corruption(e) if e.kind == CorruptionKind::MissingDatabaseHeader)
                    ),
                }
                prop_assert!(!path.with_added_extension("wal").exists());
                if state == 0 {
                    prop_assert!(!path.exists());
                } else {
                    prop_assert_eq!(fs::read(&path)?, if state == 1 { vec![] } else { header() });
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn damaged_headers_and_partial_files_are_rejected_without_touching_database_or_wal(
        index in prop_oneof![0usize..12, 12usize..PAGE_SIZE], bit in 0u8..8, tail in 1usize..PAGE_SIZE,
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let wal = path.with_added_extension("wal");
        let mut damaged = header();
        damaged[index] ^= 1 << bit;
        let mut partial = header();
        partial.extend(vec![0; tail]);
        for bytes in [damaged, partial, vec![0; tail]] {
            for mode in 0..3 {
                fs::write(&path, &bytes)?;
                fs::write(&wal, b"must survive rejected open")?;
                let result = open(&path, mode);
                if mode == 0 {
                    prop_assert!(matches!(result, Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists));
                } else {
                    prop_assert!(matches!(result, Err(StorageError::Corruption(_))));
                }
                prop_assert_eq!(&fs::read(&path)?, &bytes);
                prop_assert_eq!(fs::read(&wal)?, b"must survive rejected open");
            }
        }
        // All failing opens must release ownership, allowing repair and retry.
        fs::write(&path, header())?;
        fs::remove_file(&wal)?;
        check_usable(open(&path, 1)?, &path)?;
    }
}

#[test]
fn invalid_options_release_ownership_and_leave_a_reopenable_file() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    prop_assert!(
        Storage::open_or_create_with_options(&path, StorageOptions { cache_frames: 0 }).is_err(),
        "zero capacity accepted"
    );
    // Creation may already have initialized the file. It must be usable after
    // retry; failed opening does not promise filesystem rollback.
    check_usable(open(&path, 2)?, &path)?;
    Ok(())
}
