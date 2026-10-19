use std::{fs, io::ErrorKind, path::Path};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{OPTIONS, check_usable, open};
use crate::{
    PAGE_SIZE, Storage,
    disk_manager::{
        DiskManager,
        tests::{Control, Effect, Operation},
    },
    error::StorageError,
};

fn assert_io(result: crate::error::StorageResult<Storage>, control: &Control) -> TestCaseResult {
    prop_assert!(
        matches!(result, Err(StorageError::Io(error)) if error.kind() == ErrorKind::Other)
    );
    prop_assert!(control.consumed(), "fault was not exercised");
    Ok(())
}

#[test]
fn initialization_errors_release_ownership_and_never_report_success() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    for (index, operation) in
        [Operation::Resize, Operation::Seek, Operation::Write, Operation::Sync]
            .into_iter()
            .enumerate()
    {
        let path = dir.path().join(format!("db-{index}"));
        let mut disk = DiskManager::create_new(&path)?;
        disk.lock_exclusive()?;
        let control = disk.faults();
        control.arm(operation, 0, Effect::Error(ErrorKind::Other));
        let result = super::super::initialize_header_page(&mut disk);
        prop_assert!(
            matches!(result, Err(StorageError::Io(error)) if error.kind() == ErrorKind::Other)
        );
        prop_assert!(control.consumed());
        drop(disk);
        prop_assert!(!path.with_added_extension("wal").exists());
        // A failed initialization may leave an empty or incomplete database.
        // Repair explicitly, then verify that failed opening released its lock.
        fs::write(&path, super::header())?;
        check_usable(open(&path, 1)?, &path)?;
    }
    Ok(())
}

#[test]
fn header_read_errors_preserve_files_and_release_ownership() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    fs::write(&path, super::header())?;
    for operation in [Operation::Seek, Operation::Read] {
        let mut disk = DiskManager::open_existing(&path)?;
        disk.lock_exclusive()?;
        let control = disk.faults();
        control.arm(operation, 0, Effect::Error(ErrorKind::Other));
        prop_assert!(
            matches!(super::super::validate_header_page(&mut disk), Err(StorageError::Io(error)) if error.kind() == ErrorKind::Other)
        );
        prop_assert!(control.consumed());
        drop(disk);
        prop_assert_eq!(fs::read(&path)?, super::header());
        prop_assert!(!path.with_added_extension("wal").exists());
        drop(open(&path, 1)?);
        fs::remove_file(path.with_added_extension("wal"))?;
    }
    Ok(())
}

fn recovery_fixture(path: &Path, committed: bool) -> Result<u64, TestCaseError> {
    let storage = open(path, 2)?;
    let mut cursor = storage.create_tree()?;
    let root = cursor.root_page_id();
    cursor.insert(b"key", b"old")?;
    storage.flush()?;
    let txn = storage.begin_transaction()?;
    let mut writer = storage.transaction_tree_cursor(txn, root)?;
    writer.update(b"key", b"new")?;
    if committed {
        storage.commit_transaction(txn)?;
    } else {
        storage.flush()?;
    } // Put loser bytes on disk so recovery must undo.
    Ok(root)
}

proptest! {
    #[test]
    fn failed_recovery_retains_wal_and_reopen_repeats_redo_or_undo(
        at in 0usize..PAGE_SIZE, committed in any::<bool>(), sync in any::<bool>(),
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let root = recovery_fixture(&path, committed)?;
        let wal_path = path.with_added_extension("wal");
        let wal = fs::read(&wal_path)?;
        let disk = DiskManager::open_existing(&path)?;
        let control = disk.faults();
        control.arm(if sync { Operation::Sync } else { Operation::Write }, if sync { 0 } else { at }, Effect::Error(ErrorKind::Other));
        assert_io(Storage::from_disk_manager(path.clone(), disk, OPTIONS), &control)?;
        prop_assert_eq!(fs::read(&wal_path)?, wal);
        prop_assert_eq!(&fs::read(&path)?[..PAGE_SIZE], super::header());
        for _ in 0..2 {
            let storage = open(&path, 1)?;
            let mut cursor = storage.tree_cursor(root)?;
            let value = cursor.get(b"key")?.expect("recovered key").with_key_value(|_, value| value.to_vec())?;
            prop_assert_eq!(value, if committed { b"new" } else { b"old" });
        }
    }
}

#[test]
fn failed_recovery_extension_keeps_redo_for_retry() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    for (index, operation) in [Operation::Resize, Operation::Sync].into_iter().enumerate() {
        let path = dir.path().join(format!("db-{index}"));
        let storage = open(&path, 2)?;
        let txn = storage.begin_transaction()?;
        let mut cursor = storage.transaction_create_tree(txn)?;
        let root = cursor.root_page_id();
        cursor.insert(b"key", b"new")?;
        storage.commit_transaction(txn)?;
        drop(cursor);
        drop(storage);
        fs::OpenOptions::new().write(true).open(&path)?.set_len(PAGE_SIZE as u64)?;
        let wal_path = path.with_added_extension("wal");
        let wal = fs::read(&wal_path)?;
        let disk = DiskManager::open_existing(&path)?;
        let control = disk.faults();
        control.arm(operation, 0, Effect::Error(ErrorKind::Other));
        assert_io(Storage::from_disk_manager(path.clone(), disk, OPTIONS), &control)?;
        prop_assert_eq!(fs::read(wal_path)?, wal);
        let storage = open(&path, 1)?;
        let mut cursor = storage.tree_cursor(root)?;
        prop_assert_eq!(
            cursor.get(b"key")?.expect("recovered allocation").with_key_value(|_, v| v.to_vec())?,
            b"new"
        );
    }
    Ok(())
}
