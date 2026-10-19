use std::fs;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{DiskManager, DiskManagerError, image};
use crate::{
    PAGE_SIZE, Storage, StorageOptions,
    error::{InvalidArgumentError, StorageError},
    log_manager::{LogManager, LogRecordKind},
};

#[test]
fn unrepresentable_page_extents_are_rejected_without_changing_the_file() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pages");
    let before = image(&[7, 9]);
    fs::write(&path, before)?;
    let mut disk = DiskManager::open_existing(&path)?;
    for id in [u64::MAX / PAGE_SIZE as u64, u64::MAX - 1, u64::MAX] {
        let result = disk.ensure_page_exists(id);
        prop_assert!(
            matches!(result, Err(DiskManagerError::InvalidPageId { page_id }) if page_id == id),
            "invalid page extent was accepted"
        );
        prop_assert_eq!(disk.page_count(), 1);
        prop_assert_eq!(fs::read(&path)?, before);
    }
    prop_assert_eq!(disk.new_page()?, 1);
    prop_assert_eq!(fs::read(&path)?, [before, [0; PAGE_SIZE]].concat());
    Ok(())
}

#[test]
fn recovery_rejects_overflowing_page_allocations_without_discarding_database_or_wal()
-> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let options = StorageOptions { cache_frames: 8 };
    drop(Storage::open_or_create_with_options(&path, options)?);
    let before = fs::read(&path)?;
    let mut log = LogManager::new(&path)?;
    log.append_record(1, LogRecordKind::Begin)?;
    let id = u64::MAX / PAGE_SIZE as u64;
    log.append_record(1, LogRecordKind::PageAlloc { page_id: id })?;
    let lsn = log.append_record(1, LogRecordKind::Commit)?;
    log.flush_through(lsn)?;
    drop(log);
    let wal_path = path.with_added_extension("wal");
    let wal = fs::read(&wal_path)?;
    let result = Storage::open_or_create_with_options(&path, options);
    prop_assert!(
        matches!(result, Err(StorageError::InvalidArgument(InvalidArgumentError::InvalidPageId { page_id })) if page_id == id),
        "overflowing recovery allocation was accepted"
    );
    prop_assert_eq!(fs::read(&path)?, before);
    prop_assert_eq!(fs::read(wal_path)?, wal);
    Ok(())
}
