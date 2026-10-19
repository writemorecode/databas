use std::{fs, io::ErrorKind};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{DiskManager, DiskManagerError, Effect, Operation, image};
use crate::PAGE_SIZE;

fn io_kind(result: super::super::DiskManagerResult<()>, expected: ErrorKind) -> TestCaseResult {
    prop_assert!(matches!(result, Err(DiskManagerError::Io(error)) if error.kind() == expected));
    Ok(())
}

proptest! {
    #[test]
    fn fragmented_and_interrupted_transfers_preserve_complete_pages(
        pattern in prop::collection::vec(any::<u8>(), 1..=32), chunk in 1usize..=257, at in 0usize..PAGE_SIZE,
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pages");
        let mut disk = DiskManager::new(&path)?;
        disk.ensure_page_exists(2)?;
        let control = disk.faults();
        control.chunk(chunk);
        let expected = image(&pattern);
        control.arm(Operation::Write, at, Effect::Error(ErrorKind::Interrupted));
        disk.write_page(1, &expected)?;
        prop_assert!(control.consumed());
        prop_assert_eq!(fs::read(&path)?, [[0; PAGE_SIZE], expected, [0; PAGE_SIZE]].concat());
        control.arm(Operation::Read, at, Effect::Error(ErrorKind::Interrupted));
        let mut actual = [0; PAGE_SIZE];
        disk.read_page(1, &mut actual)?;
        prop_assert!(control.consumed());
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn failed_transfers_report_errors_and_preserve_other_pages(at in 0usize..PAGE_SIZE, zero in any::<bool>()) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pages");
        let before = [image(&[11, 12]), image(&[21, 22]), image(&[31, 32])];
        fs::write(&path, before.concat())?;
        let mut disk = DiskManager::open_existing(&path)?;
        let control = disk.faults();
        let effect = if zero { Effect::Zero } else { Effect::Error(ErrorKind::PermissionDenied) };
        control.arm(Operation::Read, at, effect);
        let mut bytes = [0; PAGE_SIZE];
        io_kind(disk.read_page(1, &mut bytes), if zero { ErrorKind::UnexpectedEof } else { ErrorKind::PermissionDenied })?;
        prop_assert!(control.consumed());
        prop_assert_eq!(fs::read(&path)?, before.concat());
        control.arm(Operation::Write, at, effect);
        let after = image(&[41, 42]);
        io_kind(disk.write_page(1, &after), if zero { ErrorKind::WriteZero } else { ErrorKind::PermissionDenied })?;
        prop_assert!(control.consumed());
        let mut expected = before;
        expected[1][..at].copy_from_slice(&after[..at]);
        prop_assert_eq!(fs::read(&path)?, expected.concat());
        // A failed page write need not be atomic. Discard the failed handle,
        // reopen, and explicitly repair the affected page.
        drop(disk);
        let mut disk = DiskManager::open_existing(&path)?;
        disk.write_page(1, &after)?;
        disk.sync()?;
        prop_assert_eq!(fs::read(path)?, [before[0], after, before[2]].concat());
    }
}

#[test]
fn failed_seeks_resizes_and_syncs_propagate_without_hiding_existing_pages() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pages");
    let before = image(&[9, 8, 7]);
    fs::write(&path, before)?;
    for operation in [Operation::Read, Operation::Write, Operation::Resize, Operation::Sync] {
        let mut disk = DiskManager::open_existing(&path)?;
        let control = disk.faults();
        control.arm(
            if matches!(operation, Operation::Read | Operation::Write) {
                Operation::Seek
            } else {
                operation
            },
            0,
            Effect::Error(ErrorKind::Other),
        );
        let result = match operation {
            Operation::Read => disk.read_page(0, &mut [0; PAGE_SIZE]),
            Operation::Write => disk.write_page(0, &[0xff; PAGE_SIZE]),
            Operation::Resize => disk.new_page().map(|_| ()),
            _ => disk.sync(),
        };
        io_kind(result, ErrorKind::Other)?;
        prop_assert!(control.consumed());
        drop(disk);
        let mut disk = DiskManager::open_existing(&path)?;
        prop_assert_eq!(disk.page_count(), 1);
        let mut actual = [0; PAGE_SIZE];
        disk.read_page(0, &mut actual)?;
        prop_assert_eq!(actual, before);
        prop_assert_eq!(fs::read(&path)?, before);
    }
    // Extension can succeed before its sync fails. No atomic rollback or
    // continued use of the failed handle is promised.
    let mut disk = DiskManager::open_existing(&path)?;
    let control = disk.faults();
    control.arm(Operation::Sync, 0, Effect::Error(ErrorKind::Other));
    io_kind(disk.ensure_page_exists(3), ErrorKind::Other)?;
    prop_assert!(control.consumed());
    drop(disk);
    let mut disk = DiskManager::open_existing(&path)?;
    prop_assert_eq!(disk.page_count(), 4);
    prop_assert_eq!(disk.new_page()?, 4);
    prop_assert_eq!(
        fs::read(path)?,
        [before, [0; PAGE_SIZE], [0; PAGE_SIZE], [0; PAGE_SIZE], [0; PAGE_SIZE]].concat()
    );
    Ok(())
}
