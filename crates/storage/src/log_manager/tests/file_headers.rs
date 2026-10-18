use std::fs;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{expected_scan, fixture_wal};
use crate::log_manager::{LogManager, read_checkpoint_log, read_recovery_log};

#[test]
fn nonempty_truncated_file_headers_are_rejected_without_rewriting() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let wal_path = path.with_added_extension("wal");
    let header = fixture_wal(12345, 9001, &[])?;
    for len in 1..header.len() {
        let bytes = &header[..len];
        fs::write(&wal_path, bytes)?;
        prop_assert!(read_recovery_log(&path).is_err(), "accepted {len}-byte file header");
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
        prop_assert!(read_checkpoint_log(&path).is_err(), "accepted {len}-byte file header");
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
        prop_assert!(LogManager::new(&path).is_err(), "accepted {len}-byte file header");
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
    }
    Ok(())
}

#[test]
fn absent_and_empty_wals_initialize_with_zero_watermarks() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let expected = expected_scan(0, 0, &[]);
    for empty_file in [false, true] {
        // Exercise initialization through each entry point independently.
        for entry_point in 0..3 {
            let path = dir.path().join(format!("db-{empty_file}-{entry_point}"));
            let wal_path = path.with_added_extension("wal");
            if empty_file {
                fs::write(&wal_path, [])?;
            }
            match entry_point {
                0 => prop_assert_eq!(read_recovery_log(&path)?, expected.clone()),
                1 => prop_assert_eq!(read_checkpoint_log(&path)?, expected.clone()),
                _ => {
                    let manager = LogManager::new(&path)?;
                    prop_assert_eq!(manager.next_lsn()?, 1);
                    prop_assert_eq!(manager.highest_txn_id(), 0);
                }
            }
            prop_assert_eq!(fs::read(&wal_path)?, fixture_wal(0, 0, &[])?);
        }
    }
    Ok(())
}
