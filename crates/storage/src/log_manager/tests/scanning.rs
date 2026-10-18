use std::{fs, io};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{ChunkedReader, encoded, expected_scan, fixture_frame, fixture_wal, transaction};
use crate::log_manager::{
    LogManager, LogManagerError, LogRecordKind, RecoveryLogRecordKind, deserialize_transaction,
    read_checkpoint_log, read_recovery_log, scan_transaction_frame,
};

proptest! {
    #[test]
    fn fragmented_streams_yield_exactly_one_frame_at_a_time(
        frames in prop::collection::vec(transaction(), 0..=5), chunk in 1usize..=127,
    ) {
        let lengths = frames.iter().map(encoded).collect::<Result<Vec<_>, _>>()?;
        let bytes = lengths.concat();
        let mut reader = ChunkedReader { bytes: &bytes, position: 0, chunk, error: None };
        let mut end = 0;
        for (model, bytes) in frames.iter().zip(lengths) {
            let frame = scan_transaction_frame(&mut reader)?.ok_or_else(|| proptest::test_runner::TestCaseError::fail("missing frame"))?;
            prop_assert_eq!(frame.txn_id, model.0);
            prop_assert_eq!(frame.record_count as usize, model.1.len());
            end += bytes.len();
            prop_assert_eq!(reader.position, end);
        }
        prop_assert!(scan_transaction_frame(&mut reader)?.is_none());
    }

    #[test]
    fn stream_and_slice_decoders_agree_on_damaged_frames(
        model in transaction(), position in any::<usize>(), bit in 0u8..8, cut in any::<usize>(),
    ) {
        let mut bytes = encoded(&model)?;
        let position = position % bytes.len();
        bytes[position] ^= 1 << bit;
        for input in [bytes.as_slice(), &bytes[..cut % bytes.len()]] {
            let decoded = deserialize_transaction(input).is_ok();
            let mut reader = io::Cursor::new(input);
            let scanned = matches!(scan_transaction_frame(&mut reader), Ok(Some(_))) && reader.position() == input.len() as u64;
            prop_assert_eq!(decoded, scanned);
        }
    }

    #[test]
    fn stream_read_failures_are_reported_without_scanning_past_them(model in transaction(), stop in any::<usize>()) {
        let bytes = encoded(&model)?;
        let stop = stop % bytes.len();
        let mut reader = ChunkedReader { bytes: &bytes, position: 0, chunk: 17, error: Some((stop, io::ErrorKind::PermissionDenied)) };
        prop_assert!(matches!(scan_transaction_frame(&mut reader), Err(LogManagerError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied));
        prop_assert_eq!(reader.position, stop);
    }

    #[test]
    fn scans_and_reopen_preserve_records_and_continue_lsn_assignment(
        frames in prop::collection::vec(transaction(), 0..=5), base_lsn in 0u64..1000, max_txn_id in any::<u64>(),
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let wal_path = path.with_added_extension("wal");
        let bytes = fixture_wal(base_lsn, max_txn_id, &frames)?;
        fs::write(&wal_path, &bytes)?;
        let mut expected = expected_scan(base_lsn, max_txn_id, &frames);
        prop_assert_eq!(&read_recovery_log(&path)?, &expected);
        prop_assert_eq!(&read_checkpoint_log(&path)?, &expected);
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
        let mut manager = LogManager::new(&path)?;
        prop_assert_eq!(manager.highest_txn_id(), expected.max_txn_id);
        prop_assert_eq!(manager.next_lsn()?, expected.last_assigned_lsn + 1);
        let lsn = manager.append_record(0, LogRecordKind::Begin)?;
        prop_assert_eq!(lsn, expected.last_assigned_lsn + 1);
        manager.flush_through(lsn)?;
        drop(manager);
        expected.last_assigned_lsn = lsn;
        expected.records.push(crate::log_manager::RecoveryLogRecord { lsn, txn_id: 0, kind: RecoveryLogRecordKind::Begin });
        prop_assert_eq!(&read_recovery_log(&path)?, &expected);
    }

    #[test]
    fn incomplete_suffix_is_removed_only_by_recovery(
        frames in prop::collection::vec(transaction(), 0..=4), tail in transaction(), cut in any::<usize>(),
        base_lsn in 0u64..1000, max_txn_id in any::<u64>(),
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let wal_path = path.with_added_extension("wal");
        let prefix = fixture_wal(base_lsn, max_txn_id, &frames)?;
        let tail = encoded(&tail)?;
        let cut = 1 + cut % (tail.len() - 1);
        let bytes = [&prefix[..], &tail[..cut]].concat();
        fs::write(&wal_path, &bytes)?;
        prop_assert!(read_checkpoint_log(&path).is_err());
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
        let mut expected = expected_scan(base_lsn, max_txn_id, &frames);
        expected.truncated_tail = true;
        prop_assert_eq!(&read_recovery_log(&path)?, &expected);
        prop_assert_eq!(fs::read(&wal_path)?, prefix);
        expected.truncated_tail = false;
        prop_assert_eq!(&read_recovery_log(&path)?, &expected);
    }

    #[test]
    fn recovery_stops_at_corruption_even_when_later_frames_are_valid(
        frames in prop::collection::vec(transaction(), 0..=4), torn in transaction(), later in transaction(),
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let wal_path = path.with_added_extension("wal");
        let prefix = fixture_wal(41, 7, &frames)?;
        let mut bytes = prefix.clone();
        let mut damaged = encoded(&torn)?;
        if let Some(last) = damaged.last_mut() { *last ^= 1; }
        bytes.extend(damaged);
        bytes.extend(encoded(&later)?);
        fs::write(&wal_path, &bytes)?;
        prop_assert!(read_checkpoint_log(&path).is_err());
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
        let mut expected = expected_scan(41, 7, &frames);
        expected.truncated_tail = true;
        prop_assert_eq!(&read_recovery_log(&path)?, &expected);
        prop_assert_eq!(fs::read(&wal_path)?, prefix);
    }
}

#[test]
fn valid_checksums_do_not_make_malformed_record_payloads_valid() -> TestCaseResult {
    let mut invalid_payloads = vec![vec![0], vec![6], vec![255], vec![5], vec![4], vec![1, 2]];
    for len in [0u32, 4095, 4097, u32::MAX] {
        for (redo, undo) in [(len, 4096u32), (4096, len)] {
            let mut payload = vec![4];
            payload.extend(1u64.to_le_bytes());
            payload.extend(redo.to_le_bytes());
            payload.extend(undo.to_le_bytes());
            payload.extend([0; 8192]);
            invalid_payloads.push(payload);
        }
    }
    for payload in invalid_payloads {
        let bytes = fixture_frame(3, 1, &payload);
        prop_assert!(deserialize_transaction(&bytes).is_err());
        prop_assert!(scan_transaction_frame(&mut bytes.as_slice()).is_err());
    }
    Ok(())
}

#[test]
fn header_damage_future_versions_and_lsn_exhaustion_preserve_the_file() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let wal_path = path.with_added_extension("wal");
    let original = fixture_wal(41, 7, &[(8, vec![RecoveryLogRecordKind::Commit])])?;
    let mut cases = vec![];
    // Every file-header byte, including reserved bytes, is integrity protected.
    for at in 0..32 {
        let mut bytes = original.clone();
        bytes[at] ^= 1;
        cases.push(bytes);
    }
    // A nonzero future frame version is incompatible, not a torn suffix.
    let mut future = original;
    future[40..42].copy_from_slice(&3u16.to_le_bytes());
    cases.push(future);
    cases.push(fixture_wal(u64::MAX, 7, &[(8, vec![RecoveryLogRecordKind::Begin])])?);
    for bytes in cases {
        fs::write(&wal_path, &bytes)?;
        prop_assert!(read_recovery_log(&path).is_err());
        prop_assert!(read_checkpoint_log(&path).is_err());
        prop_assert!(LogManager::new(&path).is_err());
        prop_assert_eq!(fs::read(&wal_path)?, bytes);
    }
    Ok(())
}
