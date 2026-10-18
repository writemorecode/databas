use std::io::{self, Write};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{borrowed, check_decoded, checksum, encoded, fixture_frame, transaction};
use crate::PAGE_SIZE;
use crate::log_manager::{
    LogManagerError, LogRecord, LogRecordKind, RecoveryLogRecordKind, serialize_transaction,
};

#[test]
fn version_two_frames_match_independent_format_fixtures() -> TestCaseResult {
    prop_assert_eq!(checksum(b"123456789"), 0xcbf4_3926);
    let txn_id = 0x0123_4567_89ab_cdef;
    let page_id = 0xfedc_ba98_7654_3210;
    let transaction = (
        txn_id,
        vec![
            RecoveryLogRecordKind::Begin,
            RecoveryLogRecordKind::PageAlloc { page_id },
            RecoveryLogRecordKind::PageUpdate {
                page_id,
                redo_data: Box::new([0xa5; PAGE_SIZE]),
                undo_data: Box::new([0x5a; PAGE_SIZE]),
            },
            RecoveryLogRecordKind::Commit,
            RecoveryLogRecordKind::Rollback,
        ],
    );
    let mut payload = vec![1, 5];
    payload.extend(page_id.to_le_bytes());
    payload.push(4);
    payload.extend(page_id.to_le_bytes());
    payload.extend([0, 16, 0, 0, 0, 16, 0, 0]); // Two 4096-byte images, little endian.
    payload.extend([0xa5; PAGE_SIZE]);
    payload.extend([0x5a; PAGE_SIZE]);
    payload.extend([2, 3]);
    for (model, expected) in
        [(transaction, fixture_frame(txn_id, 5, &payload)), ((0, vec![]), fixture_frame(0, 0, &[]))]
    {
        prop_assert_eq!(&encoded(&model)?, &expected);
        check_decoded(&expected, &model)?;
    }
    Ok(())
}

proptest! {
    #[test]
    fn frames_preserve_record_order_ids_and_both_page_images(model in transaction()) {
        check_decoded(&encoded(&model)?, &model)?;
    }

    #[test]
    fn incomplete_frames_and_trailing_bytes_are_rejected(
        model in transaction(), cut in any::<usize>(), suffix in prop::collection::vec(any::<u8>(), 1..=32),
    ) {
        let mut bytes = encoded(&model)?;
        let cut = cut % bytes.len();
        prop_assert!(crate::log_manager::deserialize_transaction(&bytes[..cut]).is_err());
        bytes.extend(suffix);
        prop_assert!(crate::log_manager::deserialize_transaction(&bytes).is_err());
    }

    #[test]
    fn changing_any_single_frame_bit_is_detected(model in transaction(), position in any::<usize>(), bit in 0u8..8) {
        let mut bytes = encoded(&model)?;
        let position = position % bytes.len();
        bytes[position] ^= 1 << bit;
        prop_assert!(crate::log_manager::deserialize_transaction(&bytes).is_err());
    }

    #[test]
    fn invalid_records_emit_no_bytes(txn_id in 0u64..u64::MAX, invalid_len in prop::sample::select(vec![0usize, 1, PAGE_SIZE - 1, PAGE_SIZE + 1])) {
        let full = [0; PAGE_SIZE];
        let invalid = vec![0; invalid_len];
        for kind in [
            LogRecordKind::PageUpdate { page_id: 0, redo_data: &invalid, undo_data: &full },
            LogRecordKind::PageUpdate { page_id: 0, redo_data: &full, undo_data: &invalid },
        ] {
            let mut bytes = Vec::new();
            let records = [LogRecord { txn_id, kind: LogRecordKind::Begin }, LogRecord { txn_id, kind }];
            prop_assert!(matches!(serialize_transaction(&mut bytes, txn_id, &records), Err(LogManagerError::InvalidPageImageLength { .. })), "invalid image accepted");
            prop_assert!(bytes.is_empty());
        }
        let mut bytes = Vec::new();
        let records = [LogRecord { txn_id, kind: LogRecordKind::Begin }, LogRecord { txn_id: txn_id + 1, kind: LogRecordKind::Commit }];
        prop_assert!(matches!(serialize_transaction(&mut bytes, txn_id, &records), Err(LogManagerError::RecordTxnIdMismatch { .. })), "foreign transaction accepted");
        prop_assert!(bytes.is_empty());
    }

    #[test]
    fn short_writes_preserve_frames_and_write_failures_propagate(model in transaction(), chunk in 1usize..=37, stop in any::<usize>()) {
        let expected = encoded(&model)?;
        let mut writer = FragmentedWriter { bytes: vec![], chunk, stop: expected.len() };
        serialize_transaction(&mut writer, model.0, &borrowed(&model))?;
        prop_assert_eq!(&writer.bytes, &expected);
        writer.bytes.clear();
        writer.stop = stop % expected.len();
        prop_assert!(matches!(serialize_transaction(&mut writer, model.0, &borrowed(&model)), Err(LogManagerError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied));
        prop_assert_eq!(&writer.bytes, &expected[..writer.stop]);
    }
}

struct FragmentedWriter {
    bytes: Vec<u8>,
    chunk: usize,
    stop: usize,
}

impl Write for FragmentedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len() == self.stop {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let len = bytes.len().min(self.chunk).min(self.stop - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..len]);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
