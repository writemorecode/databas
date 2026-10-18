use std::io;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{ChunkedReader, encoded};
use crate::PAGE_SIZE;
use crate::log_manager::{RecoveryLogRecordKind, scan_transaction_frame};

#[test]
fn interrupted_reads_preserve_frames_at_header_payload_footer_and_eof() -> TestCaseResult {
    let model = (
        7,
        vec![
            RecoveryLogRecordKind::Begin,
            RecoveryLogRecordKind::PageUpdate {
                page_id: 3,
                redo_data: Box::new([0xa5; PAGE_SIZE]),
                undo_data: Box::new([0x5a; PAGE_SIZE]),
            },
            RecoveryLogRecordKind::Commit,
        ],
    );
    let bytes = encoded(&model)?;
    // Every header/footer byte boundary plus payload and page-image boundaries.
    for at in (0..30).chain([30, 31, 40, 48, 4096, 8192]).chain(bytes.len() - 20..=bytes.len()) {
        let mut reader = ChunkedReader {
            bytes: &bytes,
            position: 0,
            chunk: 17,
            error: Some((at, io::ErrorKind::Interrupted)),
        };
        let frame = scan_transaction_frame(&mut reader)?;
        prop_assert!(frame.is_some(), "missing frame after interruption at {at}");
        if let Some(frame) = frame {
            prop_assert_eq!(frame.txn_id, model.0);
            prop_assert_eq!(frame.record_count as usize, model.1.len());
        }
        prop_assert_eq!(reader.position, bytes.len());
        prop_assert!(scan_transaction_frame(&mut reader)?.is_none());
    }
    Ok(())
}
