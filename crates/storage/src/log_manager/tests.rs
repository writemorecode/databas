//! Exercise the byte-format and valid-prefix contracts, independent of recovery replay.

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{LogRecord, LogRecordKind, RecoveryLogRecordKind, TxnId};
use crate::PAGE_SIZE;

mod encoding;
mod file_headers;
mod interrupted_reads;
mod scanning;

type Transaction = (TxnId, Vec<RecoveryLogRecordKind>);

fn image() -> impl Strategy<Value = Box<[u8; PAGE_SIZE]>> {
    // Arbitrary repeating patterns shrink cheaply while exercising every image byte.
    prop::collection::vec(any::<u8>(), 1..=64)
        .prop_map(|pattern| Box::new(std::array::from_fn(|i| pattern[i % pattern.len()])))
}

fn transaction() -> impl Strategy<Value = Transaction> {
    let record = prop_oneof![
        Just(RecoveryLogRecordKind::Begin),
        Just(RecoveryLogRecordKind::Commit),
        Just(RecoveryLogRecordKind::Rollback),
        any::<u64>().prop_map(|page_id| RecoveryLogRecordKind::PageAlloc { page_id }),
        (any::<u64>(), image(), image()).prop_map(|(page_id, redo_data, undo_data)| {
            RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data }
        }),
    ];
    // Frames are encoding units, not necessarily whole logical transactions.
    (any::<TxnId>(), prop::collection::vec(record, 0..=8))
}

fn borrowed((txn_id, records): &Transaction) -> Vec<LogRecord<'_>> {
    records
        .iter()
        .map(|kind| {
            let kind = match kind {
                RecoveryLogRecordKind::Begin => LogRecordKind::Begin,
                RecoveryLogRecordKind::Commit => LogRecordKind::Commit,
                RecoveryLogRecordKind::Rollback => LogRecordKind::Rollback,
                RecoveryLogRecordKind::PageAlloc { page_id } => {
                    LogRecordKind::PageAlloc { page_id: *page_id }
                }
                RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                    LogRecordKind::PageUpdate {
                        page_id: *page_id,
                        redo_data: redo_data.as_ref(),
                        undo_data: undo_data.as_ref(),
                    }
                }
            };
            LogRecord { txn_id: *txn_id, kind }
        })
        .collect()
}

fn encoded(transaction: &Transaction) -> Result<Vec<u8>, super::LogManagerError> {
    let mut bytes = Vec::new();
    super::serialize_transaction(&mut bytes, transaction.0, &borrowed(transaction))?;
    Ok(bytes)
}

fn check_decoded(bytes: &[u8], expected: &Transaction) -> TestCaseResult {
    let actual = super::deserialize_transaction(bytes)?;
    prop_assert_eq!(actual.txn_id, expected.0);
    prop_assert_eq!(actual.records.len(), expected.1.len());
    for (actual, expected_kind) in actual.records.iter().zip(&expected.1) {
        prop_assert_eq!(actual.txn_id, expected.0);
        match (&actual.kind, expected_kind) {
            (LogRecordKind::Begin, RecoveryLogRecordKind::Begin)
            | (LogRecordKind::Commit, RecoveryLogRecordKind::Commit)
            | (LogRecordKind::Rollback, RecoveryLogRecordKind::Rollback) => {}
            (
                LogRecordKind::PageAlloc { page_id: actual },
                RecoveryLogRecordKind::PageAlloc { page_id: expected },
            ) => prop_assert_eq!(actual, expected),
            (
                LogRecordKind::PageUpdate { page_id, redo_data, undo_data },
                RecoveryLogRecordKind::PageUpdate {
                    page_id: expected_id,
                    redo_data: redo,
                    undo_data: undo,
                },
            ) => {
                prop_assert_eq!(page_id, expected_id);
                prop_assert_eq!(*redo_data, redo.as_ref());
                prop_assert_eq!(*undo_data, undo.as_ref());
            }
            _ => prop_assert!(false, "record kind changed: {actual:?} versus {expected_kind:?}"),
        }
    }
    Ok(())
}

// Independent bit-at-a-time IEEE CRC32 oracle, not the production CRC library.
fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 * (crc & 1));
        }
    }
    !crc
}

fn fixture_frame(txn_id: TxnId, count: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"DBWALHDR\x02\x00".to_vec();
    bytes.extend(txn_id.to_le_bytes());
    bytes.extend(count.to_le_bytes());
    bytes.extend((payload.len() as u64).to_le_bytes());
    bytes.extend(payload);
    bytes.extend(b"DBWALFTR");
    bytes.extend(txn_id.to_le_bytes());
    bytes.extend(checksum(payload).to_le_bytes());
    bytes
}

fn fixture_wal(
    base_lsn: u64,
    max_txn_id: TxnId,
    frames: &[Transaction],
) -> Result<Vec<u8>, super::LogManagerError> {
    let mut bytes = vec![0; 32];
    bytes[..10].copy_from_slice(b"DBWALHDR\x02\x00");
    bytes[16..24].copy_from_slice(&base_lsn.to_le_bytes());
    bytes[24..32].copy_from_slice(&max_txn_id.to_le_bytes());
    let crc = checksum(&bytes);
    bytes[10..14].copy_from_slice(&crc.to_le_bytes());
    for frame in frames {
        bytes.extend(encoded(frame)?);
    }
    Ok(bytes)
}

fn expected_scan(
    base_lsn: u64,
    max_txn_id: TxnId,
    frames: &[Transaction],
) -> super::RecoveryLogScan {
    let mut result = super::RecoveryLogScan {
        records: vec![],
        truncated_tail: false,
        max_txn_id,
        last_assigned_lsn: base_lsn,
    };
    for (txn_id, kinds) in frames {
        result.max_txn_id = result.max_txn_id.max(*txn_id);
        for kind in kinds {
            result.last_assigned_lsn += 1;
            result.records.push(super::RecoveryLogRecord {
                lsn: result.last_assigned_lsn,
                txn_id: *txn_id,
                kind: kind.clone(),
            });
        }
    }
    result
}

struct ChunkedReader<'a> {
    bytes: &'a [u8],
    position: usize,
    chunk: usize,
    error: Option<(usize, std::io::ErrorKind)>,
}

impl std::io::Read for ChunkedReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let until = if let Some((at, kind)) = self.error {
            if self.position == at {
                self.error = None;
                return Err(kind.into());
            }
            at - self.position
        } else {
            usize::MAX
        };
        let len = out.len().min(self.chunk).min(until).min(self.bytes.len() - self.position);
        out[..len].copy_from_slice(&self.bytes[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }
}
