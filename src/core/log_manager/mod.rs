//! Write-ahead log (WAL) serialization, append, flush, and recovery scanning.
//!
//! The WAL is stored next to the database file using the same path with an
//! additional `.wal` extension. Records are physically grouped into transaction
//! frames:
//!
//! ```text
//! header magic | format version | transaction id | record count | payload length
//! payload records...
//! footer magic | transaction id | payload CRC32
//! ```
//!
//! LSNs are not stored explicitly in the frame. They are assigned logically by
//! record position across complete frames, starting at 1. Appending advances
//! the highest appended LSN, while [`LogManager::flush_through`] is responsible
//! for making appended bytes durable before pages protected by those LSNs are
//! written to the database file.

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::core::{PAGE_SIZE, PageId};

mod frame;

#[cfg(test)]
pub(crate) use frame::read_log_record_kinds_for_test;
#[cfg(test)]
use frame::{CRC32, FOOTER_LEN};
use frame::{
    HEADER_LEN, deserialize_transaction, scan_transaction_frame, serialize_transaction,
    transaction_frame_len, validate_record_txn_ids, wal_open_error,
};

/// Monotonic identifier for a transaction in the WAL.
pub(crate) type TxnId = u64;
/// Log sequence number assigned to one WAL record.
pub(crate) type Lsn = u64;
/// Sentinel LSN used for pages that do not reflect any logged update.
pub(crate) const ZERO_LSN: Lsn = 0;

const WAL_READ_BUFFER_LEN: usize = 64 * 1024;
const WAL_WRITE_BUFFER_LEN: usize = 64 * 1024;

/// Errors raised while opening, writing, or decoding WAL frames.
#[derive(Debug, Error)]
pub enum LogManagerError {
    /// Underlying filesystem operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The supplied database path could not be transformed into a WAL path.
    #[error("invalid database file path: {db_file_path}")]
    InvalidDbFilePath { db_file_path: PathBuf },
    /// A frame did not start with the expected WAL header marker.
    #[error("invalid WAL header magic: {actual:?}")]
    InvalidHeaderMagic { actual: [u8; 8] },
    /// A frame did not end with the expected WAL footer marker.
    #[error("invalid WAL footer magic: {actual:?}")]
    InvalidFooterMagic { actual: [u8; 8] },
    /// The WAL was produced by an incompatible format version.
    #[error("unsupported WAL format version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u16, actual: u16 },
    /// A frame ended before the number of bytes promised by its header.
    #[error("truncated WAL frame: needed {needed} bytes, remaining {remaining}")]
    TruncatedFrame { needed: usize, remaining: usize },
    /// A payload length from disk cannot fit in this process's address space.
    #[error("WAL payload length does not fit in memory: {payload_len}")]
    PayloadLengthTooLarge { payload_len: u64 },
    /// Frame length arithmetic overflowed while sizing a WAL payload.
    #[error("WAL payload length overflow")]
    PayloadLengthOverflow,
    /// A frame payload failed CRC validation.
    #[error("WAL checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    /// A payload contained an unrecognized record-kind tag.
    #[error("unknown WAL log record kind: {kind}")]
    UnknownRecordKind { kind: u8 },
    /// The footer transaction id did not match the frame header.
    #[error("WAL footer txn id mismatch: expected {expected}, got {actual}")]
    FooterTxnIdMismatch { expected: TxnId, actual: TxnId },
    /// A record was appended in a transaction frame for a different transaction.
    #[error("WAL record txn id mismatch: expected {expected}, got {actual}")]
    RecordTxnIdMismatch { expected: TxnId, actual: TxnId },
    /// The decoded payload record count differed from the header count.
    #[error("WAL record count mismatch: expected {expected}, got {actual}")]
    RecordCountMismatch { expected: u32, actual: u32 },
    /// A transaction frame contains more records than the format can encode.
    #[error("too many WAL records in transaction: {count}")]
    TooManyRecords { count: usize },
    /// The next LSN would exceed the numeric range of [`Lsn`].
    #[error("WAL LSN exhausted")]
    LsnExhausted,
    /// A page update record did not contain full `PAGE_SIZE` images.
    #[error("WAL full-page image has invalid length: expected {expected}, got {actual}")]
    InvalidPageImageLength { expected: usize, actual: usize },
}

/// Errors raised while forcing appended WAL bytes to durable storage.
#[derive(Debug, Error)]
pub(crate) enum LogManagerFlushError {
    /// The filesystem sync failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The caller requested durability for an LSN that has not been appended.
    #[error(
        "requested WAL flush through LSN {requested_lsn}, but highest appended LSN is {highest_appended_lsn:?}"
    )]
    LsnNotAppended { requested_lsn: Lsn, highest_appended_lsn: Option<Lsn> },
}

/// Borrowed WAL record used while serializing a transaction frame.
#[derive(Debug)]
pub(crate) struct LogRecord<'a> {
    /// Transaction that owns this record.
    pub(crate) txn_id: TxnId,
    /// Record payload.
    pub(crate) kind: LogRecordKind<'a>,
}

/// Borrowed WAL record payload.
#[derive(Debug)]
pub(crate) enum LogRecordKind<'a> {
    /// Transaction start marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Transaction rollback-complete marker.
    Rollback,
    /// Full-page physical update.
    ///
    /// `redo_data` is the complete page image to install while redoing a
    /// committed transaction. `undo_data` is the complete page image to restore
    /// while undoing an incomplete transaction during recovery or explicit
    /// rollback.
    PageUpdate { page_id: PageId, redo_data: &'a [u8], undo_data: &'a [u8] },
    /// Allocation of a database page by a transaction.
    PageAlloc { page_id: PageId },
}

/// Owned WAL record returned by recovery scans.
///
/// Unlike [`LogRecord`], this form owns page images and includes the LSN
/// assigned from the record's position in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryLogRecord {
    /// Logical LSN assigned to this record.
    pub(crate) lsn: Lsn,
    /// Transaction that owns this record.
    pub(crate) txn_id: TxnId,
    /// Owned record payload.
    pub(crate) kind: RecoveryLogRecordKind,
}

/// Owned WAL record payload used by crash recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryLogRecordKind {
    /// Transaction start marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Transaction rollback-complete marker.
    Rollback,
    /// Full-page physical update with owned redo and undo page images.
    PageUpdate { page_id: PageId, redo_data: Box<[u8; PAGE_SIZE]>, undo_data: Box<[u8; PAGE_SIZE]> },
    /// Allocation of a database page by a transaction.
    PageAlloc { page_id: PageId },
}

/// Result of scanning the WAL for recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryLogScan {
    /// Complete records decoded from valid WAL frames, in log order.
    pub(crate) records: Vec<RecoveryLogRecord>,
    /// Whether an incomplete final frame was found and truncated.
    pub(crate) truncated_tail: bool,
    /// Largest transaction id observed in complete frames.
    pub(crate) max_txn_id: TxnId,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnedLogRecordKind {
    Begin,
    Commit,
    Rollback,
    PageUpdate { page_id: PageId },
    PageAlloc { page_id: PageId },
}

#[derive(Debug)]
pub(crate) struct LogTransaction<'a> {
    /// Transaction id shared by all records in this frame.
    pub(crate) txn_id: TxnId,
    /// Records decoded from the frame payload.
    pub(crate) records: Vec<LogRecord<'a>>,
}

/// Append-only manager for the database write-ahead log.
///
/// `LogManager` tracks two positions: the highest LSN appended to the WAL file
/// and the highest LSN known to be durable after a successful flush. Page-cache
/// code uses that distinction to enforce the write-ahead rule before writing a
/// dirty page whose page header references a logged LSN.
#[derive(Debug)]
pub(crate) struct LogManager {
    wal_writer: BufWriter<File>,

    highest_txn_id: TxnId,
    highest_appended_lsn: Option<Lsn>,
    highest_durable_lsn: Option<Lsn>,
    #[cfg(test)]
    fail_next_flush: bool,
}

impl LogManager {
    /// Opens or creates the WAL file associated with `db_file_path`.
    ///
    /// Existing complete frames are scanned to reconstruct the highest appended
    /// LSN and highest transaction id. Opening does not mark existing bytes as
    /// durable in this process; callers must still use [`Self::flush_through`]
    /// before relying on a newly appended record.
    pub(crate) fn new(db_file_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let db_file_path = db_file_path.as_ref().to_path_buf();
        let wal_file_path = db_file_path.with_added_extension("wal");
        let mut wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .truncate(false)
            .open(wal_file_path)?;

        let mut highest_appended_lsn = None;
        let mut highest_txn_id = 0;
        wal_file.seek(std::io::SeekFrom::Start(0))?;
        {
            let mut wal_reader = BufReader::with_capacity(WAL_READ_BUFFER_LEN, &mut wal_file);
            while let Some(frame) =
                scan_transaction_frame(&mut wal_reader).map_err(wal_open_error)?
            {
                highest_txn_id = highest_txn_id.max(frame.txn_id);
                if frame.record_count > 0 {
                    let next_lsn = highest_appended_lsn
                        .unwrap_or(ZERO_LSN)
                        .checked_add(u64::from(frame.record_count))
                        .ok_or(LogManagerError::LsnExhausted)
                        .map_err(wal_open_error)?;
                    highest_appended_lsn = Some(next_lsn);
                }
            }
        }
        wal_file.seek(std::io::SeekFrom::End(0))?;

        let wal_writer = BufWriter::with_capacity(WAL_WRITE_BUFFER_LEN, wal_file);

        Ok(Self {
            wal_writer,
            highest_txn_id,
            highest_durable_lsn: None,
            highest_appended_lsn,
            #[cfg(test)]
            fail_next_flush: false,
        })
    }

    /// Returns the largest transaction id observed in the WAL.
    pub(crate) fn highest_txn_id(&self) -> TxnId {
        self.highest_txn_id
    }

    /// Returns the LSN that would be assigned to the next appended record.
    pub(crate) fn next_lsn(&self) -> Result<Lsn, LogManagerError> {
        self.highest_appended_lsn
            .unwrap_or(ZERO_LSN)
            .checked_add(1)
            .ok_or(LogManagerError::LsnExhausted)
    }

    /// Appends one WAL record as a single-record transaction frame.
    ///
    /// Returns the LSN assigned to the appended record. The record is appended
    /// to the operating system file handle but is not forced to durable storage;
    /// use [`Self::flush_through`] for durability.
    pub(crate) fn append_record<'a>(
        &mut self,
        txn_id: TxnId,
        kind: LogRecordKind<'a>,
    ) -> Result<Lsn, LogManagerError> {
        self.append_transaction(txn_id, &[LogRecord { txn_id, kind }])
    }

    /// Appends a transaction frame containing `records`.
    ///
    /// All records must belong to `txn_id`. LSNs are assigned one per record in
    /// frame order, and the return value is the highest LSN assigned by this
    /// append. Empty frames are allowed for serializer symmetry and return the
    /// current highest appended LSN without advancing it.
    pub(crate) fn append_transaction<'a>(
        &mut self,
        txn_id: TxnId,
        records: &[LogRecord<'a>],
    ) -> Result<Lsn, LogManagerError> {
        validate_record_txn_ids(txn_id, records)?;

        let record_count = u64::try_from(records.len()).expect("usize record count fits in u64");
        let lsn = self
            .highest_appended_lsn
            .unwrap_or(ZERO_LSN)
            .checked_add(record_count)
            .ok_or(LogManagerError::LsnExhausted)?;

        serialize_transaction(&mut self.wal_writer, txn_id, records)?;

        self.highest_txn_id = self.highest_txn_id.max(txn_id);
        if record_count > 0 {
            self.highest_appended_lsn = Some(lsn);
        }
        Ok(lsn)
    }

    /// Forces WAL bytes through `requested_lsn` to durable storage.
    ///
    /// This is the durability half of the write-ahead rule. The call is a no-op
    /// for [`ZERO_LSN`] and for LSNs already known durable. On success, the
    /// manager conservatively marks all currently appended records durable
    /// because the underlying file sync covers the whole WAL file.
    pub(crate) fn flush_through(&mut self, requested_lsn: Lsn) -> Result<(), LogManagerFlushError> {
        if requested_lsn == ZERO_LSN {
            return Ok(());
        }

        if self.highest_durable_lsn.unwrap_or(ZERO_LSN) >= requested_lsn {
            return Ok(());
        }

        let Some(highest_appended_lsn) = self.highest_appended_lsn else {
            return Err(LogManagerFlushError::LsnNotAppended {
                requested_lsn,
                highest_appended_lsn: None,
            });
        };

        if highest_appended_lsn < requested_lsn {
            return Err(LogManagerFlushError::LsnNotAppended {
                requested_lsn,
                highest_appended_lsn: Some(highest_appended_lsn),
            });
        }

        self.wal_writer.flush()?;

        #[cfg(test)]
        if self.fail_next_flush {
            self.fail_next_flush = false;
            return Err(LogManagerFlushError::Io(std::io::Error::other(
                "injected WAL flush failure",
            )));
        }

        self.wal_writer.get_ref().sync_all()?;
        self.highest_durable_lsn = Some(highest_appended_lsn);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn highest_appended_lsn(&self) -> Option<Lsn> {
        self.highest_appended_lsn
    }

    #[cfg(test)]
    pub(crate) fn highest_durable_lsn(&self) -> Option<Lsn> {
        self.highest_durable_lsn
    }

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&mut self) {
        self.highest_appended_lsn = Some(Lsn::MAX);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_flush_for_test(&mut self) {
        self.fail_next_flush = true;
    }

    #[cfg(test)]
    pub(crate) fn flush_buffer_for_test(&mut self) -> std::io::Result<()> {
        self.wal_writer.flush()
    }
}

/// Reads complete WAL frames and returns owned records for crash recovery.
///
/// A torn or incomplete final frame is treated as a non-durable tail: it is
/// truncated from the WAL and reported through [`RecoveryLogScan::truncated_tail`].
/// Corruption in a complete-looking frame, such as bad magic or a checksum
/// mismatch, is returned as an error and the WAL is left intact.
pub(crate) fn read_recovery_log(
    db_file_path: impl AsRef<Path>,
) -> Result<RecoveryLogScan, LogManagerError> {
    let wal_file_path = db_file_path.as_ref().with_added_extension("wal");
    let mut wal_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(wal_file_path)?;
    let mut buf = Vec::new();
    wal_file.read_to_end(&mut buf)?;

    let mut records = Vec::new();
    let mut offset = 0;
    let mut next_lsn = ZERO_LSN;
    let mut max_txn_id = 0;
    let mut truncated_tail = false;

    while offset < buf.len() {
        if buf.len() - offset < HEADER_LEN {
            truncated_tail = true;
            break;
        }

        let frame_len = match transaction_frame_len(&buf[offset..]) {
            Ok(frame_len) => frame_len,
            Err(LogManagerError::TruncatedFrame { .. }) => {
                truncated_tail = true;
                break;
            }
            Err(err) => return Err(err),
        };

        let frame_end =
            offset.checked_add(frame_len).ok_or(LogManagerError::PayloadLengthOverflow)?;
        if frame_end > buf.len() {
            truncated_tail = true;
            break;
        }

        let transaction = deserialize_transaction(&buf[offset..frame_end])?;
        max_txn_id = max_txn_id.max(transaction.txn_id);
        for record in transaction.records {
            next_lsn = next_lsn.checked_add(1).ok_or(LogManagerError::LsnExhausted)?;
            records.push(RecoveryLogRecord {
                lsn: next_lsn,
                txn_id: record.txn_id,
                kind: RecoveryLogRecordKind::from_log_record_kind(record.kind)?,
            });
        }
        offset = frame_end;
    }

    if truncated_tail {
        wal_file.set_len(offset as u64)?;
        wal_file.sync_all()?;
    }

    Ok(RecoveryLogScan { records, truncated_tail, max_txn_id })
}

/// Removes all WAL contents after recovery has made the database file consistent.
pub(crate) fn truncate_wal(db_file_path: impl AsRef<Path>) -> Result<(), LogManagerError> {
    let wal_file_path = db_file_path.as_ref().with_added_extension("wal");
    let wal_file =
        OpenOptions::new().create(true).write(true).truncate(true).open(wal_file_path)?;
    wal_file.sync_all()?;
    Ok(())
}

impl RecoveryLogRecordKind {
    fn from_log_record_kind(kind: LogRecordKind<'_>) -> Result<Self, LogManagerError> {
        match kind {
            LogRecordKind::Begin => Ok(Self::Begin),
            LogRecordKind::Commit => Ok(Self::Commit),
            LogRecordKind::Rollback => Ok(Self::Rollback),
            LogRecordKind::PageUpdate { page_id, redo_data, undo_data } => Ok(Self::PageUpdate {
                page_id,
                redo_data: page_image_array(redo_data)?,
                undo_data: page_image_array(undo_data)?,
            }),
            LogRecordKind::PageAlloc { page_id } => Ok(Self::PageAlloc { page_id }),
        }
    }
}

fn page_image_array(image: &[u8]) -> Result<Box<[u8; PAGE_SIZE]>, LogManagerError> {
    let image = image.try_into().map_err(|_| LogManagerError::InvalidPageImageLength {
        expected: PAGE_SIZE,
        actual: image.len(),
    })?;
    Ok(Box::new(image))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use tempfile::NamedTempFile;

    use super::*;

    fn serialize_to_vec(txn_id: TxnId, records: &[LogRecord<'_>]) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_transaction(&mut buf, txn_id, records).unwrap();
        buf
    }

    #[test]
    fn serializes_and_deserializes_empty_transaction() {
        let records = [];
        let buf = serialize_to_vec(42, &records);
        let transaction = deserialize_transaction(&buf).unwrap();

        assert_eq!(transaction.txn_id, 42);
        assert!(transaction.records.is_empty());
    }

    #[test]
    fn serializes_and_deserializes_transaction_with_all_record_kinds() {
        let redo = [1; PAGE_SIZE];
        let undo = [9; PAGE_SIZE];
        let records = [
            LogRecord { txn_id: 7, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 7, kind: LogRecordKind::PageAlloc { page_id: 99 } },
            LogRecord {
                txn_id: 7,
                kind: LogRecordKind::PageUpdate {
                    page_id: 100,
                    redo_data: &redo,
                    undo_data: &undo,
                },
            },
            LogRecord { txn_id: 7, kind: LogRecordKind::Rollback },
            LogRecord { txn_id: 7, kind: LogRecordKind::Commit },
        ];

        let buf = serialize_to_vec(7, &records);
        let transaction = deserialize_transaction(&buf).unwrap();

        assert_eq!(transaction.txn_id, 7);
        assert_eq!(transaction.records.len(), records.len());
        assert!(matches!(transaction.records[0].kind, LogRecordKind::Begin));
        assert!(matches!(transaction.records[1].kind, LogRecordKind::PageAlloc { page_id: 99 }));
        match &transaction.records[2].kind {
            LogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                assert_eq!(*page_id, 100);
                assert_eq!(*redo_data, redo);
                assert_eq!(*undo_data, undo);
            }
            kind => panic!("unexpected record kind: {kind:?}"),
        }
        assert!(matches!(transaction.records[3].kind, LogRecordKind::Rollback));
        assert!(matches!(transaction.records[4].kind, LogRecordKind::Commit));
    }

    #[test]
    fn rejects_record_with_mismatched_transaction_id() {
        let records = [LogRecord { txn_id: 8, kind: LogRecordKind::Begin }];
        let err = serialize_transaction(Vec::new(), 7, &records).unwrap_err();

        assert!(matches!(err, LogManagerError::RecordTxnIdMismatch { expected: 7, actual: 8 }));
    }

    #[test]
    fn rejects_page_update_without_full_page_images() {
        let redo = [1, 2, 3, 4];
        let undo = [9; PAGE_SIZE];
        let records = [LogRecord {
            txn_id: 7,
            kind: LogRecordKind::PageUpdate { page_id: 100, redo_data: &redo, undo_data: &undo },
        }];

        let err = serialize_transaction(Vec::new(), 7, &records).unwrap_err();

        assert!(matches!(
            err,
            LogManagerError::InvalidPageImageLength { expected: PAGE_SIZE, actual: 4 }
        ));
    }

    #[test]
    fn rejects_invalid_header_magic() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        buf[0] = b'X';

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::InvalidHeaderMagic { .. })
        ));
    }

    #[test]
    fn rejects_invalid_footer_magic() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        let footer_start = buf.len() - FOOTER_LEN;
        buf[footer_start] = b'X';

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::InvalidFooterMagic { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_version() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        buf[8..10].copy_from_slice(&3u16.to_le_bytes());

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::UnsupportedVersion { expected: 2, actual: 3 })
        ));
    }

    #[test]
    fn rejects_unknown_record_kind() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        buf[HEADER_LEN] = 99;
        rewrite_crc(&mut buf);

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::UnknownRecordKind { kind: 99 })
        ));
    }

    #[test]
    fn rejects_truncated_header() {
        let buf = [0; HEADER_LEN - 1];

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::PageAlloc { page_id: 3 } }];
        let mut buf = serialize_to_vec(1, &records);
        buf.truncate(buf.len() - FOOTER_LEN - 1);

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn rejects_mismatched_footer_txn_id() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        let footer_txn_id_start = buf.len() - FOOTER_LEN + 8;
        buf[footer_txn_id_start..footer_txn_id_start + 8].copy_from_slice(&2u64.to_le_bytes());

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::FooterTxnIdMismatch { expected: 1, actual: 2 })
        ));
    }

    #[test]
    fn rejects_bad_crc() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        let crc_start = buf.len() - 4;
        buf[crc_start] ^= 1;

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_wrong_record_count() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        let entry_count_start = 8 + 2 + 8;
        buf[entry_count_start..entry_count_start + 4].copy_from_slice(&2u32.to_le_bytes());

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::RecordCountMismatch { expected: 2, actual: 1 })
        ));
    }

    #[test]
    fn rejects_trailing_bytes_after_frame() {
        let records = [LogRecord { txn_id: 1, kind: LogRecordKind::Begin }];
        let mut buf = serialize_to_vec(1, &records);
        buf.push(0);

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::TruncatedFrame { needed: 0, remaining: 1 })
        ));
    }

    #[test]
    fn append_transaction_writes_frame_without_marking_it_durable() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();
        let records = [
            LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
        ];

        let lsn = manager.append_transaction(11, &records).unwrap();

        assert_eq!(lsn, 2);
        assert_eq!(manager.highest_appended_lsn(), Some(2));
        assert_eq!(manager.highest_durable_lsn(), None);

        manager.flush_buffer_for_test().unwrap();
        assert_eq!(manager.highest_durable_lsn(), None);

        let mut wal_file = File::open(file.path().with_added_extension("wal")).unwrap();
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf).unwrap();
        let transaction = deserialize_transaction(&buf).unwrap();

        assert_eq!(transaction.txn_id, 11);
        assert_eq!(transaction.records.len(), 2);
        assert!(matches!(transaction.records[0].kind, LogRecordKind::Begin));
        assert!(matches!(transaction.records[1].kind, LogRecordKind::Commit));
    }

    #[test]
    fn append_transaction_assigns_one_lsn_per_record() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();
        let first_records = [
            LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
        ];
        let second_records = [
            LogRecord { txn_id: 12, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 12, kind: LogRecordKind::PageAlloc { page_id: 7 } },
            LogRecord { txn_id: 12, kind: LogRecordKind::Commit },
        ];

        assert_eq!(manager.append_transaction(11, &first_records).unwrap(), 2);
        assert_eq!(manager.append_transaction(12, &second_records).unwrap(), 5);
        assert_eq!(manager.highest_appended_lsn(), Some(5));
        assert_eq!(manager.highest_durable_lsn(), None);
    }

    #[test]
    fn read_recovery_log_assigns_lsns_across_complete_frames() {
        let file = NamedTempFile::new().unwrap();
        let redo = [1; PAGE_SIZE];
        let undo = [9; PAGE_SIZE];
        {
            let mut manager = LogManager::new(file.path()).unwrap();
            manager
                .append_transaction(
                    11,
                    &[
                        LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
                        LogRecord {
                            txn_id: 11,
                            kind: LogRecordKind::PageUpdate {
                                page_id: 100,
                                redo_data: &redo,
                                undo_data: &undo,
                            },
                        },
                        LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
                    ],
                )
                .unwrap();
            manager
                .append_transaction(
                    12,
                    &[
                        LogRecord { txn_id: 12, kind: LogRecordKind::Begin },
                        LogRecord { txn_id: 12, kind: LogRecordKind::Rollback },
                    ],
                )
                .unwrap();
        }

        let scan = read_recovery_log(file.path()).unwrap();

        assert!(!scan.truncated_tail);
        assert_eq!(scan.max_txn_id, 12);
        assert_eq!(
            scan.records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert!(matches!(scan.records[0].kind, RecoveryLogRecordKind::Begin));
        match &scan.records[1].kind {
            RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                assert_eq!(*page_id, 100);
                assert_eq!(redo_data.as_ref(), &redo);
                assert_eq!(undo_data.as_ref(), &undo);
            }
            kind => panic!("unexpected record kind: {kind:?}"),
        }
        assert!(matches!(scan.records[2].kind, RecoveryLogRecordKind::Commit));
        assert!(matches!(scan.records[3].kind, RecoveryLogRecordKind::Begin));
        assert!(matches!(scan.records[4].kind, RecoveryLogRecordKind::Rollback));
    }

    #[test]
    fn read_recovery_log_truncates_incomplete_final_frame_tail() {
        let file = NamedTempFile::new().unwrap();
        let valid_frame = serialize_to_vec(
            11,
            &[
                LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
                LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
            ],
        );
        let wal_file_path = file.path().with_added_extension("wal");
        {
            let mut wal_file = File::create(&wal_file_path).unwrap();
            wal_file.write_all(&valid_frame).unwrap();
            wal_file.write_all(b"DBWAL").unwrap();
        }

        let scan = read_recovery_log(file.path()).unwrap();

        assert!(scan.truncated_tail);
        assert_eq!(scan.max_txn_id, 11);
        assert_eq!(scan.records.len(), 2);
        assert_eq!(std::fs::metadata(wal_file_path).unwrap().len(), valid_frame.len() as u64);
    }

    #[test]
    fn new_scans_existing_wal_frames_without_marking_them_durable() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut manager = LogManager::new(file.path()).unwrap();
            let first_records = [
                LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
                LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
            ];
            let second_records = [
                LogRecord { txn_id: 12, kind: LogRecordKind::Begin },
                LogRecord { txn_id: 12, kind: LogRecordKind::PageAlloc { page_id: 7 } },
                LogRecord { txn_id: 12, kind: LogRecordKind::Commit },
            ];
            manager.append_transaction(11, &first_records).unwrap();
            manager.append_transaction(12, &second_records).unwrap();
        }

        let manager = LogManager::new(file.path()).unwrap();

        assert_eq!(manager.highest_txn_id(), 12);
        assert_eq!(manager.highest_appended_lsn(), Some(5));
        assert_eq!(manager.highest_durable_lsn(), None);
    }

    #[test]
    fn flush_through_zero_lsn_is_noop() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();

        manager.flush_through(ZERO_LSN).unwrap();

        assert_eq!(manager.highest_appended_lsn(), None);
        assert_eq!(manager.highest_durable_lsn(), None);
    }

    #[test]
    fn flush_through_already_durable_lsn_is_noop() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();
        let records = [LogRecord { txn_id: 11, kind: LogRecordKind::Begin }];
        let lsn = manager.append_transaction(11, &records).unwrap();
        manager.flush_through(lsn).unwrap();

        manager.flush_through(lsn).unwrap();

        assert_eq!(manager.highest_appended_lsn(), Some(lsn));
        assert_eq!(manager.highest_durable_lsn(), Some(lsn));
    }

    #[test]
    fn flush_through_non_appended_lsn_returns_error() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();

        let result = manager.flush_through(9);

        assert!(matches!(
            result,
            Err(LogManagerFlushError::LsnNotAppended {
                requested_lsn: 9,
                highest_appended_lsn: None
            })
        ));
    }

    #[test]
    fn flush_through_syncs_wal_and_marks_appended_lsn_durable() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();
        let records = [LogRecord { txn_id: 11, kind: LogRecordKind::Begin }];
        let lsn = manager.append_transaction(11, &records).unwrap();
        manager.highest_durable_lsn = None;

        manager.flush_through(lsn).unwrap();

        assert_eq!(manager.highest_appended_lsn(), Some(lsn));
        assert_eq!(manager.highest_durable_lsn(), Some(lsn));
    }

    fn rewrite_crc(buf: &mut [u8]) {
        let payload_len_start = 8 + 2 + 8 + 4;
        let payload_len = u64::from_le_bytes(
            buf[payload_len_start..payload_len_start + 8]
                .try_into()
                .expect("payload length slice has fixed width"),
        ) as usize;
        let payload_start = HEADER_LEN;
        let payload_end = payload_start + payload_len;
        let crc = CRC32.checksum(&buf[payload_start..payload_end]);
        let crc_start = buf.len() - 4;
        buf[crc_start..].copy_from_slice(&crc.to_le_bytes());
    }
}
