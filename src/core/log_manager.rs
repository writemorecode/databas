use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};
use thiserror::Error;

use crate::core::{PageId, SlotId};

pub(crate) type TxnId = u64;
pub(crate) type Lsn = u64;
pub(crate) const ZERO_LSN: Lsn = 0;

const HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const FOOTER_MAGIC: [u8; 8] = *b"DBWALFTR";
const WAL_FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 8 + 2 + 8 + 4 + 8;
const FOOTER_LEN: usize = 8 + 8 + 4;
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

const KIND_BEGIN: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ROLLBACK: u8 = 3;
const KIND_PAGE_UPDATE: u8 = 4;
const KIND_PAGE_ALLOC: u8 = 5;

#[derive(Debug, Error)]
pub enum LogManagerError<'a> {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid database file path: {db_file_path}")]
    InvalidDbFilePath { db_file_path: &'a Path },
    #[error("invalid WAL header magic: {actual:?}")]
    InvalidHeaderMagic { actual: [u8; 8] },
    #[error("invalid WAL footer magic: {actual:?}")]
    InvalidFooterMagic { actual: [u8; 8] },
    #[error("unsupported WAL format version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u16, actual: u16 },
    #[error("truncated WAL frame: needed {needed} bytes, remaining {remaining}")]
    TruncatedFrame { needed: usize, remaining: usize },
    #[error("WAL payload length does not fit in memory: {payload_len}")]
    PayloadLengthTooLarge { payload_len: u64 },
    #[error("WAL payload length overflow")]
    PayloadLengthOverflow,
    #[error("WAL checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    #[error("unknown WAL log record kind: {kind}")]
    UnknownRecordKind { kind: u8 },
    #[error("WAL footer txn id mismatch: expected {expected}, got {actual}")]
    FooterTxnIdMismatch { expected: TxnId, actual: TxnId },
    #[error("WAL record txn id mismatch: expected {expected}, got {actual}")]
    RecordTxnIdMismatch { expected: TxnId, actual: TxnId },
    #[error("WAL record count mismatch: expected {expected}, got {actual}")]
    RecordCountMismatch { expected: u32, actual: u32 },
    #[error("too many WAL records in transaction: {count}")]
    TooManyRecords { count: usize },
    #[error("WAL LSN exhausted")]
    LsnExhausted,
}

#[derive(Debug, Error)]
pub(crate) enum LogManagerFlushError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "requested WAL flush through LSN {requested_lsn}, but highest appended LSN is {highest_appended_lsn:?}"
    )]
    LsnNotAppended { requested_lsn: Lsn, highest_appended_lsn: Option<Lsn> },
}

#[derive(Debug)]
pub(crate) struct LogRecord<'a> {
    pub(crate) txn_id: TxnId,
    pub(crate) kind: LogRecordKind<'a>,
}

#[derive(Debug)]
pub(crate) enum LogRecordKind<'a> {
    Begin,
    Commit,
    Rollback,
    PageUpdate { page_id: PageId, slot_id: SlotId, redo_data: &'a [u8], undo_data: &'a [u8] },
    PageAlloc { page_id: PageId },
}

#[derive(Debug)]
pub(crate) struct LogTransaction<'a> {
    pub(crate) txn_id: TxnId,
    pub(crate) records: Vec<LogRecord<'a>>,
}

#[derive(Debug)]
pub(crate) struct LogManager {
    wal_file: File,

    highest_appended_lsn: Option<Lsn>,
    highest_durable_lsn: Option<Lsn>,
}

impl LogManager {
    pub(crate) fn new(db_file_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let db_file_path = db_file_path.as_ref().to_path_buf();
        let wal_file_path = db_file_path.with_added_extension("wal");
        let wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .truncate(false)
            .open(wal_file_path)?;

        // TODO : validation of wal file data

        Ok(Self { wal_file, highest_durable_lsn: None, highest_appended_lsn: None })
    }

    pub(crate) fn append_transaction<'a>(
        &mut self,
        txn_id: TxnId,
        records: &[LogRecord<'a>],
    ) -> Result<Lsn, LogManagerError<'a>> {
        validate_record_txn_ids(txn_id, records)?;

        let record_count = u64::try_from(records.len()).expect("usize record count fits in u64");
        let lsn = self
            .highest_appended_lsn
            .unwrap_or(ZERO_LSN)
            .checked_add(record_count)
            .ok_or(LogManagerError::LsnExhausted)?;

        serialize_transaction(&mut self.wal_file, txn_id, records)?;
        if record_count > 0 {
            self.highest_appended_lsn = Some(lsn);
        }
        self.wal_file.sync_all()?;
        if record_count > 0 {
            self.highest_durable_lsn = Some(lsn);
        }
        Ok(lsn)
    }

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

        self.wal_file.sync_all()?;
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
}

pub(crate) fn serialize_transaction<'a, W: Write>(
    mut writer: W,
    txn_id: TxnId,
    records: &[LogRecord<'a>],
) -> Result<(), LogManagerError<'a>> {
    validate_record_txn_ids(txn_id, records)?;
    let entry_count = u32::try_from(records.len())
        .map_err(|_| LogManagerError::TooManyRecords { count: records.len() })?;
    let payload_len = serialized_records_len(records)?;

    write_header(&mut writer, txn_id, entry_count, payload_len)?;

    let mut digest = CRC32.digest();
    for record in records {
        write_log_record_payload(&mut writer, &mut digest, &record.kind)?;
    }

    write_footer(&mut writer, txn_id, digest.finalize())?;
    Ok(())
}

pub(crate) fn deserialize_transaction(
    buf: &'_ [u8],
) -> Result<LogTransaction<'_>, LogManagerError<'_>> {
    if buf.len() < HEADER_LEN {
        return Err(LogManagerError::TruncatedFrame { needed: HEADER_LEN, remaining: buf.len() });
    }

    let mut cursor = Cursor::new(buf);
    let header_magic = cursor.read_array::<8>()?;
    if header_magic != HEADER_MAGIC {
        return Err(LogManagerError::InvalidHeaderMagic { actual: header_magic });
    }

    let version = cursor.read_u16()?;
    if version != WAL_FORMAT_VERSION {
        return Err(LogManagerError::UnsupportedVersion {
            expected: WAL_FORMAT_VERSION,
            actual: version,
        });
    }

    let txn_id = cursor.read_u64()?;
    let entry_count = cursor.read_u32()?;
    let payload_len = cursor.read_u64()?;
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| LogManagerError::PayloadLengthTooLarge { payload_len })?;

    let payload_start = cursor.position;
    let payload_end =
        payload_start.checked_add(payload_len).ok_or(LogManagerError::PayloadLengthOverflow)?;
    let frame_len =
        payload_end.checked_add(FOOTER_LEN).ok_or(LogManagerError::PayloadLengthOverflow)?;
    if buf.len() < frame_len {
        return Err(LogManagerError::TruncatedFrame {
            needed: frame_len - cursor.position,
            remaining: cursor.remaining(),
        });
    }

    let payload = &buf[payload_start..payload_end];
    let mut payload_cursor = Cursor::new(payload);
    let mut records = Vec::new();
    while payload_cursor.remaining() > 0 {
        records.push(deserialize_log_record(&mut payload_cursor, txn_id)?);
    }

    if records.len() != entry_count as usize {
        return Err(LogManagerError::RecordCountMismatch {
            expected: entry_count,
            actual: records.len() as u32,
        });
    }

    cursor.position = payload_end;
    let footer_magic = cursor.read_array::<8>()?;
    if footer_magic != FOOTER_MAGIC {
        return Err(LogManagerError::InvalidFooterMagic { actual: footer_magic });
    }

    let footer_txn_id = cursor.read_u64()?;
    if footer_txn_id != txn_id {
        return Err(LogManagerError::FooterTxnIdMismatch {
            expected: txn_id,
            actual: footer_txn_id,
        });
    }

    let expected_crc = cursor.read_u32()?;
    let actual_crc = CRC32.checksum(payload);
    if actual_crc != expected_crc {
        return Err(LogManagerError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    if cursor.remaining() > 0 {
        return Err(LogManagerError::TruncatedFrame { needed: 0, remaining: cursor.remaining() });
    }

    Ok(LogTransaction { txn_id, records })
}

fn deserialize_log_record<'a>(
    cursor: &mut Cursor<'a>,
    txn_id: TxnId,
) -> Result<LogRecord<'a>, LogManagerError<'a>> {
    let kind = match cursor.read_u8()? {
        KIND_BEGIN => LogRecordKind::Begin,
        KIND_COMMIT => LogRecordKind::Commit,
        KIND_ROLLBACK => LogRecordKind::Rollback,
        KIND_PAGE_UPDATE => {
            let page_id = cursor.read_u64()?;
            let slot_id = cursor.read_u16()?;
            let redo_len = cursor.read_u32()? as usize;
            let undo_len = cursor.read_u32()? as usize;
            let redo_data = cursor.read_slice(redo_len)?;
            let undo_data = cursor.read_slice(undo_len)?;
            LogRecordKind::PageUpdate { page_id, slot_id, redo_data, undo_data }
        }
        KIND_PAGE_ALLOC => {
            let page_id = cursor.read_u64()?;
            LogRecordKind::PageAlloc { page_id }
        }
        kind => return Err(LogManagerError::UnknownRecordKind { kind }),
    };
    Ok(LogRecord { txn_id, kind })
}

fn validate_record_txn_ids<'a>(
    txn_id: TxnId,
    records: &[LogRecord<'a>],
) -> Result<(), LogManagerError<'a>> {
    for record in records {
        if record.txn_id != txn_id {
            return Err(LogManagerError::RecordTxnIdMismatch {
                expected: txn_id,
                actual: record.txn_id,
            });
        }
    }
    Ok(())
}

fn serialized_records_len<'a>(records: &[LogRecord<'a>]) -> Result<u64, LogManagerError<'a>> {
    let mut len = 0u64;
    for record in records {
        len = len
            .checked_add(serialized_record_len(&record.kind)?)
            .ok_or(LogManagerError::PayloadLengthOverflow)?;
    }
    Ok(len)
}

fn serialized_record_len<'a>(kind: &LogRecordKind<'a>) -> Result<u64, LogManagerError<'a>> {
    match kind {
        LogRecordKind::Begin | LogRecordKind::Commit | LogRecordKind::Rollback => Ok(1),
        LogRecordKind::PageUpdate { redo_data, undo_data, .. } => {
            let redo_len = u32::try_from(redo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;
            Ok(1 + 8 + 2 + 4 + 4 + u64::from(redo_len) + u64::from(undo_len))
        }
        LogRecordKind::PageAlloc { .. } => Ok(1 + 8),
    }
}

fn write_header<W: Write>(
    writer: &mut W,
    txn_id: TxnId,
    entry_count: u32,
    payload_len: u64,
) -> std::io::Result<()> {
    writer.write_all(&HEADER_MAGIC)?;
    writer.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&txn_id.to_le_bytes())?;
    writer.write_all(&entry_count.to_le_bytes())?;
    writer.write_all(&payload_len.to_le_bytes())
}

fn write_footer<W: Write>(
    writer: &mut W,
    txn_id: TxnId,
    payload_crc32: u32,
) -> std::io::Result<()> {
    writer.write_all(&FOOTER_MAGIC)?;
    writer.write_all(&txn_id.to_le_bytes())?;
    writer.write_all(&payload_crc32.to_le_bytes())
}

fn write_log_record_payload<'a, W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    kind: &LogRecordKind<'a>,
) -> Result<(), LogManagerError<'a>> {
    match kind {
        LogRecordKind::Begin => write_crc_u8(writer, digest, KIND_BEGIN)?,
        LogRecordKind::Commit => write_crc_u8(writer, digest, KIND_COMMIT)?,
        LogRecordKind::Rollback => write_crc_u8(writer, digest, KIND_ROLLBACK)?,
        LogRecordKind::PageUpdate { page_id, slot_id, redo_data, undo_data } => {
            let redo_len = u32::try_from(redo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;

            write_crc_u8(writer, digest, KIND_PAGE_UPDATE)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u16(writer, digest, *slot_id)?;
            write_crc_u32(writer, digest, redo_len)?;
            write_crc_u32(writer, digest, undo_len)?;
            write_crc_bytes(writer, digest, redo_data)?;
            write_crc_bytes(writer, digest, undo_data)?;
        }
        LogRecordKind::PageAlloc { page_id } => {
            write_crc_u8(writer, digest, KIND_PAGE_ALLOC)?;
            write_crc_u64(writer, digest, *page_id)?;
        }
    }
    Ok(())
}

fn write_crc_u8<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    value: u8,
) -> std::io::Result<()> {
    write_crc_bytes(writer, digest, &[value])
}

fn write_crc_u16<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    value: u16,
) -> std::io::Result<()> {
    write_crc_bytes(writer, digest, &value.to_le_bytes())
}

fn write_crc_u32<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    value: u32,
) -> std::io::Result<()> {
    write_crc_bytes(writer, digest, &value.to_le_bytes())
}

fn write_crc_u64<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    value: u64,
) -> std::io::Result<()> {
    write_crc_bytes(writer, digest, &value.to_le_bytes())
}

fn write_crc_bytes<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    bytes: &[u8],
) -> std::io::Result<()> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

struct Cursor<'a> {
    buf: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.position
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], LogManagerError<'a>> {
        let slice = self.read_slice(N)?;
        Ok(slice.try_into().expect("slice length is fixed"))
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], LogManagerError<'a>> {
        let end = self.position.checked_add(len).ok_or(LogManagerError::PayloadLengthOverflow)?;
        if end > self.buf.len() {
            return Err(LogManagerError::TruncatedFrame {
                needed: len,
                remaining: self.remaining(),
            });
        }
        let slice = &self.buf[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, LogManagerError<'a>> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, LogManagerError<'a>> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, LogManagerError<'a>> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, LogManagerError<'a>> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

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
        let redo = [1, 2, 3, 4];
        let undo = [9, 8, 7];
        let records = [
            LogRecord { txn_id: 7, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 7, kind: LogRecordKind::PageAlloc { page_id: 99 } },
            LogRecord {
                txn_id: 7,
                kind: LogRecordKind::PageUpdate {
                    page_id: 100,
                    slot_id: 3,
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
            LogRecordKind::PageUpdate { page_id, slot_id, redo_data, undo_data } => {
                assert_eq!(*page_id, 100);
                assert_eq!(*slot_id, 3);
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
        buf[8..10].copy_from_slice(&2u16.to_le_bytes());

        assert!(matches!(
            deserialize_transaction(&buf),
            Err(LogManagerError::UnsupportedVersion { expected: 1, actual: 2 })
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
    fn append_transaction_writes_and_syncs_frame() {
        let file = NamedTempFile::new().unwrap();
        let mut manager = LogManager::new(file.path()).unwrap();
        let records = [
            LogRecord { txn_id: 11, kind: LogRecordKind::Begin },
            LogRecord { txn_id: 11, kind: LogRecordKind::Commit },
        ];

        let lsn = manager.append_transaction(11, &records).unwrap();

        assert_eq!(lsn, 2);
        assert_eq!(manager.highest_appended_lsn(), Some(2));
        assert_eq!(manager.highest_durable_lsn(), Some(2));

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
        assert_eq!(manager.highest_durable_lsn(), Some(5));
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
