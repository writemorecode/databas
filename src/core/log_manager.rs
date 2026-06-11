use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};
use thiserror::Error;

use crate::core::{PAGE_SIZE, PageId};

pub(crate) type TxnId = u64;
pub(crate) type Lsn = u64;
pub(crate) const ZERO_LSN: Lsn = 0;

const HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const FOOTER_MAGIC: [u8; 8] = *b"DBWALFTR";
const WAL_FORMAT_VERSION: u16 = 2;
const HEADER_LEN: usize = 8 + 2 + 8 + 4 + 8;
const FOOTER_LEN: usize = 8 + 8 + 4;
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
const WAL_READ_BUFFER_LEN: usize = 64 * 1024;
const WAL_SCAN_BUFFER_LEN: usize = 8192;

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
    #[error("WAL full-page image has invalid length: expected {expected}, got {actual}")]
    InvalidPageImageLength { expected: usize, actual: usize },
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
    PageUpdate { page_id: PageId, redo_data: &'a [u8], undo_data: &'a [u8] },
    PageAlloc { page_id: PageId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryLogRecord {
    pub(crate) lsn: Lsn,
    pub(crate) txn_id: TxnId,
    pub(crate) kind: RecoveryLogRecordKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryLogRecordKind {
    Begin,
    Commit,
    Rollback,
    PageUpdate { page_id: PageId, redo_data: Box<[u8; PAGE_SIZE]>, undo_data: Box<[u8; PAGE_SIZE]> },
    PageAlloc { page_id: PageId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryLogScan {
    pub(crate) records: Vec<RecoveryLogRecord>,
    pub(crate) truncated_tail: bool,
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
    pub(crate) txn_id: TxnId,
    pub(crate) records: Vec<LogRecord<'a>>,
}

#[derive(Debug)]
pub(crate) struct LogManager {
    wal_file: File,

    highest_txn_id: TxnId,
    highest_appended_lsn: Option<Lsn>,
    highest_durable_lsn: Option<Lsn>,
}

impl LogManager {
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

        Ok(Self { wal_file, highest_txn_id, highest_durable_lsn: None, highest_appended_lsn })
    }

    pub(crate) fn highest_txn_id(&self) -> TxnId {
        self.highest_txn_id
    }

    pub(crate) fn next_lsn(&self) -> Result<Lsn, LogManagerError<'static>> {
        self.highest_appended_lsn
            .unwrap_or(ZERO_LSN)
            .checked_add(1)
            .ok_or(LogManagerError::LsnExhausted)
    }

    pub(crate) fn append_record<'a>(
        &mut self,
        txn_id: TxnId,
        kind: LogRecordKind<'a>,
    ) -> Result<Lsn, LogManagerError<'a>> {
        self.append_transaction(txn_id, &[LogRecord { txn_id, kind }])
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
        self.highest_txn_id = self.highest_txn_id.max(txn_id);
        if record_count > 0 {
            self.highest_appended_lsn = Some(lsn);
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

    #[cfg(test)]
    pub(crate) fn force_next_lsn_exhausted_for_test(&mut self) {
        self.highest_appended_lsn = Some(Lsn::MAX);
    }
}

pub(crate) fn read_recovery_log(
    db_file_path: impl AsRef<Path>,
) -> Result<RecoveryLogScan, LogManagerError<'static>> {
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
            Err(err) => return Err(err.into_static()),
        };

        let frame_end =
            offset.checked_add(frame_len).ok_or(LogManagerError::PayloadLengthOverflow)?;
        if frame_end > buf.len() {
            truncated_tail = true;
            break;
        }

        let transaction = deserialize_transaction(&buf[offset..frame_end])
            .map_err(LogManagerError::into_static)?;
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

pub(crate) fn truncate_wal(db_file_path: impl AsRef<Path>) -> Result<(), LogManagerError<'static>> {
    let wal_file_path = db_file_path.as_ref().with_added_extension("wal");
    let wal_file =
        OpenOptions::new().create(true).write(true).truncate(true).open(wal_file_path)?;
    wal_file.sync_all()?;
    Ok(())
}

impl RecoveryLogRecordKind {
    fn from_log_record_kind(kind: LogRecordKind<'_>) -> Result<Self, LogManagerError<'static>> {
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

fn page_image_array(image: &[u8]) -> Result<Box<[u8; PAGE_SIZE]>, LogManagerError<'static>> {
    let image = image.try_into().map_err(|_| LogManagerError::InvalidPageImageLength {
        expected: PAGE_SIZE,
        actual: image.len(),
    })?;
    Ok(Box::new(image))
}

impl LogManagerError<'_> {
    fn into_static(self) -> LogManagerError<'static> {
        match self {
            LogManagerError::Io(err) => LogManagerError::Io(err),
            LogManagerError::InvalidDbFilePath { .. } => LogManagerError::InvalidDbFilePath {
                db_file_path: Path::new("<invalid database file path>"),
            },
            LogManagerError::InvalidHeaderMagic { actual } => {
                LogManagerError::InvalidHeaderMagic { actual }
            }
            LogManagerError::InvalidFooterMagic { actual } => {
                LogManagerError::InvalidFooterMagic { actual }
            }
            LogManagerError::UnsupportedVersion { expected, actual } => {
                LogManagerError::UnsupportedVersion { expected, actual }
            }
            LogManagerError::TruncatedFrame { needed, remaining } => {
                LogManagerError::TruncatedFrame { needed, remaining }
            }
            LogManagerError::PayloadLengthTooLarge { payload_len } => {
                LogManagerError::PayloadLengthTooLarge { payload_len }
            }
            LogManagerError::PayloadLengthOverflow => LogManagerError::PayloadLengthOverflow,
            LogManagerError::ChecksumMismatch { expected, actual } => {
                LogManagerError::ChecksumMismatch { expected, actual }
            }
            LogManagerError::UnknownRecordKind { kind } => {
                LogManagerError::UnknownRecordKind { kind }
            }
            LogManagerError::FooterTxnIdMismatch { expected, actual } => {
                LogManagerError::FooterTxnIdMismatch { expected, actual }
            }
            LogManagerError::RecordTxnIdMismatch { expected, actual } => {
                LogManagerError::RecordTxnIdMismatch { expected, actual }
            }
            LogManagerError::RecordCountMismatch { expected, actual } => {
                LogManagerError::RecordCountMismatch { expected, actual }
            }
            LogManagerError::TooManyRecords { count } => LogManagerError::TooManyRecords { count },
            LogManagerError::LsnExhausted => LogManagerError::LsnExhausted,
            LogManagerError::InvalidPageImageLength { expected, actual } => {
                LogManagerError::InvalidPageImageLength { expected, actual }
            }
        }
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
    let frame_len = transaction_frame_len(buf)?;
    if buf.len() > frame_len {
        return Err(LogManagerError::TruncatedFrame {
            needed: 0,
            remaining: buf.len() - frame_len,
        });
    }

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

struct ScannedWalFrame {
    txn_id: TxnId,
    record_count: u32,
}

struct WalFrameHeader {
    txn_id: TxnId,
    entry_count: u32,
    payload_len: u64,
}

fn scan_transaction_frame<R: Read>(
    reader: &mut R,
) -> Result<Option<ScannedWalFrame>, LogManagerError<'static>> {
    let Some(header_bytes) = read_exact_or_eof::<_, HEADER_LEN>(reader)? else {
        return Ok(None);
    };
    let header = parse_header(&header_bytes)?;
    let payload_len = usize::try_from(header.payload_len)
        .map_err(|_| LogManagerError::PayloadLengthTooLarge { payload_len: header.payload_len })?;
    let mut remaining = payload_len;
    let mut digest = CRC32.digest();
    let mut actual_record_count = 0u32;

    while remaining > 0 {
        scan_log_record_payload(reader, &mut digest, &mut remaining)?;
        actual_record_count = actual_record_count.checked_add(1).ok_or({
            LogManagerError::RecordCountMismatch { expected: header.entry_count, actual: u32::MAX }
        })?;
    }

    if actual_record_count != header.entry_count {
        return Err(LogManagerError::RecordCountMismatch {
            expected: header.entry_count,
            actual: actual_record_count,
        });
    }

    let footer_bytes = read_exact_or_eof::<_, FOOTER_LEN>(reader)?
        .ok_or(LogManagerError::TruncatedFrame { needed: FOOTER_LEN, remaining: 0 })?;
    validate_footer(&footer_bytes, header.txn_id, digest.finalize())?;
    Ok(Some(ScannedWalFrame { txn_id: header.txn_id, record_count: actual_record_count }))
}

fn scan_log_record_payload<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<(), LogManagerError<'static>> {
    match read_crc_u8(reader, digest, remaining)? {
        KIND_BEGIN | KIND_COMMIT | KIND_ROLLBACK => Ok(()),
        KIND_PAGE_ALLOC => {
            read_crc_u64(reader, digest, remaining)?;
            Ok(())
        }
        KIND_PAGE_UPDATE => {
            read_crc_u64(reader, digest, remaining)?;
            let redo_len = read_crc_u32(reader, digest, remaining)? as usize;
            let undo_len = read_crc_u32(reader, digest, remaining)? as usize;
            validate_page_image_len_with_lifetime(redo_len)?;
            validate_page_image_len_with_lifetime(undo_len)?;
            read_crc_discard(reader, digest, remaining, redo_len)?;
            read_crc_discard(reader, digest, remaining, undo_len)
        }
        kind => Err(LogManagerError::UnknownRecordKind { kind }),
    }
}

fn read_exact_or_eof<R: Read, const N: usize>(reader: &mut R) -> std::io::Result<Option<[u8; N]>> {
    let mut buf = [0; N];
    let mut read_len = 0;
    while read_len < N {
        match reader.read(&mut buf[read_len..])? {
            0 if read_len == 0 => return Ok(None),
            0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            len => read_len += len,
        }
    }
    Ok(Some(buf))
}

fn read_crc_u8<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<u8, LogManagerError<'static>> {
    let bytes = read_crc_array::<_, 1>(reader, digest, remaining)?;
    Ok(bytes[0])
}

fn read_crc_u32<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<u32, LogManagerError<'static>> {
    let bytes = read_crc_array::<_, 4>(reader, digest, remaining)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_crc_u64<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<u64, LogManagerError<'static>> {
    let bytes = read_crc_array::<_, 8>(reader, digest, remaining)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_crc_array<R: Read, const N: usize>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<[u8; N], LogManagerError<'static>> {
    if *remaining < N {
        return Err(LogManagerError::TruncatedFrame { needed: N, remaining: *remaining });
    }
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    digest.update(&bytes);
    *remaining -= N;
    Ok(bytes)
}

fn read_crc_discard<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
    len: usize,
) -> Result<(), LogManagerError<'static>> {
    if *remaining < len {
        return Err(LogManagerError::TruncatedFrame { needed: len, remaining: *remaining });
    }

    let mut buf = [0; WAL_SCAN_BUFFER_LEN];
    let mut left = len;
    while left > 0 {
        let chunk_len = left.min(buf.len());
        reader.read_exact(&mut buf[..chunk_len])?;
        digest.update(&buf[..chunk_len]);
        left -= chunk_len;
        *remaining -= chunk_len;
    }
    Ok(())
}

fn parse_header(
    header_bytes: &[u8; HEADER_LEN],
) -> Result<WalFrameHeader, LogManagerError<'static>> {
    let header_magic: [u8; 8] = header_bytes[0..8].try_into().expect("header magic len is fixed");
    if header_magic != HEADER_MAGIC {
        return Err(LogManagerError::InvalidHeaderMagic { actual: header_magic });
    }

    let version = u16::from_le_bytes(header_bytes[8..10].try_into().expect("version len fixed"));
    if version != WAL_FORMAT_VERSION {
        return Err(LogManagerError::UnsupportedVersion {
            expected: WAL_FORMAT_VERSION,
            actual: version,
        });
    }

    Ok(WalFrameHeader {
        txn_id: u64::from_le_bytes(header_bytes[10..18].try_into().expect("txn id len fixed")),
        entry_count: u32::from_le_bytes(
            header_bytes[18..22].try_into().expect("entry count len fixed"),
        ),
        payload_len: u64::from_le_bytes(
            header_bytes[22..30].try_into().expect("payload len fixed"),
        ),
    })
}

fn validate_footer(
    footer_bytes: &[u8; FOOTER_LEN],
    txn_id: TxnId,
    actual_crc: u32,
) -> Result<(), LogManagerError<'static>> {
    let footer_magic: [u8; 8] = footer_bytes[0..8].try_into().expect("footer magic len is fixed");
    if footer_magic != FOOTER_MAGIC {
        return Err(LogManagerError::InvalidFooterMagic { actual: footer_magic });
    }

    let footer_txn_id =
        u64::from_le_bytes(footer_bytes[8..16].try_into().expect("footer txn id len fixed"));
    if footer_txn_id != txn_id {
        return Err(LogManagerError::FooterTxnIdMismatch {
            expected: txn_id,
            actual: footer_txn_id,
        });
    }

    let expected_crc =
        u32::from_le_bytes(footer_bytes[16..20].try_into().expect("footer crc len fixed"));
    if actual_crc != expected_crc {
        return Err(LogManagerError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    Ok(())
}

fn wal_open_error(err: LogManagerError<'_>) -> std::io::Error {
    match err {
        LogManagerError::Io(err) => err,
        err => std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()),
    }
}

fn transaction_frame_len(buf: &'_ [u8]) -> Result<usize, LogManagerError<'_>> {
    if buf.len() < HEADER_LEN {
        return Err(LogManagerError::TruncatedFrame { needed: HEADER_LEN, remaining: buf.len() });
    }

    let header = parse_header(buf[..HEADER_LEN].try_into().expect("header len is fixed"))?;
    let payload_len = usize::try_from(header.payload_len)
        .map_err(|_| LogManagerError::PayloadLengthTooLarge { payload_len: header.payload_len })?;

    HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(FOOTER_LEN))
        .ok_or(LogManagerError::PayloadLengthOverflow)
}

#[cfg(test)]
pub(crate) fn read_log_record_kinds_for_test(
    db_file_path: impl AsRef<Path>,
) -> Vec<(TxnId, OwnedLogRecordKind)> {
    let wal_file_path = db_file_path.as_ref().with_added_extension("wal");
    let mut wal_file = File::open(wal_file_path).unwrap();
    let mut buf = Vec::new();
    wal_file.read_to_end(&mut buf).unwrap();

    let mut records = Vec::new();
    let mut offset = 0;
    while offset < buf.len() {
        let frame_len = transaction_frame_len(&buf[offset..]).unwrap();
        let transaction = deserialize_transaction(&buf[offset..offset + frame_len]).unwrap();
        for record in transaction.records {
            let kind = match record.kind {
                LogRecordKind::Begin => OwnedLogRecordKind::Begin,
                LogRecordKind::Commit => OwnedLogRecordKind::Commit,
                LogRecordKind::Rollback => OwnedLogRecordKind::Rollback,
                LogRecordKind::PageUpdate { page_id, .. } => {
                    OwnedLogRecordKind::PageUpdate { page_id }
                }
                LogRecordKind::PageAlloc { page_id } => OwnedLogRecordKind::PageAlloc { page_id },
            };
            records.push((record.txn_id, kind));
        }
        offset += frame_len;
    }

    records
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
            let redo_len = cursor.read_u32()? as usize;
            let undo_len = cursor.read_u32()? as usize;
            let redo_data = cursor.read_slice(redo_len)?;
            let undo_data = cursor.read_slice(undo_len)?;
            validate_page_image_len(redo_data)?;
            validate_page_image_len(undo_data)?;
            LogRecordKind::PageUpdate { page_id, redo_data, undo_data }
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
            validate_page_image_len(redo_data)?;
            validate_page_image_len(undo_data)?;
            let redo_len = u32::try_from(redo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;
            Ok(1 + 8 + 4 + 4 + u64::from(redo_len) + u64::from(undo_len))
        }
        LogRecordKind::PageAlloc { .. } => Ok(1 + 8),
    }
}

fn validate_page_image_len<'a>(image: &[u8]) -> Result<(), LogManagerError<'a>> {
    validate_page_image_len_with_lifetime(image.len())
}

fn validate_page_image_len_with_lifetime<'a>(len: usize) -> Result<(), LogManagerError<'a>> {
    if len == PAGE_SIZE {
        Ok(())
    } else {
        Err(LogManagerError::InvalidPageImageLength { expected: PAGE_SIZE, actual: len })
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
        LogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
            validate_page_image_len(redo_data)?;
            validate_page_image_len(undo_data)?;
            let redo_len = u32::try_from(redo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;

            write_crc_u8(writer, digest, KIND_PAGE_UPDATE)?;
            write_crc_u64(writer, digest, *page_id)?;
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
