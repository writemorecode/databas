use std::io::{Read, Write};
#[cfg(test)]
use std::{fs::File, path::Path};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};

use crate::core::PAGE_SIZE;

#[cfg(test)]
use super::OwnedLogRecordKind;
use super::{LogManagerError, LogRecord, LogRecordKind, LogTransaction, TxnId};

const HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const FOOTER_MAGIC: [u8; 8] = *b"DBWALFTR";
const WAL_FORMAT_VERSION: u16 = 2;
pub(super) const HEADER_LEN: usize = 8 + 2 + 8 + 4 + 8;
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const FOOTER_LEN: usize = 8 + 8 + 4;
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
const WAL_SCAN_BUFFER_LEN: usize = 8192;

const KIND_BEGIN: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ROLLBACK: u8 = 3;
const KIND_PAGE_UPDATE: u8 = 4;
const KIND_PAGE_ALLOC: u8 = 5;

/// Serializes a complete transaction frame to `writer`.
///
/// The frame includes the header, all payload records, and a footer containing
/// the transaction id and CRC32 of the payload bytes. It does not assign or
/// persist LSNs; callers derive LSNs from record position in append order.
pub(super) fn serialize_transaction<'a, W: Write>(
    mut writer: W,
    txn_id: TxnId,
    records: &[LogRecord<'a>],
) -> Result<(), LogManagerError> {
    validate_record_txn_ids(txn_id, records)?;
    let entry_count = u32::try_from(records.len())
        .map_err(|_| LogManagerError::TooManyRecords { count: records.len() })?;
    let payload_len = serialized_records_len(records)?;

    write_frame_header(&mut writer, txn_id, entry_count, payload_len)?;

    let mut digest = CRC32.digest();
    for record in records {
        write_log_record_payload(&mut writer, &mut digest, &record.kind)?;
    }

    write_frame_footer(&mut writer, txn_id, digest.finalize())?;
    Ok(())
}

/// Deserializes exactly one transaction frame from `buf`.
///
/// The returned records borrow their page-image slices from `buf`. The input
/// must contain one complete frame and no trailing bytes.
pub(super) fn deserialize_transaction(
    buf: &'_ [u8],
) -> Result<LogTransaction<'_>, LogManagerError> {
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

    let mut cursor = FrameReader::new(buf);
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
    let mut payload_cursor = FrameReader::new(payload);
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

pub(super) struct ScannedWalFrame {
    pub(super) txn_id: TxnId,
    pub(super) record_count: u32,
}

struct WalFrameHeader {
    txn_id: TxnId,
    entry_count: u32,
    payload_len: u64,
}

pub(super) fn scan_transaction_frame<R: Read>(
    reader: &mut R,
) -> Result<Option<ScannedWalFrame>, LogManagerError> {
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
) -> Result<(), LogManagerError> {
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
            validate_page_image_len_value(redo_len)?;
            validate_page_image_len_value(undo_len)?;
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
) -> Result<u8, LogManagerError> {
    let bytes = read_crc_array::<_, 1>(reader, digest, remaining)?;
    Ok(bytes[0])
}

fn read_crc_u32<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<u32, LogManagerError> {
    let bytes = read_crc_array::<_, 4>(reader, digest, remaining)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_crc_u64<R: Read>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<u64, LogManagerError> {
    let bytes = read_crc_array::<_, 8>(reader, digest, remaining)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_crc_array<R: Read, const N: usize>(
    reader: &mut R,
    digest: &mut Digest<'_, u32>,
    remaining: &mut usize,
) -> Result<[u8; N], LogManagerError> {
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
) -> Result<(), LogManagerError> {
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

fn parse_header(header_bytes: &[u8; HEADER_LEN]) -> Result<WalFrameHeader, LogManagerError> {
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
) -> Result<(), LogManagerError> {
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

pub(super) fn wal_open_error(err: LogManagerError) -> std::io::Error {
    match err {
        LogManagerError::Io(err) => err,
        err => std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()),
    }
}

pub(super) fn transaction_frame_len(buf: &'_ [u8]) -> Result<usize, LogManagerError> {
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
    let mut offset = super::WAL_FILE_HEADER_LEN;
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
    cursor: &mut FrameReader<'a>,
    txn_id: TxnId,
) -> Result<LogRecord<'a>, LogManagerError> {
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

pub(super) fn validate_record_txn_ids<'a>(
    txn_id: TxnId,
    records: &[LogRecord<'a>],
) -> Result<(), LogManagerError> {
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

fn serialized_records_len<'a>(records: &[LogRecord<'a>]) -> Result<u64, LogManagerError> {
    let mut len = 0u64;
    for record in records {
        len = len
            .checked_add(serialized_record_len(&record.kind)?)
            .ok_or(LogManagerError::PayloadLengthOverflow)?;
    }
    Ok(len)
}

fn serialized_record_len<'a>(kind: &LogRecordKind<'a>) -> Result<u64, LogManagerError> {
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

fn validate_page_image_len(image: &[u8]) -> Result<(), LogManagerError> {
    validate_page_image_len_value(image.len())
}

fn validate_page_image_len_value(len: usize) -> Result<(), LogManagerError> {
    if len == PAGE_SIZE {
        Ok(())
    } else {
        Err(LogManagerError::InvalidPageImageLength { expected: PAGE_SIZE, actual: len })
    }
}

fn write_frame_header<W: Write>(
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

fn write_frame_footer<W: Write>(
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
) -> Result<(), LogManagerError> {
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

struct FrameReader<'a> {
    buf: &'a [u8],
    position: usize,
}

impl<'a> FrameReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.position
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], LogManagerError> {
        let slice = self.read_slice(N)?;
        Ok(slice.try_into().expect("slice length is fixed"))
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], LogManagerError> {
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

    fn read_u8(&mut self) -> Result<u8, LogManagerError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, LogManagerError> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, LogManagerError> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, LogManagerError> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }
}
