//! WAL transaction-frame encoding and shared validation for both read paths.

use std::io::{Read, Write};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};

use crate::PAGE_SIZE;

use super::{LogManagerError, LogRecord, LogRecordKind, Lsn, RecoveryLogRecordKind, TxnId};

pub(super) type DecodedRecord = (Lsn, RecoveryLogRecordKind);

const HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const FOOTER_MAGIC: [u8; 8] = *b"DBWALFTR";
const WAL_FORMAT_VERSION: u16 = 4;
pub(super) const HEADER_LEN: usize = 8 + 2 + 8 + 4 + 8;
const FOOTER_LEN: usize = 8 + 8 + 4;
pub(super) const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

const KIND_BEGIN: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ROLLBACK: u8 = 3;
const KIND_PAGE_UPDATE: u8 = 4;
const KIND_PAGE_ALLOC: u8 = 5;
const KIND_PAGE_DEALLOC: u8 = 6;
const KIND_BITMAP_IMAGE: u8 = 7;

/// Serializes a complete transaction frame to `writer`.
///
/// The frame includes the header, all payload records, and a footer containing
/// the transaction id and CRC32 of the payload bytes. It does not assign or
/// persist LSNs; callers derive LSNs from record position in append order.
pub(super) fn serialize_transaction<W: Write>(
    mut writer: W,
    txn_id: TxnId,
    records: &[LogRecord<'_>],
) -> Result<(), LogManagerError> {
    validate_record_txn_ids(txn_id, records)?;
    let entry_count = u32::try_from(records.len())
        .map_err(|_out_of_range| LogManagerError::TooManyRecords { count: records.len() })?;
    let payload_len = serialized_records_len(records)?;

    write_frame_header(&mut writer, txn_id, entry_count, payload_len)?;

    let mut digest = CRC32.digest();
    for record in records {
        write_crc_u64(&mut writer, &mut digest, record.prev_lsn)?;
        write_log_record_payload(&mut writer, &mut digest, &record.kind)?;
    }

    write_frame_footer(&mut writer, txn_id, digest.finalize())?;
    Ok(())
}

pub(super) struct WalFrameHeader {
    pub(super) txn_id: TxnId,
    pub(super) record_count: u32,
    payload_len: u64,
}

impl WalFrameHeader {
    pub(super) fn parse(bytes: &[u8; HEADER_LEN]) -> Result<Self, LogManagerError> {
        let mut bytes = bytes.as_slice();
        let magic = read_array(&mut bytes)?;
        if magic != HEADER_MAGIC {
            return Err(LogManagerError::InvalidHeaderMagic { actual: magic });
        }
        let version = u16::from_le_bytes(read_array(&mut bytes)?);
        if version != WAL_FORMAT_VERSION {
            return Err(LogManagerError::UnsupportedVersion {
                expected: WAL_FORMAT_VERSION,
                actual: version,
            });
        }
        Ok(Self {
            txn_id: u64::from_le_bytes(read_array(&mut bytes)?),
            record_count: u32::from_le_bytes(read_array(&mut bytes)?),
            payload_len: u64::from_le_bytes(read_array(&mut bytes)?),
        })
    }

    fn payload_len(&self) -> Result<usize, LogManagerError> {
        usize::try_from(self.payload_len).map_err(|_out_of_range| {
            LogManagerError::PayloadLengthTooLarge { payload_len: self.payload_len }
        })
    }

    pub(super) fn frame_len(&self) -> Result<usize, LogManagerError> {
        self.payload_len()?
            .checked_add(HEADER_LEN + FOOTER_LEN)
            .ok_or(LogManagerError::PayloadLengthOverflow)
    }

    /// Decode records from the exact body (payload and footer) read for this header.
    pub(super) fn decode_records(
        &self,
        mut body: &[u8],
    ) -> Result<Vec<DecodedRecord>, LogManagerError> {
        let mut records = Vec::new();
        self.read_body(&mut body, Some(&mut records))?;
        debug_assert!(body.is_empty());
        debug_assert_eq!(records.len(), self.record_count as usize);
        Ok(records)
    }

    fn read_body<R: Read>(
        &self,
        reader: &mut R,
        mut records: Option<&mut Vec<DecodedRecord>>,
    ) -> Result<(), LogManagerError> {
        let mut payload =
            FramePayload { reader, remaining: self.payload_len()?, digest: CRC32.digest() };
        let mut count = 0;
        while payload.remaining != 0 {
            if let Some(record) = payload.read_record(records.is_some())?
                && let Some(records) = records.as_deref_mut()
            {
                records.push(record);
            }
            // Every record consumes at least one byte, so count cannot exceed payload length.
            count += 1;
        }
        if u32::try_from(count) != Ok(self.record_count) {
            return Err(LogManagerError::RecordCountMismatch {
                expected: self.record_count,
                actual: count,
            });
        }
        let footer = read_exact_or_eof::<_, FOOTER_LEN>(payload.reader)?
            .ok_or(LogManagerError::TruncatedFrame { needed: FOOTER_LEN, remaining: 0 })?;
        validate_footer(&footer, self.txn_id, payload.digest.finalize())
    }
}

/// Validate a frame without retaining any records or allocating page images.
pub(super) fn scan_transaction_frame<R: Read>(
    reader: &mut R,
) -> Result<Option<WalFrameHeader>, LogManagerError> {
    let Some(bytes) = read_exact_or_eof::<_, HEADER_LEN>(reader)? else {
        return Ok(None);
    };
    let header = WalFrameHeader::parse(&bytes)?;
    header.read_body(reader, None)?;
    Ok(Some(header))
}

struct FramePayload<'a, R> {
    reader: &'a mut R,
    remaining: usize,
    digest: Digest<'static, u32>,
}

impl<R: Read> FramePayload<'_, R> {
    fn read_record(&mut self, collect: bool) -> Result<Option<DecodedRecord>, LogManagerError> {
        use RecoveryLogRecordKind as Kind;
        let prev_lsn = u64::from_le_bytes(self.read_array()?);
        let kind = match self.read_array::<1>()?[0] {
            KIND_BEGIN => collect.then_some(Kind::Begin),
            KIND_COMMIT => collect.then_some(Kind::Commit),
            KIND_ROLLBACK => collect.then_some(Kind::Rollback),
            tag @ (KIND_PAGE_ALLOC | KIND_PAGE_DEALLOC) => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                collect.then_some(if tag == KIND_PAGE_ALLOC {
                    Kind::PageAlloc { page_id }
                } else {
                    Kind::PageDealloc { page_id }
                })
            }
            KIND_BITMAP_IMAGE => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                self.read_image(collect)?.map(|image| Kind::BitmapImage { page_id, image })
            }
            KIND_PAGE_UPDATE => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let redo_len = u32::from_le_bytes(self.read_array()?) as usize;
                let undo_len = u32::from_le_bytes(self.read_array()?) as usize;
                validate_page_image_len_value(redo_len)?;
                validate_page_image_len_value(undo_len)?;
                let redo = self.read_image(collect)?;
                let undo = self.read_image(collect)?;
                redo.zip(undo).map(|(redo_data, undo_data)| Kind::PageUpdate {
                    page_id,
                    redo_data,
                    undo_data,
                })
            }
            kind => return Err(LogManagerError::UnknownRecordKind { kind }),
        };
        Ok(kind.map(|kind| (prev_lsn, kind)))
    }

    fn read_image(
        &mut self,
        collect: bool,
    ) -> Result<Option<Box<[u8; PAGE_SIZE]>>, LogManagerError> {
        let image = self.read_array()?;
        Ok(collect.then(|| Box::new(image)))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], LogManagerError> {
        if self.remaining < N {
            return Err(LogManagerError::TruncatedFrame { needed: N, remaining: self.remaining });
        }
        let bytes = read_array(&mut *self.reader)?;
        self.digest.update(&bytes);
        self.remaining -= N;
        Ok(bytes)
    }
}

fn read_array<const N: usize>(mut reader: impl Read) -> std::io::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_exact_or_eof<R: Read, const N: usize>(reader: &mut R) -> std::io::Result<Option<[u8; N]>> {
    let mut buf = [0; N];
    let mut read_len = 0;
    while read_len < N {
        match reader.read(&mut buf[read_len..]) {
            Ok(0) if read_len == 0 => return Ok(None),
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(len) => read_len += len,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(Some(buf))
}

fn validate_footer(
    footer_bytes: &[u8; FOOTER_LEN],
    txn_id: TxnId,
    actual_crc: u32,
) -> Result<(), LogManagerError> {
    let mut bytes = footer_bytes.as_slice();
    let magic = read_array(&mut bytes)?;
    if magic != FOOTER_MAGIC {
        return Err(LogManagerError::InvalidFooterMagic { actual: magic });
    }
    let footer_txn_id = u64::from_le_bytes(read_array(&mut bytes)?);
    if footer_txn_id != txn_id {
        return Err(LogManagerError::FooterTxnIdMismatch {
            expected: txn_id,
            actual: footer_txn_id,
        });
    }
    let expected_crc = u32::from_le_bytes(read_array(&mut bytes)?);
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

pub(super) fn validate_record_txn_ids(
    txn_id: TxnId,
    records: &[LogRecord<'_>],
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

fn serialized_records_len(records: &[LogRecord<'_>]) -> Result<u64, LogManagerError> {
    let mut len = 0u64;
    for record in records {
        len = len
            .checked_add(8 + serialized_record_len(&record.kind)?)
            .ok_or(LogManagerError::PayloadLengthOverflow)?;
    }
    Ok(len)
}

fn serialized_record_len(kind: &LogRecordKind<'_>) -> Result<u64, LogManagerError> {
    match kind {
        LogRecordKind::Begin | LogRecordKind::Commit | LogRecordKind::Rollback => Ok(1),
        LogRecordKind::PageUpdate { redo_data, undo_data, .. } => {
            validate_page_image_len(redo_data)?;
            validate_page_image_len(undo_data)?;
            let redo_len = u32::try_from(redo_data.len()).map_err(|_out_of_range| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_out_of_range| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;
            Ok(1 + 8 + 4 + 4 + u64::from(redo_len) + u64::from(undo_len))
        }
        LogRecordKind::PageAlloc { .. } | LogRecordKind::PageDealloc { .. } => Ok(1 + 8),
        LogRecordKind::BitmapImage { image, .. } => {
            validate_page_image_len(image)?;
            Ok(1 + 8 + PAGE_SIZE as u64)
        }
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

fn write_log_record_payload<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    kind: &LogRecordKind<'_>,
) -> Result<(), LogManagerError> {
    match kind {
        LogRecordKind::Begin => write_crc_u8(writer, digest, KIND_BEGIN)?,
        LogRecordKind::Commit => write_crc_u8(writer, digest, KIND_COMMIT)?,
        LogRecordKind::Rollback => write_crc_u8(writer, digest, KIND_ROLLBACK)?,
        LogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
            validate_page_image_len(redo_data)?;
            validate_page_image_len(undo_data)?;
            let redo_len = u32::try_from(redo_data.len()).map_err(|_out_of_range| {
                LogManagerError::PayloadLengthTooLarge { payload_len: redo_data.len() as u64 }
            })?;
            let undo_len = u32::try_from(undo_data.len()).map_err(|_out_of_range| {
                LogManagerError::PayloadLengthTooLarge { payload_len: undo_data.len() as u64 }
            })?;

            write_crc_u8(writer, digest, KIND_PAGE_UPDATE)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u32(writer, digest, redo_len)?;
            write_crc_u32(writer, digest, undo_len)?;
            write_crc_bytes(writer, digest, redo_data)?;
            write_crc_bytes(writer, digest, undo_data)?;
        }
        LogRecordKind::BitmapImage { page_id, image } => {
            validate_page_image_len(image)?;
            write_crc_u8(writer, digest, KIND_BITMAP_IMAGE)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_bytes(writer, digest, image)?;
        }
        LogRecordKind::PageAlloc { page_id } | LogRecordKind::PageDealloc { page_id } => {
            let kind = if matches!(kind, LogRecordKind::PageAlloc { .. }) {
                KIND_PAGE_ALLOC
            } else {
                KIND_PAGE_DEALLOC
            };
            write_crc_u8(writer, digest, kind)?;
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
