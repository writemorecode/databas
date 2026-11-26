//! WAL transaction-frame encoding and shared validation for both read paths.

use std::io::{Read, Write};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};

use crate::logical_undo::{Inverse, LogicalUndo};
use crate::page::format::MAX_INLINE_OVERFLOW_PAYLOAD_BYTES;
use crate::page_operation::{LeafOperation, LeafPayload, PageOperation};
use crate::page_patch::{InvalidPagePatch, MAX_PATCH_BYTES, PagePatch};
use crate::page_recovery::PageInitKind;

use super::{LogManagerError, LogRecord, LogRecordKind, Lsn, RecoveryLogRecordKind, TxnId};

pub(super) type DecodedRecord = (Lsn, RecoveryLogRecordKind);

const HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const FOOTER_MAGIC: [u8; 8] = *b"DBWALFTR";
const WAL_FORMAT_VERSION: u16 = 9;
pub(super) const HEADER_LEN: usize = 8 + 2 + 8 + 4 + 8;
const FOOTER_LEN: usize = 8 + 8 + 4;
pub(super) const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

const KIND_BEGIN: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ROLLBACK: u8 = 3;
const KIND_PAGE_UPDATE: u8 = 4;
const KIND_PAGE_INIT: u8 = 5;
const KIND_PAGE_DEALLOC: u8 = 6;
const KIND_PAGE_COMPENSATION: u8 = 7;
const KIND_ALLOCATION_COMPENSATION: u8 = 8;
const KIND_LOGICAL_EDIT: u8 = 9;
const KIND_SKIP_UNDO: u8 = 10;

const INVERSE_DELETE: u8 = 1;
const INVERSE_INSERT: u8 = 2;
const INVERSE_REPLACE: u8 = 3;

const OP_PATCH: u8 = 1;
const OP_LEAF_INSERT: u8 = 2;
const OP_LEAF_REPLACE: u8 = 3;
const OP_LEAF_DELETE: u8 = 4;

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

/// Validate a frame without retaining any records or allocating page patches.
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
            KIND_PAGE_INIT => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let tag = self.read_array::<1>()?[0];
                let kind = PageInitKind::from_tag(tag)
                    .ok_or(LogManagerError::InvalidPageInitKind { tag })?;
                collect.then_some(Kind::PageInit { page_id, kind })
            }
            KIND_PAGE_DEALLOC => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                collect.then_some(Kind::PageDealloc { page_id })
            }
            KIND_PAGE_COMPENSATION => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let incarnation = u64::from_le_bytes(self.read_array()?);
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                self.read_patch(collect)?.map(|redo| Kind::PageCompensation {
                    page_id,
                    incarnation,
                    redo,
                    undo_next_lsn,
                })
            }
            KIND_ALLOCATION_COMPENSATION => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                let allocated = match self.read_array::<1>()?[0] {
                    0 => false,
                    1 => true,
                    state => return Err(LogManagerError::InvalidAllocationState { state }),
                };
                collect.then_some(Kind::AllocationCompensation {
                    page_id,
                    allocated,
                    undo_next_lsn,
                })
            }
            KIND_LOGICAL_EDIT => self.read_logical_undo(collect)?,
            KIND_SKIP_UNDO => {
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                collect.then_some(Kind::SkipUndo { undo_next_lsn })
            }
            KIND_PAGE_UPDATE => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let incarnation = u64::from_le_bytes(self.read_array()?);
                let before_lsn = u64::from_le_bytes(self.read_array()?);
                let redo = self.read_operation(collect)?;
                let undo = self.read_patch(collect)?;
                redo.zip(undo).map(|(redo, undo)| Kind::PageUpdate {
                    page_id,
                    incarnation,
                    before_lsn,
                    redo,
                    undo,
                })
            }
            kind => return Err(LogManagerError::UnknownRecordKind { kind }),
        };
        Ok(kind.map(|kind| (prev_lsn, kind)))
    }

    fn read_logical_undo(
        &mut self,
        collect: bool,
    ) -> Result<Option<RecoveryLogRecordKind>, LogManagerError> {
        let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
        let root = u64::from_le_bytes(self.read_array()?);
        let incarnation = u64::from_le_bytes(self.read_array()?);
        let tag = self.read_array::<1>()?[0];
        let key_len = u16::from_le_bytes(self.read_array()?);
        let value_len = u16::from_le_bytes(self.read_array()?);
        if root == 0
            || incarnation == 0
            || key_len.checked_add(value_len).is_none()
            || !matches!(tag, INVERSE_DELETE | INVERSE_INSERT | INVERSE_REPLACE)
            || (tag == INVERSE_DELETE && value_len != 0)
        {
            return Err(LogManagerError::InvalidLogicalUndo);
        }
        let key = self.read_owned_payload(usize::from(key_len), collect)?;
        let value = self.read_owned_payload(usize::from(value_len), collect)?;
        Ok(key.zip(value).map(|(key, value)| RecoveryLogRecordKind::LogicalEdit {
            undo: LogicalUndo {
                root,
                incarnation,
                key,
                action: match tag {
                    INVERSE_DELETE => Inverse::Delete,
                    INVERSE_INSERT => Inverse::Insert(value),
                    _ => Inverse::Replace(value),
                },
            },
            undo_next_lsn,
        }))
    }

    fn read_owned_payload(
        &mut self,
        len: usize,
        collect: bool,
    ) -> Result<Option<Box<[u8]>>, LogManagerError> {
        if self.remaining < len {
            return Err(LogManagerError::TruncatedFrame { needed: len, remaining: self.remaining });
        }
        if collect {
            let mut bytes = vec![0; len].into_boxed_slice();
            self.read_payload(&mut bytes)?;
            Ok(Some(bytes))
        } else {
            let mut bytes = [0; 1024];
            let mut left = len;
            while left > 0 {
                let chunk = left.min(bytes.len());
                self.read_payload(&mut bytes[..chunk])?;
                left -= chunk;
            }
            Ok(None)
        }
    }

    fn read_operation(&mut self, collect: bool) -> Result<Option<PageOperation>, LogManagerError> {
        let tag = self.read_array::<1>()?[0];
        match tag {
            OP_PATCH => Ok(self.read_patch(collect)?.map(PageOperation::Patch)),
            OP_LEAF_DELETE => {
                let slot = u16::from_le_bytes(self.read_array()?);
                Ok(collect.then_some(PageOperation::Leaf(LeafOperation::Delete { slot })))
            }
            OP_LEAF_INSERT | OP_LEAF_REPLACE => {
                let slot = u16::from_le_bytes(self.read_array()?);
                let key_len = u16::from_le_bytes(self.read_array()?);
                let value_len = u16::from_le_bytes(self.read_array()?);
                let overflow = match u64::from_le_bytes(self.read_array()?) {
                    u64::MAX => None,
                    page_id => Some(page_id),
                };
                let len = LeafPayload::inline_len(key_len, value_len, overflow)
                    .ok_or(LogManagerError::InvalidPageOperation { tag })?;
                let mut buffer = [0; MAX_INLINE_OVERFLOW_PAYLOAD_BYTES];
                let inline = &mut buffer[..len];
                self.read_payload(inline)?;
                Ok(collect.then(|| {
                    let cell = LeafPayload { key_len, value_len, overflow, inline: inline.into() };
                    PageOperation::Leaf(if tag == OP_LEAF_INSERT {
                        LeafOperation::Insert { slot, cell }
                    } else {
                        LeafOperation::Replace { slot, cell }
                    })
                }))
            }
            _ => Err(LogManagerError::InvalidPageOperation { tag }),
        }
    }

    fn read_patch(&mut self, collect: bool) -> Result<Option<PagePatch>, LogManagerError> {
        let len = usize::from(u16::from_le_bytes(self.read_array()?));
        let mut buffer = [0; MAX_PATCH_BYTES];
        let bytes = buffer.get_mut(..len).ok_or(InvalidPagePatch)?;
        self.read_payload(bytes)?;
        Ok(PagePatch::decode(bytes, collect)?)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], LogManagerError> {
        let mut bytes = [0; N];
        self.read_payload(&mut bytes)?;
        Ok(bytes)
    }

    fn read_payload(&mut self, bytes: &mut [u8]) -> Result<(), LogManagerError> {
        if self.remaining < bytes.len() {
            return Err(LogManagerError::TruncatedFrame {
                needed: bytes.len(),
                remaining: self.remaining,
            });
        }
        self.reader.read_exact(bytes)?;
        self.digest.update(bytes);
        self.remaining -= bytes.len();
        Ok(())
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
    Ok(match kind {
        LogRecordKind::Begin | LogRecordKind::Commit | LogRecordKind::Rollback => 1,
        LogRecordKind::PageUpdate { redo, undo, .. } => {
            1 + 8 + 8 + 8 + operation_len(redo) + 2 + undo.bytes().len() as u64
        }
        LogRecordKind::PageInit { .. } => 1 + 8 + 1,
        LogRecordKind::PageDealloc { .. } => 1 + 8,
        LogRecordKind::PageCompensation { redo, .. } => {
            1 + 8 + 8 + 8 + 2 + redo.bytes().len() as u64
        }
        LogRecordKind::AllocationCompensation { .. } => 1 + 8 + 8 + 1,
        LogRecordKind::LogicalEdit { undo, .. } => {
            let (_, value) = inverse_payload(&undo.action);
            let (key_len, value_len) = logical_lengths(undo, value)?;
            1 + 8 + 8 + 8 + 1 + 2 + 2 + u64::from(key_len) + u64::from(value_len)
        }
        LogRecordKind::SkipUndo { .. } => 1 + 8,
    })
}

fn logical_lengths(undo: &LogicalUndo, value: &[u8]) -> Result<(u16, u16), LogManagerError> {
    let key_len =
        u16::try_from(undo.key.len()).map_err(|_too_large| LogManagerError::InvalidLogicalUndo)?;
    let value_len =
        u16::try_from(value.len()).map_err(|_too_large| LogManagerError::InvalidLogicalUndo)?;
    if undo.root == 0 || undo.incarnation == 0 || key_len.checked_add(value_len).is_none() {
        return Err(LogManagerError::InvalidLogicalUndo);
    }
    Ok((key_len, value_len))
}

fn inverse_payload(inverse: &Inverse) -> (u8, &[u8]) {
    match inverse {
        Inverse::Delete => (INVERSE_DELETE, &[]),
        Inverse::Insert(value) => (INVERSE_INSERT, value),
        Inverse::Replace(value) => (INVERSE_REPLACE, value),
    }
}

fn operation_len(operation: &PageOperation) -> u64 {
    1 + match operation {
        PageOperation::Patch(patch) => 2 + patch.bytes().len() as u64,
        PageOperation::Leaf(
            LeafOperation::Insert { cell, .. } | LeafOperation::Replace { cell, .. },
        ) => 2 + 2 + 2 + 8 + cell.inline.len() as u64,
        PageOperation::Leaf(LeafOperation::Delete { .. }) => 2,
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
        LogRecordKind::PageUpdate { page_id, incarnation, before_lsn, redo, undo } => {
            write_crc_u8(writer, digest, KIND_PAGE_UPDATE)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u64(writer, digest, *incarnation)?;
            write_crc_u64(writer, digest, *before_lsn)?;
            write_operation(writer, digest, redo)?;
            write_patch(writer, digest, undo)?;
        }
        LogRecordKind::PageCompensation { page_id, incarnation, redo, undo_next_lsn } => {
            write_crc_u8(writer, digest, KIND_PAGE_COMPENSATION)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u64(writer, digest, *incarnation)?;
            write_crc_u64(writer, digest, *undo_next_lsn)?;
            write_patch(writer, digest, redo)?;
        }
        LogRecordKind::AllocationCompensation { page_id, allocated, undo_next_lsn } => {
            write_crc_u8(writer, digest, KIND_ALLOCATION_COMPENSATION)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u64(writer, digest, *undo_next_lsn)?;
            write_crc_u8(writer, digest, u8::from(*allocated))?;
        }
        LogRecordKind::LogicalEdit { undo, undo_next_lsn } => {
            let (tag, value) = inverse_payload(&undo.action);
            let (key_len, value_len) = logical_lengths(undo, value)?;
            write_crc_u8(writer, digest, KIND_LOGICAL_EDIT)?;
            write_crc_u64(writer, digest, *undo_next_lsn)?;
            write_crc_u64(writer, digest, undo.root)?;
            write_crc_u64(writer, digest, undo.incarnation)?;
            write_crc_u8(writer, digest, tag)?;
            write_crc_bytes(writer, digest, &key_len.to_le_bytes())?;
            write_crc_bytes(writer, digest, &value_len.to_le_bytes())?;
            write_crc_bytes(writer, digest, &undo.key)?;
            write_crc_bytes(writer, digest, value)?;
        }
        LogRecordKind::SkipUndo { undo_next_lsn } => {
            write_crc_u8(writer, digest, KIND_SKIP_UNDO)?;
            write_crc_u64(writer, digest, *undo_next_lsn)?;
        }
        LogRecordKind::PageInit { page_id, kind } => {
            write_crc_u8(writer, digest, KIND_PAGE_INIT)?;
            write_crc_u64(writer, digest, *page_id)?;
            write_crc_u8(writer, digest, *kind as u8)?;
        }
        LogRecordKind::PageDealloc { page_id } => {
            write_crc_u8(writer, digest, KIND_PAGE_DEALLOC)?;
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

fn write_operation<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    operation: &PageOperation,
) -> Result<(), LogManagerError> {
    match operation {
        PageOperation::Patch(patch) => {
            write_crc_u8(writer, digest, OP_PATCH)?;
            write_patch(writer, digest, patch)?;
        }
        PageOperation::Leaf(LeafOperation::Delete { slot }) => {
            write_crc_u8(writer, digest, OP_LEAF_DELETE)?;
            write_crc_bytes(writer, digest, &slot.to_le_bytes())?;
        }
        PageOperation::Leaf(
            op @ (LeafOperation::Insert { slot, cell } | LeafOperation::Replace { slot, cell }),
        ) => {
            let tag = if matches!(op, LeafOperation::Insert { .. }) {
                OP_LEAF_INSERT
            } else {
                OP_LEAF_REPLACE
            };
            if LeafPayload::inline_len(cell.key_len, cell.value_len, cell.overflow)
                != Some(cell.inline.len())
            {
                return Err(LogManagerError::InvalidPageOperation { tag });
            }
            write_crc_u8(writer, digest, tag)?;
            write_crc_bytes(writer, digest, &slot.to_le_bytes())?;
            write_crc_bytes(writer, digest, &cell.key_len.to_le_bytes())?;
            write_crc_bytes(writer, digest, &cell.value_len.to_le_bytes())?;
            write_crc_u64(writer, digest, cell.overflow.unwrap_or(u64::MAX))?;
            write_crc_bytes(writer, digest, &cell.inline)?;
        }
    }
    Ok(())
}

fn write_patch<W: Write>(
    writer: &mut W,
    digest: &mut Digest<'_, u32>,
    patch: &PagePatch,
) -> Result<(), LogManagerError> {
    let len = u16::try_from(patch.bytes().len()).map_err(|_out_of_range| InvalidPagePatch)?;
    write_crc_bytes(writer, digest, &len.to_le_bytes())?;
    write_crc_bytes(writer, digest, patch.bytes())?;
    Ok(())
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
