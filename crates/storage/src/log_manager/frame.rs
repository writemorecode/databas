//! WAL transaction-frame encoding and validation.

use std::io::{Read, Write};

use crc::{CRC_32_ISO_HDLC, Crc, Digest};

use crate::logical_undo::{Inverse, LogicalUndo};
use crate::page_operation::{LeafOperation, LeafPayload, PageOperation};
use crate::page_patch::{InvalidPagePatch, PagePatch};
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
    record_count: u32,
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
        body: &[u8],
    ) -> Result<Vec<DecodedRecord>, LogManagerError> {
        let payload_len = self.payload_len()?;
        let (bytes, footer) =
            body.split_at_checked(payload_len).ok_or(LogManagerError::TruncatedFrame {
                needed: payload_len,
                remaining: body.len(),
            })?;
        let mut payload = FramePayload(bytes);
        let mut records = Vec::new();
        while !payload.0.is_empty() {
            records.push(payload.read_record()?);
        }
        if u32::try_from(records.len()) != Ok(self.record_count) {
            return Err(LogManagerError::RecordCountMismatch {
                expected: self.record_count,
                actual: records.len(),
            });
        }
        let footer = footer.try_into().map_err(|_length| LogManagerError::TruncatedFrame {
            needed: FOOTER_LEN,
            remaining: footer.len(),
        })?;
        validate_footer(footer, self.txn_id, CRC32.checksum(bytes))?;
        Ok(records)
    }
}

struct FramePayload<'a>(&'a [u8]);

impl<'a> FramePayload<'a> {
    fn read_record(&mut self) -> Result<DecodedRecord, LogManagerError> {
        use RecoveryLogRecordKind as Kind;
        let prev_lsn = u64::from_le_bytes(self.read_array()?);
        let kind = match self.read_array::<1>()?[0] {
            KIND_BEGIN => Kind::Begin,
            KIND_COMMIT => Kind::Commit,
            KIND_ROLLBACK => Kind::Rollback,
            KIND_PAGE_INIT => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let tag = self.read_array::<1>()?[0];
                let kind = PageInitKind::from_tag(tag)
                    .ok_or(LogManagerError::InvalidPageInitKind { tag })?;
                Kind::PageInit { page_id, kind }
            }
            KIND_PAGE_DEALLOC => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                Kind::PageDealloc { page_id }
            }
            KIND_PAGE_COMPENSATION => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let incarnation = u64::from_le_bytes(self.read_array()?);
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                Kind::PageCompensation {
                    page_id,
                    incarnation,
                    redo: self.read_patch()?,
                    undo_next_lsn,
                }
            }
            KIND_ALLOCATION_COMPENSATION => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                let allocated = match self.read_array::<1>()?[0] {
                    0 => false,
                    1 => true,
                    state => return Err(LogManagerError::InvalidAllocationState { state }),
                };
                Kind::AllocationCompensation { page_id, allocated, undo_next_lsn }
            }
            KIND_LOGICAL_EDIT => self.read_logical_undo()?,
            KIND_SKIP_UNDO => {
                let undo_next_lsn = u64::from_le_bytes(self.read_array()?);
                Kind::SkipUndo { undo_next_lsn }
            }
            KIND_PAGE_UPDATE => {
                let page_id = u64::from_le_bytes(self.read_array()?);
                let incarnation = u64::from_le_bytes(self.read_array()?);
                let before_lsn = u64::from_le_bytes(self.read_array()?);
                Kind::PageUpdate {
                    page_id,
                    incarnation,
                    before_lsn,
                    redo: self.read_operation()?,
                    undo: self.read_patch()?,
                }
            }
            kind => return Err(LogManagerError::UnknownRecordKind { kind }),
        };
        Ok((prev_lsn, kind))
    }

    fn read_logical_undo(&mut self) -> Result<RecoveryLogRecordKind, LogManagerError> {
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
        let key = self.take(usize::from(key_len))?.into();
        let value = self.take(usize::from(value_len))?;
        let action = match tag {
            INVERSE_DELETE => Inverse::Delete,
            INVERSE_INSERT => Inverse::Insert(value.into()),
            _ => Inverse::Replace(value.into()),
        };
        Ok(RecoveryLogRecordKind::LogicalEdit {
            undo: LogicalUndo { root, incarnation, key, action },
            undo_next_lsn,
        })
    }

    fn read_operation(&mut self) -> Result<PageOperation, LogManagerError> {
        let tag = self.read_array::<1>()?[0];
        match tag {
            OP_PATCH => Ok(PageOperation::Patch(self.read_patch()?)),
            OP_LEAF_DELETE => {
                let slot = u16::from_le_bytes(self.read_array()?);
                Ok(PageOperation::Leaf(LeafOperation::Delete { slot }))
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
                let inline = self.take(len)?.into();
                let cell = LeafPayload { key_len, value_len, overflow, inline };
                Ok(PageOperation::Leaf(if tag == OP_LEAF_INSERT {
                    LeafOperation::Insert { slot, cell }
                } else {
                    LeafOperation::Replace { slot, cell }
                }))
            }
            _ => Err(LogManagerError::InvalidPageOperation { tag }),
        }
    }

    fn read_patch(&mut self) -> Result<PagePatch, LogManagerError> {
        let len = usize::from(u16::from_le_bytes(self.read_array()?));
        Ok(PagePatch::decode(self.take(len)?)?)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], LogManagerError> {
        let (bytes, rest) = self
            .0
            .split_first_chunk()
            .ok_or(LogManagerError::TruncatedFrame { needed: N, remaining: self.0.len() })?;
        self.0 = rest;
        Ok(*bytes)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], LogManagerError> {
        let (bytes, rest) = self
            .0
            .split_at_checked(len)
            .ok_or(LogManagerError::TruncatedFrame { needed: len, remaining: self.0.len() })?;
        self.0 = rest;
        Ok(bytes)
    }
}

fn read_array<const N: usize>(mut reader: impl Read) -> std::io::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
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
