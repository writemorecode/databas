//! Write-ahead log (WAL) serialization, append, flush, and recovery scanning.
//!
//! The WAL is stored next to the database file using the same path with an
//! additional `.wal` extension. Records are physically grouped into transaction
//! frames:
//!
//! ```text
//! WAL file header | transaction frame...
//!
//! transaction frame:
//!   header magic | format version | transaction id | record count | payload length
//!   payload records...
//!   footer magic | transaction id | payload CRC32
//! ```
//!
//! LSNs are not stored explicitly in transaction frames. The WAL file header
//! stores the durable high-water LSN before the current WAL segment, and records
//! in complete frames are assigned logical LSNs after that value. Appending
//! advances the highest appended LSN, while [`LogManager::flush_through`] is
//! responsible for making appended bytes durable before pages protected by
//! those LSNs are written to the database file.

#![allow(clippy::redundant_pub_crate)]

use std::{
    collections::HashMap,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use thiserror::Error;

#[allow(clippy::redundant_pub_crate)] // Lsn is intentionally crate-private.
pub(crate) use crate::Lsn;
pub use crate::TxnId;
use crate::filesystem::{File, FileSystem, OpenMode};
use crate::{PAGE_SIZE, PageId};

mod frame;
mod replacement;

pub(crate) use replacement::retire_empty_wal;

use frame::{
    CRC32, HEADER_LEN, WalFrameHeader, scan_transaction_frame, serialize_transaction,
    wal_open_error,
};

/// Sentinel LSN used for pages that do not reflect any logged update.
pub const ZERO_LSN: Lsn = 0;

const WAL_READ_BUFFER_LEN: usize = 64 * 1024;
const WAL_WRITE_BUFFER_LEN: usize = 64 * 1024;
const WAL_FILE_HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const WAL_FILE_HEADER_VERSION: u16 = 4;
pub const WAL_FILE_HEADER_LEN: usize = 32;
const WAL_FILE_HEADER_CHECKSUM_RANGE: std::ops::Range<usize> = 10..14;

trait DurableWrite: Write {
    fn sync_all(&mut self) -> std::io::Result<()>;
}

impl DurableWrite for BufWriter<File> {
    fn sync_all(&mut self) -> std::io::Result<()> {
        self.get_ref().sync_all()
    }
}

type ActiveWalWriter = BufWriter<File>;

fn make_wal_writer(file: File) -> ActiveWalWriter {
    BufWriter::with_capacity(WAL_WRITE_BUFFER_LEN, file)
}

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
    RecordCountMismatch { expected: u32, actual: usize },
    /// A transaction frame contains more records than the format can encode.
    #[error("too many WAL records in transaction: {count}")]
    TooManyRecords { count: usize },
    /// The next LSN would exceed the numeric range of [`Lsn`].
    #[error("WAL LSN exhausted")]
    LsnExhausted,
    /// A page update record did not contain full `PAGE_SIZE` images.
    #[error("WAL full-page image has invalid length: expected {expected}, got {actual}")]
    InvalidPageImageLength { expected: usize, actual: usize },
    /// The WAL file header failed its integrity checksum.
    #[error("WAL file header checksum mismatch: expected {expected}, got {actual}")]
    FileHeaderChecksumMismatch { expected: u32, actual: u32 },
    /// A compensation record must encode either allocated or free.
    #[error("invalid WAL allocation state: {state}")]
    InvalidAllocationState { state: u8 },
    /// A transaction/compensation link was forward, missing, or crossed owners.
    #[error("invalid WAL chain for transaction {txn_id} at LSN {lsn}: {link}")]
    InvalidChain { txn_id: TxnId, lsn: Lsn, link: Lsn },
    /// A previous append may have left a partial frame in this WAL stream.
    #[error("WAL manager is poisoned after a failed append")]
    Poisoned,
    /// A WAL frame buffer could not be allocated during recovery.
    #[error("failed to allocate {byte_count} bytes for a WAL frame: {source}")]
    FrameAllocationFailed {
        byte_count: usize,
        #[source]
        source: std::collections::TryReserveError,
    },
}

/// Errors raised while forcing appended WAL bytes to durable storage.
#[derive(Debug, Error)]
pub enum LogManagerFlushError {
    /// The filesystem sync failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The caller requested durability for an LSN that has not been appended.
    #[error(
        "requested WAL flush through LSN {requested_lsn}, but highest appended LSN is {highest_appended_lsn:?}"
    )]
    LsnNotAppended { requested_lsn: Lsn, highest_appended_lsn: Option<Lsn> },
    /// A previous append may have left a partial frame in this WAL stream.
    #[error("WAL manager is poisoned after a failed append")]
    Poisoned,
}

/// Borrowed WAL record used while serializing a transaction frame.
#[derive(Debug)]
pub struct LogRecord<'a> {
    /// Transaction that owns this record.
    pub(crate) txn_id: TxnId,
    /// Checksummed link to the previous record owned by this transaction.
    pub(crate) prev_lsn: Lsn,
    /// Record payload.
    pub(crate) kind: LogRecordKind<'a>,
}

/// Borrowed WAL record payload.
#[derive(Debug)]
pub enum LogRecordKind<'a> {
    /// Transaction start marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Transaction rollback-complete marker.
    Rollback,
    /// Full-page physical update.
    ///
    /// `redo_data` is installed during repeat-history redo, including for
    /// losers. `undo_data` is the before-image used to construct compensation.
    PageUpdate { page_id: PageId, redo_data: &'a [u8], undo_data: &'a [u8] },
    /// Allocation of a database page by a transaction.
    PageAlloc { page_id: PageId },
    /// Deallocation of a database page by a transaction.
    PageDealloc { page_id: PageId },
    /// Redo-only page compensation with the next LSN to undo.
    PageCompensation { page_id: PageId, redo_data: &'a [u8], undo_next_lsn: Lsn },
    /// Redo-only compensation of one allocation bit.
    AllocationCompensation { page_id: PageId, allocated: bool, undo_next_lsn: Lsn },
}

/// Owned WAL record returned by recovery scans.
///
/// Unlike [`LogRecord`], this form owns page images and includes the LSN
/// assigned from the record's position in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryLogRecord {
    /// Logical LSN assigned to this record.
    pub(crate) lsn: Lsn,
    /// Transaction-chain link preserved verbatim across WAL replacement.
    pub(crate) prev_lsn: Lsn,
    /// Transaction that owns this record.
    pub(crate) txn_id: TxnId,
    /// Owned record payload.
    pub(crate) kind: RecoveryLogRecordKind,
}

/// Owned WAL record payload used by crash recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryLogRecordKind {
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
    /// Deallocation of a database page by a transaction.
    PageDealloc { page_id: PageId },
    /// Redo-only page compensation with the next LSN to undo.
    PageCompensation { page_id: PageId, redo_data: Box<[u8; PAGE_SIZE]>, undo_next_lsn: Lsn },
    /// Redo-only compensation of one allocation bit.
    AllocationCompensation { page_id: PageId, allocated: bool, undo_next_lsn: Lsn },
}

/// Result of scanning the WAL for recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryLogScan {
    /// Complete records decoded from valid WAL frames, in log order.
    pub(crate) records: Vec<RecoveryLogRecord>,
    /// Whether an invalid or incomplete frame and its suffix were truncated.
    pub(crate) truncated_tail: bool,
    /// Largest transaction id observed in complete frames.
    pub(crate) max_txn_id: TxnId,
    /// Highest LSN assigned before this WAL segment plus all complete records.
    pub(crate) last_assigned_lsn: Lsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalFileHeader {
    last_assigned_lsn: Lsn,
    max_txn_id: TxnId,
}

fn ensure_wal_file_header(wal_file: &mut File) -> Result<WalFileHeader, LogManagerError> {
    if wal_file.len()? == 0 {
        write_wal_file_header(wal_file, ZERO_LSN, 0)?;
        wal_file.set_len(WAL_FILE_HEADER_LEN as u64)?;
        wal_file.sync_all()?;
        return Ok(WalFileHeader { last_assigned_lsn: ZERO_LSN, max_txn_id: 0 });
    }
    read_wal_file_header(wal_file)
}

fn read_wal_file_header(wal_file: &mut File) -> Result<WalFileHeader, LogManagerError> {
    let mut header = [0; WAL_FILE_HEADER_LEN];
    wal_file.seek(SeekFrom::Start(0))?;
    wal_file.read_exact(&mut header)?;

    let mut magic = [0; 8];
    magic.copy_from_slice(&header[..8]);
    if magic != WAL_FILE_HEADER_MAGIC {
        return Err(LogManagerError::InvalidHeaderMagic { actual: magic });
    }

    let mut version_bytes = [0; 2];
    version_bytes.copy_from_slice(&header[8..10]);
    let version = u16::from_le_bytes(version_bytes);
    if version != WAL_FILE_HEADER_VERSION {
        return Err(LogManagerError::UnsupportedVersion {
            expected: WAL_FILE_HEADER_VERSION,
            actual: version,
        });
    }

    let mut checksum_bytes = [0; 4];
    checksum_bytes.copy_from_slice(&header[WAL_FILE_HEADER_CHECKSUM_RANGE]);
    let expected = u32::from_le_bytes(checksum_bytes);
    header[WAL_FILE_HEADER_CHECKSUM_RANGE].fill(0);
    let actual = CRC32.checksum(&header);
    if actual != expected {
        return Err(LogManagerError::FileHeaderChecksumMismatch { expected, actual });
    }

    let mut last_assigned_lsn_bytes = [0; 8];
    last_assigned_lsn_bytes.copy_from_slice(&header[16..24]);
    let last_assigned_lsn = u64::from_le_bytes(last_assigned_lsn_bytes);
    let mut max_txn_id_bytes = [0; 8];
    max_txn_id_bytes.copy_from_slice(&header[24..32]);
    let max_txn_id = u64::from_le_bytes(max_txn_id_bytes);
    Ok(WalFileHeader { last_assigned_lsn, max_txn_id })
}

fn write_wal_file_header(
    wal_file: &mut File,
    last_assigned_lsn: Lsn,
    max_txn_id: TxnId,
) -> Result<(), LogManagerError> {
    let mut header = [0; WAL_FILE_HEADER_LEN];
    header[0..8].copy_from_slice(&WAL_FILE_HEADER_MAGIC);
    header[8..10].copy_from_slice(&WAL_FILE_HEADER_VERSION.to_le_bytes());
    header[16..24].copy_from_slice(&last_assigned_lsn.to_le_bytes());
    header[24..32].copy_from_slice(&max_txn_id.to_le_bytes());
    let checksum = CRC32.checksum(&header);
    header[WAL_FILE_HEADER_CHECKSUM_RANGE].copy_from_slice(&checksum.to_le_bytes());
    wal_file.seek(SeekFrom::Start(0))?;
    wal_file.write_all(&header)?;
    Ok(())
}

/// Append-only manager for the database write-ahead log.
///
/// `LogManager` tracks two positions: the highest LSN appended to the WAL file
/// and the highest LSN known to be durable after a successful flush. Page-cache
/// code uses that distinction to enforce the write-ahead rule before writing a
/// dirty page whose page header references a logged LSN.
#[derive(Debug)]
pub struct LogManager {
    fs: FileSystem,
    wal_path: PathBuf,
    wal_writer: ActiveWalWriter,

    highest_txn_id: TxnId,
    transaction_heads: HashMap<TxnId, Lsn>,
    highest_appended_lsn: Option<Lsn>,
    highest_durable_lsn: Option<Lsn>,
    poisoned: bool,
}

impl LogManager {
    pub(crate) fn flush_all(&mut self) -> Result<(), LogManagerFlushError> {
        self.flush_through(self.highest_appended_lsn.unwrap_or(ZERO_LSN))
    }

    /// Opens or creates the WAL file associated with `db_file_path`.
    ///
    /// Existing complete frames are scanned to reconstruct the highest appended
    /// LSN and highest transaction id. Opening does not mark existing bytes as
    /// durable in this process; callers must still use [`Self::flush_through`]
    /// before relying on a newly appended record.
    pub(crate) fn new(fs: FileSystem, db_file_path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_wal(fs, db_file_path.as_ref().with_added_extension("wal"))
    }

    fn open_wal(fs: FileSystem, wal_path: PathBuf) -> std::io::Result<Self> {
        let wal_was_created = !fs.exists(&wal_path)?;
        let mut wal_file = fs.open(&wal_path, OpenMode::Create)?;
        let wal_header = ensure_wal_file_header(&mut wal_file).map_err(wal_open_error)?;
        if wal_was_created {
            fs.sync_parent(&wal_path)?;
        }

        let mut highest_appended_lsn = if wal_header.last_assigned_lsn == ZERO_LSN {
            None
        } else {
            Some(wal_header.last_assigned_lsn)
        };
        let mut highest_txn_id = wal_header.max_txn_id;
        let mut transaction_heads = HashMap::new();
        wal_file.seek(SeekFrom::Start(WAL_FILE_HEADER_LEN as u64))?;
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
                    transaction_heads.insert(frame.txn_id, next_lsn);
                }
            }
        }
        wal_file.seek(SeekFrom::End(0))?;

        let wal_writer = make_wal_writer(wal_file);

        Ok(Self {
            fs,
            wal_path,
            wal_writer,
            highest_txn_id,
            transaction_heads,
            highest_durable_lsn: None,
            highest_appended_lsn,
            poisoned: false,
        })
    }

    /// Last appended record of a transaction, or zero for a new transaction.
    pub(crate) fn transaction_head(&self, txn_id: TxnId) -> Lsn {
        self.transaction_heads.get(&txn_id).copied().unwrap_or(ZERO_LSN)
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
    pub(crate) fn append_record(
        &mut self,
        txn_id: TxnId,
        kind: LogRecordKind<'_>,
    ) -> Result<Lsn, LogManagerError> {
        if self.poisoned {
            return Err(LogManagerError::Poisoned);
        }
        let lsn = self.next_lsn()?;
        let prev_lsn = self.transaction_head(txn_id);
        let completed = matches!(kind, LogRecordKind::Commit | LogRecordKind::Rollback);
        let record = LogRecord { txn_id, prev_lsn, kind };
        if let Err(error) = serialize_transaction(&mut self.wal_writer, txn_id, &[record]) {
            // Serialization validates and sizes the complete transaction before emitting bytes,
            // so only an I/O error can leave a partial frame in the WAL stream.
            if matches!(&error, LogManagerError::Io(_)) {
                self.poisoned = true;
            }
            return Err(error);
        }

        self.highest_txn_id = self.highest_txn_id.max(txn_id);
        self.highest_appended_lsn = Some(lsn);
        if completed {
            self.transaction_heads.remove(&txn_id);
        } else {
            self.transaction_heads.insert(txn_id, lsn);
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
        if self.poisoned {
            return Err(LogManagerFlushError::Poisoned);
        }
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
        self.wal_writer.sync_all()?;
        self.highest_durable_lsn = Some(highest_appended_lsn);
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum LogReadMode {
    Recovery,
    Checkpoint,
}

/// Reads complete WAL frames and returns owned records for crash recovery.
///
/// The first torn or incomplete frame and all following bytes are treated as
/// a non-durable suffix, truncated from the WAL and reported through
/// [`RecoveryLogScan::truncated_tail`]. Later unsynced sectors can survive a
/// crash even when an earlier frame did not. Successfully synced bytes are
/// assumed intact. File-header errors, unsupported nonzero frame versions, and
/// I/O or allocation failures propagate without discarding the suffix.
pub fn read_recovery_log(
    fs: &FileSystem,
    db_file_path: impl AsRef<Path>,
) -> Result<RecoveryLogScan, LogManagerError> {
    read_log(fs, &db_file_path.as_ref().with_added_extension("wal"), LogReadMode::Recovery)
}

fn read_log(
    fs: &FileSystem,
    wal_path: &Path,
    mode: LogReadMode,
) -> Result<RecoveryLogScan, LogManagerError> {
    let wal_was_created = !fs.exists(wal_path)?;
    let mut wal_file = fs.open(wal_path, OpenMode::Create)?;
    let wal_header = ensure_wal_file_header(&mut wal_file)?;
    if wal_was_created {
        fs.sync_parent(wal_path)?;
    }
    let wal_len = wal_file.len()?;
    wal_file.seek(SeekFrom::Start(WAL_FILE_HEADER_LEN as u64))?;

    let mut offset = WAL_FILE_HEADER_LEN as u64;
    let mut max_txn_id = wal_header.max_txn_id;
    let mut truncated_tail = false;
    let mut next_lsn = wal_header.last_assigned_lsn;
    let mut records = Vec::new();

    while offset < wal_len {
        if wal_len - offset < HEADER_LEN as u64 {
            truncated_tail = true;
            break;
        }

        let mut frame_header = [0; HEADER_LEN];
        wal_file.read_exact(&mut frame_header)?;
        let header_and_len = WalFrameHeader::parse(&frame_header).and_then(|header| {
            let len = header.frame_len()?;
            Ok((header, len))
        });
        let (header, frame_len) = match header_and_len {
            Ok(header_and_len) => header_and_len,
            Err(LogManagerError::TruncatedFrame { .. })
            | Err(LogManagerError::InvalidHeaderMagic { .. }) => {
                truncated_tail = true;
                break;
            }
            Err(LogManagerError::UnsupportedVersion { actual: 0, .. })
            | Err(LogManagerError::PayloadLengthTooLarge { .. })
            | Err(LogManagerError::PayloadLengthOverflow)
                if mode == LogReadMode::Recovery =>
            {
                // Incomplete persistence can leave zero version bytes or a
                // malformed payload length; neither makes a valid frame.
                truncated_tail = true;
                break;
            }
            Err(error) => return Err(error),
        };

        // Check against the remaining bytes before adding, so a malformed
        // length cannot overflow the absolute frame-end offset.
        if frame_len as u64 > wal_len - offset {
            truncated_tail = true;
            break;
        }
        let frame_end = offset + frame_len as u64;

        // Read the whole frame body first: filesystem errors must propagate
        // rather than being classified as an invalid frame during recovery.
        let body_len = frame_len - HEADER_LEN;
        let mut body = Vec::new();
        body.try_reserve_exact(body_len).map_err(|source| {
            LogManagerError::FrameAllocationFailed { byte_count: body_len, source }
        })?;
        body.resize(body_len, 0);
        wal_file.read_exact(&mut body)?;

        let kinds = match header.decode_records(&body) {
            Ok(kinds) => kinds,
            Err(_) if mode == LogReadMode::Recovery || frame_end == wal_len => {
                // A crash can retain later unsynced sectors after a torn frame.
                // Nothing after the first invalid frame belongs to the valid
                // WAL prefix, even when it resembles complete frames. Synced
                // bytes are assumed intact; this is not media-bitrot repair.
                truncated_tail = true;
                break;
            }
            Err(error) => return Err(error),
        };
        max_txn_id = max_txn_id.max(header.txn_id);
        for (prev_lsn, kind) in kinds {
            next_lsn = next_lsn.checked_add(1).ok_or(LogManagerError::LsnExhausted)?;
            records.push(RecoveryLogRecord {
                lsn: next_lsn,
                txn_id: header.txn_id,
                prev_lsn,
                kind,
            });
        }
        offset = frame_end;
    }

    if truncated_tail {
        if mode == LogReadMode::Checkpoint {
            return Err(LogManagerError::TruncatedFrame { needed: 1, remaining: 0 });
        }
        wal_file.set_len(offset)?;
        wal_file.sync_all()?;
    }

    Ok(RecoveryLogScan { records, truncated_tail, max_txn_id, last_assigned_lsn: next_lsn })
}

impl RecoveryLogRecordKind {
    fn as_log_record_kind(&self) -> LogRecordKind<'_> {
        match self {
            Self::Begin => LogRecordKind::Begin,
            Self::Commit => LogRecordKind::Commit,
            Self::Rollback => LogRecordKind::Rollback,
            Self::PageAlloc { page_id } => LogRecordKind::PageAlloc { page_id: *page_id },
            Self::PageDealloc { page_id } => LogRecordKind::PageDealloc { page_id: *page_id },
            Self::PageCompensation { page_id, redo_data, undo_next_lsn } => {
                LogRecordKind::PageCompensation {
                    page_id: *page_id,
                    redo_data: redo_data.as_ref(),
                    undo_next_lsn: *undo_next_lsn,
                }
            }
            Self::AllocationCompensation { page_id, allocated, undo_next_lsn } => {
                LogRecordKind::AllocationCompensation {
                    page_id: *page_id,
                    allocated: *allocated,
                    undo_next_lsn: *undo_next_lsn,
                }
            }
            Self::PageUpdate { page_id, redo_data, undo_data } => LogRecordKind::PageUpdate {
                page_id: *page_id,
                redo_data: redo_data.as_ref(),
                undo_data: undo_data.as_ref(),
            },
        }
    }
}
