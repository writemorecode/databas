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
//! binds the segment to the database file's filesystem identity and stores the
//! preceding durable high-water LSN. Records in complete frames are assigned
//! logical LSNs after that value. Appending advances the highest appended LSN,
//! while [`LogManager::flush_through`] is
//! responsible for making appended bytes durable before pages protected by
//! those LSNs are written to the database file.

#![allow(clippy::redundant_pub_crate)]

use std::{
    collections::HashMap,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use thiserror::Error;

#[allow(clippy::redundant_pub_crate)] // Lsn is intentionally crate-private.
pub(crate) use crate::Lsn;
use crate::PageId;
pub use crate::TxnId;
use crate::filesystem::{File, FileSystem, OpenMode};
use crate::logical_undo::LogicalUndo;
use crate::page_operation::PageOperation;
use crate::page_patch::{InvalidPagePatch, PagePatch};
use crate::page_recovery::PageInitKind;

mod frame;
mod replacement;

use frame::{CRC32, HEADER_LEN, WalFrameHeader, serialize_transaction};

/// Sentinel LSN used for pages that do not reflect any logged update.
pub const ZERO_LSN: Lsn = 0;

const WAL_WRITE_BUFFER_LEN: usize = 64 * 1024;
const WAL_FILE_HEADER_MAGIC: [u8; 8] = *b"DBWALHDR";
const WAL_FILE_HEADER_VERSION: u16 = 9;
pub const WAL_FILE_HEADER_LEN: usize = 56;
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
    /// Page patches must be ordered, nonoverlapping runs within the page body.
    #[error(transparent)]
    InvalidPagePatch(#[from] InvalidPagePatch),
    /// A page operation has an unknown tag or invalid payload.
    #[error("invalid WAL page operation with tag {tag}")]
    InvalidPageOperation { tag: u8 },
    /// Invalid encoding of a tree/key inverse.
    #[error("invalid WAL logical undo payload")]
    InvalidLogicalUndo,
    /// The WAL file header failed its integrity checksum.
    #[error("WAL file header checksum mismatch: expected {expected}, got {actual}")]
    FileHeaderChecksumMismatch { expected: u32, actual: u32 },
    /// A valid WAL header identifies a different database filesystem object.
    #[error("WAL belongs to a different database file")]
    ForeignDatabase,
    /// A checkpoint's redo horizon requires an absent durable WAL prefix.
    #[error("invalid checkpoint redo horizon {redo_from} after LSN {last_lsn}")]
    InvalidRedoHorizon { redo_from: Lsn, last_lsn: Lsn },
    /// An initializer must specify a supported data-page kind.
    #[error("invalid WAL page initialization kind: {tag}")]
    InvalidPageInitKind { tag: u8 },
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
    /// Page-local redo and inverse byte runs within one page incarnation.
    PageUpdate {
        page_id: PageId,
        incarnation: Lsn,
        before_lsn: Lsn,
        redo: &'a PageOperation,
        undo: &'a PagePatch,
    },
    /// Reserve a page and initialize a new incarnation without logging an image.
    PageInit { page_id: PageId, kind: PageInitKind },
    /// Deallocation of a database page by a transaction.
    PageDealloc { page_id: PageId },
    /// Redo-only page compensation with the next LSN to undo.
    PageCompensation { page_id: PageId, incarnation: Lsn, redo: &'a PagePatch, undo_next_lsn: Lsn },
    /// Redo-only compensation of one allocation bit.
    AllocationCompensation { page_id: PageId, allocated: bool, undo_next_lsn: Lsn },
    /// A completed edit replaces its physical undo entries with a key inverse.
    LogicalEdit { undo: &'a LogicalUndo, undo_next_lsn: Lsn },
    /// End a nested top action or a logical inverse; undo skips its physical work.
    SkipUndo { undo_next_lsn: Lsn },
}

/// Owned WAL record returned by recovery scans.
///
/// Unlike [`LogRecord`], this form owns page operations and includes the LSN
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
    /// Page-local redo and inverse byte runs within one page incarnation.
    PageUpdate {
        page_id: PageId,
        incarnation: Lsn,
        before_lsn: Lsn,
        redo: PageOperation,
        undo: PagePatch,
    },
    /// Reserve a page and initialize a new incarnation without logging an image.
    PageInit { page_id: PageId, kind: PageInitKind },
    /// Deallocation of a database page by a transaction.
    PageDealloc { page_id: PageId },
    /// Redo-only page compensation with the next LSN to undo.
    PageCompensation { page_id: PageId, incarnation: Lsn, redo: PagePatch, undo_next_lsn: Lsn },
    /// Redo-only compensation of one allocation bit.
    AllocationCompensation { page_id: PageId, allocated: bool, undo_next_lsn: Lsn },
    /// A completed edit replaces its physical undo entries with a key inverse.
    LogicalEdit { undo: LogicalUndo, undo_next_lsn: Lsn },
    /// End a nested top action or a logical inverse; undo skips its physical work.
    SkipUndo { undo_next_lsn: Lsn },
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
    /// Earliest data-page redo that might not be durable at home.
    pub(crate) redo_from: Lsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalFileHeader {
    database_identity: [u8; 16],
    last_assigned_lsn: Lsn,
    max_txn_id: TxnId,
    redo_from: Lsn,
}

fn read_wal_file_header(
    wal_file: &mut File,
    database_identity: [u8; 16],
) -> Result<WalFileHeader, LogManagerError> {
    let mut header = [0; WAL_FILE_HEADER_LEN];
    wal_file.seek(SeekFrom::Start(0))?;
    // Read the stable prefix first: an older header can be shorter.
    wal_file.read_exact(&mut header[..10])?;

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

    wal_file.read_exact(&mut header[10..])?;
    let mut checksum_bytes = [0; 4];
    checksum_bytes.copy_from_slice(&header[WAL_FILE_HEADER_CHECKSUM_RANGE]);
    let expected = u32::from_le_bytes(checksum_bytes);
    header[WAL_FILE_HEADER_CHECKSUM_RANGE].fill(0);
    let actual = CRC32.checksum(&header);
    if actual != expected {
        return Err(LogManagerError::FileHeaderChecksumMismatch { expected, actual });
    }

    if header[40..] != database_identity {
        return Err(LogManagerError::ForeignDatabase);
    }

    let mut last_assigned_lsn_bytes = [0; 8];
    last_assigned_lsn_bytes.copy_from_slice(&header[16..24]);
    let last_assigned_lsn = u64::from_le_bytes(last_assigned_lsn_bytes);
    let mut max_txn_id_bytes = [0; 8];
    max_txn_id_bytes.copy_from_slice(&header[24..32]);
    let max_txn_id = u64::from_le_bytes(max_txn_id_bytes);
    let mut redo_from_bytes = [0; 8];
    redo_from_bytes.copy_from_slice(&header[32..40]);
    let redo_from = u64::from_le_bytes(redo_from_bytes);
    Ok(WalFileHeader { database_identity, last_assigned_lsn, max_txn_id, redo_from })
}

fn write_wal_file_header(wal_file: &mut File, wal: WalFileHeader) -> Result<(), LogManagerError> {
    let mut header = [0; WAL_FILE_HEADER_LEN];
    header[0..8].copy_from_slice(&WAL_FILE_HEADER_MAGIC);
    header[8..10].copy_from_slice(&WAL_FILE_HEADER_VERSION.to_le_bytes());
    header[16..24].copy_from_slice(&wal.last_assigned_lsn.to_le_bytes());
    header[24..32].copy_from_slice(&wal.max_txn_id.to_le_bytes());
    header[32..40].copy_from_slice(&wal.redo_from.to_le_bytes());
    header[40..].copy_from_slice(&wal.database_identity);
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
    database_identity: [u8; 16],

    transaction_heads: HashMap<TxnId, Lsn>,
    highest_appended_lsn: Option<Lsn>,
    highest_durable_lsn: Option<Lsn>,
    redo_from: Lsn,
    poisoned: bool,
}

impl LogManager {
    pub(crate) fn flush_all(&mut self) -> Result<(), LogManagerFlushError> {
        self.flush_through(self.highest_appended_lsn.unwrap_or(ZERO_LSN))
    }

    /// Validate the WAL and position its writer after the surviving records.
    pub(crate) fn open_for_recovery(
        fs: FileSystem,
        db_file_path: &Path,
        database_identity: [u8; 16],
    ) -> Result<(Self, RecoveryLogScan), LogManagerError> {
        let wal_path = db_file_path.with_added_extension("wal");
        let (file, scan) = read_log(&fs, &wal_path, database_identity, LogReadMode::Recovery)?;
        let log = Self {
            fs,
            wal_path,
            wal_writer: make_wal_writer(file),
            database_identity,
            transaction_heads: scan
                .records
                .iter()
                .map(|record| (record.txn_id, record.lsn))
                .collect(),
            highest_durable_lsn: None,
            redo_from: scan.redo_from,
            highest_appended_lsn: (scan.last_assigned_lsn != ZERO_LSN)
                .then_some(scan.last_assigned_lsn),
            poisoned: false,
        };
        Ok((log, scan))
    }

    /// Last appended record of a transaction, or zero for a new transaction.
    pub(crate) fn transaction_head(&self, txn_id: TxnId) -> Lsn {
        self.transaction_heads.get(&txn_id).copied().unwrap_or(ZERO_LSN)
    }

    pub(crate) fn retain_transaction_heads(&mut self, mut active: impl FnMut(TxnId) -> bool) {
        self.transaction_heads.retain(|txn_id, _| active(*txn_id));
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
#[cfg(feature = "simulation")]
pub fn read_recovery_log(
    fs: &FileSystem,
    db_file_path: &Path,
    database_identity: [u8; 16],
) -> Result<RecoveryLogScan, LogManagerError> {
    let (_, scan) = read_log(
        fs,
        &db_file_path.with_added_extension("wal"),
        database_identity,
        LogReadMode::Recovery,
    )?;
    Ok(scan)
}

fn read_log(
    fs: &FileSystem,
    wal_path: &Path,
    database_identity: [u8; 16],
    mode: LogReadMode,
) -> Result<(File, RecoveryLogScan), LogManagerError> {
    let (mut wal_file, wal_header) = match fs.open(wal_path, OpenMode::Existing) {
        Ok(mut file) => {
            let header = read_wal_file_header(&mut file, database_identity)?;
            (file, header)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && mode == LogReadMode::Recovery =>
        {
            // Initial bootstrap: initialize only a new file. An existing but
            // incomplete header cannot prove identity or supply a high-water LSN.
            let mut file = fs.open(wal_path, OpenMode::CreateNew)?;
            let header = WalFileHeader {
                database_identity,
                last_assigned_lsn: 0,
                max_txn_id: 0,
                redo_from: 0,
            };
            write_wal_file_header(&mut file, header)?;
            file.sync_all()?;
            (file, header)
        }
        Err(error) => return Err(error.into()),
    };
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

    if wal_header.redo_from > next_lsn.saturating_add(1) {
        return Err(LogManagerError::InvalidRedoHorizon {
            redo_from: wal_header.redo_from,
            last_lsn: next_lsn,
        });
    }
    // Only publish the namespace once both the file and frame versions pass.
    // A previous replacement may have renamed the WAL but failed directory sync;
    // restart writes depending on that WAL must wait for its name to be durable.
    if mode == LogReadMode::Recovery {
        fs.sync_parent(wal_path)?;
    }
    if truncated_tail {
        if mode == LogReadMode::Checkpoint {
            return Err(LogManagerError::TruncatedFrame { needed: 1, remaining: 0 });
        }
        wal_file.set_len(offset)?;
        wal_file.sync_all()?;
        wal_file.seek(SeekFrom::Start(offset))?;
    }

    let scan = RecoveryLogScan {
        records,
        truncated_tail,
        max_txn_id,
        last_assigned_lsn: next_lsn,
        redo_from: wal_header.redo_from,
    };
    Ok((wal_file, scan))
}

impl RecoveryLogRecordKind {
    fn as_log_record_kind(&self) -> LogRecordKind<'_> {
        match self {
            Self::Begin => LogRecordKind::Begin,
            Self::Commit => LogRecordKind::Commit,
            Self::Rollback => LogRecordKind::Rollback,
            Self::PageInit { page_id, kind } => {
                LogRecordKind::PageInit { page_id: *page_id, kind: *kind }
            }
            Self::PageDealloc { page_id } => LogRecordKind::PageDealloc { page_id: *page_id },
            Self::PageCompensation { page_id, incarnation, redo, undo_next_lsn } => {
                LogRecordKind::PageCompensation {
                    page_id: *page_id,
                    incarnation: *incarnation,
                    redo,
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
            Self::LogicalEdit { undo, undo_next_lsn } => {
                LogRecordKind::LogicalEdit { undo, undo_next_lsn: *undo_next_lsn }
            }
            Self::SkipUndo { undo_next_lsn } => {
                LogRecordKind::SkipUndo { undo_next_lsn: *undo_next_lsn }
            }
            Self::PageUpdate { page_id, incarnation, before_lsn, redo, undo } => {
                LogRecordKind::PageUpdate {
                    page_id: *page_id,
                    incarnation: *incarnation,
                    before_lsn: *before_lsn,
                    redo,
                    undo,
                }
            }
        }
    }
}
