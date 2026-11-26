//! Crash-safe WAL replacement after the covered database pages are durable.

use std::{
    io::{BufWriter, Write},
    path::Path,
};

use super::{
    LogManager, LogManagerError, LogReadMode, LogRecord, Lsn, RecoveryLogRecord, RecoveryLogScan,
    WAL_WRITE_BUFFER_LEN, WalFileHeader, ZERO_LSN, read_log, serialize_transaction,
    write_wal_file_header,
};
use crate::error::StorageResult;
use crate::filesystem::{FileSystem, OpenMode};

impl LogManager {
    /// Retain suffix records at their original LSNs without invoking recovery.
    pub(crate) fn reclaim_prefix(&mut self, retain_from: Lsn) -> StorageResult<usize> {
        self.flush_all()?;
        let scan = match read_log(&self.fs, &self.wal_path, LogReadMode::Checkpoint) {
            Ok(scan) => scan,
            Err(error) => {
                self.poisoned = true;
                return Err(error.into());
            }
        };
        let prefix_len = scan.records.partition_point(|record| record.lsn < retain_from);
        if prefix_len == 0 {
            return Ok(scan.records.len());
        }
        let header = WalFileHeader {
            last_assigned_lsn: scan.records[prefix_len - 1].lsn,
            max_txn_id: scan.max_txn_id,
        };
        let suffix = &scan.records[prefix_len..];
        publish_replacement(&self.fs, &self.wal_path, header, suffix, Some(&mut self.poisoned))?;
        let mut reopened = Self::open_wal(self.fs.clone(), self.wal_path.clone())?;
        reopened.transaction_heads.retain(|txn, _| self.transaction_heads.contains_key(txn));
        debug_assert_eq!(self.transaction_heads, reopened.transaction_heads);
        debug_assert_eq!(self.highest_appended_lsn, reopened.highest_appended_lsn);
        debug_assert_eq!(self.highest_txn_id, reopened.highest_txn_id);
        *self = reopened;
        Ok(suffix.len())
    }

    /// Discard replayed records, including compensation written during recovery.
    /// The caller must sync the database before retiring the WAL.
    pub(crate) fn retire_after_recovery(self) -> Result<(), LogManagerError> {
        let header = WalFileHeader {
            last_assigned_lsn: self.highest_appended_lsn.unwrap_or(ZERO_LSN),
            max_txn_id: self.highest_txn_id,
        };
        drop(self.wal_writer);
        publish_replacement(&self.fs, &self.wal_path, header, &[], None)
    }
}

/// Retire an already empty recovery log without opening an append writer.
pub(crate) fn retire_empty_wal(
    fs: &FileSystem,
    db_path: &Path,
    scan: &RecoveryLogScan,
) -> Result<(), LogManagerError> {
    debug_assert!(scan.records.is_empty());
    let header =
        WalFileHeader { last_assigned_lsn: scan.last_assigned_lsn, max_txn_id: scan.max_txn_id };
    publish_replacement(fs, &db_path.with_added_extension("wal"), header, &[], None)
}

fn publish_replacement(
    fs: &FileSystem,
    wal_path: &Path,
    header: WalFileHeader,
    suffix: &[RecoveryLogRecord],
    live_writer_poisoned: Option<&mut bool>,
) -> Result<(), LogManagerError> {
    let staged_path = wal_path.with_added_extension("checkpoint");
    let mut staged = fs.open(&staged_path, OpenMode::Truncate)?;
    write_wal_file_header(&mut staged, header.last_assigned_lsn, header.max_txn_id)?;
    if !suffix.is_empty() {
        let mut writer = BufWriter::with_capacity(WAL_WRITE_BUFFER_LEN, &mut staged);
        for record in suffix {
            serialize_transaction(
                &mut writer,
                record.txn_id,
                &[LogRecord {
                    txn_id: record.txn_id,
                    prev_lsn: record.prev_lsn,
                    kind: record.kind.as_log_record_kind(),
                }],
            )?;
        }
        writer.flush()?;
    }
    staged.sync_all()?;
    // Once replacement begins, a handle to the old file must not accept appends
    // even if publication or reopening reports an error.
    if let Some(poisoned) = live_writer_poisoned {
        *poisoned = true;
    }
    fs.rename(&staged_path, wal_path)?;
    fs.sync_parent(wal_path)?;
    Ok(())
}
