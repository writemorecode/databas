//! Crash-safe WAL replacement after the covered database pages are durable.

use std::{
    io::{BufWriter, Write},
    path::Path,
};

use super::{
    LogManager, LogManagerError, LogReadMode, LogRecord, Lsn, RecoveryLogRecord,
    WAL_WRITE_BUFFER_LEN, WalFileHeader, make_wal_writer, read_log, serialize_transaction,
    write_wal_file_header,
};
use crate::error::StorageResult;
use crate::filesystem::{File, FileSystem, OpenMode};

impl LogManager {
    /// Retain suffix records at their original LSNs without invoking recovery.
    pub(crate) fn reclaim_prefix(
        &mut self,
        retain_from: Lsn,
        redo_from: Lsn,
    ) -> StorageResult<usize> {
        self.flush_all()?;
        let (_, scan) = match read_log(&self.fs, &self.wal_path, LogReadMode::Checkpoint) {
            Ok(scan) => scan,
            Err(error) => {
                self.poisoned = true;
                return Err(error.into());
            }
        };
        let prefix_len = scan.records.partition_point(|record| record.lsn < retain_from);
        let redo_from = redo_from.max(self.redo_from);
        if prefix_len == 0 && redo_from == self.redo_from {
            return Ok(scan.records.len());
        }
        let suffix = &scan.records[prefix_len..];
        let header = WalFileHeader {
            last_assigned_lsn: suffix
                .first()
                .map_or(scan.last_assigned_lsn, |record| record.lsn - 1),
            max_txn_id: scan.max_txn_id,
            redo_from,
        };
        let file =
            publish_replacement(&self.fs, &self.wal_path, header, suffix, &mut self.poisoned)?;
        self.wal_writer = make_wal_writer(file);
        self.redo_from = redo_from;
        self.poisoned = false;
        Ok(suffix.len())
    }
}

fn publish_replacement(
    fs: &FileSystem,
    wal_path: &Path,
    header: WalFileHeader,
    suffix: &[RecoveryLogRecord],
    live_writer_poisoned: &mut bool,
) -> Result<File, LogManagerError> {
    let staged_path = wal_path.with_added_extension("checkpoint");
    let mut staged = fs.open(&staged_path, OpenMode::Truncate)?;
    write_wal_file_header(&mut staged, header)?;
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
    // even if publication reports an error.
    *live_writer_poisoned = true;
    fs.rename(&staged_path, wal_path)?;
    fs.sync_parent(wal_path)?;
    Ok(staged)
}
