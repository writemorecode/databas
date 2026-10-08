//! Crash-safe WAL prefix reclamation after covered database pages are synced.
use super::*;
use crate::core::error::StorageResult;

pub(crate) struct CheckpointProgress {
    pub(crate) remaining_records: usize,
}

impl LogManager {
    /// Retain suffix records at their original LSNs without invoking recovery.
    pub(crate) fn reclaim_prefix(
        &mut self,
        path: &Path,
        retain_from: Lsn,
    ) -> StorageResult<CheckpointProgress> {
        self.flush_all()?;
        let scan = match read_checkpoint_log(path) {
            Ok(scan) => scan,
            Err(error) => {
                self.poison();
                return Err(error.into());
            }
        };
        let prefix_len = scan.records.partition_point(|record| record.lsn < retain_from);
        if prefix_len == 0 {
            return Ok(CheckpointProgress { remaining_records: scan.records.len() });
        }
        let base_lsn = scan.records[prefix_len - 1].lsn;
        let wal_path = path.with_added_extension("wal");
        let staged_path = wal_path.with_added_extension("checkpoint");
        let mut staged = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&staged_path)?;
        write_wal_file_header(&mut staged, base_lsn, scan.max_txn_id)?;
        {
            let mut writer = BufWriter::with_capacity(WAL_WRITE_BUFFER_LEN, &mut staged);
            for record in &scan.records[prefix_len..] {
                serialize_transaction(
                    &mut writer,
                    record.txn_id,
                    &[LogRecord { txn_id: record.txn_id, kind: record.kind.as_log_record_kind() }],
                )?;
            }
            writer.flush()?;
        }
        staged.sync_all()?;
        self.poison();
        std::fs::rename(&staged_path, &wal_path)?;
        sync_parent_directory(&wal_path)?;
        *self = Self::new(path)?;
        Ok(CheckpointProgress { remaining_records: scan.records.len() - prefix_len })
    }
}
