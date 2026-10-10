//! Crash-safe WAL prefix reclamation after covered database pages are synced.
use std::collections::HashSet;

use super::*;
use crate::{core::error::StorageResult, storage::page_allocator::replay_lifecycle};

pub(crate) struct CheckpointProgress {
    pub(crate) remaining_records: usize,
}

impl LogManager {
    /// Retain suffix records at their original LSNs without invoking recovery.
    /// Allocator history is folded into an authoritative replacement snapshot;
    /// online publication must not overwrite free pages still in the cache.
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
        let mut prefix_len = scan.records.partition_point(|record| record.lsn < retain_from);
        let has_allocator = matches!(
            scan.records.first().map(|record| &record.kind),
            Some(RecoveryLogRecordKind::FreelistCheckpoint { .. })
        );
        if has_allocator {
            // A membership-only snapshot cannot represent pending ownership or
            // cancellation targets. Fold only through the last boundary with
            // no open transactions, including completed transactions straddling
            // a deferred page's retention LSN.
            let mut active = HashSet::new();
            let mut safe_prefix_len = 0;
            for (index, record) in scan.records[..prefix_len].iter().enumerate() {
                match record.kind {
                    RecoveryLogRecordKind::Begin => {
                        active.insert(record.txn_id);
                    }
                    RecoveryLogRecordKind::Commit | RecoveryLogRecordKind::Rollback => {
                        active.remove(&record.txn_id);
                    }
                    _ => {}
                }
                if active.is_empty() {
                    safe_prefix_len = index + 1;
                }
            }
            prefix_len = safe_prefix_len;
        }
        if prefix_len == 0 || (has_allocator && prefix_len == 1) {
            return Ok(CheckpointProgress {
                remaining_records: scan.records.len() - usize::from(has_allocator),
            });
        }
        let allocator =
            if has_allocator { replay_lifecycle(&scan.records[..prefix_len])? } else { None };
        let last_reclaimed_lsn = scan.records[prefix_len - 1].lsn;
        // Reuse the last reclaimed LSN for the snapshot. Suffix LSNs (notably
        // LifecycleCancel targets) and the next assigned LSN stay unchanged.
        let base_lsn = last_reclaimed_lsn - u64::from(allocator.is_some());
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
            if let Some(allocator) = allocator {
                serialize_transaction(
                    &mut writer,
                    0,
                    &[LogRecord {
                        txn_id: 0,
                        kind: LogRecordKind::FreelistCheckpoint {
                            page_count: allocator.page_count,
                            pages: allocator.free.into_iter().collect(),
                        },
                    }],
                )?;
            }
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

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn allocator_baseline_preserves_suffix_lsns_and_cancellation_targets() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let mut log = LogManager::new(path).unwrap();
        log.append_record(
            0,
            LogRecordKind::FreelistCheckpoint { page_count: 6, pages: vec![4, 5] },
        )
        .unwrap();
        log.append_record(1, LogRecordKind::Begin).unwrap();
        log.append_record(1, LogRecordKind::PageReserve { page_id: 4 }).unwrap();
        let commit_lsn = log.append_record(1, LogRecordKind::Commit).unwrap();
        let begin_lsn = log.append_record(2, LogRecordKind::Begin).unwrap();
        let reserve_lsn = log.append_record(2, LogRecordKind::PageReserve { page_id: 5 }).unwrap();
        let retire_lsn = log.append_record(2, LogRecordKind::PageRetire { page_id: 5 }).unwrap();
        log.append_record(2, LogRecordKind::LifecycleCancel { target_lsn: retire_lsn }).unwrap();
        log.append_record(2, LogRecordKind::LifecycleCancel { target_lsn: reserve_lsn }).unwrap();
        log.append_record(2, LogRecordKind::PageReserve { page_id: 5 }).unwrap();
        log.append_record(2, LogRecordKind::Commit).unwrap();
        log.flush_all().unwrap();
        let original = read_checkpoint_log(path).unwrap();
        let next_lsn = log.next_lsn().unwrap();

        // Model a completed transaction straddling a deferred dirty page's LSN.
        let progress = log.reclaim_prefix(path, retire_lsn).unwrap();
        let retained = read_checkpoint_log(path).unwrap();
        assert_eq!(retained.records[0].lsn, commit_lsn);
        assert_eq!(retained.records[0].txn_id, 0);
        assert_eq!(
            retained.records[0].kind,
            RecoveryLogRecordKind::FreelistCheckpoint { page_count: 6, pages: vec![5] }
        );
        assert_eq!(&retained.records[1..], &original.records[4..]);
        assert_eq!(progress.remaining_records, original.records.len() - 4);
        assert_eq!(log.next_lsn().unwrap(), next_lsn);
        let allocator = replay_lifecycle(&retained.records).unwrap().unwrap();
        assert!(allocator.free.is_empty());
        assert_eq!(allocator.page_count, 6);

        // Repeated partial passes retain the same baseline and stable targets.
        log.reclaim_prefix(path, begin_lsn).unwrap();
        assert_eq!(read_checkpoint_log(path).unwrap(), retained);
        assert_eq!(log.reclaim_prefix(path, next_lsn).unwrap().remaining_records, 0);
        let completed = read_checkpoint_log(path).unwrap();
        assert_eq!(completed.records.len(), 1);
        assert!(replay_lifecycle(&completed.records).unwrap().unwrap().free.is_empty());
        assert_eq!(log.next_lsn().unwrap(), next_lsn);
    }

    #[test]
    fn allocator_prefix_does_not_fold_interleaved_pending_transactions() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let mut log = LogManager::new(path).unwrap();
        log.append_record(
            0,
            LogRecordKind::FreelistCheckpoint { page_count: 6, pages: vec![4, 5] },
        )
        .unwrap();
        log.append_record(1, LogRecordKind::Begin).unwrap();
        log.append_record(1, LogRecordKind::PageReserve { page_id: 4 }).unwrap();
        log.append_record(2, LogRecordKind::Begin).unwrap();
        log.append_record(2, LogRecordKind::PageReserve { page_id: 5 }).unwrap();
        let boundary = log.append_record(1, LogRecordKind::Commit).unwrap();
        log.flush_all().unwrap();
        let original = read_checkpoint_log(path).unwrap();
        log.reclaim_prefix(path, boundary).unwrap();
        assert_eq!(read_checkpoint_log(path).unwrap(), original);
        // Crash recovery frees only the loser's reservation.
        assert_eq!(
            replay_lifecycle(&original.records).unwrap().unwrap().free,
            [5].into_iter().collect()
        );
    }
}
