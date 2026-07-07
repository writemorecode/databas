use std::{collections::HashMap, path::Path};

use crate::core::{PAGE_SIZE, PageId, error::StorageResult};
use crate::storage::{
    disk_manager::DiskManager,
    log_manager::{Lsn, RecoveryLogRecordKind, TxnId, ZERO_LSN, read_recovery_log, truncate_wal},
    transaction_manager::page_lsn,
};

#[derive(Debug, Default)]
struct TransactionRecovery {
    page_allocs: Vec<PageId>,
    updates: Vec<RecoveryPageUpdate>,
    committed: bool,
    completed: bool,
}

#[derive(Debug, Clone)]
struct RecoveryPageUpdate {
    lsn: Lsn,
    page_id: PageId,
    redo_data: Box<[u8; PAGE_SIZE]>,
    undo_data: Box<[u8; PAGE_SIZE]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryResult {
    pub(crate) max_txn_id: TxnId,
}

pub(crate) fn recover_from_wal(
    path: &Path,
    disk: &mut DiskManager,
) -> StorageResult<RecoveryResult> {
    let scan = read_recovery_log(path)?;
    let max_txn_id = scan.max_txn_id;
    if scan.records.is_empty() {
        if max_txn_id > 0 || scan.truncated_tail {
            truncate_wal(path, scan.last_assigned_lsn)?;
        }
        return Ok(RecoveryResult { max_txn_id });
    }
    let last_assigned_lsn = scan.last_assigned_lsn;

    let mut transactions: HashMap<TxnId, TransactionRecovery> = HashMap::new();
    let mut committed_page_allocs = Vec::new();
    let mut committed_updates = Vec::new();

    for record in scan.records {
        let transaction = transactions.entry(record.txn_id).or_default();
        match record.kind {
            RecoveryLogRecordKind::Begin => {}
            RecoveryLogRecordKind::Commit => {
                transaction.committed = true;
                transaction.completed = true;
                committed_page_allocs.extend(transaction.page_allocs.iter().copied());
                committed_updates.extend(transaction.updates.iter().cloned());
            }
            RecoveryLogRecordKind::Rollback => {
                transaction.completed = true;
            }
            RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                transaction.updates.push(RecoveryPageUpdate {
                    lsn: record.lsn,
                    page_id,
                    redo_data,
                    undo_data,
                });
            }
            RecoveryLogRecordKind::PageAlloc { page_id } => {
                transaction.page_allocs.push(page_id);
            }
        }
    }

    committed_page_allocs.sort_unstable();
    committed_page_allocs.dedup();
    for page_id in committed_page_allocs {
        disk.ensure_page_exists(page_id)?;
    }

    committed_updates.sort_by_key(|update| update.lsn);
    for update in &committed_updates {
        redo_update(disk, update)?;
    }

    let mut loser_updates = Vec::new();
    for transaction in transactions.values() {
        if transaction.committed || transaction.completed {
            continue;
        }
        loser_updates.extend(transaction.updates.iter().cloned());
    }

    loser_updates.sort_by_key(|update| std::cmp::Reverse(update.lsn));
    for update in &loser_updates {
        undo_update(disk, update)?;
    }

    disk.sync()?;
    truncate_wal(path, last_assigned_lsn)?;
    Ok(RecoveryResult { max_txn_id })
}

fn redo_update(disk: &mut DiskManager, update: &RecoveryPageUpdate) -> StorageResult<()> {
    disk.ensure_page_exists(update.page_id)?;
    let mut current = [0; PAGE_SIZE];
    disk.read_page(update.page_id, &mut current)?;
    if should_apply_redo(&current, update.lsn) {
        disk.write_page(update.page_id, update.redo_data.as_ref())?;
    }
    Ok(())
}

fn undo_update(disk: &mut DiskManager, update: &RecoveryPageUpdate) -> StorageResult<()> {
    if update.page_id >= disk.page_count() {
        return Ok(());
    }

    let mut current = [0; PAGE_SIZE];
    disk.read_page(update.page_id, &mut current)?;
    if should_apply_undo(&current, update.lsn) {
        disk.write_page(update.page_id, update.undo_data.as_ref())?;
    }
    Ok(())
}

fn should_apply_redo(page: &[u8; PAGE_SIZE], update_lsn: Lsn) -> bool {
    let current_lsn = page_lsn(page);
    current_lsn == ZERO_LSN || current_lsn < update_lsn
}

fn should_apply_undo(page: &[u8; PAGE_SIZE], update_lsn: Lsn) -> bool {
    let current_lsn = page_lsn(page);
    current_lsn == ZERO_LSN || current_lsn == update_lsn
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
    };

    use tempfile::NamedTempFile;

    use super::*;
    use crate::storage::{
        log_manager::{LogManager, LogRecord, LogRecordKind},
        page,
        page::format::PageKind,
        storage_runtime::StorageRuntime,
    };

    const WAL_FILE_HEADER_LEN: u64 = 24;

    fn formatted_page(seed: u8, lsn: Lsn) -> [u8; PAGE_SIZE] {
        let mut page = [seed; PAGE_SIZE];
        page[page::format::KIND_OFFSET] = PageKind::RawLeaf as u8;
        page[page::format::VERSION_OFFSET] = page::format::FORMAT_VERSION;
        page::format::write_u16(&mut page, page::format::SLOT_COUNT_OFFSET, 0);
        page::format::write_u16(
            &mut page,
            page::format::CONTENT_START_OFFSET,
            page::format::USABLE_SPACE_END as u16,
        );
        page::format::write_u64(&mut page, page::format::LSN_OFFSET, lsn);
        page
    }

    fn append_transaction(path: &Path, txn_id: TxnId, records: &[LogRecord<'_>]) {
        let mut log = LogManager::new(path).unwrap();
        log.append_transaction(txn_id, records).unwrap();
    }

    fn read_disk_page(path: &Path, page_id: PageId) -> [u8; PAGE_SIZE] {
        let mut disk = DiskManager::new(path).unwrap();
        let mut page = [0; PAGE_SIZE];
        disk.read_page(page_id, &mut page).unwrap();
        page
    }

    fn wal_len(path: &Path) -> u64 {
        std::fs::metadata(path.with_added_extension("wal")).unwrap().len()
    }

    #[test]
    fn recovery_redoes_committed_update_not_flushed_to_database_file() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let after = formatted_page(2, 2);
        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &before).unwrap();
        }
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Commit },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), after);
    }

    #[test]
    fn recovery_skips_committed_update_when_page_lsn_is_current() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let current = formatted_page(2, 2);
        let stale_redo = formatted_page(3, 2);
        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &current).unwrap();
        }
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &stale_redo,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Commit },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), current);
    }

    #[test]
    fn recovery_undoes_uncommitted_update_that_reached_database_file() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let after = formatted_page(2, 2);
        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &after).unwrap();
        }
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), before);
    }

    #[test]
    fn recovery_does_not_undo_explicitly_rolled_back_transaction() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let after = formatted_page(2, 2);
        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &after).unwrap();
        }
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Rollback },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), after);
    }

    #[test]
    fn recovery_extends_database_for_committed_allocated_page_update() {
        let file = NamedTempFile::new().unwrap();
        let before = [0; PAGE_SIZE];
        let after = formatted_page(8, 3);
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord { txn_id: 1, kind: LogRecordKind::PageAlloc { page_id: 3 } },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 3,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Commit },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(disk.page_count(), 4);
        assert_eq!(read_disk_page(file.path(), 3), after);
    }

    #[test]
    fn recovery_truncates_wal_after_success_and_preserves_next_transaction_id() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let after = formatted_page(2, 2);
        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &before).unwrap();
        }
        append_transaction(
            file.path(),
            41,
            &[
                LogRecord { txn_id: 41, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 41,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 41, kind: LogRecordKind::Commit },
            ],
        );

        let runtime =
            StorageRuntime::new(file.path().to_path_buf(), DiskManager::new(file.path()).unwrap())
                .unwrap();

        assert_eq!(read_disk_page(file.path(), 0), after);
        assert_eq!(wal_len(file.path()), WAL_FILE_HEADER_LEN);
        assert_eq!(runtime.begin_transaction().unwrap(), 42);
    }

    #[test]
    fn recovery_redoes_update_after_wal_truncation_despite_stale_page_lsn() {
        let file = NamedTempFile::new().unwrap();
        let initial = formatted_page(1, ZERO_LSN);
        let first_update = formatted_page(2, 2);
        let second_update = formatted_page(3, 3);
        let old_wal_update = formatted_page(4, 4);
        let new_wal_update = formatted_page(5, 2);

        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &initial).unwrap();
        }

        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &first_update,
                        undo_data: &initial,
                    },
                },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &second_update,
                        undo_data: &first_update,
                    },
                },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &old_wal_update,
                        undo_data: &second_update,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Commit },
            ],
        );

        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            recover_from_wal(file.path(), &mut disk).unwrap();
        }

        assert_eq!(read_disk_page(file.path(), 0), old_wal_update);
        assert_eq!(wal_len(file.path()), WAL_FILE_HEADER_LEN);

        append_transaction(
            file.path(),
            2,
            &[
                LogRecord { txn_id: 2, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 2,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &new_wal_update,
                        undo_data: &old_wal_update,
                    },
                },
                LogRecord { txn_id: 2, kind: LogRecordKind::Commit },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), new_wal_update);
    }

    #[test]
    fn recovery_does_not_truncate_wal_when_scan_fails() {
        let file = NamedTempFile::new().unwrap();
        let before = formatted_page(1, ZERO_LSN);
        let after = formatted_page(2, 2);
        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &after,
                        undo_data: &before,
                    },
                },
                LogRecord { txn_id: 1, kind: LogRecordKind::Commit },
            ],
        );
        let wal_file_path = file.path().with_added_extension("wal");
        let original_len = wal_len(file.path());
        {
            let mut wal_file =
                OpenOptions::new().read(true).write(true).open(&wal_file_path).unwrap();
            let mut bytes = Vec::new();
            wal_file.read_to_end(&mut bytes).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            wal_file.seek(SeekFrom::Start(0)).unwrap();
            wal_file.write_all(&bytes).unwrap();
            wal_file.sync_all().unwrap();
        }

        let mut disk = DiskManager::new(file.path()).unwrap();
        assert!(recover_from_wal(file.path(), &mut disk).is_err());

        assert_eq!(wal_len(file.path()), original_len);
    }

    #[test]
    fn recovery_does_not_overwrite_newer_committed_update_with_older_loser_undo() {
        let file = NamedTempFile::new().unwrap();
        let initial = formatted_page(1, ZERO_LSN);
        let update1 = formatted_page(2, 2); // LSN 2
        let update2 = formatted_page(3, 5); // LSN 5

        {
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(0).unwrap();
            disk.write_page(0, &update2).unwrap();
        }

        append_transaction(
            file.path(),
            1,
            &[
                LogRecord { txn_id: 1, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 1,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &update1,
                        undo_data: &initial,
                    },
                },
            ],
        );

        append_transaction(
            file.path(),
            2,
            &[
                LogRecord { txn_id: 2, kind: LogRecordKind::Begin },
                LogRecord {
                    txn_id: 2,
                    kind: LogRecordKind::PageUpdate {
                        page_id: 0,
                        redo_data: &update2,
                        undo_data: &update1,
                    },
                },
                LogRecord { txn_id: 2, kind: LogRecordKind::Commit },
            ],
        );

        let mut disk = DiskManager::new(file.path()).unwrap();
        recover_from_wal(file.path(), &mut disk).unwrap();

        assert_eq!(read_disk_page(file.path(), 0), update2);
    }
}
