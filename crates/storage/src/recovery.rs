use std::{collections::HashMap, path::Path};

use crate::disk_manager::DiskManager;
use crate::error::StorageResult;
use crate::log_manager::{Lsn, RecoveryLogRecordKind, TxnId, read_recovery_log, truncate_wal};
use crate::{PAGE_SIZE, PageId};

#[derive(Debug, Default)]
struct TransactionRecovery {
    page_allocs: Vec<PageId>,
    updates: Vec<RecoveryPageUpdate>,
    committed: bool,
    completed: bool,
}

#[derive(Debug)]
struct RecoveryPageUpdate {
    lsn: Lsn,
    page_id: PageId,
    redo_data: Box<[u8; PAGE_SIZE]>,
    undo_data: Box<[u8; PAGE_SIZE]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryResult {
    pub(crate) max_txn_id: TxnId,
}

pub fn recover_from_wal(path: &Path, disk: &mut DiskManager) -> StorageResult<RecoveryResult> {
    let scan = read_recovery_log(path)?;
    let max_txn_id = scan.max_txn_id;
    if scan.records.is_empty() {
        if max_txn_id > 0 || scan.truncated_tail {
            truncate_wal(path, scan.last_assigned_lsn, max_txn_id)?;
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

    let mut loser_updates = Vec::new();
    for transaction in transactions.into_values() {
        if transaction.committed {
            committed_page_allocs.extend(transaction.page_allocs);
            committed_updates.extend(transaction.updates);
        } else if !transaction.completed {
            loser_updates.extend(transaction.updates);
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
    let latest_committed_lsn_by_page: HashMap<PageId, Lsn> =
        committed_updates.iter().map(|update| (update.page_id, update.lsn)).collect();

    loser_updates.sort_by_key(|update| std::cmp::Reverse(update.lsn));
    for update in &loser_updates {
        let has_later_committed_update = latest_committed_lsn_by_page
            .get(&update.page_id)
            .is_some_and(|committed_lsn| *committed_lsn > update.lsn);
        if !has_later_committed_update {
            undo_update(disk, update)?;
        }
    }

    disk.sync()?;
    truncate_wal(path, last_assigned_lsn, max_txn_id)?;
    Ok(RecoveryResult { max_txn_id })
}

fn redo_update(disk: &mut DiskManager, update: &RecoveryPageUpdate) -> StorageResult<()> {
    disk.ensure_page_exists(update.page_id)?;
    disk.write_page(update.page_id, update.redo_data.as_ref())?;
    Ok(())
}

fn undo_update(disk: &mut DiskManager, update: &RecoveryPageUpdate) -> StorageResult<()> {
    if update.page_id >= disk.page_count() {
        return Ok(());
    }

    disk.write_page(update.page_id, update.undo_data.as_ref())?;
    Ok(())
}
