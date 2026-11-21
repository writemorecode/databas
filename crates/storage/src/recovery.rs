use std::{collections::HashMap, path::Path};

use crate::disk_manager::DiskManager;
use crate::error::StorageResult;
use crate::filesystem::FileSystem;
use crate::log_manager::{
    LogManager, Lsn, RecoveryLogRecordKind, TxnId, read_recovery_log, retire_empty_wal,
};
use crate::page_allocator::PageAllocator;
use crate::{PAGE_SIZE, PageId};

#[derive(Debug, Default)]
struct TransactionRecovery {
    allocations: Vec<(Lsn, PageId, bool)>,
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

pub fn recover_from_wal(
    fs: &FileSystem,
    path: &Path,
    disk: &mut DiskManager,
) -> StorageResult<RecoveryResult> {
    let scan = read_recovery_log(fs, path)?;
    let max_txn_id = scan.max_txn_id;
    if scan.records.is_empty() {
        PageAllocator::load(disk)?.validate_bounds(disk.page_count())?;
        if max_txn_id > 0 || scan.truncated_tail {
            retire_empty_wal(fs, path, &scan)?;
        }
        return Ok(RecoveryResult { max_txn_id });
    }
    let mut bitmap_images = HashMap::new();
    let mut transactions: HashMap<TxnId, TransactionRecovery> = HashMap::new();
    let mut committed_allocations = Vec::new();
    let mut undone_allocations = Vec::new();
    let mut committed_updates = Vec::new();

    for record in scan.records {
        let transaction = transactions.entry(record.txn_id);
        match record.kind {
            RecoveryLogRecordKind::Begin => {}
            RecoveryLogRecordKind::Commit => {
                let transaction = transaction.or_default();
                transaction.committed = true;
                transaction.completed = true;
            }
            RecoveryLogRecordKind::Rollback => {
                transaction.or_default().completed = true;
            }
            RecoveryLogRecordKind::PageUpdate { page_id, redo_data, undo_data } => {
                transaction.or_default().updates.push(RecoveryPageUpdate {
                    lsn: record.lsn,
                    page_id,
                    redo_data,
                    undo_data,
                });
            }
            RecoveryLogRecordKind::PageAlloc { page_id } => {
                transaction.or_default().allocations.push((record.lsn, page_id, true));
            }
            RecoveryLogRecordKind::PageDealloc { page_id } => {
                transaction.or_default().allocations.push((record.lsn, page_id, false));
            }
            RecoveryLogRecordKind::BitmapImage { page_id, image } => {
                bitmap_images.insert(page_id, image);
            }
        }
    }

    let mut loser_updates = Vec::new();
    for transaction in transactions.into_values() {
        if transaction.committed {
            committed_allocations.extend(transaction.allocations);
            committed_updates.extend(transaction.updates);
        } else {
            if !transaction.completed {
                loser_updates.extend(transaction.updates);
            }
            // Completed rollback also has allocation undo work: the outcome
            // can be durable before its bitmap changes have reached disk.
            let mut originals = HashMap::new();
            for (lsn, id, allocated) in transaction.allocations {
                originals.entry(id).or_insert((lsn, !allocated));
            }
            undone_allocations
                .extend(originals.into_iter().map(|(id, (lsn, allocated))| (lsn, id, allocated)));
        }
    }

    // Page images are replayed before publishing any free bits. Nothing can
    // allocate during recovery, and bitmap durability precedes WAL reclamation.
    let mut allocator = PageAllocator::load_with_images(disk, &bitmap_images)?;
    committed_allocations.sort_by_key(|&(lsn, _, _)| lsn);
    let mut latest_allocation = HashMap::new();
    for &(lsn, page_id, allocated) in &committed_allocations {
        if PageAllocator::is_metadata(page_id) {
            return Err(crate::error::StorageError::Corruption(crate::error::CorruptionError {
                component: crate::error::CorruptionComponent::DatabaseFile,
                page_id: Some(page_id),
                kind: crate::error::CorruptionKind::InvalidAllocationBitmap,
            }));
        }
        if allocated {
            allocator.ensure_page(disk, page_id)?;
        } else if page_id >= disk.page_count() {
            return Err(crate::error::StorageError::Corruption(crate::error::CorruptionError {
                component: crate::error::CorruptionComponent::DatabaseFile,
                page_id: Some(page_id),
                kind: crate::error::CorruptionKind::InvalidAllocationBitmap,
            }));
        }
        allocator.set_allocated(page_id, allocated);
        latest_allocation.insert(page_id, lsn);
    }
    // Keep replay order stable even when transaction and page lookup maps
    // use independently randomized hashers.
    undone_allocations.sort_unstable_by_key(|&(lsn, _, _)| lsn);
    for (lsn, page_id, allocated) in undone_allocations {
        if !latest_allocation.get(&page_id).is_some_and(|later| *later > lsn)
            && allocator.contains_page(page_id)
            && !PageAllocator::is_metadata(page_id)
        {
            allocator.set_allocated(page_id, allocated);
        }
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

    allocator.validate_bounds(disk.page_count())?;
    // Recovery can also be interrupted while writing the repaired bitmaps.
    let mut log = LogManager::new(fs.clone(), path)?;
    allocator.flush(disk, &mut log)?;
    disk.sync()?;
    log.retire_after_recovery()?;
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
