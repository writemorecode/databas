//! Repeat durable history, then hand reconstructed loser transactions to the
//! same cache-backed undo path used by live rollback. No cursor is exposed
//! until all losers are compensated and the recovered pages are durable.

use std::path::Path;

use crate::disk_manager::DiskManager;
use crate::error::StorageResult;
use crate::filesystem::FileSystem;
use crate::log_manager::{LogManager, read_recovery_log};
use crate::page_allocator::PageAllocator;
use crate::transaction_manager::TransactionManager;

mod analysis;
mod data;
mod replay;

pub struct Restart {
    pub log: LogManager,
    pub allocator: PageAllocator,
    pub transactions: TransactionManager,
    pub needs_checkpoint: bool,
}

pub fn repeat_history(
    fs: FileSystem,
    path: &Path,
    disk: &mut DiskManager,
) -> StorageResult<Restart> {
    let scan = read_recovery_log(&fs, path)?;
    let analysis = analysis::Analysis::new(&scan.records)?;
    let mut log = LogManager::new(fs, path)?;
    let mut transactions = TransactionManager::new(scan.max_txn_id.max(log.highest_txn_id()));
    let needs_checkpoint = !scan.records.is_empty();
    if needs_checkpoint {
        // The surviving prefix can contain records not yet forced by the last
        // process. Protect the home writes and any newly generated compensation.
        log.flush_all()?;
    }
    let mut allocator = PageAllocator::load(disk)?;
    if needs_checkpoint {
        let mut allocations = replay::redo(&scan.records, scan.redo_from, disk, &mut allocator)?;
        analysis.restore_losers(&mut transactions, |id| allocations.take_pending(id))?;
        log.retain_transaction_heads(|id| transactions.transaction_is_active(id));
    } else {
        allocator.validate_bounds(disk.page_count())?;
    }
    Ok(Restart { log, allocator, transactions, needs_checkpoint })
}
