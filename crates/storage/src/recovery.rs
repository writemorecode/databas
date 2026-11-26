//! Repair the database from the valid WAL prefix before opening it to transactions.
//!
//! Analyze transaction chains, repeat physical history, then undo incomplete
//! transactions with redo-only compensation (`recovery_protocol`, R1). Redo
//! remains image-based until all pages have the required recovery metadata.

use std::path::Path;

use crate::disk_manager::DiskManager;
use crate::error::StorageResult;
use crate::filesystem::FileSystem;
use crate::log_manager::{LogManager, TxnId, read_recovery_log, retire_empty_wal};
use crate::page_allocator::PageAllocator;

mod analysis;
mod replay;

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

    let analysis = analysis::Analysis::new(&scan.records)?;
    let mut log = LogManager::new(fs.clone(), path)?;
    // A valid prefix can include bytes that survived without an earlier sync.
    // Force them before depending on them for any protected home write.
    log.flush_all()?;
    let mut allocator = PageAllocator::load(disk)?;
    replay::redo(&scan.records, disk, &mut allocator)?;
    replay::undo(analysis, &mut log, disk, &mut allocator)?;
    allocator.validate_bounds(disk.page_count())?;
    allocator.flush(disk, &mut log)?;
    disk.sync()?;
    log.retire_after_recovery()?;
    Ok(RecoveryResult { max_txn_id })
}
