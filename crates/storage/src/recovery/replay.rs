//! Repeat physical history and compensate losers before handing pages to the cache.

use std::collections::{BTreeMap, HashMap};

use crate::disk_manager::DiskManager;
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::log_manager::{
    LogManager, LogManagerError, LogRecordKind, RecoveryLogRecord, RecoveryLogRecordKind as Kind,
};
use crate::page_allocator::PageAllocator;
use crate::page_recovery::PageRecovery;
use crate::{Lsn, PAGE_SIZE, PageId, TxnId};

use super::analysis::{Analysis, undo_next};

/// Allocations reserve bits at PageInit. Frees and allocation CLRs change only
/// the pending state until Commit/Rollback, exactly as during normal execution.
/// Applying them earlier would make a persisted bitmap LSN unsafe for redo.
#[derive(Default)]
pub(super) struct AllocationReplay {
    pending: HashMap<TxnId, BTreeMap<PageId, bool>>,
}

impl AllocationReplay {
    fn request(&mut self, txn_id: TxnId, page_id: PageId, allocated: bool) {
        self.pending.entry(txn_id).or_default().insert(page_id, allocated);
    }

    fn publish(
        &mut self,
        disk: &mut DiskManager,
        allocator: &mut PageAllocator,
        txn_id: TxnId,
        lsn: Lsn,
    ) -> StorageResult<()> {
        if let Some(changes) = self.pending.remove(&txn_id) {
            for (page_id, allocated) in changes {
                set_allocation(disk, allocator, page_id, allocated, lsn)?;
            }
        }
        Ok(())
    }
}

/// Apply retained effects in LSN order, including losers and CLRs.
/// Data-page incarnations are persisted, but redo still installs each update.
pub(super) fn redo(
    records: &[RecoveryLogRecord],
    disk: &mut DiskManager,
    allocator: &mut PageAllocator,
) -> StorageResult<AllocationReplay> {
    let mut allocations = AllocationReplay::default();
    for record in records {
        match &record.kind {
            Kind::PageUpdate { page_id, redo_data, .. }
            | Kind::PageCompensation { page_id, redo_data, .. } => {
                write_image(disk, *page_id, redo_data)?;
            }
            Kind::PageInit { page_id, kind } => {
                set_allocation(disk, allocator, *page_id, true, record.lsn)?;
                allocations.request(record.txn_id, *page_id, true);
                disk.write_page(*page_id, &kind.image(record.lsn))?;
            }
            Kind::PageDealloc { page_id } => allocations.request(record.txn_id, *page_id, false),
            Kind::AllocationCompensation { page_id, allocated, .. } => {
                allocations.request(record.txn_id, *page_id, *allocated);
            }
            Kind::Commit | Kind::Rollback => {
                allocations.publish(disk, allocator, record.txn_id, record.lsn)?;
            }
            Kind::Begin => {}
        }
    }
    Ok(allocations)
}

/// Append compensation before installing its effect. A restart redoes any
/// surviving CLRs and uses their skip links rather than undoing them again.
pub(super) fn undo(
    mut analysis: Analysis<'_>,
    mut allocations: AllocationReplay,
    log: &mut LogManager,
    disk: &mut DiskManager,
    allocator: &mut PageAllocator,
) -> StorageResult<()> {
    while let Some(record) = analysis.take_loser()? {
        let txn_id = record.txn_id;
        let next = undo_next(record);
        match &record.kind {
            Kind::PageUpdate { page_id, undo_data, .. } => {
                let mut image = **undo_data;
                PageRecovery::stamp_lsn(&mut image, log.next_lsn()?);
                let lsn = log.append_record(
                    txn_id,
                    LogRecordKind::PageCompensation {
                        page_id: *page_id,
                        redo_data: &image,
                        undo_next_lsn: next,
                    },
                )?;
                log.flush_through(lsn)?;
                write_image(disk, *page_id, &image)?;
            }
            Kind::PageInit { page_id, .. } | Kind::PageDealloc { page_id } => {
                // Undo the reservation, not the initializer: restoring a former
                // incarnation would make later redo mistake it for current.
                let allocated = matches!(record.kind, Kind::PageDealloc { .. });
                log.append_record(
                    txn_id,
                    LogRecordKind::AllocationCompensation {
                        page_id: *page_id,
                        allocated,
                        undo_next_lsn: next,
                    },
                )?;
                allocations.request(txn_id, *page_id, allocated);
            }
            Kind::Begin | Kind::PageCompensation { .. } | Kind::AllocationCompensation { .. } => {}
            Kind::Commit | Kind::Rollback => {
                return Err(
                    LogManagerError::InvalidChain { txn_id, lsn: record.lsn, link: next }.into()
                );
            }
        }
        if next == 0 {
            let lsn = log.append_record(txn_id, LogRecordKind::Rollback)?;
            log.flush_through(lsn)?;
            allocations.publish(disk, allocator, txn_id, lsn)?;
        } else {
            analysis.resume_at(txn_id, next);
        }
    }
    Ok(())
}

fn set_allocation(
    disk: &mut DiskManager,
    allocator: &mut PageAllocator,
    page_id: PageId,
    allocated: bool,
    lsn: Lsn,
) -> StorageResult<()> {
    if PageAllocator::is_metadata(page_id) || (!allocated && page_id >= disk.page_count()) {
        return Err(StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::DatabaseFile,
            page_id: Some(page_id),
            kind: CorruptionKind::InvalidAllocationBitmap,
        }));
    }
    if allocated {
        allocator.ensure_page(disk, page_id)?;
    }
    // Several different bits on one bitmap can share the outcome LSN. Equal
    // LSNs are safe to reapply because the transitions set absolute states.
    if allocator.bitmap_lsn(page_id) <= lsn {
        allocator.set_allocated(page_id, allocated, lsn);
    }
    Ok(())
}

fn write_image(
    disk: &mut DiskManager,
    page_id: PageId,
    image: &[u8; PAGE_SIZE],
) -> StorageResult<()> {
    disk.ensure_page_exists(page_id)?;
    disk.write_page(page_id, image)?;
    Ok(())
}
