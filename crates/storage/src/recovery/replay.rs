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
use super::data::{DataRedo, apply_operation};

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

/// Repeat retained history, including losers and CLRs, subject to each
/// verified page's redo watermark.
pub(super) fn redo(
    records: &[RecoveryLogRecord],
    disk: &mut DiskManager,
    allocator: &mut PageAllocator,
) -> StorageResult<AllocationReplay> {
    let mut allocations = AllocationReplay::default();
    let mut data = DataRedo::new(records);
    for record in records {
        match &record.kind {
            Kind::PageUpdate { page_id, incarnation, before_lsn, redo, undo } => {
                let target = PageRecovery { incarnation: *incarnation, lsn: record.lsn };
                data.replay(disk, *page_id, target, Some((*before_lsn, undo)), |image| {
                    redo.apply(image)
                })?;
            }
            Kind::PageCompensation { page_id, incarnation, redo, .. } => {
                let target = PageRecovery { incarnation: *incarnation, lsn: record.lsn };
                data.replay(disk, *page_id, target, None, |image| {
                    Ok(redo.apply(image).map_err(LogManagerError::from)?)
                })?;
            }
            Kind::PageInit { page_id, kind } => {
                set_allocation(disk, allocator, *page_id, true, record.lsn)?;
                allocations.request(record.txn_id, *page_id, true);
                data.initialize(disk, *page_id, record.lsn, *kind)?;
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
            Kind::PageUpdate { page_id, incarnation, undo, .. } => {
                let mut image = [0; PAGE_SIZE];
                disk.read_page(*page_id, &mut image)?;
                debug_assert!(PageRecovery::read(&image).lsn >= record.lsn);
                let target = PageRecovery { incarnation: *incarnation, lsn: log.next_lsn()? };
                apply_operation(*page_id, &mut image, target, |image| {
                    Ok(undo.apply(image).map_err(LogManagerError::from)?)
                })?;
                let lsn = log.append_record(
                    txn_id,
                    LogRecordKind::PageCompensation {
                        page_id: *page_id,
                        incarnation: *incarnation,
                        redo: undo,
                        undo_next_lsn: next,
                    },
                )?;
                debug_assert_eq!(lsn, target.lsn);
                log.flush_through(lsn)?;
                disk.write_page(*page_id, &image)?;
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
