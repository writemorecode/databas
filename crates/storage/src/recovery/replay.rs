//! Repeat physical history before handing pages to logical or physical undo.

use std::collections::{BTreeMap, HashMap};

use crate::disk_manager::DiskManager;
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::log_manager::{LogManagerError, RecoveryLogRecord, RecoveryLogRecordKind as Kind};
use crate::page_allocator::PageAllocator;
use crate::page_recovery::PageRecovery;
use crate::{Lsn, PageId, TxnId};

use super::data::DataRedo;

/// Allocations reserve bits at PageInit. Frees and allocation CLRs change only
/// the pending state until Commit/Rollback, exactly as during normal execution.
/// Applying them earlier would make a persisted bitmap LSN unsafe for redo.
#[derive(Default)]
pub(super) struct AllocationReplay {
    pending: HashMap<TxnId, BTreeMap<PageId, bool>>,
}

impl AllocationReplay {
    pub(super) fn take_pending(&mut self, txn_id: TxnId) -> BTreeMap<PageId, bool> {
        self.pending.remove(&txn_id).unwrap_or_default()
    }

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
            Kind::Begin | Kind::LogicalEdit { .. } | Kind::SkipUndo { .. } => {}
        }
    }
    Ok(allocations)
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
