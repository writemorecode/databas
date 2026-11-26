//! Repeat physical history and compensate losers before handing pages to the cache.

use crate::disk_manager::DiskManager;
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::log_manager::{
    LogManager, LogManagerError, LogRecordKind, RecoveryLogRecord, RecoveryLogRecordKind as Kind,
};
use crate::page_allocator::PageAllocator;
use crate::transaction_manager::stamp_page_lsn;
use crate::{PAGE_SIZE, PageId};

use super::analysis::{Analysis, undo_next};

/// Apply every retained effect in LSN order, including losers and CLRs. Until
/// pages have allocation incarnations, the images are authoritative; a page LSN
/// does not justify skipping redo. Allocation only changes one bit at a time.
pub(super) fn redo(
    records: &[RecoveryLogRecord],
    disk: &mut DiskManager,
    allocator: &mut PageAllocator,
) -> StorageResult<()> {
    for record in records {
        match &record.kind {
            Kind::PageUpdate { page_id, redo_data, .. }
            | Kind::PageCompensation { page_id, redo_data, .. } => {
                write_image(disk, *page_id, redo_data)?;
            }
            Kind::PageAlloc { page_id } => set_allocation(disk, allocator, *page_id, true)?,
            Kind::PageDealloc { page_id } => set_allocation(disk, allocator, *page_id, false)?,
            Kind::AllocationCompensation { page_id, allocated, .. } => {
                set_allocation(disk, allocator, *page_id, *allocated)?;
            }
            Kind::Begin | Kind::Commit | Kind::Rollback => {}
        }
    }
    Ok(())
}

/// Append compensation before installing its effect. A restart redoes any
/// surviving CLRs and uses their skip links rather than undoing them again.
pub(super) fn undo(
    mut analysis: Analysis<'_>,
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
                stamp_page_lsn(&mut image, log.next_lsn()?);
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
            Kind::PageAlloc { page_id } | Kind::PageDealloc { page_id } => {
                let allocated = matches!(record.kind, Kind::PageDealloc { .. });
                log.append_record(
                    txn_id,
                    LogRecordKind::AllocationCompensation {
                        page_id: *page_id,
                        allocated,
                        undo_next_lsn: next,
                    },
                )?;
                // Changed bits stay in memory until allocator.flush forces WAL.
                set_allocation(disk, allocator, *page_id, allocated)?;
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
    allocator.set_allocated(page_id, allocated);
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
