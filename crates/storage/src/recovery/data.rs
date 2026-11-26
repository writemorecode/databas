//! Per-page redo, guarded by the checksum-verified incarnation and page LSN.

use std::collections::HashMap;

use crate::disk_manager::{DiskManager, DiskManagerError};
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::log_manager::{LogManagerError, RecoveryLogRecord, RecoveryLogRecordKind as Kind};
use crate::page_allocator::PageAllocator;
use crate::page_patch::PagePatch;
use crate::page_recovery::{PageInitKind, PageRecovery};
use crate::{Lsn, PAGE_SIZE, PageId};

pub(super) struct DataRedo<'a> {
    records: &'a [RecoveryLogRecord],
    pages: HashMap<PageId, PageRecovery>,
}

impl<'a> DataRedo<'a> {
    pub(super) fn new(records: &'a [RecoveryLogRecord]) -> Self {
        Self { records, pages: HashMap::new() }
    }

    pub(super) fn initialize(
        &mut self,
        disk: &mut DiskManager,
        page_id: PageId,
        lsn: Lsn,
        kind: PageInitKind,
    ) -> StorageResult<()> {
        let target = PageRecovery { incarnation: lsn, lsn };
        if self.needs_redo(disk, page_id, target, true)? {
            disk.write_page(page_id, &kind.image(lsn))?;
            self.pages.insert(page_id, target);
        }
        Ok(())
    }

    pub(super) fn patch(
        &mut self,
        disk: &mut DiskManager,
        page_id: PageId,
        target: PageRecovery,
        redo: &PagePatch,
        baseline: Option<(Lsn, &PagePatch)>,
    ) -> StorageResult<()> {
        if target.incarnation == 0 || target.incarnation >= target.lsn {
            return Err(invalid_metadata(page_id, target));
        }
        if let Some((before, _)) = baseline
            && !(target.incarnation..target.lsn).contains(&before)
        {
            return Err(invalid_metadata(page_id, PageRecovery { lsn: before, ..target }));
        }
        if self.needs_redo(disk, page_id, target, false)? {
            let mut image = [0; PAGE_SIZE];
            disk.read_page(page_id, &mut image)?;
            let current = validate_data(page_id, &image)?;
            if let Some((before_lsn, undo)) = baseline
                && (current.lsn != before_lsn
                    || !undo.matches(&image).map_err(LogManagerError::from)?)
            {
                return Err(corruption(
                    page_id,
                    CorruptionKind::PagePatchBaselineMismatch {
                        expected: before_lsn,
                        actual: current.lsn,
                    },
                ));
            }
            apply_patch(page_id, &mut image, target, redo)?;
            disk.write_page(page_id, &image)?;
            self.pages.insert(page_id, target);
        }
        Ok(())
    }

    fn needs_redo(
        &mut self,
        disk: &mut DiskManager,
        page_id: PageId,
        target: PageRecovery,
        initializing: bool,
    ) -> StorageResult<bool> {
        let current = if let Some(current) = self.pages.get(&page_id) {
            Some(*current)
        } else {
            let mut image = [0; PAGE_SIZE];
            match disk.read_page(page_id, &mut image) {
                Ok(()) if initializing && image.iter().all(|byte| *byte == 0) => None,
                // A durable initializer can reconstruct this page without
                // interpreting any bytes from the former owner or a hole.
                Err(DiskManagerError::ChecksumMismatch { .. }) if initializing => None,
                result => {
                    result?;
                    let current = validate_data(page_id, &image)?;
                    self.pages.insert(page_id, current);
                    Some(current)
                }
            }
        };
        let Some(current) = current else {
            return Ok(true);
        };
        if current.incarnation == target.incarnation && current.lsn < target.lsn {
            return Ok(true);
        }
        if initializing && current.lsn < target.lsn {
            // An older occupant cannot constrain the new initialization.
            return Ok(true);
        }
        if current.incarnation != target.incarnation {
            // A distinct incarnation only supersedes this record if a later
            // initializer in this very WAL establishes it for this address.
            if current.incarnation <= target.lsn
                || !matches!(self.record(current.incarnation), Some(Kind::PageInit { page_id: owner, .. }) if *owner == page_id)
            {
                current.require_incarnation(page_id, target.incarnation)?;
            }
        }
        if !self.record_matches(page_id, current) {
            return Err(invalid_metadata(page_id, current));
        }
        Ok(false)
    }

    fn record(&self, lsn: Lsn) -> Option<&Kind> {
        let offset = usize::try_from(lsn.checked_sub(self.records.first()?.lsn)?).ok()?;
        self.records.get(offset).map(|record| &record.kind)
    }

    fn record_matches(&self, page_id: PageId, current: PageRecovery) -> bool {
        match self.record(current.lsn) {
            Some(Kind::PageInit { page_id: owner, .. }) => {
                *owner == page_id && current.incarnation == current.lsn
            }
            Some(
                Kind::PageUpdate { page_id: owner, incarnation, .. }
                | Kind::PageCompensation { page_id: owner, incarnation, .. },
            ) => *owner == page_id && *incarnation == current.incarnation,
            _ => false,
        }
    }
}

pub(super) fn validate_data(
    page_id: PageId,
    image: &[u8; PAGE_SIZE],
) -> StorageResult<PageRecovery> {
    let recovery = PageRecovery::read(image);
    if PageAllocator::is_metadata(page_id)
        || recovery.incarnation == 0
        || recovery.lsn < recovery.incarnation
        || PageInitKind::from_tag(image[crate::page::format::KIND_OFFSET]).is_none()
        || image[crate::page::format::VERSION_OFFSET] != crate::page::format::FORMAT_VERSION
        || image[crate::types::PAGE_CHECKSUM_OFFSET..].iter().any(|byte| *byte != 0)
    {
        return Err(invalid_metadata(page_id, recovery));
    }
    Ok(recovery)
}

fn invalid_metadata(page_id: PageId, recovery: PageRecovery) -> StorageError {
    corruption(
        page_id,
        CorruptionKind::InvalidPageRecovery {
            incarnation: recovery.incarnation,
            lsn: recovery.lsn,
        },
    )
}

pub(super) fn apply_patch(
    page_id: PageId,
    image: &mut [u8; PAGE_SIZE],
    target: PageRecovery,
    patch: &PagePatch,
) -> StorageResult<()> {
    let current = validate_data(page_id, image)?;
    current.require_incarnation(page_id, target.incarnation)?;
    if current.lsn >= target.lsn {
        return Err(invalid_metadata(page_id, current));
    }
    patch.apply(image).map_err(LogManagerError::from)?;
    target.write(image);
    validate_data(page_id, image)?;
    Ok(())
}

fn corruption(page_id: PageId, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::DiskPage,
        page_id: Some(page_id),
        kind,
    })
}
