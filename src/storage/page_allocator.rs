//! Logical page ownership and the SQLite-style on-disk freelist checkpoint.
//!
//! The checkpoint's trunks may be consumed during normal operation. A durable
//! WAL membership snapshot is therefore forced before any allocation. Recovery
//! replays all reservations (including losers), restores physical pages, then
//! rebuilds trunks and syncs the database before replacing WAL. Database flushes
//! alone do not checkpoint membership. No file truncation is performed.
//!
//! Header offsets 12/20 store the first trunk and total free count, including
//! trunks. Trunks hold `FREEPAGE`, next/count at offsets 8/16, and little-endian
//! u64 page ids from offset 24. Pages 0–3 are protected catalog/header pages.
//! This layout is inspired by SQLite but is not SQLite-compatible.
use std::collections::{BTreeSet, HashMap, HashSet};

use super::{
    database_header::DatabaseHeader,
    disk_manager::DiskManager,
    log_manager::{Lsn, RecoveryLogRecord, RecoveryLogRecordKind, TxnId},
};
use crate::core::{
    PAGE_SIZE, PageId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
};

const CAPACITY: usize = (PAGE_SIZE - 24) / 8;

pub(crate) fn invalid() -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::DatabaseFile,
        page_id: Some(0),
        kind: CorruptionKind::InvalidFreelist,
    })
}
fn get(page: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&page[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}
fn put(page: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    page[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

/// Logical ownership changes, independent of physical page undo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageAction {
    Reserve,
    Retire,
}

#[derive(Debug, Clone, Copy)]
struct Event {
    page: PageId,
    action: PageAction,
}

/// Free membership is disjoint from outstanding reservations and retirements.
/// Retirements remain unavailable even to their owner until durable commit;
/// rollback returns reservations only after physical undo. Owner 0 denotes an
/// immediate bootstrap/low-level operation. State uses O(free + pending) memory.
#[derive(Debug, Clone)]
pub(crate) struct PageAllocator {
    pub(crate) free: BTreeSet<PageId>,
    pub(crate) page_count: u64,
    events: HashMap<TxnId, HashMap<Lsn, Event>>,
    reservations: HashMap<PageId, TxnId>,
    retirements: HashSet<PageId>,
}
impl PageAllocator {
    pub(crate) fn new(page_count: u64, pages: &[PageId]) -> StorageResult<Self> {
        if page_count == 0 || page_count > u64::MAX / PAGE_SIZE as u64 {
            return Err(invalid());
        }
        let mut free = BTreeSet::new();
        for &page in pages {
            if page < 4 || page >= page_count || !free.insert(page) {
                return Err(invalid());
            }
        }
        Ok(Self {
            free,
            page_count,
            events: HashMap::new(),
            reservations: HashMap::new(),
            retirements: HashSet::new(),
        })
    }
    pub(crate) fn next_page(&self) -> PageId {
        self.free.first().copied().unwrap_or(self.page_count)
    }
    /// Validate before WAL insertion so rejected operations leave no durable record.
    pub(crate) fn validate(
        &self,
        owner: TxnId,
        page: PageId,
        action: PageAction,
    ) -> StorageResult<()> {
        let valid = match action {
            PageAction::Reserve => {
                page != 0
                    && (page >= 4 || owner == 0)
                    && !self.reservations.contains_key(&page)
                    && ((page == self.page_count && page < u64::MAX / PAGE_SIZE as u64)
                        || self.free.contains(&page))
            }
            PageAction::Retire => {
                page >= 4
                    && page < self.page_count
                    && !self.free.contains(&page)
                    && self.reservations.get(&page).is_none_or(|id| *id == owner)
            }
        };
        if !valid || self.retirements.contains(&page) {
            return Err(invalid());
        }
        Ok(())
    }

    /// Apply under the allocator latch, after logging. Replay uses the same checks.
    pub(crate) fn apply(
        &mut self,
        owner: TxnId,
        lsn: Lsn,
        page: PageId,
        action: PageAction,
    ) -> StorageResult<()> {
        self.validate(owner, page, action)?;
        match action {
            PageAction::Reserve => {
                self.page_count += u64::from(page == self.page_count);
                self.free.remove(&page);
                if owner != 0 {
                    self.reservations.insert(page, owner);
                }
            }
            PageAction::Retire if owner == 0 => {
                self.free.insert(page);
            }
            PageAction::Retire => {
                self.retirements.insert(page);
            }
        }
        if owner != 0 {
            self.events.entry(owner).or_default().insert(lsn, Event { page, action });
        }
        Ok(())
    }
    pub(crate) fn cancel(&mut self, owner: TxnId, target: Lsn) -> StorageResult<()> {
        let event =
            *self.events.get(&owner).and_then(|events| events.get(&target)).ok_or_else(invalid)?;
        if event.action == PageAction::Reserve {
            if self.retirements.contains(&event.page) {
                return Err(invalid());
            }
            self.reservations.remove(&event.page);
            self.free.insert(event.page);
        } else {
            self.retirements.remove(&event.page);
        }
        let events = self.events.get_mut(&owner).ok_or_else(invalid)?;
        events.remove(&target);
        if events.is_empty() {
            self.events.remove(&owner);
        }
        Ok(())
    }
    pub(crate) fn finish(&mut self, owner: TxnId, committed: bool) {
        let Some(events) = self.events.remove(&owner) else {
            return;
        };
        for event in events.into_values() {
            if event.action == PageAction::Reserve {
                self.reservations.remove(&event.page);
            } else {
                self.retirements.remove(&event.page);
            }
            if (event.action == PageAction::Reserve) != committed {
                self.free.insert(event.page);
            }
        }
    }
    pub(crate) fn finish_losers(&mut self) {
        let owners: Vec<_> = self.events.keys().copied().collect();
        for owner in owners {
            self.finish(owner, false);
        }
    }
}

/// Read only when WAL has no authoritative membership snapshot.
pub(crate) fn read_checkpoint(disk: &mut DiskManager) -> StorageResult<Option<PageAllocator>> {
    if disk.page_count() == 0 {
        return Ok(None);
    }
    let mut header = [0; PAGE_SIZE];
    disk.read_page(0, &mut header)?;
    if &header[..8] != b"DATABAS\0" {
        return Ok(None);
    }
    DatabaseHeader::validate_page(&header)?;
    let mut allocator = PageAllocator::new(disk.page_count(), &[])?;
    let mut current = get(&header, 12);
    while current != 0 {
        if current < 4 || current >= disk.page_count() || !allocator.free.insert(current) {
            return Err(invalid());
        }
        let mut trunk = [0; PAGE_SIZE];
        disk.read_page(current, &mut trunk)?;
        if &trunk[..8] != b"FREEPAGE" {
            return Err(invalid());
        }
        let count = usize::try_from(get(&trunk, 16)).map_err(|_overflow| invalid())?;
        if count > CAPACITY {
            return Err(invalid());
        }
        for index in 0..count {
            let id = get(&trunk, 24 + index * 8);
            if id < 4 || id >= disk.page_count() || !allocator.free.insert(id) {
                return Err(invalid());
            }
        }
        current = get(&trunk, 8);
    }
    if allocator.free.len() as u64 != get(&header, 20) {
        return Err(invalid());
    }
    Ok(Some(allocator))
}

/// Replay ownership chronologically, INCLUDING reservations of losing users.
/// Never inspect trunks when a snapshot exists: their pages may now hold data.
pub(crate) fn replay_lifecycle(
    records: &[RecoveryLogRecord],
) -> StorageResult<Option<PageAllocator>> {
    let Some(first) = records.first() else {
        return Ok(None);
    };
    let RecoveryLogRecordKind::FreelistCheckpoint { page_count, pages } = &first.kind else {
        if records.iter().any(|record| {
            matches!(
                record.kind,
                RecoveryLogRecordKind::FreelistCheckpoint { .. }
                    | RecoveryLogRecordKind::PageReserve { .. }
                    | RecoveryLogRecordKind::PageRetire { .. }
                    | RecoveryLogRecordKind::LifecycleCancel { .. }
            )
        }) {
            return Err(invalid());
        }
        return Ok(None);
    };
    if first.txn_id != 0 {
        return Err(invalid());
    }
    let mut allocator = PageAllocator::new(*page_count, pages)?;
    for record in &records[1..] {
        match record.kind {
            RecoveryLogRecordKind::PageReserve { page_id } => {
                allocator.apply(record.txn_id, record.lsn, page_id, PageAction::Reserve)?
            }
            RecoveryLogRecordKind::PageRetire { page_id } => {
                allocator.apply(record.txn_id, record.lsn, page_id, PageAction::Retire)?
            }
            RecoveryLogRecordKind::LifecycleCancel { target_lsn } => {
                allocator.cancel(record.txn_id, target_lsn)?
            }
            RecoveryLogRecordKind::Commit => allocator.finish(record.txn_id, true),
            RecoveryLogRecordKind::Rollback => allocator.finish(record.txn_id, false),
            RecoveryLogRecordKind::FreelistCheckpoint { .. } => return Err(invalid()),
            _ => {}
        }
    }
    allocator.finish_losers();
    Ok(Some(allocator))
}

/// Rebuild ONLY after physical redo/undo; sync the file before WAL replacement.
/// A crash during rebuilding repeats recovery from the still-live WAL snapshot.
pub(crate) fn write_checkpoint(
    disk: &mut DiskManager,
    allocator: &PageAllocator,
) -> StorageResult<()> {
    disk.ensure_page_exists(allocator.page_count - 1)?;
    let pages: Vec<_> = allocator.free.iter().copied().collect();
    let mut chunks = pages.chunks(CAPACITY + 1).peekable();
    while let Some(chunk) = chunks.next() {
        let mut trunk = [0; PAGE_SIZE];
        trunk[..8].copy_from_slice(b"FREEPAGE");
        put(&mut trunk, 8, chunks.peek().map_or(0, |next| next[0]));
        put(&mut trunk, 16, (chunk.len() - 1) as u64);
        for (slot, id) in chunk[1..].iter().enumerate() {
            put(&mut trunk, 24 + slot * 8, *id);
        }
        disk.write_page(chunk[0], &trunk)?;
    }
    let mut header = [0; PAGE_SIZE];
    disk.read_page(0, &mut header)?;
    put(&mut header, 12, pages.first().copied().unwrap_or(0));
    put(&mut header, 20, pages.len() as u64);
    disk.write_page(0, &header)?;
    Ok(())
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use PageAction::{Reserve, Retire};
    use proptest::prelude::*;

    fn record(lsn: Lsn, txn_id: TxnId, kind: RecoveryLogRecordKind) -> RecoveryLogRecord {
        RecoveryLogRecord { lsn, txn_id, kind }
    }

    #[test]
    fn reservations_and_retirements_require_their_owner_and_reverse_cancellation_order() {
        let mut allocator = PageAllocator::new(6, &[4]).unwrap();
        allocator.apply(1, 10, 4, Reserve).unwrap();
        for (owner, page, action) in
            [(2, 4, Reserve), (2, 4, Retire), (1, 3, Retire), (1, 6, Retire)]
        {
            assert!(allocator.apply(owner, 11, page, action).is_err());
        }
        assert!(allocator.cancel(2, 10).is_err());
        allocator.apply(1, 12, 4, Retire).unwrap();
        assert!(allocator.cancel(1, 10).is_err());
        assert!(allocator.apply(1, 13, 4, Retire).is_err());
        allocator.cancel(1, 12).unwrap();
        allocator.cancel(1, 10).unwrap();
        allocator.apply(2, 14, 4, Reserve).unwrap();
        assert!(allocator.cancel(1, 10).is_err()); // stale LSN cannot release a new owner
        allocator.finish(2, true);
        allocator.apply(1, 15, 4, Retire).unwrap();
        assert_eq!(allocator.next_page(), 6); // own pending retirement is unavailable
        allocator.finish(1, false);
        assert!(allocator.free.is_empty());
        allocator.apply(2, 16, 4, Retire).unwrap();
        allocator.finish(2, true);
        assert_eq!(allocator.free, BTreeSet::from([4]));
    }

    #[test]
    fn invalid_membership_and_file_extent_are_rejected() {
        for pages in [&[4, 4][..], &[0], &[3], &[8]] {
            assert!(PageAllocator::new(8, pages).is_err());
        }
        for count in [0, u64::MAX] {
            assert!(PageAllocator::new(count, &[]).is_err());
        }
        let mut allocator = PageAllocator::new(u64::MAX / PAGE_SIZE as u64, &[]).unwrap();
        assert!(allocator.apply(1, 1, allocator.page_count, Reserve).is_err());
        let mut allocator = PageAllocator::new(1, &[]).unwrap();
        assert!(allocator.apply(1, 1, 1, Reserve).is_err());
        for page in 1..4 {
            // bootstrap alone may reserve the catalog roots
            allocator.apply(0, page, page, Reserve).unwrap();
            assert!(allocator.apply(0, page, page, Retire).is_err());
        }
    }

    #[test]
    fn checkpoint_roundtrips_empty_full_and_multiple_trunks() {
        for count in [0, 1, CAPACITY + 1, CAPACITY + 2, 2 * (CAPACITY + 1) + 1] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut disk = DiskManager::new(file.path()).unwrap();
            let pages: Vec<_> = (4..4 + count as u64).collect();
            let allocator = PageAllocator::new(5 + count as u64, &pages).unwrap();
            disk.ensure_page_exists(allocator.page_count - 1).unwrap();
            disk.write_page(0, &DatabaseHeader::encode_page()).unwrap();
            disk.write_page(allocator.page_count - 1, &[79; PAGE_SIZE]).unwrap();
            write_checkpoint(&mut disk, &allocator).unwrap();
            let restored = read_checkpoint(&mut disk).unwrap().unwrap();
            assert_eq!(restored.free, allocator.free);
            assert_eq!(restored.page_count, allocator.page_count);
            let mut live = [0; PAGE_SIZE];
            disk.read_page(allocator.page_count - 1, &mut live).unwrap();
            assert_eq!(live, [79; PAGE_SIZE]);
        }
    }

    #[test]
    fn checkpoint_rejects_cycles_duplicates_bad_counts_and_invalid_page_ids() {
        for (page_id, offset, value) in [
            (0, 12, 3),
            (0, 12, 6),
            (0, 20, 1),
            (4, 0, 0),
            (4, 8, 4),
            (4, 8, 5),
            (4, 16, CAPACITY as u64 + 1),
            (4, 24, 3),
            (4, 24, 6),
            (4, 24, 4),
        ] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut disk = DiskManager::new(file.path()).unwrap();
            disk.ensure_page_exists(5).unwrap();
            disk.write_page(0, &DatabaseHeader::encode_page()).unwrap();
            write_checkpoint(&mut disk, &PageAllocator::new(6, &[4, 5]).unwrap()).unwrap();
            let mut page = [0; PAGE_SIZE];
            disk.read_page(page_id, &mut page).unwrap();
            put(&mut page, offset, value);
            disk.write_page(page_id, &page).unwrap();
            assert!(
                matches!(read_checkpoint(&mut disk), Err(StorageError::Corruption(_))),
                "page {page_id}, offset {offset}, value {value}"
            );
        }
    }

    #[test]
    fn lifecycle_rejects_missing_misplaced_and_user_owned_snapshots() {
        use RecoveryLogRecordKind::*;
        let snapshot = || FreelistCheckpoint { page_count: 6, pages: vec![4, 5] };
        let invalid_histories = [
            vec![record(1, 1, PageReserve { page_id: 4 })],
            vec![record(1, 1, Begin), record(2, 0, snapshot())],
            vec![record(1, 1, snapshot())],
            vec![record(1, 0, snapshot()), record(2, 0, snapshot())],
            vec![record(1, 0, snapshot()), record(2, 1, LifecycleCancel { target_lsn: 99 })],
            vec![
                record(1, 0, snapshot()),
                record(2, 1, PageReserve { page_id: 4 }),
                record(3, 2, PageReserve { page_id: 4 }),
            ],
        ];
        for records in invalid_histories {
            assert!(replay_lifecycle(&records).is_err());
        }
        assert!(replay_lifecycle(&[]).unwrap().is_none());
        assert!(replay_lifecycle(&[record(1, 1, Begin)]).unwrap().is_none());
    }

    proptest! {
        #[test]
        fn replay_preserves_exactly_committed_allocations(
            transactions in prop::collection::vec((0usize..8, any::<bool>(), any::<bool>(), any::<bool>()), 1..30)
        ) {
            use RecoveryLogRecordKind::*;
            let mut records = vec![record(1, 0, FreelistCheckpoint { page_count: 8, pages: vec![4, 5, 6, 7] })];
            let mut allocator = PageAllocator::new(8, &[4, 5, 6, 7]).unwrap();
            let mut live = BTreeSet::new();
            let mut pending_retirements = BTreeSet::new();
            for (index, (count, commit, crash, retire)) in transactions.into_iter().enumerate() {
                let owner = index as u64 + 1;
                let retired = live.iter().find(|page| !pending_retirements.contains(*page)).copied().filter(|_| retire);
                if let Some(page) = retired {
                    let lsn = records.len() as u64 + 1;
                    allocator.apply(owner, lsn, page, Retire).unwrap();
                    records.push(record(lsn, owner, PageRetire { page_id: page }));
                    pending_retirements.insert(page);
                }
                let mut allocated = Vec::new();
                for _ in 0..count {
                    let page = allocator.next_page();
                    let lsn = records.len() as u64 + 1;
                    allocator.apply(owner, lsn, page, Reserve).unwrap();
                    records.push(record(lsn, owner, PageReserve { page_id: page }));
                    allocated.push(page);
                }
                if commit {
                    if let Some(page) = retired { live.remove(&page); }
                    live.extend(allocated);
                    allocator.finish(owner, true);
                    records.push(record(records.len() as u64 + 1, owner, Commit));
                } else if !crash {
                    allocator.finish(owner, false);
                    records.push(record(records.len() as u64 + 1, owner, Rollback));
                }
                if (commit || !crash) && let Some(page) = retired {
                    pending_retirements.remove(&page);
                } // crashed owners stay quarantined while later writers run
            }
            let replayed = replay_lifecycle(&records).unwrap().unwrap();
            let expected: BTreeSet<_> = (4..allocator.page_count).filter(|page| !live.contains(page)).collect();
            prop_assert_eq!(replayed.free, expected);
            prop_assert_eq!(replayed.page_count, allocator.page_count);
        }
    }
}
