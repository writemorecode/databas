//! Decide which WAL effects survive, then apply them without revisiting transaction outcomes.

use std::collections::{HashMap, HashSet};

use crate::disk_manager::DiskManager;
use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::log_manager::{Lsn, RecoveryLogRecord, RecoveryLogRecordKind as Kind};
use crate::page_allocator::PageAllocator;
use crate::{PAGE_SIZE, PageId};

type PageImage = Box<[u8; PAGE_SIZE]>;

#[derive(Clone, Copy)]
enum Outcome {
    Committed,
    RolledBack,
    Incomplete,
}

#[derive(Clone, Copy)]
enum Direction {
    Redo,
    Undo,
}

struct AllocationRepair {
    page_id: PageId,
    allocated: bool,
    direction: Direction,
}

struct PageWrite {
    page_id: PageId,
    image: PageImage,
    direction: Direction,
}

#[derive(Default)]
struct Replay {
    bitmap_images: HashMap<PageId, PageImage>,
    allocations: Vec<AllocationRepair>,
    writes: Vec<PageWrite>,
}

/// Repair the pages and allocation state. The caller must persist repaired bitmaps
/// and sync the database before reclaiming the WAL.
pub(super) fn repair(
    disk: &mut DiskManager,
    records: Vec<RecoveryLogRecord>,
) -> StorageResult<PageAllocator> {
    Replay::from_records(records).apply(disk)
}

impl Replay {
    fn from_records(records: Vec<RecoveryLogRecord>) -> Self {
        debug_assert!(records.windows(2).all(|pair| pair[0].lsn < pair[1].lsn));
        let mut outcomes = HashMap::new();
        for record in &records {
            match record.kind {
                Kind::Commit => {
                    outcomes.insert(record.txn_id, Outcome::Committed);
                }
                Kind::Rollback => {
                    outcomes.entry(record.txn_id).or_insert(Outcome::RolledBack);
                }
                _ => {}
            }
        }

        let mut replay = Self::default();
        let mut seen_uncommitted_allocations = HashSet::new();
        let mut latest_committed_allocation = HashMap::new();
        let mut latest_committed_write = HashMap::new();
        let mut allocation_undo = Vec::new();
        let mut page_undo = Vec::new();

        // WAL order gives ascending redo order and each page's first allocation
        // transition without sorting by transaction or walking hash maps.
        for RecoveryLogRecord { lsn, txn_id, kind } in records {
            let outcome = outcomes.get(&txn_id).copied().unwrap_or(Outcome::Incomplete);
            match kind {
                Kind::Begin | Kind::Commit | Kind::Rollback => {}
                Kind::BitmapImage { page_id, image } => {
                    replay.bitmap_images.insert(page_id, image);
                }
                kind @ (Kind::PageAlloc { page_id } | Kind::PageDealloc { page_id }) => {
                    let allocated = matches!(kind, Kind::PageAlloc { .. });
                    if matches!(outcome, Outcome::Committed) {
                        latest_committed_allocation.insert(page_id, lsn);
                        replay.allocations.push(AllocationRepair {
                            page_id,
                            allocated,
                            direction: Direction::Redo,
                        });
                    } else if seen_uncommitted_allocations.insert((txn_id, page_id)) {
                        // Completed rollback can still need allocation undo: its
                        // outcome can reach disk before its bitmap changes do.
                        allocation_undo.push((lsn, page_id, !allocated));
                    }
                }
                Kind::PageUpdate { page_id, redo_data, undo_data } => match outcome {
                    Outcome::Committed => {
                        latest_committed_write.insert(page_id, lsn);
                        replay.writes.push(PageWrite {
                            page_id,
                            image: redo_data,
                            direction: Direction::Redo,
                        });
                    }
                    Outcome::Incomplete => page_undo.push((lsn, page_id, undo_data)),
                    Outcome::RolledBack => {} // Page undo was synced before the outcome.
                },
            }
        }

        // A later commit on the same page wins. Restore original allocation
        // bits in WAL order, but walk physical before-images in reverse order.
        replay.allocations.extend(allocation_undo.into_iter().filter_map(
            |(lsn, page_id, allocated)| {
                (!superseded(&latest_committed_allocation, page_id, lsn))
                    .then_some(AllocationRepair { page_id, allocated, direction: Direction::Undo })
            },
        ));
        replay.writes.extend(page_undo.into_iter().rev().filter_map(|(lsn, page_id, image)| {
            (!superseded(&latest_committed_write, page_id, lsn)).then_some(PageWrite {
                page_id,
                image,
                direction: Direction::Undo,
            })
        }));
        replay
    }

    fn apply(self, disk: &mut DiskManager) -> StorageResult<PageAllocator> {
        let mut allocator = PageAllocator::load_with_images(disk, &self.bitmap_images)?;
        for AllocationRepair { page_id, allocated, direction } in self.allocations {
            match direction {
                Direction::Redo => {
                    if PageAllocator::is_metadata(page_id)
                        || (!allocated && page_id >= disk.page_count())
                    {
                        return Err(StorageError::Corruption(CorruptionError {
                            component: CorruptionComponent::DatabaseFile,
                            page_id: Some(page_id),
                            kind: CorruptionKind::InvalidAllocationBitmap,
                        }));
                    }
                    if allocated {
                        allocator.ensure_page(disk, page_id)?;
                    }
                }
                Direction::Undo => {
                    if !allocator.contains_page(page_id) || PageAllocator::is_metadata(page_id) {
                        continue;
                    }
                }
            }
            allocator.set_allocated(page_id, allocated);
        }
        for PageWrite { page_id, image, direction } in self.writes {
            match direction {
                Direction::Redo => disk.ensure_page_exists(page_id)?,
                Direction::Undo if page_id >= disk.page_count() => continue,
                Direction::Undo => {}
            }
            disk.write_page(page_id, image.as_ref())?;
        }
        allocator.validate_bounds(disk.page_count())?;
        Ok(allocator)
    }
}

fn superseded(latest_commit: &HashMap<PageId, Lsn>, page_id: PageId, lsn: Lsn) -> bool {
    latest_commit.get(&page_id).is_some_and(|later| *later > lsn)
}
