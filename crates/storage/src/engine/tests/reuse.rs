use std::{collections::BTreeMap, fs, io::ErrorKind};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{OPTIONS, open};
use crate::{
    CheckpointOutcome, PAGE_SIZE, PageId, Storage,
    disk_manager::{
        DiskManager,
        tests::{Control, Effect, Operation},
    },
    log_manager::{RecoveryLogRecordKind, read_checkpoint_log},
    page_allocator::PAGES_PER_EXTENT,
};

fn allocate(storage: &Storage, txn: Option<u64>) -> Result<PageId, TestCaseError> {
    let (id, pin) = storage.page_cache.new_page(txn)?;
    prop_assert!(pin.read()?.page().iter().all(|byte| *byte == 0));
    Ok(id)
}

fn bitmap(path: &std::path::Path, id: PageId) -> bool {
    let bytes = fs::read(path).unwrap();
    let base = id / PAGES_PER_EXTENT * PAGES_PER_EXTENT;
    let bit = (id % PAGES_PER_EXTENT) as usize;
    bytes[usize::try_from(base).unwrap() * PAGE_SIZE + 16 + bit / 8] & (1 << (bit % 8)) != 0
}

#[test]
fn frees_are_reserved_until_commit_and_rollback_restores_allocation_state() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let first = allocate(&storage, None)?;
    storage.flush()?;
    let txn = storage.begin_transaction()?;
    storage.page_cache.dealloc_page(Some(txn), first)?;
    prop_assert!(storage.page_cache.fetch_page(first).is_err());
    prop_assert!(storage.page_cache.dealloc_page(Some(txn), first).is_err());
    let other = storage.begin_transaction()?;
    let second = allocate(&storage, Some(other))?;
    prop_assert_ne!(first, second);
    storage.commit_transaction(other)?;
    storage.rollback_transaction(txn)?;
    prop_assert!(storage.page_cache.fetch_page(first).is_ok());
    let txn = storage.begin_transaction()?;
    storage.page_cache.dealloc_page(Some(txn), first)?;
    storage.commit_transaction(txn)?;
    let txn = storage.begin_transaction()?;
    prop_assert_eq!(allocate(&storage, Some(txn))?, first);
    storage.rollback_transaction(txn)?;
    // A rollback releases both recycled and appended allocations.
    prop_assert_eq!(allocate(&storage, None)?, first);
    let txn = storage.begin_transaction()?;
    let appended = allocate(&storage, Some(txn))?;
    storage.rollback_transaction(txn)?;
    prop_assert_eq!(allocate(&storage, None)?, appended);
    storage.flush()?;
    prop_assert!(bitmap(&path, first));
    prop_assert!(bitmap(&path, second));
    Ok(())
}

#[test]
fn metadata_double_frees_invalid_ids_and_pinned_pages_are_rejected() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let storage = open(&dir.path().join("db"), 2)?;
    let txn = storage.begin_transaction()?;
    for id in [0, PAGES_PER_EXTENT, u64::MAX, 1] {
        prop_assert!(storage.page_cache.dealloc_page(Some(txn), id).is_err());
    }
    let (id, pin) = storage.page_cache.new_page(Some(txn))?;
    prop_assert!(storage.page_cache.dealloc_page(Some(txn), id).is_err());
    drop(pin);
    storage.page_cache.dealloc_page(Some(txn), id)?;
    storage.commit_transaction(txn)?;
    prop_assert!(storage.page_cache.dealloc_page(None, id).is_err());
    // Invalid transaction handles cannot silently turn allocations into raw writes.
    prop_assert!(allocate(&storage, Some(txn)).is_err());
    Ok(())
}

#[test]
fn nested_savepoints_compensate_allocations_and_frees_after_restart() -> TestCaseResult {
    for commit in [true, false] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let storage = open(&path, 2)?;
        let root = allocate(&storage, None)?;
        storage.flush()?;
        let txn = storage.begin_transaction()?;
        let outer = storage.statement_savepoint(txn)?;
        storage.page_cache.dealloc_page(Some(txn), root)?;
        let temporary = allocate(&storage, Some(txn))?;
        let inner = storage.statement_savepoint(txn)?;
        storage.page_cache.dealloc_page(Some(txn), temporary)?;
        let last = allocate(&storage, Some(txn))?;
        storage.rollback_to_savepoint(inner)?;
        // Reservations restored to a savepoint are retained until completion.
        let second = storage.begin_transaction()?;
        let reserved = allocate(&storage, Some(second))?;
        prop_assert_ne!(reserved, last);
        storage.commit_transaction(second)?;
        storage.rollback_to_savepoint(outer)?;
        // Repeating a rollback must remain safe, including compensation records.
        storage.rollback_to_savepoint(outer)?;
        if commit {
            storage.commit_transaction(txn)?;
        } else {
            storage.rollback_transaction(txn)?;
        }
        drop(storage);
        let storage = open(&path, 1)?;
        prop_assert!(storage.page_cache.fetch_page(root).is_ok());
        prop_assert!(storage.page_cache.fetch_page(reserved).is_ok());
        prop_assert_eq!(allocate(&storage, None)?, temporary);
        prop_assert_eq!(allocate(&storage, None)?, last);
    }
    Ok(())
}

#[test]
fn restart_replays_committed_frees_and_undoes_loser_allocations_and_frees() -> TestCaseResult {
    for outcome in ["commit", "rollback", "crash"] {
        for checkpoint in [false, true] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("db");
            let storage = open(&path, 2)?;
            let first = allocate(&storage, None)?;
            storage.flush()?;
            let txn = storage.begin_transaction()?;
            let second = allocate(&storage, Some(txn))?;
            storage.page_cache.dealloc_page(Some(txn), first)?;
            if checkpoint {
                prop_assert_eq!(storage.checkpoint()?, CheckpointOutcome::Partial);
            }
            match outcome {
                "commit" => storage.commit_transaction(txn)?,
                "rollback" => storage.rollback_transaction(txn)?,
                _ => storage.flush()?, // Persist active reservations and WAL, simulating STEAL.
            }
            drop(storage);
            let storage = open(&path, 1)?;
            if outcome == "commit" {
                prop_assert!(storage.page_cache.fetch_page(first).is_err());
                prop_assert!(storage.page_cache.fetch_page(second).is_ok());
                prop_assert_eq!(allocate(&storage, None)?, first);
            } else {
                prop_assert!(storage.page_cache.fetch_page(first).is_ok());
                prop_assert!(storage.page_cache.fetch_page(second).is_err());
                prop_assert_eq!(allocate(&storage, None)?, second);
            }
        }
    }
    Ok(())
}

#[test]
fn later_committed_reuse_survives_older_rolled_back_wal_records() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let first = storage.begin_transaction()?;
    let id = allocate(&storage, Some(first))?;
    storage.rollback_transaction(first)?;
    let second = storage.begin_transaction()?;
    let (reused, pin) = storage.page_cache.new_page(Some(second))?;
    prop_assert_eq!(id, reused);
    pin.write(Some(second))?.page_mut().fill(91);
    drop(pin);
    storage.commit_transaction(second)?;
    drop(storage);
    for _ in 0..2 {
        let storage = open(&path, 1)?;
        prop_assert!(bitmap(&path, id));
        prop_assert_eq!(*storage.page_cache.fetch_page(id)?.read()?.page(), [91; PAGE_SIZE]);
        drop(storage);
    }
    Ok(())
}

#[test]
fn interleaved_transactions_changing_one_bitmap_byte_do_not_undo_each_other() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let a = storage.begin_transaction()?;
    let b = storage.begin_transaction()?;
    let x = allocate(&storage, Some(a))?;
    let y = allocate(&storage, Some(b))?;
    storage.checkpoint()?;
    storage.commit_transaction(b)?;
    storage.rollback_transaction(a)?;
    drop(storage);
    let storage = open(&path, 1)?;
    prop_assert!(!bitmap(&path, x));
    prop_assert!(bitmap(&path, y));
    prop_assert_eq!(allocate(&storage, None)?, x);
    prop_assert_ne!(allocate(&storage, None)?, y);
    Ok(())
}

#[test]
fn concurrent_allocators_never_issue_the_same_page() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let ids = std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let txn = storage.begin_transaction().unwrap();
                    let ids: Vec<_> =
                        (0..24).map(|_| allocate(&storage, Some(txn)).unwrap()).collect();
                    storage.commit_transaction(txn).unwrap();
                    ids
                })
            })
            .collect();
        tasks.into_iter().flat_map(|task| task.join().unwrap()).collect::<Vec<_>>()
    });
    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    prop_assert_eq!(unique.len(), ids.len());
    storage.checkpoint()?;
    drop(storage);
    let storage = open(&path, 1)?;
    for id in ids {
        prop_assert!(storage.page_cache.fetch_page(id).is_ok());
    }
    Ok(())
}

fn fault_storage(path: &std::path::Path) -> crate::error::StorageResult<(Storage, Control)> {
    let disk = DiskManager::open_existing(path)?;
    let faults = disk.faults();
    let storage = Storage::from_disk_manager(path.to_path_buf(), disk, OPTIONS)?;
    Ok((storage, faults))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn partial_bitmap_writes_and_sync_failures_keep_wal_for_retry(at in 0usize..PAGE_SIZE, sync in any::<bool>()) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let storage = open(&path, 2)?;
        let txn = storage.begin_transaction()?;
        let root = allocate(&storage, Some(txn))?;
        storage.commit_transaction(txn)?;
        storage.checkpoint()?;
        drop(storage);
        let (storage, faults) = fault_storage(&path)?;
        let txn = storage.begin_transaction()?;
        storage.page_cache.dealloc_page(Some(txn), root)?;
        storage.commit_transaction(txn)?;
        let wal_path = path.with_added_extension("wal");
        let before = fs::read(&wal_path)?;
        faults.arm(if sync { Operation::Sync } else { Operation::Write }, if sync { 0 } else { at }, Effect::Error(ErrorKind::Other));
        prop_assert!(storage.checkpoint().is_err());
        prop_assert!(faults.consumed());
        prop_assert_eq!(fs::read(&wal_path)?, before);
        drop(storage);
        let storage = open(&path, 1)?;
        prop_assert!(!bitmap(&path, root));
        prop_assert_eq!(allocate(&storage, None)?, root);
    }

    #[test]
    fn allocation_lifecycle_matches_a_model_across_savepoints_outcomes_and_restarts(
        ops in prop::collection::vec((0u8..6, any::<u8>()), 1..50), commit in any::<bool>(),
    ) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let storage = open(&path, 2)?;
        let mut initial = BTreeMap::new();
        for _ in 0..4 { initial.insert(allocate(&storage, None)?, true); }
        storage.flush()?;
        let txn = storage.begin_transaction()?;
        let mut model = initial.clone();
        let mut savepoint = None;
        for (op, pick) in ops {
            match op {
                0 | 1 => { model.insert(allocate(&storage, Some(txn))?, true); }
                2 => {
                    let live: Vec<_> = model.iter().filter_map(|(&id, &live)| live.then_some(id)).collect();
                    if !live.is_empty() {
                        let id = live[pick as usize % live.len()];
                        storage.page_cache.dealloc_page(Some(txn), id)?;
                        model.insert(id, false);
                    }
                }
                3 => { savepoint = Some((storage.statement_savepoint(txn)?, model.clone())); }
                4 => {
                    if let Some((point, saved)) = &savepoint {
                        storage.rollback_to_savepoint(*point)?;
                        for live in model.values_mut() { *live = false; }
                        model.extend(saved.iter().map(|(&id, &live)| (id, live)));
                    }
                }
                _ => { storage.checkpoint()?; }
            }
        }
        if commit { storage.commit_transaction(txn)?; } else { storage.rollback_transaction(txn)?; }
        drop(storage);
        let storage = open(&path, 1)?;
        for (&id, &live) in &model {
            let expected = if commit { live } else { initial.contains_key(&id) };
            prop_assert_eq!(bitmap(&path, id), expected);
            prop_assert_eq!(storage.page_cache.fetch_page(id).is_ok(), expected);
        }
        // All free slots are available exactly once before the file can grow.
        let free: Vec<_> = model.keys().copied().filter(|id| !bitmap(&path, *id)).collect();
        for id in free { prop_assert_eq!(allocate(&storage, None)?, id); }
    }
}

#[test]
fn page_dealloc_records_are_retained_by_active_checkpoint_and_removed_after_flush() -> TestCaseResult
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let id = allocate(&storage, None)?;
    storage.flush()?;
    let txn = storage.begin_transaction()?;
    storage.page_cache.dealloc_page(Some(txn), id)?;
    storage.checkpoint()?;
    prop_assert!(
        read_checkpoint_log(&path)?.records.iter().any(
            |r| matches!(r.kind, RecoveryLogRecordKind::PageDealloc { page_id } if page_id == id)
        ),
        "missing page deallocation record"
    );
    storage.commit_transaction(txn)?;
    prop_assert_eq!(storage.checkpoint()?, CheckpointOutcome::Completed);
    prop_assert!(!bitmap(&path, id));
    prop_assert!(read_checkpoint_log(&path)?.records.is_empty());
    Ok(())
}

#[test]
fn failed_lifecycle_logging_keeps_pages_reserved_and_poisoned_transactions_cannot_commit()
-> TestCaseResult {
    for allocating in [false, true] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let storage = open(&path, 2)?;
        let id = allocate(&storage, None)?;
        storage.flush()?;
        drop(storage);
        crate::log_manager::truncate_wal(&path, u64::MAX - 1, 0)?;
        let storage = open(&path, 1)?;
        let txn = storage.begin_transaction()?;
        if allocating {
            prop_assert!(allocate(&storage, Some(txn)).is_err());
        } else {
            prop_assert!(storage.page_cache.dealloc_page(Some(txn), id).is_err());
        }
        prop_assert!(storage.transaction_is_poisoned(txn)?);
        prop_assert!(storage.commit_transaction(txn).is_err());
        prop_assert!(storage.page_cache.fetch_page(id).is_ok());
        prop_assert!(bitmap(&path, id));
    }
    Ok(())
}

#[test]
fn recycled_cached_frames_are_zeroed_and_never_overwrite_the_new_owner() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let storage = open(&path, 2)?;
    let txn = storage.begin_transaction()?;
    let (id, pin) = storage.page_cache.new_page(Some(txn))?;
    pin.write(Some(txn))?.page_mut().fill(17);
    drop(pin);
    storage.commit_transaction(txn)?;
    let txn = storage.begin_transaction()?;
    storage.page_cache.dealloc_page(Some(txn), id)?;
    storage.commit_transaction(txn)?;
    let txn = storage.begin_transaction()?;
    let (reused, pin) = storage.page_cache.new_page(Some(txn))?;
    prop_assert_eq!(reused, id);
    prop_assert_eq!(*pin.read()?.page(), [0; PAGE_SIZE]);
    pin.write(Some(txn))?.page_mut().fill(31);
    drop(pin);
    for _ in 0..32 {
        allocate(&storage, Some(txn))?;
    }
    storage.commit_transaction(txn)?;
    storage.checkpoint()?;
    drop(storage);
    let storage = open(&path, 1)?;
    prop_assert_eq!(*storage.page_cache.fetch_page(id)?.read()?.page(), [31; PAGE_SIZE]);
    Ok(())
}

#[test]
fn recovery_reconciles_bitmap_writes_that_outlive_their_file_extension() -> TestCaseResult {
    for commit in [true, false] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let storage = open(&path, 2)?;
        let txn = storage.begin_transaction()?;
        let root = allocate(&storage, Some(txn))?;
        if commit {
            storage.commit_transaction(txn)?;
        }
        storage.flush()?;
        prop_assert!(bitmap(&path, root));
        drop(storage);
        // Retain the new bitmap sector, lose only the extension metadata.
        fs::OpenOptions::new().write(true).open(&path)?.set_len(PAGE_SIZE as u64)?;
        let storage = open(&path, 1)?;
        prop_assert_eq!(storage.page_cache.fetch_page(root).is_ok(), commit);
        if !commit {
            prop_assert_eq!(allocate(&storage, None)?, root);
        }
    }
    Ok(())
}

#[test]
fn recovery_rejects_deallocation_of_metadata_or_missing_pages_without_extending_the_file()
-> TestCaseResult {
    use crate::log_manager::{LogManager, LogRecordKind};
    let dir = tempfile::tempdir()?;
    for id in [0, 1, PAGES_PER_EXTENT, u64::MAX] {
        let path = dir.path().join(format!("db-{id}"));
        drop(open(&path, 2)?);
        let before = fs::read(&path)?;
        let mut log = LogManager::new(&path)?;
        log.append_record(1, LogRecordKind::Begin)?;
        log.append_record(1, LogRecordKind::PageDealloc { page_id: id })?;
        let lsn = log.append_record(1, LogRecordKind::Commit)?;
        log.flush_through(lsn)?;
        drop(log);
        let wal_path = path.with_added_extension("wal");
        let wal = fs::read(&wal_path)?;
        prop_assert!(open(&path, 1).is_err());
        prop_assert_eq!(fs::read(&path)?, before);
        prop_assert_eq!(fs::read(&wal_path)?, wal);
    }
    Ok(())
}
