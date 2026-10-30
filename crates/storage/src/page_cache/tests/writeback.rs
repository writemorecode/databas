use std::{fs, sync::Arc};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{PageCache, check_disk, disk_image, image, setup, setup_runtime, snapshot};
use crate::log_manager::{RecoveryLogRecordKind, read_checkpoint_log};
use crate::{
    CheckpointOutcome, PAGE_SIZE, disk_manager::DiskManager, storage_runtime::StorageRuntime,
};

#[test]
fn failed_page_reads_preserve_the_victim_and_allow_retry() -> TestCaseResult {
    let (dir, cache, mut model) = setup(1, 2)?;
    let path = dir.path().join("cache.db");
    let pin = cache.fetch_page(1)?;
    pin.write(None)?.page_mut().copy_from_slice(&image(91));
    model[0] = image(91);
    drop(pin);
    // A short backing-file read exercises a real I/O failure without a mock cache.
    fs::OpenOptions::new().write(true).open(&path)?.set_len(2 * PAGE_SIZE as u64)?;
    prop_assert!(cache.fetch_page(2).is_err());
    prop_assert_eq!(snapshot(&cache.fetch_page(1)?)?, model[0]);
    // Repair the missing backing page and retry through the same cache.
    let mut disk = DiskManager::open_existing(&path)?;
    disk.ensure_page_exists(2)?;
    disk.write_page(2, &model[1])?;
    prop_assert_eq!(snapshot(&cache.fetch_page(2)?)?, model[1]);
    prop_assert_eq!(snapshot(&cache.fetch_page(1)?)?, model[0]);
    check_disk(&path, &model)
}

#[test]
fn checkpoint_flushes_other_pages_while_a_dirty_page_remains_pinned() -> TestCaseResult {
    for held in [0usize, 1] {
        let (dir, cache, mut model) = setup(2, 2)?;
        let path = dir.path().join("cache.db");
        let mut pins = vec![];
        for (id, bytes) in model.iter_mut().enumerate() {
            let pin = cache.fetch_page(id as u64 + 1)?;
            bytes[100] ^= 0xff;
            pin.write(None)?.page_mut().copy_from_slice(bytes);
            pins.push(pin);
        }
        let pinned = pins.swap_remove(held);
        drop(pins);
        prop_assert_eq!(cache.flush_checkpoint()?, Some(0));
        prop_assert_ne!(disk_image(&path, held as u64)?, model[held].to_vec());
        prop_assert_eq!(disk_image(&path, (1 - held) as u64)?, model[1 - held].to_vec());
        drop(pinned);
        prop_assert_eq!(cache.flush_checkpoint()?, None);
        check_disk(&path, &model)?;
    }
    Ok(())
}

#[test]
fn checkpoint_defers_pinned_writes_and_retains_their_redo_until_flushed() -> TestCaseResult {
    let (dir, runtime, mut model) = setup_runtime(2, 0)?;
    let path = dir.path().join("cache.db");
    let cache = PageCache::new(runtime.clone(), 2)?;
    let txn = runtime.begin_transaction()?;
    let pin = cache.fetch_page(1)?;
    // Clean pins and no-op guards must not hold back a checkpoint.
    prop_assert_eq!(cache.flush_checkpoint()?, None);
    drop(pin.write(Some(txn))?);
    prop_assert_eq!(cache.flush_checkpoint()?, None);
    let first_update = runtime.next_wal_lsn()?;
    let mut write = pin.write(Some(txn))?;
    write.page_mut().copy_from_slice(&image(32));
    prop_assert_eq!(cache.flush_checkpoint()?, Some(first_update));
    prop_assert_eq!(
        runtime.checkpoint(|| Ok(cache.flush_checkpoint()?))?,
        CheckpointOutcome::Partial
    );
    drop(write);
    pin.write(Some(txn))?.page_mut().copy_from_slice(&image(64));
    drop(pin.write(Some(txn))?); // A later no-op must preserve the earlier dependency.
    model[0] = image(64);
    prop_assert_eq!(cache.flush_checkpoint()?, Some(first_update));
    runtime.commit_transaction(txn)?;
    prop_assert_eq!(
        runtime.checkpoint(|| Ok(cache.flush_checkpoint()?))?,
        CheckpointOutcome::Partial
    );
    prop_assert_ne!(disk_image(&path, 0)?, model[0].to_vec());
    let retained = read_checkpoint_log(&path)?;
    prop_assert!(retained.records.iter().any(|record| matches!(&record.kind,
        RecoveryLogRecordKind::PageUpdate { page_id: 1, redo_data, .. } if redo_data.as_ref() == &model[0])), "checkpoint discarded deferred redo");
    drop(pin);
    prop_assert_eq!(
        runtime.checkpoint(|| Ok(cache.flush_checkpoint()?))?,
        CheckpointOutcome::Completed
    );
    check_disk(&path, &model)?;
    prop_assert!(read_checkpoint_log(&path)?.records.is_empty());
    drop(cache);
    drop(runtime);
    let runtime = Arc::new(StorageRuntime::new(path.clone(), DiskManager::open_existing(&path)?)?);
    let reopened = PageCache::new(runtime, 1)?;
    prop_assert_eq!(snapshot(&reopened.fetch_page(1)?)?, model[0]);
    Ok(())
}

#[test]
fn dirty_eviction_flushes_wal_and_reopen_replays_transaction_outcomes() -> TestCaseResult {
    for outcome in ["incomplete", "commit", "rollback"] {
        let (dir, runtime, mut model) = setup_runtime(2, 0)?;
        let path = dir.path().join("cache.db");
        let cache = PageCache::new(runtime.clone(), 1)?;
        let txn = runtime.begin_transaction()?;
        let pin = cache.fetch_page(1)?;
        pin.write(Some(txn))?.page_mut().copy_from_slice(&image(82));
        pin.write(Some(txn))?.page_mut().copy_from_slice(&image(83));
        drop(pin);
        drop(cache.fetch_page(2)?); // One frame forces dirty eviction.
        prop_assert_eq!(disk_image(&path, 0)?, image(83).to_vec());
        // Observe the WAL while its writer is still alive: eviction must have
        // flushed the undo/redo dependency, not merely relied on writer drop.
        let scan = read_checkpoint_log(&path)?;
        for (before, after) in [(model[0], image(82)), (image(82), image(83))] {
            prop_assert!(
                scan.records.iter().any(|record| matches!(&record.kind,
                RecoveryLogRecordKind::PageUpdate { page_id: 1, redo_data, undo_data }
                if redo_data.as_ref() == &after && undo_data.as_ref() == &before)),
                "evicted page has no readable WAL dependency"
            );
        }
        if outcome == "commit" {
            runtime.commit_transaction(txn)?;
            model[0] = image(83);
        } else if outcome == "rollback" {
            let rollback = runtime.prepare_rollback_pages(txn)?;
            cache.restore_rollback_pages(rollback.pages)?;
            for page_id in rollback.flush_page_ids {
                cache.flush_page(page_id)?;
            }
            runtime.sync_database_file()?;
            runtime.finish_rollback(txn)?;
            prop_assert_eq!(snapshot(&cache.fetch_page(1)?)?, model[0]);
        }
        drop(cache);
        drop(runtime);
        let runtime =
            Arc::new(StorageRuntime::new(path.clone(), DiskManager::open_existing(&path)?)?);
        let reopened = PageCache::new(runtime, 1)?;
        prop_assert_eq!(snapshot(&reopened.fetch_page(1)?)?, model[0]);
        check_disk(&path, &model)?;
    }
    Ok(())
}

#[test]
fn cached_btree_bytes_match_the_logged_redo_including_its_lsn() -> TestCaseResult {
    use crate::page::{Leaf, RawLeaf, Write, format};

    let (dir, runtime, _) = setup_runtime(2, 0)?;
    let path = dir.path().join("cache.db");
    let cache = PageCache::new(runtime.clone(), 1)?;
    let pin = cache.fetch_page(1)?;
    RawLeaf::<Write<'_>>::initialize(pin.write(None)?.page_mut());
    let before = snapshot(&pin)?;
    let txn = runtime.begin_transaction()?;
    let lsn = runtime.next_wal_lsn()?;
    pin.write(Some(txn))?.open_mut::<Leaf>()?.insert_payload_at(0, 1, 1, None, b"kv")?;
    let after = snapshot(&pin)?;
    prop_assert_eq!(format::read_u64(&after, format::LSN_OFFSET), lsn);
    drop(pin);
    drop(cache.fetch_page(2)?);
    prop_assert_eq!(disk_image(&path, 0)?, after.to_vec());
    prop_assert!(
        read_checkpoint_log(&path)?.records.iter().any(|record| matches!(&record.kind,
        RecoveryLogRecordKind::PageUpdate { page_id: 1, redo_data, undo_data }
        if record.lsn == lsn && redo_data.as_ref() == &after && undo_data.as_ref() == &before)),
        "cache bytes differ from WAL images"
    );
    Ok(())
}

#[test]
fn failed_logging_restores_bytes_without_losing_earlier_dirty_changes() -> TestCaseResult {
    for previously_dirty in [false, true] {
        let (dir, runtime, mut model) = setup_runtime(1, u64::MAX - 1)?;
        let path = dir.path().join("cache.db");
        let cache = PageCache::new(runtime.clone(), 1)?;
        let pin = cache.fetch_page(1)?;
        if previously_dirty {
            pin.write(None)?.page_mut().copy_from_slice(&image(18));
            model[0] = image(18);
        }
        let txn = runtime.begin_transaction()?; // The final representable LSN.
        pin.write(Some(txn))?.page_mut().copy_from_slice(&image(47));
        prop_assert!(runtime.transaction_is_poisoned(txn)?);
        prop_assert!(runtime.commit_transaction(txn).is_err());
        prop_assert_eq!(snapshot(&pin)?, model[0]);
        drop(pin);
        cache.flush_all()?;
        check_disk(&path, &model)?;
    }
    Ok(())
}
