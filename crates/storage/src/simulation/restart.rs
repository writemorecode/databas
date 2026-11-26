//! Focused crash sweeps alongside the randomized workload. Cut power after
//! every recovery I/O, then check recovery from that intermediate state.

use std::{io, path::Path};

use crate::{
    PAGE_SIZE, PageId, Storage, StorageOptions,
    disk_manager::DiskManager,
    error::{InternalError, InvariantViolation, StorageError},
    filesystem::{
        FileSystem, OpenMode,
        memory::{Effect, Fault, MemoryFs, Operation},
    },
    page::format::{KIND_OFFSET, PageKind},
    page_allocator::PageAllocator,
};

use super::{RunError, RunResult, oracle};

const DATABASE: &str = "restart.db";

// Regression for a WAL rename that reached the OS cache but whose directory
// sync failed. Found with seed 3 at event 118445.
pub(super) fn sweep_unpublished_wal() -> RunResult<()> {
    let fs = MemoryFs::default();
    let storage = open(&fs, OpenMode::CreateNew)?;
    let mut committed = storage.create_tree()?;
    let root = committed.root_page_id();
    committed.insert(b"key", b"committed")?;
    let txn = storage.begin_transaction()?;
    let mut loser = storage.transaction_tree_cursor(txn, root)?;
    loser.update(b"key", &[0x5c; 5000])?;
    storage.flush()?;

    fs.arm(Fault {
        skip: 0,
        effect: Effect::Error {
            operation: Operation::SyncDirectory,
            kind: io::ErrorKind::Other,
            after: false,
        },
    });
    if !matches!(storage.checkpoint(), Err(StorageError::Io(error)) if error.kind() == io::ErrorKind::Other)
        || fs.take_hit().is_none()
    {
        return Err(RunError::Mismatch(
            "checkpoint did not leave an unpublished WAL rename".into(),
        ));
    }
    let failed = fs.restart(false);
    drop(loser);
    drop(committed);
    drop(storage);
    sweep_recovery_cuts(&failed, "unpublished WAL", |_, storage| {
        let mut tree = storage.tree_cursor(root)?;
        let value =
            tree.get(b"key")?.map(|record| record.with_value(<[u8]>::to_vec)).transpose()?;
        if value.as_deref() != Some(b"committed") {
            return Err(RunError::Mismatch("lost the committed value".into()));
        }
        Ok(())
    })
}

pub(super) fn sweep_retained_structure() -> RunResult<()> {
    let fs = MemoryFs::default();
    let storage = open(&fs, OpenMode::CreateNew)?;
    let mut tree = storage.create_tree()?;
    let root = tree.root_page_id();
    let value = [0x5c; 499];
    let mut expected = vec![(1..=6).map(|key| (vec![key], value.to_vec())).collect()];
    for key in 1..=6 {
        tree.insert(&[key], &value)?;
    }
    storage.checkpoint()?;

    let txn = storage.begin_transaction()?;
    let savepoint = storage.statement_savepoint(txn)?;
    let mut loser = storage.transaction_tree_cursor(txn, root)?;
    loser.insert(&[7], &value)?;
    let before_split = storage.statement_savepoint(txn)?;
    fs.arm(Fault {
        skip: 0,
        effect: Effect::Error {
            operation: Operation::Resize,
            kind: io::ErrorKind::Other,
            after: false,
        },
    });
    if loser.insert(&[8], &value).is_ok() || fs.take_hit().is_none() {
        return Err(RunError::Mismatch("resize fault did not interrupt the split".into()));
    }
    if !matches!(
        storage.commit_transaction(txn),
        Err(StorageError::Internal(InternalError::InvariantViolation(
            InvariantViolation::UnfinishedTreeAction { .. }
        )))
    ) {
        return Err(RunError::Mismatch("commit did not reject an unfinished split".into()));
    }
    storage.rollback_to_savepoint(before_split)?;
    loser.insert(&[8], &value)?; // Splits before inserting; moves the earlier row too.
    storage.checkpoint()?; // WAL retained for undo even though the pages are durable.
    check_layout(&fs, root, PageKind::RawInterior, 3)?;
    sweep_recovery_cuts(&fs, "retained split", |fs, storage| {
        oracle::reconcile(storage, &[root], &mut expected, None)?;
        check_layout(fs, root, PageKind::RawInterior, 3)
    })?;

    storage.rollback_to_savepoint(savepoint)?;
    storage.commit_transaction(txn)?;
    storage.checkpoint()?;
    oracle::reconcile(&storage, &[root], &mut expected, None)?;
    check_layout(&fs, root, PageKind::RawInterior, 3)?;

    let txn = storage.begin_transaction()?;
    let mut loser = storage.transaction_tree_cursor(txn, root)?;
    loser.update(&[6], &[0x11; 499])?;
    loser.delete(&[1])?; // Merges and shrinks the root without preserving old slots.
    storage.checkpoint()?;
    // Unlinked children must not be reusable while the transaction is open.
    check_layout(&fs, root, PageKind::RawLeaf, 3)?;
    sweep_recovery_cuts(&fs, "retained merge", |fs, storage| {
        oracle::reconcile(storage, &[root], &mut expected, None)?;
        check_layout(fs, root, PageKind::RawLeaf, 1)
    })?;
    storage.rollback_transaction(txn)?;
    storage.checkpoint()?;
    oracle::reconcile(&storage, &[root], &mut expected, None)?;
    check_layout(&fs, root, PageKind::RawLeaf, 1)?;

    let txn = storage.begin_transaction()?;
    let mut newborn = storage.transaction_create_tree(txn)?;
    let newborn_root = newborn.root_page_id();
    for key in 1..=8 {
        newborn.insert(&[key], &value)?;
    }
    storage.checkpoint()?;
    check_layout(&fs, newborn_root, PageKind::RawInterior, 4)?;
    storage.rollback_transaction(txn)?;
    storage.checkpoint()?;
    check_layout(&fs, root, PageKind::RawLeaf, 1)
}

/// Overflow rebuilds exceed the small cache and force fragments of the open
/// split to WAL. Cut during the action, not just after its completion record.
pub(super) fn sweep_split_prefixes() -> RunResult<()> {
    let fs = MemoryFs::default();
    let storage = open(&fs, OpenMode::CreateNew)?;
    let mut tree = storage.create_tree()?;
    let root = tree.root_page_id();
    let value = [0x53; 8000];
    let mut expected = vec![(1..=7).map(|key| (vec![key], value.to_vec())).collect()];
    for key in 1..=7 {
        tree.insert(&[key], &value)?;
    }
    storage.checkpoint()?;
    check_layout(&fs, root, PageKind::RawLeaf, 15)?;

    let run_split = |fs: &MemoryFs, cut| -> RunResult<u64> {
        let storage = open(fs, OpenMode::Existing)?;
        let txn = storage.begin_transaction()?;
        let mut cursor = storage.transaction_tree_cursor(txn, root)?;
        let before = fs.io_count();
        if let Some(skip) = cut {
            fs.arm(Fault { skip, effect: Effect::PowerLoss { after: true } });
            let _ = cursor.insert(&[8], &value);
            if fs.take_hit().is_none() {
                return Err(RunError::Mismatch(format!("split cut {skip} did not trigger")));
            }
        } else {
            cursor.insert(&[8], &value)?;
        }
        Ok(fs.io_count() - before)
    };

    let calls = run_split(&fs.fork_for_recovery(), None)?;
    for cut in 0..calls {
        let device = fs.fork_for_recovery();
        run_split(&device, Some(cut))?;
        let restarted = device.restart(true);
        let mut check = || -> RunResult<()> {
            let storage = open(&restarted, OpenMode::Existing)?;
            oracle::reconcile(&storage, &[root], &mut expected, None)?;
            let (kind, reserved) = read_layout(&restarted, root)?;
            if (kind, reserved) != (PageKind::RawLeaf as u8, 15)
                && (kind, reserved) != (PageKind::RawInterior as u8, 17)
            {
                return Err(RunError::Mismatch(format!(
                    "unexpected split layout tag {kind} with {reserved} reservations"
                )));
            }
            Ok(())
        };
        check().map_err(|error| RunError::Mismatch(format!("split prefix cut {cut}: {error}")))?;
    }
    Ok(())
}

fn check_layout(fs: &MemoryFs, root: PageId, kind: PageKind, reserved: usize) -> RunResult<()> {
    let (actual_kind, actual) = read_layout(fs, root)?;
    if actual_kind != kind as u8 || actual != reserved {
        return Err(RunError::Mismatch(format!(
            "expected {kind:?} root and {reserved} reserved pages, got tag {actual_kind} and {actual}",
        )));
    }
    Ok(())
}

fn read_layout(fs: &MemoryFs, root: PageId) -> RunResult<(u8, usize)> {
    let fs = FileSystem::Memory(fs.fork_for_recovery());
    let path = Path::new(DATABASE);
    let mut disk = DiskManager::open(&fs, path, OpenMode::Existing).map_err(StorageError::from)?;
    disk.lock_exclusive().map_err(StorageError::from)?;
    if !disk.repair_doublewrite(&fs, path).map_err(StorageError::from)? {
        return Err(RunError::Mismatch("layout probe is missing doublewrite".into()));
    }
    let mut page = [0; PAGE_SIZE];
    disk.read_page(root, &mut page).map_err(StorageError::from)?;
    let allocator = PageAllocator::load(&mut disk)?;
    let actual = (0..disk.page_count())
        .filter(|&id| !PageAllocator::is_metadata(id) && allocator.is_allocated(id))
        .count();
    Ok((page[KIND_OFFSET], actual))
}

fn sweep_recovery_cuts(
    base: &MemoryFs,
    scenario: &str,
    mut verify: impl FnMut(&MemoryFs, &Storage) -> RunResult<()>,
) -> RunResult<()> {
    let probe = base.fork_for_recovery();
    let before = probe.io_count();
    drop(open(&probe, OpenMode::Existing)?);
    let recovery_calls = probe.io_count() - before;

    for skip in 0..recovery_calls {
        let fs = base.fork_for_recovery();
        fs.arm(Fault { skip, effect: Effect::PowerLoss { after: true } });
        let first = open(&fs, OpenMode::Existing);
        if fs.take_hit().is_none() {
            return Err(RunError::Mismatch(format!(
                "{scenario}: recovery cut {skip} did not trigger"
            )));
        }
        let restarted = fs.restart(true);
        drop(first);
        open(&restarted, OpenMode::Existing)
            .map_err(RunError::from)
            .and_then(|storage| verify(&restarted, &storage))
            .map_err(|error| {
                RunError::Mismatch(format!("{scenario}: restart after cut {skip}: {error}"))
            })?;
    }
    Ok(())
}

fn open(fs: &MemoryFs, mode: OpenMode) -> Result<Storage, StorageError> {
    Storage::open_in(
        FileSystem::Memory(fs.clone()),
        Path::new(DATABASE),
        StorageOptions { cache_frames: 16 },
        mode,
    )
}
