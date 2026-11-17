//! Public API reclamation and recovery, including abrupt process exits.
use std::{fs, path::Path, process::Command};

use databas_storage::{CheckpointOutcome, Storage, StorageOptions, TreeCursor};

const OPTIONS: StorageOptions = StorageOptions { cache_frames: 8 };
const PAGE_SIZE: usize = 4096;
// Enough for leaf splits/merges, multi-page overflow, eviction, and full
// doublewrite batches. Retain larger, multi-level trees in the stress test.
const ROWS: u32 = 12;

#[allow(clippy::unwrap_used)]
fn open(path: &Path) -> Storage {
    Storage::open_or_create_with_options(path, OPTIONS).unwrap()
}

fn key(id: u32) -> Vec<u8> {
    let mut bytes = id.to_be_bytes().to_vec();
    bytes.resize(700, 0xa5);
    bytes
}

fn value(id: u32) -> Vec<u8> {
    vec![id.to_le_bytes()[0]; 9000]
}

#[allow(clippy::unwrap_used)]
fn seed(storage: &Storage, rows: u32) -> u64 {
    let txn = storage.begin_transaction().unwrap();
    let mut tree = storage.transaction_create_tree(txn).unwrap();
    let root = tree.root_page_id();
    for id in 0..rows {
        tree.insert(&key(id), &value(id)).unwrap();
    }
    storage.commit_transaction(txn).unwrap();
    storage.checkpoint().unwrap();
    root
}

#[allow(clippy::unwrap_used)]
fn verify(tree: &mut TreeCursor, rows: u32) {
    for id in 0..rows {
        assert_eq!(
            tree.get(&key(id))
                .unwrap()
                .unwrap()
                .with_key_value(|k, v| (k.to_vec(), v.to_vec()))
                .unwrap(),
            (key(id), value(id))
        );
    }
    assert!(tree.seek_to_first().unwrap());
    let mut current = tree.current().unwrap();
    for id in 0..rows {
        assert_eq!(current.unwrap().with_key_value(|k, _| k.to_vec()).unwrap(), key(id));
        current = tree.next_record().unwrap();
    }
    assert!(current.is_none());
}

#[allow(clippy::unwrap_used)]
fn allocated_count(path: &Path) -> usize {
    let bytes = fs::read(path).unwrap();
    let extent_pages = (PAGE_SIZE - 16) * 8;
    bytes
        .chunks(PAGE_SIZE * extent_pages)
        .map(|extent| {
            extent[16..PAGE_SIZE].iter().map(|byte| byte.count_ones() as usize).sum::<usize>()
        })
        .sum()
}

#[allow(clippy::unwrap_used)]
fn copy_checkpointed_database(source: &Path, destination: &Path) {
    fs::copy(source, destination).unwrap();
    fs::copy(source.with_added_extension("wal"), destination.with_added_extension("wal")).unwrap();
}

#[test]
fn destroying_a_tree_reclaims_every_leaf_interior_and_overflow_page() {
    check_tree_destruction(ROWS);
}

#[allow(clippy::unwrap_used)]
fn check_tree_destruction(rows: u32) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = open(&path);
    let root = seed(&storage, rows);
    let mut sentinel = storage.create_tree().unwrap();
    let sentinel_root = sentinel.root_page_id();
    sentinel.insert(b"keep", b"alive").unwrap();
    storage.flush().unwrap();
    let txn = storage.begin_transaction().unwrap();
    storage.transaction_destroy_tree(txn, root).unwrap();
    storage.commit_transaction(txn).unwrap();
    assert_eq!(storage.checkpoint().unwrap(), CheckpointOutcome::Completed);
    assert_eq!(allocated_count(&path), 2); // Header and sentinel root.
    let mut reused = storage.create_tree().unwrap();
    assert_eq!(reused.root_page_id(), root);
    assert!(!reused.seek_to_first().unwrap());
    reused.insert(b"new", b"owner").unwrap();
    assert_eq!(
        sentinel.get(b"keep").unwrap().unwrap().with_key_value(|_, v| v.to_vec()).unwrap(),
        b"alive"
    );
    storage.flush().unwrap();
    drop(reused);
    drop(sentinel);
    drop(storage);
    let storage = open(&path);
    assert_eq!(
        storage
            .tree_cursor(sentinel_root)
            .unwrap()
            .get(b"keep")
            .unwrap()
            .unwrap()
            .with_key_value(|_, v| v.to_vec())
            .unwrap(),
        b"alive"
    );
}

#[test]
fn deleting_all_records_reclaims_merges_root_children_and_overflow_chains() {
    check_record_deletion(ROWS);
}

#[allow(clippy::unwrap_used)]
fn check_record_deletion(rows: u32) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = open(&path);
    let root = seed(&storage, rows);
    let txn = storage.begin_transaction().unwrap();
    let mut tree = storage.transaction_tree_cursor(txn, root).unwrap();
    for id in 0..rows {
        tree.delete(&key(id)).unwrap();
    }
    assert_eq!(tree.root_page_id(), root);
    assert!(!tree.seek_to_first().unwrap());
    storage.commit_transaction(txn).unwrap();
    storage.checkpoint().unwrap();
    assert_eq!(allocated_count(&path), 2); // Header and stable empty root.
    let size = fs::metadata(&path).unwrap().len();
    let txn = storage.begin_transaction().unwrap();
    tree = storage.transaction_tree_cursor(txn, root).unwrap();
    for id in 0..rows {
        tree.insert(&key(id), &value(id)).unwrap();
    }
    storage.commit_transaction(txn).unwrap();
    storage.checkpoint().unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        size,
        "insertion grew the file instead of reusing freed pages"
    );
    verify(&mut tree, rows);
}

#[test]
fn savepoint_and_full_rollback_restore_deleted_trees_and_their_overflow() {
    check_rollback(ROWS);
}

#[allow(clippy::unwrap_used)]
fn check_rollback(rows: u32) {
    let fixture_dir = tempfile::tempdir().unwrap();
    let fixture = fixture_dir.path().join("db");
    let storage = open(&fixture);
    let root = seed(&storage, rows);
    drop(storage);
    let before = allocated_count(&fixture);

    for commit in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        copy_checkpointed_database(&fixture, &path);
        let storage = open(&path);
        let txn = storage.begin_transaction().unwrap();
        let point = storage.statement_savepoint(txn).unwrap();
        storage.transaction_destroy_tree(txn, root).unwrap();
        storage.rollback_to_savepoint(point).unwrap();
        verify(&mut storage.transaction_tree_cursor(txn, root).unwrap(), rows);
        let mut tree = storage.transaction_tree_cursor(txn, root).unwrap();
        for id in 0..rows {
            tree.delete(&key(id)).unwrap();
        }
        storage.rollback_to_savepoint(point).unwrap();
        verify(&mut tree, rows);
        if commit {
            storage.commit_transaction(txn).unwrap();
        } else {
            storage.rollback_transaction(txn).unwrap();
        }
        drop(tree);
        drop(storage);
        let storage = open(&path);
        verify(&mut storage.tree_cursor(root).unwrap(), rows);
        assert_eq!(allocated_count(&path), before);
    }
}

#[test]
fn overflow_value_replacement_reuses_its_old_chains_without_leaks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = open(&path);
    let mut tree = storage.create_tree().unwrap();
    tree.insert(b"key", &vec![7; 12000]).unwrap();
    for i in 0..12 {
        let txn = storage.begin_transaction().unwrap();
        tree = storage.transaction_tree_cursor(txn, tree.root_page_id()).unwrap();
        tree.update(b"key", &vec![i; if i % 2 == 0 { 18000 } else { 12000 }]).unwrap();
        storage.commit_transaction(txn).unwrap();
    }
    let txn = storage.begin_transaction().unwrap();
    tree = storage.transaction_tree_cursor(txn, tree.root_page_id()).unwrap();
    tree.update(b"key", b"inline").unwrap();
    storage.commit_transaction(txn).unwrap();
    storage.checkpoint().unwrap();
    assert_eq!(allocated_count(&path), 2);
    assert_eq!(
        tree.get(b"key").unwrap().unwrap().with_key_value(|_, v| v.to_vec()).unwrap(),
        b"inline"
    );
}

#[test]
fn crash_probe() {
    let Some(path) = std::env::var_os("DATABAS_REUSE_CRASH_PATH") else {
        return;
    };
    let mode = std::env::var("DATABAS_REUSE_CRASH_MODE").unwrap();
    let root: u64 = std::env::var("DATABAS_REUSE_CRASH_ROOT").unwrap().parse().unwrap();
    let rows: u32 = std::env::var("DATABAS_REUSE_CRASH_ROWS").unwrap().parse().unwrap();
    let storage = open(Path::new(&path));
    let txn = storage.begin_transaction().unwrap();
    if mode.starts_with("destroy") {
        storage.transaction_destroy_tree(txn, root).unwrap();
    } else {
        let mut tree = storage.transaction_tree_cursor(txn, root).unwrap();
        for id in 0..rows {
            tree.delete(&key(id)).unwrap();
        }
    }
    // Persist uncommitted pages and reservations, retaining active undo history.
    assert_eq!(storage.checkpoint().unwrap(), CheckpointOutcome::Partial);
    if mode.ends_with("commit") {
        storage.commit_transaction(txn).unwrap();
    }
    // No destructors: committed recovery must use WAL; losers must undo STEAL.
    std::process::exit(0);
}

#[test]
fn abrupt_exit_recovers_tree_deletion_and_merges_for_winners_and_losers() {
    check_crash_recovery(ROWS);
}

#[allow(clippy::unwrap_used)]
fn check_crash_recovery(rows: u32) {
    let fixture_dir = tempfile::tempdir().unwrap();
    let fixture = fixture_dir.path().join("db");
    let storage = open(&fixture);
    let root = seed(&storage, rows);
    drop(storage);

    for mode in ["destroy-commit", "destroy-loser", "merge-commit", "merge-loser"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // Start each child from its own copy of the checkpointed database and WAL.
        copy_checkpointed_database(&fixture, &path);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_probe", "--nocapture"])
            .env("DATABAS_REUSE_CRASH_PATH", &path)
            .env("DATABAS_REUSE_CRASH_MODE", mode)
            .env("DATABAS_REUSE_CRASH_ROOT", root.to_string())
            .env("DATABAS_REUSE_CRASH_ROWS", rows.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let storage = open(&path);
        let mut tree = storage.tree_cursor(root).unwrap();
        if mode.ends_with("loser") {
            verify(&mut tree, rows);
        } else if mode.starts_with("merge") {
            assert!(!tree.seek_to_first().unwrap());
            assert_eq!(allocated_count(&path), 2);
        } else {
            assert!(tree.seek_to_first().is_err());
            assert_eq!(allocated_count(&path), 1);
            assert_eq!(storage.create_tree().unwrap().root_page_id(), root);
        }
    }
}

#[test]
#[ignore = "fsync-heavy multi-level reclamation/recovery stress test; run with --ignored"]
fn large_trees_reclaim_and_recover_without_leaks() {
    // Run the original larger fixtures through the same assertions, including
    // all four abrupt-exit modes and both savepoint/full rollback outcomes.
    check_tree_destruction(72);
    check_record_deletion(72);
    check_rollback(72);
    check_crash_recovery(72);
}
