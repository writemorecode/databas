//! Crash sweep for a WAL replacement whose rename reached the OS cache but
//! whose directory sync failed. Found with seed 3 at event 118445.

use std::{io, path::Path};

use crate::{
    Storage, StorageOptions,
    error::StorageError,
    filesystem::{
        FileSystem, OpenMode,
        memory::{Effect, Fault, MemoryFs, Operation},
    },
};

use super::{RunError, RunResult};

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
    let probe = failed.fork_for_recovery();
    let before = probe.io_count();
    drop(open(&probe, OpenMode::Existing)?);
    let recovery_calls = probe.io_count() - before;

    for skip in 0..recovery_calls {
        let fs = failed.fork_for_recovery();
        fs.arm(Fault { skip, effect: Effect::PowerLoss { after: true } });
        let first = open(&fs, OpenMode::Existing);
        if fs.take_hit().is_none() {
            return Err(RunError::Mismatch(format!("recovery cut {skip} did not trigger")));
        }
        let restarted = fs.restart(true);
        drop(first);
        let storage = open(&restarted, OpenMode::Existing).map_err(|error| {
            RunError::Mismatch(format!("restart after unpublished WAL (cut {skip}): {error}"))
        })?;
        let mut tree = storage.tree_cursor(root)?;
        let value =
            tree.get(b"key")?.map(|record| record.with_value(<[u8]>::to_vec)).transpose()?;
        if value.as_deref() != Some(b"committed") {
            return Err(RunError::Mismatch(format!(
                "unpublished WAL restart (cut {skip}) lost the committed value"
            )));
        }
    }
    Ok(())
}

fn open(fs: &MemoryFs, mode: OpenMode) -> Result<Storage, StorageError> {
    Storage::open_in(
        FileSystem::Memory(fs.clone()),
        Path::new("unpublished-wal.db"),
        StorageOptions { cache_frames: 16 },
        mode,
    )
}
