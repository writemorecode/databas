//! Exercise startup's format gates on the simulated device, with repair slots
//! occupied. A rejected open must preserve both bytes and absent names.

use std::{
    io::{Read, Write},
    path::Path,
};

use crate::{
    PAGE_SIZE, Storage, StorageOptions,
    database_header::FORMAT_VERSION,
    disk_manager::DiskManager,
    error::{StorageError, StorageResult},
    filesystem::{FileSystem, OpenMode, memory::MemoryFs},
    log_manager::WAL_FILE_HEADER_LEN,
};

use super::{RunError, RunResult};

pub(super) fn check_incompatible_opens() -> RunResult<()> {
    let fs = FileSystem::Memory(MemoryFs::default());
    let target = Path::new("target.db");
    let wal = target.with_added_extension("wal");
    let doublewrite = target.with_added_extension("doublewrite");
    let storage = open(&fs, target, OpenMode::CreateNew)?;
    let root = storage.create_tree()?.root_page_id();
    drop(storage);
    let original_wal = bytes(&fs, &wal)?;

    let foreign = Path::new("other.db");
    let other = open(&fs, foreign, OpenMode::CreateNew)?;
    other.create_tree()?;
    let foreign_wal = bytes(&fs, &foreign.with_added_extension("wal"))?;
    other.checkpoint()?;
    let replacement = bytes(&fs, &foreign.with_added_extension("wal"))?;

    // Leave a verified page zero in staging, then tear the home header.
    stage_header(&fs, target, FORMAT_VERSION)?;
    let good_slots = bytes(&fs, &doublewrite)?;
    let mut torn = bytes(&fs, target)?;
    torn[..512].fill(0);
    overwrite(&fs, target, &torn)?;
    let mut unsupported_frame = original_wal.clone();
    unsupported_frame[WAL_FILE_HEADER_LEN + 8..WAL_FILE_HEADER_LEN + 10]
        .copy_from_slice(&u16::MAX.to_le_bytes());
    let mut old_header = original_wal.clone();
    old_header[8..10].copy_from_slice(&8u16.to_le_bytes());
    for incompatible in [foreign_wal.as_slice(), &replacement, &unsupported_frame, &old_header, &[]]
    {
        overwrite(&fs, &wal, incompatible)?;
        require_rejection(&fs, target, OpenMode::Existing)?;
    }
    // Conversely, a foreign doublewrite must not cause an otherwise
    // recoverable WAL tail to be truncated.
    let mut torn_wal = original_wal;
    torn_wal.push(0);
    overwrite(&fs, &wal, &torn_wal)?;
    overwrite(&fs, &doublewrite, &bytes(&fs, &foreign.with_added_extension("doublewrite"))?)?;
    require_rejection(&fs, target, OpenMode::Existing)?;
    overwrite(&fs, &doublewrite, &good_slots)?;
    open(&fs, target, OpenMode::Existing)?.validate_tree_page_formats(root)?;

    // A staged header cannot override an intact incompatible home, nor may
    // an incompatible staged header be installed over a good home. With the
    // WAL absent, even creating an empty header would violate read-only open.
    let good_home = bytes(&fs, target)?;
    stage_header(&fs, target, FORMAT_VERSION - 1)?;
    let incompatible_slots = bytes(&fs, &doublewrite)?;
    fs.rename(&wal, Path::new("saved.wal")).map_err(StorageError::from)?;
    overwrite(&fs, &doublewrite, &good_slots)?;
    require_rejection(&fs, target, OpenMode::Create)?;
    overwrite(&fs, target, &good_home)?;
    overwrite(&fs, &doublewrite, &incompatible_slots)?;
    require_rejection(&fs, target, OpenMode::Existing)?;

    check_short_header(&fs, &foreign_wal)?;

    let fresh = Path::new("fresh.db");
    overwrite(&fs, &fresh.with_added_extension("wal"), &foreign_wal)?;
    require_rejection(&fs, fresh, OpenMode::CreateNew)?;
    require_rejection(&fs, fresh, OpenMode::Create)?;
    overwrite(&fs, fresh, &[])?;
    require_rejection(&fs, fresh, OpenMode::Create)
}

fn check_short_header(fs: &FileSystem, foreign_wal: &[u8]) -> RunResult<()> {
    let path = Path::new("short.db");
    let wal = path.with_added_extension("wal");
    drop(open(fs, path, OpenMode::CreateNew)?);
    let good_wal = bytes(fs, &wal)?;
    stage_header(fs, path, FORMAT_VERSION)?;
    let header = bytes(fs, path)?;
    overwrite(fs, path, &header[..100])?;

    // Even with a usable staged header, no extension or retirement before WAL preflight.
    overwrite(fs, &wal, foreign_wal)?;
    require_rejection(fs, path, OpenMode::Existing)?;
    overwrite(fs, &wal, &good_wal)?;
    drop(open(fs, path, OpenMode::Existing)?);
    if bytes(fs, path)? != header {
        return Err(RunError::Mismatch("short page zero not restored from staging".into()));
    }

    // With staging retired, a short but plausible header prefix is not enough.
    // Preparation must reject it before the otherwise recoverable WAL tail is truncated.
    overwrite(fs, path, &header[..100])?;
    let mut torn_wal = good_wal;
    torn_wal.push(0);
    overwrite(fs, &wal, &torn_wal)?;
    require_rejection(fs, path, OpenMode::Create)
}

fn stage_header(fs: &FileSystem, path: &Path, version: u16) -> StorageResult<()> {
    let mut disk = DiskManager::open(fs, path, OpenMode::Existing)?;
    disk.lock_exclusive()?;
    let repair = disk.prepare_doublewrite(fs, path)?;
    disk.repair_doublewrite(repair)?;
    let mut header = [0; PAGE_SIZE];
    disk.read_page(0, &mut header)?;
    header[8..10].copy_from_slice(&version.to_le_bytes());
    disk.write_page(0, &header)?;
    Ok(())
}

fn open(fs: &FileSystem, path: &Path, mode: OpenMode) -> StorageResult<Storage> {
    Storage::open_in(fs.clone(), path, StorageOptions { cache_frames: 16 }, mode)
}

fn require_rejection(fs: &FileSystem, path: &Path, mode: OpenMode) -> RunResult<()> {
    let snapshot = || -> StorageResult<Vec<_>> {
        [
            path.to_owned(),
            path.with_added_extension("wal"),
            path.with_added_extension("doublewrite"),
            path.with_added_extension("wal.checkpoint"),
        ]
        .iter()
        .map(|p| fs.exists(p)?.then(|| bytes(fs, p)).transpose())
        .collect()
    };
    let before = snapshot()?;
    if open(fs, path, mode).is_ok() || snapshot()? != before {
        return Err(RunError::Mismatch(format!(
            "incompatible {mode:?} of {} accepted or modified files",
            path.display()
        )));
    }
    Ok(())
}

fn bytes(fs: &FileSystem, path: &Path) -> StorageResult<Vec<u8>> {
    let mut bytes = Vec::new();
    fs.open(path, OpenMode::Existing)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn overwrite(fs: &FileSystem, path: &Path, bytes: &[u8]) -> StorageResult<()> {
    fs.open(path, OpenMode::Truncate)?.write_all(bytes)?;
    Ok(())
}
