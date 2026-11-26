//! Exercise repair directly, before WAL redo can mask a regressed home page.

use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    Lsn, PAGE_SIZE,
    database_header::DatabaseHeader,
    disk_manager::{DiskManager, DiskManagerResult},
    error::{StorageError, StorageResult},
    filesystem::{
        FileSystem, OpenMode,
        memory::{Effect, Fault, MemoryFs, Operation},
    },
    page_recovery::{PageInitKind, PageRecovery},
    types::PAGE_CHECKSUM_OFFSET,
};

use super::{RunError, RunResult};

const PATH: &str = "repair.db";
type Page = [u8; PAGE_SIZE];

fn data(incarnation: Lsn, lsn: Lsn, marker: u8) -> Page {
    let mut page = PageInitKind::Overflow.image(incarnation);
    PageRecovery::stamp_lsn(&mut page, lsn);
    page[100] = marker;
    page
}

fn header(lsn: Lsn) -> Page {
    let mut page = DatabaseHeader::encode_page();
    PageRecovery::stamp_lsn(&mut page, lsn);
    page
}

fn scenario() -> DiskManagerResult<(MemoryFs, Vec<Page>)> {
    let device = MemoryFs::default();
    let fs = FileSystem::Memory(device.clone());
    let path = Path::new(PATH);
    let mut disk = DiskManager::open(&fs, path, OpenMode::CreateNew)?;
    disk.lock_exclusive()?;
    disk.create_doublewrite(&fs, path)?;
    let mut expected = vec![
        header(25),
        data(22, 25, 1),
        data(10, 25, 2),
        data(10, 25, 3),
        data(80, 80, 4),
        data(10, 10, 5),
    ];
    for page in &expected {
        disk.new_page(page)?;
    }
    disk.sync()?;

    // Power cuts keep the synced slots, but lose these home writes. Deliberately
    // offer stale snapshots: an older bitmap, prior occupant, and older page LSN.
    disk.write_page(0, &header(20))?;
    disk.write_page(1, &data(10, 25, 9))?;
    disk.write_page(2, &data(10, 20, 9))?;
    // Same address and stamp, different bytes. Last slot wins the tie, not
    // the whole batch: a later, obsolete slot must never be installed.
    expected[3] = data(10, 25, 33);
    disk.write_page(3, &data(10, 25, 30))?;
    disk.write_page(3, &expected[3])?;
    disk.write_page(3, &data(10, 20, 9))?;
    expected[4] = data(30, 30, 44);
    expected[5] = data(30, 30, 55);
    disk.write_page(4, &expected[4])?;
    disk.write_page(5, &expected[5])?;
    let cut = device.restart(true);
    let mut home = cut.open(path, OpenMode::Existing)?;
    // A tear leaves an apparently newer trailer. Only the checksum can tell.
    home.seek(SeekFrom::Start(4 * PAGE_SIZE as u64 + 100))?;
    home.write_all(&[0xff])?;
    home.set_len(5 * PAGE_SIZE as u64 + 111)?;
    home.sync_all()?;
    Ok((cut, expected))
}

fn repair(fs: &MemoryFs) -> StorageResult<DiskManager> {
    let fs = FileSystem::Memory(fs.clone());
    let path = Path::new(PATH);
    let mut disk = DiskManager::open(&fs, path, OpenMode::Existing)?;
    disk.lock_exclusive()?;
    let pending = disk.prepare_doublewrite(&fs, path)?;
    disk.repair_doublewrite(pending)?;
    Ok(disk)
}

fn verify(disk: &mut DiskManager, expected: &[Page]) -> RunResult<()> {
    for (id, page) in expected.iter().enumerate() {
        let mut actual = [0; PAGE_SIZE];
        disk.read_page(id as u64, &mut actual).map_err(StorageError::from)?;
        if &actual != page {
            return Err(RunError::Mismatch(format!("wrong doublewrite image for page {id}")));
        }
    }
    Ok(())
}

// Inspect the OS cache at the cut, before another repair (or losing dirty
// sectors) can hide a transient rewind of one of the four intact home pages.
fn verify_intermediate(fs: &MemoryFs, expected: &[Page]) -> RunResult<()> {
    let mut file = fs.open(Path::new(PATH), OpenMode::Existing).map_err(StorageError::from)?;
    let mut page = [0; PAGE_SIZE];
    for (id, final_image) in expected.iter().take(4).enumerate() {
        file.read_exact(&mut page).map_err(StorageError::from)?;
        page[PAGE_CHECKSUM_OFFSET..].fill(0);
        if &page != final_image && !(id == 3 && page == data(10, 25, 3)) {
            return Err(RunError::Mismatch(format!("intermediate rewind of page {id}")));
        }
    }
    Ok(())
}

pub(super) fn sweep_repair() -> RunResult<()> {
    let (base, expected) = scenario().map_err(StorageError::from)?;
    let probe = base.fork_for_recovery();
    let before = probe.io_count();
    let mut disk = repair(&probe)?;
    let calls = probe.io_count() - before;
    verify(&mut disk, &expected)?;

    for skip in 0..calls {
        let fs = base.fork_for_recovery();
        fs.arm(Fault { skip, effect: Effect::PowerLoss { after: true } });
        let _ = repair(&fs);
        if fs.take_hit().is_none() {
            return Err(RunError::Mismatch(format!("doublewrite cut {skip} did not trigger")));
        }
        verify_intermediate(&fs.fork_for_recovery(), &expected)
            .map_err(|e| RunError::Mismatch(format!("doublewrite cut {skip}: {e}")))?;
        let mut disk = repair(&fs.restart(true))?;
        verify(&mut disk, &expected)
            .map_err(|e| RunError::Mismatch(format!("doublewrite repair cut {skip}: {e}")))?;
    }

    // Fail each read in turn (including home verification). EIO is not a tear.
    for skip in 0..calls {
        let fs = base.fork_for_recovery();
        fs.arm(Fault {
            skip,
            effect: Effect::Error {
                operation: Operation::Read,
                kind: io::ErrorKind::Other,
                after: false,
            },
        });
        let result = repair(&fs);
        if fs.take_hit().is_none() {
            result?;
            break;
        }
        if !matches!(result, Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::Other) {
            return Err(RunError::Mismatch(format!("doublewrite swallowed read error {skip}")));
        }
        let mut disk = repair(&fs.restart(true))?;
        verify(&mut disk, &expected)?;
    }
    Ok(())
}
