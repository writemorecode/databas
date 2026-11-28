//! Bounded doublewrite protection for one database file.
//!
//! Slots are appended and synced before their corresponding home writes. All
//! occupied slots stay reserved until a successful home-file sync, followed by
//! synced retirement of the entire batch. Startup validates the staged prefix
//! in append order. DiskManager selects one snapshot per address and compares
//! its recovery trailer only with a checksum-verified home trailer. An incomplete
//! staging suffix cannot have authorized home I/O. Slot checksums cover both
//! the destination and data.
//!
//! Home writes are no-force until an explicit sync or the batch fills. Staging
//! currently syncs each slot; grouping those syncs is a later optimization.
//! This protects torn home writes, not arbitrary corruption. DiskManager seals
//! each immutable snapshot (including its incarnation and page LSN) with an
//! address-bound checksum before staging.

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc};

use crate::filesystem::{File, FileSystem, OpenMode};
use crate::{PAGE_SIZE, PageId, database_header::FORMAT_VERSION};

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
const MAGIC: &[u8; 8] = b"DBDBLWR\0";
const SLOT_MAGIC: &[u8; 8] = b"DBSTAGE\0";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 64;
const SLOT_HEADER_LEN: usize = 24;
const SLOT_LEN: usize = SLOT_HEADER_LEN + PAGE_SIZE + 4;
const MAX_SLOTS: usize = 32;

type PendingPages = Vec<(PageId, [u8; PAGE_SIZE])>;

/// Reject bootstrap mismatches across filesystem objects, not just pathnames.
/// This binding is not a durable database UUID/incarnation. Empty recreations
/// are rejected explicitly. Databases with occupied
/// slots must be recovered before being moved/copied, and unsupported platforms
/// fail closed rather than silently omitting the filesystem identity check.
pub struct Doublewrite {
    file: File,
    poisoned: bool,
    occupied: usize,
}

impl Doublewrite {
    pub(crate) fn create(fs: &FileSystem, path: &Path, database: &File) -> std::io::Result<Self> {
        let bytes = header(database.identity()?);
        let sidecar = path.with_added_extension("doublewrite");
        let mut file = fs.open(&sidecar, OpenMode::CreateNew)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs.sync_parent(&sidecar)?;
        Ok(Self { file, poisoned: false, occupied: 0 })
    }

    pub(crate) fn open(
        fs: &FileSystem,
        path: &Path,
        database: &File,
    ) -> std::io::Result<Option<Self>> {
        let sidecar = path.with_added_extension("doublewrite");
        let mut file = match fs.open(&sidecar, OpenMode::Existing) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if database.len()? == 0 {
            // Do not resurrect a truncated database at a reused pathname/inode.
            return Err(invalid("empty database with existing doublewrite bootstrap"));
        }
        let mut actual = [0; HEADER_LEN];
        file.read_exact(&mut actual)?;
        if actual != header(database.identity()?) {
            return Err(invalid("incompatible or foreign doublewrite header"));
        }
        Ok(Some(Self { file, poisoned: false, occupied: 0 }))
    }

    /// Validate a contiguous prefix before repairing anything. Later retained
    /// sectors after an incomplete slot are not evidence of durable staging.
    pub(crate) fn pending(&mut self) -> std::io::Result<PendingPages> {
        let len = self.file.len()?;
        // Bound scanning/allocation independently of an untrusted crash-tail
        // length. No home write can be authorized by a slot beyond MAX_SLOTS.
        let complete_slots =
            (len.saturating_sub(HEADER_LEN as u64) / SLOT_LEN as u64).min(MAX_SLOTS as u64);
        let mut pending = Vec::new();
        self.file.seek(SeekFrom::Start(HEADER_LEN as u64))?;
        let mut bytes = [0; SLOT_LEN];
        for _ in 0..complete_slots {
            self.file.read_exact(&mut bytes)?;
            let checksum = u32::from_le_bytes(
                bytes[SLOT_LEN - 4..]
                    .try_into()
                    .map_err(|_length| invalid("short slot checksum"))?,
            );
            if CRC.checksum(&bytes[..SLOT_LEN - 4]) != checksum {
                break;
            }
            if &bytes[..8] != SLOT_MAGIC || bytes[16..24] != (PAGE_SIZE as u64).to_le_bytes() {
                return Err(invalid("invalid doublewrite slot descriptor"));
            }
            let page_id = u64::from_le_bytes(
                bytes[8..16].try_into().map_err(|_length| invalid("short slot page id"))?,
            );
            let mut page = [0; PAGE_SIZE];
            page.copy_from_slice(&bytes[SLOT_HEADER_LEN..SLOT_LEN - 4]);
            pending.push((page_id, page));
        }
        self.poisoned = true;
        Ok(pending)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    pub(crate) fn full(&self) -> bool {
        self.occupied == MAX_SLOTS
    }

    pub(crate) fn ensure_healthy(&self) -> std::io::Result<()> {
        if self.poisoned {
            return Err(std::io::Error::other("doublewrite is poisoned; reopen the database"));
        }
        Ok(())
    }

    pub(crate) fn poison(&mut self) {
        self.poisoned = true;
    }

    pub(crate) fn stage(&mut self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        self.ensure_healthy()?;
        self.poisoned = true;
        if self.full() {
            return Err(invalid("doublewrite batch is full"));
        }
        let mut bytes = [0; SLOT_LEN];
        bytes[..8].copy_from_slice(SLOT_MAGIC);
        bytes[8..16].copy_from_slice(&page_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&(PAGE_SIZE as u64).to_le_bytes());
        bytes[SLOT_HEADER_LEN..SLOT_LEN - 4].copy_from_slice(page);
        let checksum = CRC.checksum(&bytes[..SLOT_LEN - 4]);
        bytes[SLOT_LEN - 4..].copy_from_slice(&checksum.to_le_bytes());
        self.file.seek(SeekFrom::Start((HEADER_LEN + self.occupied * SLOT_LEN) as u64))?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        self.occupied += 1;
        Ok(())
    }

    pub(crate) fn home_written(&mut self) {
        // Allow another append, NOT reuse: this slot stays reserved until sync.
        self.poisoned = false;
    }

    /// The caller must have synced the home file, including required extensions.
    pub(crate) fn retire(&mut self) -> std::io::Result<()> {
        self.file.set_len(HEADER_LEN as u64)?;
        self.file.sync_all()?;
        self.poisoned = false;
        self.occupied = 0;
        Ok(())
    }
}

fn header(identity: [u8; 16]) -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes[16..32].copy_from_slice(&identity);
    let checksum = CRC.checksum(&bytes[..HEADER_LEN - 4]);
    bytes[HEADER_LEN - 4..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn invalid(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
