use std::{
    collections::BTreeMap,
    io::{Read, Seek, Write},
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc};
use thiserror::Error;

use crate::database_header::{DATABASE_HEADER_PAGE_ID, DatabaseHeader, missing_header};
use crate::doublewrite::Doublewrite;
use crate::error::StorageResult;
use crate::filesystem::{File, FileSystem, OpenMode};
use crate::page_recovery::PageRecovery;
use crate::types::PAGE_CHECKSUM_OFFSET;
use crate::{PAGE_SIZE, PageId};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[derive(Debug, Error)]
pub enum DiskManagerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("invalid file size (not multiple of page size): {size}")]
    InvalidFileSize { size: u64 },
    #[error("page {page_id} CRC32 mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    ChecksumMismatch { page_id: PageId, expected: u32, actual: u32 },
}

pub type DiskManagerResult<T> = Result<T, DiskManagerError>;

/// A checked bootstrap and its staged prefix. Preparation must not change either file.
pub struct PendingRepair {
    protection: Doublewrite,
    latest: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    home_len: u64,
    required_len: u64,
}

/// Reads and writes pages to and from a database file.
pub struct DiskManager {
    file: File,
    page_count: u64,
    locked: bool,
    doublewrite: Option<Doublewrite>,
}

impl DiskManager {
    pub(crate) fn open(fs: &FileSystem, path: &Path, mode: OpenMode) -> DiskManagerResult<Self> {
        let file = fs.open(path, mode)?;
        let page_count = file.len()?.div_ceil(PAGE_SIZE as u64);
        Ok(Self { file, page_count, locked: false, doublewrite: None })
    }

    /// Acquire the process-wide exclusive lock held for an open database.
    pub(crate) fn lock_exclusive(&mut self) -> DiskManagerResult<()> {
        if !self.locked {
            self.file.try_lock()?;
            // Another owner may have grown or initialized the file since open.
            // Only the length observed under ownership is safe for allocation.
            // Count a partial final page as nonempty; repair must either
            // supply its complete image or reject the file before use.
            self.page_count = self.file.len()?.div_ceil(PAGE_SIZE as u64);
            self.locked = true;
        }
        Ok(())
    }

    pub(crate) fn create_doublewrite(
        &mut self,
        fs: &FileSystem,
        path: &Path,
    ) -> DiskManagerResult<()> {
        debug_assert!(self.locked && self.page_count == 0 && self.doublewrite.is_none());
        self.doublewrite = Some(Doublewrite::create(fs, path, &self.file)?);
        Ok(())
    }

    pub(crate) fn identity(&self) -> std::io::Result<[u8; 16]> {
        self.file.identity()
    }

    pub(crate) fn prepare_doublewrite(
        &mut self,
        fs: &FileSystem,
        path: &Path,
    ) -> StorageResult<PendingRepair> {
        debug_assert!(self.locked && self.doublewrite.is_none());
        let home_len = self.file.len()?;
        if home_len == 0 {
            return Err(missing_header());
        }
        let complete_pages = home_len / PAGE_SIZE as u64;
        let short_tail = !home_len.is_multiple_of(PAGE_SIZE as u64);
        let mut home = [0; PAGE_SIZE];
        let home_checksum = if complete_pages > 0 {
            self.read_page_unchecked(DATABASE_HEADER_PAGE_ID, &mut home)?;
            let checksum = Self::verify_checksum(DATABASE_HEADER_PAGE_ID, &home);
            if checksum.is_ok() {
                // A staged copy must never mask an intact, incompatible database.
                DatabaseHeader::validate_page(&home)?;
            }
            checksum
        } else {
            Err(DiskManagerError::InvalidFileSize { size: home_len })
        };
        let Some(mut protection) = Doublewrite::open(fs, path, &self.file)? else {
            if short_tail {
                return Err(DiskManagerError::InvalidFileSize { size: home_len }.into());
            }
            // Legacy formats predate the checksum and bootstrap sidecar.
            DatabaseHeader::validate_page(&home)?;
            return Err(missing_doublewrite().into());
        };
        let mut latest = BTreeMap::new();
        let mut required_len = home_len;
        // Validate even superseded slots. Installing intermediate versions would
        // expose them to a second crash during repair.
        for (id, image) in protection.pending()? {
            Self::verify_checksum(id, &image)?;
            if id == DATABASE_HEADER_PAGE_ID {
                DatabaseHeader::validate_page(&image)?;
            }
            required_len = required_len.max(Self::page_end(id)?);
            if latest.get(&id).is_none_or(|prior| version(&image) >= version(prior)) {
                latest.insert(id, image);
            }
        }
        if short_tail && !latest.contains_key(&complete_pages) {
            return Err(DiskManagerError::InvalidFileSize { size: home_len }.into());
        }
        if !latest.contains_key(&DATABASE_HEADER_PAGE_ID) {
            home_checksum?;
        }
        Ok(PendingRepair { protection, latest, home_len, required_len })
    }

    pub(crate) fn repair_doublewrite(&mut self, repair: PendingRepair) -> DiskManagerResult<()> {
        let PendingRepair { mut protection, latest, home_len, required_len } = repair;
        let complete_pages = home_len / PAGE_SIZE as u64;
        if required_len > home_len {
            self.file.set_len(required_len)?;
            self.page_count = required_len / PAGE_SIZE as u64;
        }
        let mut home = [0; PAGE_SIZE];
        for (id, image) in latest {
            if id < complete_pages {
                // Retry partial reads via read_exact. Failure reading a page known
                // to exist is not evidence that its bytes are torn or older.
                self.read_page_unchecked(id, &mut home)?;
                if Self::verify_checksum(id, &home).is_ok()
                    && (version(&home) > version(&image) || home == image)
                {
                    continue;
                }
            }
            // Equal stamps need not imply equal bytes. The last protected write
            // breaks ties (e.g. multiple bitmap changes at one commit LSN).
            Self::write_home(&mut self.file, id, &image)?;
        }
        // Even skipped home writes may only be in the OS cache.
        self.file.sync_all()?;
        protection.retire()?;
        self.doublewrite = Some(protection);
        Ok(())
    }

    pub(crate) fn page_count(&self) -> u64 {
        self.page_count
    }

    pub(crate) fn ensure_page_exists(&mut self, page_id: PageId) -> DiskManagerResult<()> {
        if page_id < self.page_count {
            return Ok(());
        }

        let new_file_size = Self::page_end(page_id)?;
        self.file.set_len(new_file_size)?;
        let first_new_page = self.page_count;
        self.page_count = page_id + 1;
        for id in first_new_page..=page_id {
            self.write_page(id, &[0; PAGE_SIZE])?;
        }
        self.sync()
    }

    pub(crate) fn sync(&mut self) -> DiskManagerResult<()> {
        let protection = self.doublewrite.as_mut().ok_or_else(missing_doublewrite)?;
        protection.ensure_healthy()?;
        if protection.is_empty() {
            self.file.sync_all()?;
        } else {
            Self::sync_and_retire(&self.file, protection)?;
        }
        Ok(())
    }

    fn sync_and_retire(file: &File, protection: &mut Doublewrite) -> DiskManagerResult<()> {
        // Failure leaves the remaining slots unavailable until restart.
        protection.poison();
        file.sync_all()?;
        protection.retire()?;
        Ok(())
    }

    /// Extends the database file by one page.
    /// Returns page ID of the new page.
    pub(crate) fn new_page(&mut self, image: &[u8; PAGE_SIZE]) -> DiskManagerResult<PageId> {
        let page_id = self.page_count;
        let new_file_size = Self::page_end(page_id)?;
        self.file.set_len(new_file_size)?;
        self.page_count += 1;
        self.write_page(page_id, image)?;
        Ok(page_id)
    }

    /// Verify a disk page and return the logical image (with a zero footer).
    pub(crate) fn read_page(
        &mut self,
        page_id: PageId,
        buf: &mut [u8; PAGE_SIZE],
    ) -> DiskManagerResult<()> {
        self.doublewrite.as_ref().ok_or_else(missing_doublewrite)?.ensure_healthy()?;
        self.read_page_unchecked(page_id, buf)?;
        Self::verify_checksum(page_id, buf)?;
        buf[PAGE_CHECKSUM_OFFSET..].fill(0);
        Ok(())
    }

    fn checksum(page_id: PageId, page: &[u8; PAGE_SIZE]) -> u32 {
        let mut digest = CRC32.digest();
        digest.update(&page_id.to_le_bytes());
        digest.update(&page[..PAGE_CHECKSUM_OFFSET]);
        digest.finalize()
    }

    fn verify_checksum(page_id: PageId, buf: &[u8; PAGE_SIZE]) -> DiskManagerResult<()> {
        let expected = u32::from_le_bytes([
            buf[PAGE_CHECKSUM_OFFSET],
            buf[PAGE_CHECKSUM_OFFSET + 1],
            buf[PAGE_CHECKSUM_OFFSET + 2],
            buf[PAGE_CHECKSUM_OFFSET + 3],
        ]);
        let actual = Self::checksum(page_id, buf);
        if expected != actual {
            return Err(DiskManagerError::ChecksumMismatch { page_id, expected, actual });
        }
        Ok(())
    }

    /// Read the raw header page for format detection before recovery.
    pub(crate) fn read_page_unchecked(
        &mut self,
        page_id: PageId,
        buf: &mut [u8; PAGE_SIZE],
    ) -> DiskManagerResult<()> {
        if page_id >= self.page_count {
            return Err(DiskManagerError::InvalidPageId { page_id });
        }
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    /// Write page buffer `buf` to page `page_id`.
    pub(crate) fn write_page(
        &mut self,
        page_id: PageId,
        buf: &[u8; PAGE_SIZE],
    ) -> DiskManagerResult<()> {
        if page_id >= self.page_count {
            return Err(DiskManagerError::InvalidPageId { page_id });
        }
        debug_assert!(buf[PAGE_CHECKSUM_OFFSET..].iter().all(|&byte| byte == 0));
        let mut page = *buf;
        let checksum = Self::checksum(page_id, &page);
        page[PAGE_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());

        let protection = self.doublewrite.as_mut().ok_or_else(missing_doublewrite)?;
        protection.ensure_healthy()?;
        if protection.full() {
            Self::sync_and_retire(&self.file, protection)?;
        }
        protection.stage(page_id, &page)?;
        Self::write_home(&mut self.file, page_id, &page)?;
        protection.home_written();
        Ok(())
    }

    fn write_home(
        file: &mut File,
        page_id: PageId,
        bytes: &[u8; PAGE_SIZE],
    ) -> DiskManagerResult<()> {
        file.seek(std::io::SeekFrom::Start(Self::page_offset(page_id)))?;
        file.write_all(bytes)?;
        Ok(())
    }

    /// Calculate disk offset for page `page_id`.
    fn page_offset(page_id: PageId) -> u64 {
        page_id * (PAGE_SIZE as u64)
    }

    /// Reject impossible extents before a wrapped length can truncate the file.
    fn page_end(page_id: PageId) -> DiskManagerResult<u64> {
        page_id
            .checked_add(1)
            .and_then(|count| count.checked_mul(PAGE_SIZE as u64))
            .ok_or(DiskManagerError::InvalidPageId { page_id })
    }
}

// PageInit LSN orders data-page incarnations. On page zero and the other
// fixed-address bitmaps the incarnation is zero, so only the page LSN varies.
fn version(page: &[u8; PAGE_SIZE]) -> (crate::Lsn, crate::Lsn) {
    let PageRecovery { incarnation, lsn } = PageRecovery::read(page);
    (incarnation, lsn)
}

pub fn missing_doublewrite() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "missing database doublewrite sidecar")
}
