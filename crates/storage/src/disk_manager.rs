use std::{
    io::{Read, Seek, Write},
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc};
use thiserror::Error;

use crate::doublewrite::Doublewrite;
use crate::filesystem::{File, FileSystem, OpenMode};
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
        let page_count = Self::file_page_count(&file)?;
        Ok(Self { file, page_count, locked: false, doublewrite: None })
    }

    /// Acquire the process-wide exclusive lock held for an open database.
    pub(crate) fn lock_exclusive(&mut self) -> DiskManagerResult<()> {
        if !self.locked {
            self.file.try_lock()?;
            // Another owner may have grown or initialized the file since open.
            // Only the length observed under ownership is safe for allocation.
            self.page_count = Self::file_page_count(&self.file)?;
            self.locked = true;
        }
        Ok(())
    }

    fn file_page_count(file: &File) -> DiskManagerResult<u64> {
        let size = file.len()?;
        if !size.is_multiple_of(PAGE_SIZE as u64) {
            return Err(DiskManagerError::InvalidFileSize { size });
        }
        Ok(size / PAGE_SIZE as u64)
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

    /// Repair before trusting any home page. A missing sidecar is reported
    /// separately so the caller can detect an incompatible database format.
    pub(crate) fn repair_doublewrite(
        &mut self,
        fs: &FileSystem,
        path: &Path,
    ) -> DiskManagerResult<bool> {
        debug_assert!(self.locked && self.doublewrite.is_none());
        let Some(mut protection) = Doublewrite::open(fs, path, &self.file)? else {
            return Ok(false);
        };
        let pending = protection.pending()?;
        let mut required_len = self.page_count * PAGE_SIZE as u64;
        // Validate the entire staged prefix before performing any home I/O.
        for (page_id, image) in &pending {
            Self::verify_checksum(*page_id, image)?;
            required_len = required_len.max(Self::page_end(*page_id)?);
        }
        if required_len > self.page_count * PAGE_SIZE as u64 {
            self.file.set_len(required_len)?;
            self.page_count = required_len / PAGE_SIZE as u64;
        }
        for (page_id, image) in pending {
            // The existing slot protects this repair, including another crash.
            Self::write_home(&mut self.file, page_id, &image)?;
        }
        self.file.sync_all()?;
        protection.retire()?;
        self.doublewrite = Some(protection);
        Ok(true)
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

pub fn missing_doublewrite() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "missing database doublewrite sidecar")
}
