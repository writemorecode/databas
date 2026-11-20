use std::{
    io::{Read, Seek, Write},
    path::Path,
};

use crc::{CRC_32_ISO_HDLC, Crc};
use thiserror::Error;

use crate::filesystem::{File, FileSystem, OpenMode};
use crate::types::PAGE_CONTENT_END;
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
}

impl DiskManager {
    pub(crate) fn open(fs: &FileSystem, path: &Path, mode: OpenMode) -> DiskManagerResult<Self> {
        let file = fs.open(path, mode)?;
        let page_count = Self::file_page_count(&file)?;
        Ok(Self { file, page_count, locked: false })
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

    pub(crate) fn sync(&self) -> DiskManagerResult<()> {
        self.file.sync_all()?;
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
        self.read_page_unchecked(page_id, buf)?;
        let expected = u32::from_le_bytes([
            buf[PAGE_CONTENT_END],
            buf[PAGE_CONTENT_END + 1],
            buf[PAGE_CONTENT_END + 2],
            buf[PAGE_CONTENT_END + 3],
        ]);
        let actual = CRC32.checksum(&buf[..PAGE_CONTENT_END]);
        if expected != actual {
            return Err(DiskManagerError::ChecksumMismatch { page_id, expected, actual });
        }
        buf[PAGE_CONTENT_END..].fill(0);
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
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        debug_assert!(buf[PAGE_CONTENT_END..].iter().all(|&byte| byte == 0));
        let mut page = *buf;
        let checksum = CRC32.checksum(&page[..PAGE_CONTENT_END]);
        page[PAGE_CONTENT_END..].copy_from_slice(&checksum.to_le_bytes());
        self.file.write_all(&page)?;
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
