use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use crate::{
    error::{StorageError, StorageResult},
    types::{PAGE_SIZE, PageId},
};

/// Reads and writes pages to and from a database file.
pub(crate) struct DiskManager {
    file: File,
    page_count: u64,
}

impl DiskManager {
    /// Create a new `DiskManager` from a path to a file.
    pub(crate) fn new(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .append(false)
            .open(path)?;

        let file_metadata = file.metadata()?;
        let file_size = file_metadata.len();

        if !file_size.is_multiple_of(PAGE_SIZE as u64) {
            return Err(StorageError::InvalidFileSize(file_size));
        }

        let page_count = file_size / (PAGE_SIZE as u64);

        Ok(Self { file, page_count })
    }

    /// Extends the database file by one page.
    /// Returns page ID of the new page.
    pub(crate) fn new_page(&mut self) -> StorageResult<PageId> {
        let page_id = self.page_count;
        let new_page_id = page_id + 1;
        let new_file_size = Self::page_offset(new_page_id);
        self.file.set_len(new_file_size)?;
        self.file.sync_all()?;
        self.page_count = self.page_count + 1;
        Ok(page_id)
    }

    /// Read page `page_id` from disk and store it in `buf`.
    pub(crate) fn read_page(
        &mut self,
        page_id: PageId,
        buf: &mut [u8; PAGE_SIZE],
    ) -> StorageResult<()> {
        if page_id >= self.page_count {
            return Err(StorageError::InvalidPageId(page_id));
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
    ) -> StorageResult<()> {
        if page_id >= self.page_count {
            return Err(StorageError::InvalidPageId(page_id));
        }
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Calculate disk offset for page `page_id`.
    fn page_offset(page_id: PageId) -> u64 {
        u64::from(page_id) * (PAGE_SIZE as u64)
    }
}
