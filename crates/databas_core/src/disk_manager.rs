use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use crate::{
    error::{StorageError, StorageResult},
    page_checksum::{checksum_matches, write_page_checksum},
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
        let mut buf = [0u8; PAGE_SIZE];
        write_page_checksum(&mut buf);
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        self.page_count += 1;
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
        if !checksum_matches(buf) {
            return Err(StorageError::InvalidPageChecksum(page_id));
        }
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
        let mut canonical_buf = *buf;
        write_page_checksum(&mut canonical_buf);
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(&canonical_buf)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Calculate disk offset for page `page_id`.
    fn page_offset(page_id: PageId) -> u64 {
        page_id * (PAGE_SIZE as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::page_checksum::{PAGE_DATA_END, write_page_checksum};
    use fastrand::Rng;
    use std::{
        fs::OpenOptions,
        io::{Seek, Write},
    };
    use tempfile::NamedTempFile;

    fn random_page_buffer(rng: &mut Rng) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        for c in &mut buf[..PAGE_DATA_END] {
            *c = rng.u8(..);
        }
        write_page_checksum(&mut buf);
        buf
    }

    #[test]
    fn can_read_written_page_after_closing_file() {
        let mut rng = fastrand::Rng::new();

        let write_buf = random_page_buffer(&mut rng);
        let file = NamedTempFile::new().unwrap();
        let page_id = {
            let mut dm = DiskManager::new(file.path()).unwrap();
            let page_id = dm.new_page().unwrap();
            dm.write_page(page_id, &write_buf).unwrap();
            page_id
        };
        let mut dm = DiskManager::new(file.path()).unwrap();
        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();
        assert_eq!(read_buf, write_buf);
        assert_eq!(dm.page_count, 1);
    }

    #[test]
    fn cannot_read_out_of_bounds_page() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        let page_id = 5000;
        let read = dm.read_page(page_id, &mut buf);
        assert!(matches!(read, Err(StorageError::InvalidPageId(id)) if id == page_id));
    }

    #[test]
    fn cannot_write_out_of_bounds_page() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let buf = [0u8; PAGE_SIZE];
        let page_id = 5000;
        let write = dm.write_page(page_id, &buf);
        assert!(matches!(write, Err(StorageError::InvalidPageId(id)) if id == page_id));
    }

    #[test]
    fn new_rejects_files_with_invalid_size() {
        let file = NamedTempFile::new().unwrap();
        let invalid_size = (PAGE_SIZE - 1) as u64;
        file.as_file().set_len(invalid_size).unwrap();

        let dm = DiskManager::new(file.path());
        assert!(matches!(
            dm,
            Err(StorageError::InvalidFileSize(size)) if size == invalid_size
        ));
    }

    #[test]
    fn new_page_allocates_sequential_page_ids_and_persists_count() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            assert_eq!(dm.new_page().unwrap(), 0);
            assert_eq!(dm.new_page().unwrap(), 1);
            assert_eq!(dm.new_page().unwrap(), 2);
            assert_eq!(dm.page_count, 3);
        }

        let mut dm = DiskManager::new(file.path()).unwrap();
        assert_eq!(dm.page_count, 3);

        let mut buf = [0u8; PAGE_SIZE];
        let mut expected = [0u8; PAGE_SIZE];
        write_page_checksum(&mut expected);
        for page_id in 0..dm.page_count {
            dm.read_page(page_id, &mut buf).unwrap();
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn page_id_equal_to_page_count_is_out_of_bounds() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let page_id = dm.new_page().unwrap();
        assert_eq!(page_id, 0);

        let invalid_page_id = dm.page_count;
        let mut read_buf = [0u8; PAGE_SIZE];
        let write_buf = [7u8; PAGE_SIZE];

        let read = dm.read_page(invalid_page_id, &mut read_buf);
        assert!(matches!(
            read,
            Err(StorageError::InvalidPageId(id)) if id == invalid_page_id
        ));

        let write = dm.write_page(invalid_page_id, &write_buf);
        assert!(matches!(
            write,
            Err(StorageError::InvalidPageId(id)) if id == invalid_page_id
        ));
    }

    #[test]
    fn multi_page_reads_and_writes_do_not_overlap() {
        let file = NamedTempFile::new().unwrap();
        let mut rng = fastrand::Rng::new();
        let mut dm = DiskManager::new(file.path()).unwrap();

        let page_count = 4_u64;
        for expected_page_id in 0..page_count {
            assert_eq!(dm.new_page().unwrap(), expected_page_id);
        }

        let write_bufs: Vec<[u8; PAGE_SIZE]> =
            (0..page_count).map(|_| random_page_buffer(&mut rng)).collect();

        for (index, buf) in write_bufs.iter().enumerate() {
            dm.write_page(index as PageId, buf).unwrap();
        }

        for (index, expected_buf) in write_bufs.iter().enumerate() {
            let mut read_buf = [0u8; PAGE_SIZE];
            dm.read_page(index as PageId, &mut read_buf).unwrap();
            assert_eq!(&read_buf, expected_buf);
        }
    }

    #[test]
    fn writing_a_page_twice_persists_the_latest_contents() {
        let file = NamedTempFile::new().unwrap();
        let mut rng = fastrand::Rng::new();

        let (page_id, expected_buf) = {
            let mut dm = DiskManager::new(file.path()).unwrap();
            let page_id = dm.new_page().unwrap();
            let first = random_page_buffer(&mut rng);
            let second = random_page_buffer(&mut rng);

            dm.write_page(page_id, &first).unwrap();
            dm.write_page(page_id, &second).unwrap();
            (page_id, second)
        };

        let mut dm = DiskManager::new(file.path()).unwrap();
        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();
        assert_eq!(read_buf, expected_buf);
    }

    #[test]
    fn newly_allocated_pages_are_zero_initialized() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let page_id = dm.new_page().unwrap();

        let mut read_buf = [1u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();
        let mut expected = [0u8; PAGE_SIZE];
        write_page_checksum(&mut expected);
        assert_eq!(read_buf, expected);
    }

    #[test]
    fn read_page_fails_when_checksum_is_invalid() {
        let file = NamedTempFile::new().unwrap();
        let page_id = {
            let mut dm = DiskManager::new(file.path()).unwrap();
            let page_id = dm.new_page().unwrap();
            dm.write_page(page_id, &random_page_buffer(&mut Rng::new())).unwrap();
            page_id
        };

        let mut handle = OpenOptions::new().read(true).write(true).open(file.path()).unwrap();
        handle.seek(std::io::SeekFrom::Start(page_id * PAGE_SIZE as u64)).unwrap();
        handle.write_all(&[0xAA]).unwrap();
        handle.sync_all().unwrap();

        let mut dm = DiskManager::new(file.path()).unwrap();
        let mut read_buf = [0u8; PAGE_SIZE];
        let err = dm.read_page(page_id, &mut read_buf).unwrap_err();
        assert!(matches!(err, StorageError::InvalidPageChecksum(id) if id == page_id));
    }

    #[test]
    fn write_page_overwrites_trailing_checksum_bytes() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let page_id = dm.new_page().unwrap();
        let mut write_buf = [9u8; PAGE_SIZE];
        write_buf[PAGE_DATA_END..].fill(0xEE);

        dm.write_page(page_id, &write_buf).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();
        assert_eq!(&read_buf[..PAGE_DATA_END], &write_buf[..PAGE_DATA_END]);
        assert_ne!(&read_buf[PAGE_DATA_END..], &write_buf[PAGE_DATA_END..]);
    }
}
