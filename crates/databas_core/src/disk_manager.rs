use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    mem,
    path::Path,
};

use crate::{
    database_header::{DatabaseHeader, DatabaseHeaderError, HEADER_PAGE_ID},
    page_checksum::{PAGE_DATA_END, checksum_matches, write_page_checksum},
    types::{PAGE_SIZE, PageId},
};

const NO_FREELIST_PAGE_ID: PageId = 0;
const FREELIST_NEXT_TRUNK_OFFSET: usize = 0;
const FREELIST_LEAF_COUNT_OFFSET: usize = FREELIST_NEXT_TRUNK_OFFSET + mem::size_of::<PageId>();
const FREELIST_LEAF_ARRAY_OFFSET: usize = FREELIST_LEAF_COUNT_OFFSET + mem::size_of::<u64>();
const FREELIST_TRUNK_CAPACITY: usize =
    (PAGE_DATA_END - FREELIST_LEAF_ARRAY_OFFSET) / mem::size_of::<PageId>();

/// Reads and writes pages to and from a database file.
pub(crate) struct DiskManager {
    file: File,
    page_count: u64,
    freelist_head: Option<PageId>,
    freelist_page_count: u64,
}

impl DiskManager {
    /// Create a new `DiskManager` from a path to a file.
    pub(crate) fn new(path: &Path) -> DiskManagerResult<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .append(false)
            .open(path)?;

        let file_metadata = file.metadata()?;
        let file_size = file_metadata.len();

        if file_size == 0 {
            let mut header_page = [0u8; PAGE_SIZE];
            DatabaseHeader::init_new(&mut header_page);
            file.set_len(PAGE_SIZE as u64)?;
            file.seek(std::io::SeekFrom::Start(0))?;
            file.write_all(&header_page)?;
            file.sync_all()?;
            return Ok(Self { file, page_count: 1, freelist_head: None, freelist_page_count: 0 });
        }

        if !file_size.is_multiple_of(PAGE_SIZE as u64) {
            return Err(DiskManagerError::InvalidFileSize { size: file_size });
        }

        let page_count = file_size / (PAGE_SIZE as u64);
        let mut header_page = [0u8; PAGE_SIZE];
        file.seek(std::io::SeekFrom::Start(Self::page_offset(HEADER_PAGE_ID)))?;
        file.read_exact(&mut header_page)?;
        if !checksum_matches(&header_page) {
            return Err(DiskManagerError::InvalidPageChecksum { page_id: HEADER_PAGE_ID });
        }
        let header =
            DatabaseHeader::read(&header_page).map_err(DiskManagerError::InvalidDatabaseHeader)?;
        header.validate(page_count).map_err(DiskManagerError::InvalidDatabaseHeader)?;

        let mut disk_manager = Self {
            file,
            page_count,
            freelist_head: decode_freelist_page_id(header.freelist_head),
            freelist_page_count: header.freelist_page_count,
        };
        disk_manager.validate_freelist()?;
        Ok(disk_manager)
    }

    /// Extends the database file by one page.
    /// Returns page ID of the new page.
    pub(crate) fn new_page(&mut self) -> DiskManagerResult<PageId> {
        if self.freelist_page_count > 0 {
            return self.allocate_from_freelist();
        }

        let page_id = self.page_count;
        let new_page_id = page_id + 1;
        let new_file_size = Self::page_offset(new_page_id);
        self.file.set_len(new_file_size)?;
        let buf = zero_page_buffer();
        let offset = Self::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        self.file.write_all(&buf)?;
        self.page_count += 1;
        self.write_header_page()?;
        Ok(page_id)
    }

    pub(crate) fn free_page(&mut self, page_id: PageId) -> DiskManagerResult<()> {
        if page_id == HEADER_PAGE_ID || page_id >= self.page_count {
            return Err(DiskManagerError::InvalidPageId { page_id });
        }

        let Some(head_page_id) = self.freelist_head else {
            let mut trunk_page = [0u8; PAGE_SIZE];
            init_freelist_trunk_page(&mut trunk_page, None);
            self.write_page(page_id, &trunk_page)?;
            self.freelist_head = Some(page_id);
            self.freelist_page_count = 1;
            self.write_header_page()?;
            return Ok(());
        };

        let mut head_page = [0u8; PAGE_SIZE];
        self.read_page(head_page_id, &mut head_page)?;
        let leaf_count =
            freelist_trunk_leaf_count(&head_page).map_err(DiskManagerError::InvalidFreelist)?;

        if leaf_count < FREELIST_TRUNK_CAPACITY {
            self.write_page(page_id, &zero_page_buffer())?;
            freelist_trunk_push_leaf(&mut head_page, page_id)
                .map_err(DiskManagerError::InvalidFreelist)?;
            self.write_page(head_page_id, &head_page)?;
        } else {
            let mut new_head_page = [0u8; PAGE_SIZE];
            init_freelist_trunk_page(&mut new_head_page, Some(head_page_id));
            self.write_page(page_id, &new_head_page)?;
            self.freelist_head = Some(page_id);
        }

        self.freelist_page_count += 1;
        self.write_header_page()?;
        Ok(())
    }

    /// Read page `page_id` from disk and store it in `buf`.
    pub(crate) fn read_page(
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
        if !checksum_matches(buf) {
            return Err(DiskManagerError::InvalidPageChecksum { page_id });
        }
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

    fn allocate_from_freelist(&mut self) -> DiskManagerResult<PageId> {
        let Some(head_page_id) = self.freelist_head else {
            return Err(DiskManagerError::InvalidFreelist(FreelistError::CountWithoutHead {
                count: self.freelist_page_count,
            }));
        };
        let mut head_page = [0u8; PAGE_SIZE];
        self.read_page(head_page_id, &mut head_page)?;
        let page_id = if let Some(leaf_page_id) =
            freelist_trunk_pop_leaf(&mut head_page).map_err(DiskManagerError::InvalidFreelist)?
        {
            self.write_page(head_page_id, &head_page)?;
            self.write_page(leaf_page_id, &zero_page_buffer())?;
            leaf_page_id
        } else {
            self.freelist_head = freelist_trunk_next(&head_page);
            self.freelist_page_count -= 1;
            self.write_header_page()?;
            self.write_page(head_page_id, &zero_page_buffer())?;
            return Ok(head_page_id);
        };

        self.freelist_page_count -= 1;
        self.write_header_page()?;
        Ok(page_id)
    }

    fn validate_freelist(&mut self) -> DiskManagerResult<()> {
        if self.freelist_page_count == 0 {
            if let Some(head) = self.freelist_head {
                return Err(DiskManagerError::InvalidFreelist(FreelistError::HeadWithoutCount {
                    head,
                }));
            }
            return Ok(());
        }

        if self.freelist_head.is_none() {
            return Err(DiskManagerError::InvalidFreelist(FreelistError::CountWithoutHead {
                count: self.freelist_page_count,
            }));
        }

        let mut next_trunk = self.freelist_head;
        let mut actual_page_count = 0_u64;

        while let Some(trunk_page_id) = next_trunk {
            validate_freelist_page_id(trunk_page_id, self.page_count)?;
            actual_page_count += 1;

            let mut trunk_page = [0u8; PAGE_SIZE];
            self.read_page(trunk_page_id, &mut trunk_page).map_err(|err| match err {
                DiskManagerError::InvalidPageChecksum { .. } => {
                    DiskManagerError::InvalidFreelist(FreelistError::InvalidChecksum {
                        page_id: trunk_page_id,
                    })
                }
                _ => err,
            })?;
            let leaf_count = freelist_trunk_leaf_count(&trunk_page)
                .map_err(DiskManagerError::InvalidFreelist)?;
            let trunk_next = freelist_trunk_next(&trunk_page);

            if let Some(trunk_next) = trunk_next {
                validate_freelist_page_id(trunk_next, self.page_count)?;
            }

            for leaf_index in 0..leaf_count {
                let leaf_page_id = freelist_trunk_leaf_page_id(&trunk_page, leaf_index);
                validate_freelist_page_id(leaf_page_id, self.page_count)?;
                actual_page_count += 1;
            }

            next_trunk = trunk_next;
        }

        if actual_page_count != self.freelist_page_count {
            self.freelist_page_count = actual_page_count;
            self.write_header_page()?;
        }

        Ok(())
    }

    fn freelist_contains(&mut self, target_page_id: PageId) -> DiskManagerResult<bool> {
        if self.freelist_page_count == 0 {
            return Ok(false);
        }

        let mut next_trunk = self.freelist_head;

        while let Some(trunk_page_id) = next_trunk {
            validate_freelist_page_id(trunk_page_id, self.page_count)?;
            if trunk_page_id == target_page_id {
                return Ok(true);
            }

            let mut trunk_page = [0u8; PAGE_SIZE];
            self.read_page(trunk_page_id, &mut trunk_page).map_err(|err| match err {
                DiskManagerError::InvalidPageChecksum { .. } => {
                    DiskManagerError::InvalidFreelist(FreelistError::InvalidChecksum {
                        page_id: trunk_page_id,
                    })
                }
                _ => err,
            })?;
            let leaf_count = freelist_trunk_leaf_count(&trunk_page)
                .map_err(DiskManagerError::InvalidFreelist)?;

            for leaf_index in 0..leaf_count {
                if freelist_trunk_leaf_page_id(&trunk_page, leaf_index) == target_page_id {
                    return Ok(true);
                }
            }

            next_trunk = freelist_trunk_next(&trunk_page);
        }

        Ok(false)
    }

    fn write_header_page(&mut self) -> DiskManagerResult<()> {
        let mut header_page = [0u8; PAGE_SIZE];
        DatabaseHeader {
            page_size: PAGE_SIZE as u16,
            page_count: self.page_count,
            freelist_head: encode_freelist_page_id(self.freelist_head),
            freelist_page_count: self.freelist_page_count,
        }
        .write(&mut header_page);
        self.file.seek(std::io::SeekFrom::Start(Self::page_offset(HEADER_PAGE_ID)))?;
        self.file.write_all(&header_page)?;
        self.file.sync_all()?;
        Ok(())
    }
}

fn init_freelist_trunk_page(page: &mut [u8; PAGE_SIZE], next_trunk: Option<PageId>) {
    page.fill(0);
    page[FREELIST_NEXT_TRUNK_OFFSET..FREELIST_NEXT_TRUNK_OFFSET + 8]
        .copy_from_slice(&encode_freelist_page_id(next_trunk).to_le_bytes());
    page[FREELIST_LEAF_COUNT_OFFSET..FREELIST_LEAF_COUNT_OFFSET + 8]
        .copy_from_slice(&0_u64.to_le_bytes());
    write_page_checksum(page);
}

fn freelist_trunk_next(page: &[u8; PAGE_SIZE]) -> Option<PageId> {
    decode_freelist_page_id(read_u64(page, FREELIST_NEXT_TRUNK_OFFSET))
}

fn freelist_trunk_leaf_count(page: &[u8; PAGE_SIZE]) -> Result<usize, FreelistError> {
    let leaf_count = read_u64(page, FREELIST_LEAF_COUNT_OFFSET);
    if leaf_count > FREELIST_TRUNK_CAPACITY as u64 {
        return Err(FreelistError::LeafCountTooLarge {
            count: leaf_count,
            max: FREELIST_TRUNK_CAPACITY,
        });
    }

    Ok(leaf_count as usize)
}

fn freelist_trunk_leaf_page_id(page: &[u8; PAGE_SIZE], leaf_index: usize) -> PageId {
    let offset = FREELIST_LEAF_ARRAY_OFFSET + (leaf_index * mem::size_of::<PageId>());
    read_u64(page, offset)
}

fn freelist_trunk_push_leaf(
    page: &mut [u8; PAGE_SIZE],
    page_id: PageId,
) -> Result<(), FreelistError> {
    let leaf_count = freelist_trunk_leaf_count(page)?;
    if leaf_count >= FREELIST_TRUNK_CAPACITY {
        return Err(FreelistError::LeafCountTooLarge {
            count: (leaf_count + 1) as u64,
            max: FREELIST_TRUNK_CAPACITY,
        });
    }

    let offset = FREELIST_LEAF_ARRAY_OFFSET + (leaf_count * mem::size_of::<PageId>());
    page[offset..offset + 8].copy_from_slice(&page_id.to_le_bytes());
    page[FREELIST_LEAF_COUNT_OFFSET..FREELIST_LEAF_COUNT_OFFSET + 8]
        .copy_from_slice(&((leaf_count + 1) as u64).to_le_bytes());
    write_page_checksum(page);
    Ok(())
}

fn freelist_trunk_pop_leaf(page: &mut [u8; PAGE_SIZE]) -> Result<Option<PageId>, FreelistError> {
    let leaf_count = freelist_trunk_leaf_count(page)?;
    if leaf_count == 0 {
        return Ok(None);
    }

    let leaf_index = leaf_count - 1;
    let page_id = freelist_trunk_leaf_page_id(page, leaf_index);
    let offset = FREELIST_LEAF_ARRAY_OFFSET + (leaf_index * mem::size_of::<PageId>());
    page[offset..offset + 8].fill(0);
    page[FREELIST_LEAF_COUNT_OFFSET..FREELIST_LEAF_COUNT_OFFSET + 8]
        .copy_from_slice(&(leaf_index as u64).to_le_bytes());
    write_page_checksum(page);
    Ok(Some(page_id))
}

fn validate_freelist_page_id(page_id: PageId, page_count: u64) -> DiskManagerResult<()> {
    if page_id == HEADER_PAGE_ID {
        return Err(DiskManagerError::InvalidFreelist(FreelistError::HeaderPageInFreelist));
    }
    if page_id >= page_count {
        return Err(DiskManagerError::InvalidFreelist(FreelistError::InvalidPageId { page_id }));
    }
    Ok(())
}

fn zero_page_buffer() -> [u8; PAGE_SIZE] {
    let mut buf = [0u8; PAGE_SIZE];
    write_page_checksum(&mut buf);
    buf
}

fn decode_freelist_page_id(page_id: PageId) -> Option<PageId> {
    (page_id != NO_FREELIST_PAGE_ID).then_some(page_id)
}

fn encode_freelist_page_id(page_id: Option<PageId>) -> PageId {
    page_id.unwrap_or(NO_FREELIST_PAGE_ID)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DiskManagerError {
    #[error("i/o error")]
    Io(#[source] std::io::Error),
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: u64 },
    #[error("page is already free: {page_id}")]
    PageAlreadyFree { page_id: u64 },
    #[error("invalid file size: {size}")]
    InvalidFileSize { size: u64 },
    #[error("invalid page checksum: {page_id}")]
    InvalidPageChecksum { page_id: u64 },
    #[error("invalid database header: {0}")]
    InvalidDatabaseHeader(#[source] DatabaseHeaderError),
    #[error("invalid freelist: {0}")]
    InvalidFreelist(#[source] FreelistError),
}

impl From<std::io::Error> for DiskManagerError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub(crate) enum FreelistError {
    #[error("freelist head is zero but free page count is {count}")]
    CountWithoutHead { count: u64 },
    #[error("freelist head {head} present but free page count is zero")]
    HeadWithoutCount { head: u64 },
    #[error("freelist page id {page_id} is invalid")]
    InvalidPageId { page_id: u64 },
    #[error("header page cannot appear in freelist")]
    HeaderPageInFreelist,
    #[error("freelist trunk leaf count {count} exceeds maximum {max}")]
    LeafCountTooLarge { count: u64, max: usize },
    #[error("invalid checksum on freelist page {page_id}")]
    InvalidChecksum { page_id: u64 },
}

impl FreelistError {
    pub(crate) fn page_id(&self) -> Option<u64> {
        match *self {
            Self::CountWithoutHead { .. } => None,
            Self::HeadWithoutCount { head } => Some(head),
            Self::InvalidPageId { page_id } | Self::InvalidChecksum { page_id } => Some(page_id),
            Self::HeaderPageInFreelist => Some(HEADER_PAGE_ID),
            Self::LeafCountTooLarge { .. } => None,
        }
    }
}

impl From<FreelistError> for crate::error::CorruptionKind {
    fn from(err: FreelistError) -> Self {
        match err {
            FreelistError::CountWithoutHead { count } => Self::FreelistCountWithoutHead { count },
            FreelistError::HeadWithoutCount { head } => Self::FreelistHeadWithoutCount { head },
            FreelistError::InvalidPageId { page_id } => Self::InvalidFreelistPageId { page_id },
            FreelistError::HeaderPageInFreelist => Self::HeaderPageInFreelist,
            FreelistError::LeafCountTooLarge { count, max } => {
                Self::FreelistLeafCountTooLarge { count, max }
            }
            FreelistError::InvalidChecksum { page_id } => Self::InvalidFreelistChecksum { page_id },
        }
    }
}
pub(crate) type DiskManagerResult<T> = Result<T, DiskManagerError>;

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        database_header::{
            DatabaseHeader, DatabaseHeaderError, FIRST_DATA_PAGE_ID, HEADER_PAGE_ID,
        },
        page_checksum::{PAGE_DATA_END, write_page_checksum},
    };
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

    fn write_raw_page(path: &Path, page_id: PageId, page: &[u8; PAGE_SIZE]) {
        let mut handle = OpenOptions::new().read(true).write(true).open(path).unwrap();
        handle.seek(std::io::SeekFrom::Start(page_id * PAGE_SIZE as u64)).unwrap();
        handle.write_all(page).unwrap();
        handle.sync_all().unwrap();
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
        assert_eq!(dm.page_count, 2);
    }

    #[test]
    fn cannot_read_out_of_bounds_page() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        let page_id = 5000;
        let read = dm.read_page(page_id, &mut buf);
        assert!(matches!(
            read,
            Err(DiskManagerError::InvalidPageId { page_id: id }) if id == page_id
        ));
    }

    #[test]
    fn cannot_write_out_of_bounds_page() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let buf = [0u8; PAGE_SIZE];
        let page_id = 5000;
        let write = dm.write_page(page_id, &buf);
        assert!(matches!(
            write,
            Err(DiskManagerError::InvalidPageId { page_id: id }) if id == page_id
        ));
    }

    #[test]
    fn new_rejects_files_with_invalid_size() {
        let file = NamedTempFile::new().unwrap();
        let invalid_size = (PAGE_SIZE - 1) as u64;
        file.as_file().set_len(invalid_size).unwrap();

        let dm = DiskManager::new(file.path());
        assert!(matches!(
            dm,
            Err(DiskManagerError::InvalidFileSize { size }) if size == invalid_size
        ));
    }

    #[test]
    fn new_page_allocates_sequential_page_ids_and_persists_count() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            assert_eq!(dm.new_page().unwrap(), 1);
            assert_eq!(dm.new_page().unwrap(), 2);
            assert_eq!(dm.new_page().unwrap(), 3);
            assert_eq!(dm.page_count, 4);
        }

        let mut dm = DiskManager::new(file.path()).unwrap();
        assert_eq!(dm.page_count, 4);

        let mut header_page = [0u8; PAGE_SIZE];
        dm.read_page(HEADER_PAGE_ID, &mut header_page).unwrap();
        let header = DatabaseHeader::read(&header_page).unwrap();
        assert_eq!(header.page_count, 4);

        let mut buf = [0u8; PAGE_SIZE];
        let mut expected = [0u8; PAGE_SIZE];
        write_page_checksum(&mut expected);
        for page_id in FIRST_DATA_PAGE_ID..dm.page_count {
            dm.read_page(page_id, &mut buf).unwrap();
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn page_id_equal_to_page_count_is_out_of_bounds() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let page_id = dm.new_page().unwrap();
        assert_eq!(page_id, FIRST_DATA_PAGE_ID);

        let invalid_page_id = dm.page_count;
        let mut read_buf = [0u8; PAGE_SIZE];
        let write_buf = [7u8; PAGE_SIZE];

        let read = dm.read_page(invalid_page_id, &mut read_buf);
        assert!(matches!(
            read,
            Err(DiskManagerError::InvalidPageId { page_id: id }) if id == invalid_page_id
        ));

        let write = dm.write_page(invalid_page_id, &write_buf);
        assert!(matches!(
            write,
            Err(DiskManagerError::InvalidPageId { page_id: id }) if id == invalid_page_id
        ));
    }

    #[test]
    fn multi_page_reads_and_writes_do_not_overlap() {
        let file = NamedTempFile::new().unwrap();
        let mut rng = fastrand::Rng::new();
        let mut dm = DiskManager::new(file.path()).unwrap();

        let page_count = 4_u64;
        for expected_page_id in FIRST_DATA_PAGE_ID..(FIRST_DATA_PAGE_ID + page_count) {
            assert_eq!(dm.new_page().unwrap(), expected_page_id);
        }

        let write_bufs: Vec<[u8; PAGE_SIZE]> =
            (0..page_count).map(|_| random_page_buffer(&mut rng)).collect();

        for (index, buf) in write_bufs.iter().enumerate() {
            dm.write_page((index as PageId) + FIRST_DATA_PAGE_ID, buf).unwrap();
        }

        for (index, expected_buf) in write_bufs.iter().enumerate() {
            let mut read_buf = [0u8; PAGE_SIZE];
            dm.read_page((index as PageId) + FIRST_DATA_PAGE_ID, &mut read_buf).unwrap();
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
        assert_eq!(page_id, FIRST_DATA_PAGE_ID);

        let mut read_buf = [1u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();
        let mut expected = [0u8; PAGE_SIZE];
        write_page_checksum(&mut expected);
        assert_eq!(read_buf, expected);
    }

    #[test]
    fn new_initializes_database_header_page() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        assert_eq!(dm.page_count, 1);
        assert_eq!(dm.freelist_head, None);

        let mut header_page = [0u8; PAGE_SIZE];
        dm.read_page(HEADER_PAGE_ID, &mut header_page).unwrap();
        let header = DatabaseHeader::read(&header_page).unwrap();
        assert_eq!(header.page_count, 1);
        assert_eq!(header.page_size, PAGE_SIZE as u16);
        assert_eq!(header.freelist_head, 0);
        assert_eq!(header.freelist_page_count, 0);
    }

    #[test]
    fn free_page_reuses_page_id_before_growing_file() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let first = dm.new_page().unwrap();
        let second = dm.new_page().unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        dm.free_page(first).unwrap();
        assert_eq!(dm.page_count, 3);
        assert_eq!(dm.freelist_head, Some(first));

        let reused = dm.new_page().unwrap();
        assert_eq!(reused, first);
        assert_eq!(dm.page_count, 3);
        assert_eq!(dm.freelist_head, None);

        let mut page = [1u8; PAGE_SIZE];
        dm.read_page(reused, &mut page).unwrap();
        assert_eq!(page, zero_page_buffer());
    }

    #[test]
    fn free_page_reuses_pages_in_lifo_order() {
        let file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(file.path()).unwrap();
        let page1 = dm.new_page().unwrap();
        let page2 = dm.new_page().unwrap();
        let page3 = dm.new_page().unwrap();

        dm.free_page(page1).unwrap();
        dm.free_page(page2).unwrap();
        dm.free_page(page3).unwrap();

        assert_eq!(dm.new_page().unwrap(), page3);
        assert_eq!(dm.new_page().unwrap(), page2);
        assert_eq!(dm.new_page().unwrap(), page1);
    }

    #[test]
    fn freelist_state_persists_across_reopen() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            let page1 = dm.new_page().unwrap();
            let page2 = dm.new_page().unwrap();
            dm.free_page(page1).unwrap();
            dm.free_page(page2).unwrap();
        }

        let mut dm = DiskManager::new(file.path()).unwrap();
        assert_eq!(dm.new_page().unwrap(), 2);
        assert_eq!(dm.new_page().unwrap(), 1);
    }

    #[test]
    fn open_rejects_freelist_count_without_head() {
        let file = NamedTempFile::new().unwrap();
        let mut header_page = [0u8; PAGE_SIZE];
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            dm.new_page().unwrap();
            dm.read_page(HEADER_PAGE_ID, &mut header_page).unwrap();
        }

        let mut header = DatabaseHeader::read(&header_page).unwrap();
        header.freelist_page_count = 1;
        header.freelist_head = 0;
        header.write(&mut header_page);
        write_raw_page(file.path(), HEADER_PAGE_ID, &header_page);

        let err = match DiskManager::new(file.path()) {
            Ok(_) => panic!("expected freelist count without head"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            DiskManagerError::InvalidFreelist(FreelistError::CountWithoutHead { count: 1 })
        ));
    }

    #[test]
    fn open_rejects_invalid_database_magic() {
        let file = NamedTempFile::new().unwrap();
        let mut page = [0u8; PAGE_SIZE];
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            dm.read_page(HEADER_PAGE_ID, &mut page).unwrap();
        }

        page[0] ^= 0xFF;
        write_page_checksum(&mut page);

        let mut handle = OpenOptions::new().read(true).write(true).open(file.path()).unwrap();
        handle.seek(std::io::SeekFrom::Start(0)).unwrap();
        handle.write_all(&page).unwrap();
        handle.sync_all().unwrap();

        let err = match DiskManager::new(file.path()) {
            Ok(_) => panic!("expected invalid database magic"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            DiskManagerError::InvalidDatabaseHeader(DatabaseHeaderError::InvalidMagic)
        ));
    }

    #[test]
    fn open_rejects_mismatched_header_page_size() {
        let file = NamedTempFile::new().unwrap();
        let mut page = [0u8; PAGE_SIZE];
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            dm.read_page(HEADER_PAGE_ID, &mut page).unwrap();
        }

        page[16..18].copy_from_slice(&0u16.to_le_bytes());
        write_page_checksum(&mut page);

        let mut handle = OpenOptions::new().read(true).write(true).open(file.path()).unwrap();
        handle.seek(std::io::SeekFrom::Start(0)).unwrap();
        handle.write_all(&page).unwrap();
        handle.sync_all().unwrap();

        let err = match DiskManager::new(file.path()) {
            Ok(_) => panic!("expected mismatched header page size"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            DiskManagerError::InvalidDatabaseHeader(DatabaseHeaderError::InvalidPageSize {
                actual: 0,
                expected
            }) if expected == PAGE_SIZE
        ));
    }

    #[test]
    fn open_rejects_mismatched_header_page_count() {
        let file = NamedTempFile::new().unwrap();
        let mut page = [0u8; PAGE_SIZE];
        {
            let mut dm = DiskManager::new(file.path()).unwrap();
            dm.new_page().unwrap();
            dm.read_page(HEADER_PAGE_ID, &mut page).unwrap();
        }

        page[18..26].copy_from_slice(&999u64.to_le_bytes());
        write_page_checksum(&mut page);

        let mut handle = OpenOptions::new().read(true).write(true).open(file.path()).unwrap();
        handle.seek(std::io::SeekFrom::Start(0)).unwrap();
        handle.write_all(&page).unwrap();
        handle.sync_all().unwrap();

        let err = match DiskManager::new(file.path()) {
            Ok(_) => panic!("expected mismatched header page count"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            DiskManagerError::InvalidDatabaseHeader(DatabaseHeaderError::PageCountMismatch {
                actual: 999,
                expected: 2,
            })
        ));
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
        assert!(matches!(
            err,
            DiskManagerError::InvalidPageChecksum { page_id: id } if id == page_id
        ));
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
