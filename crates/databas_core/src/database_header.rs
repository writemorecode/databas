use crate::{
    error::{StorageError, StorageResult},
    page_checksum::write_page_checksum,
    types::{PAGE_SIZE, PageId},
};

pub(crate) const HEADER_PAGE_ID: PageId = 0;
pub(crate) const FIRST_DATA_PAGE_ID: PageId = 1;

const DATABASE_MAGIC: [u8; 16] = *b"databas format1\0";
const MAGIC_OFFSET: usize = 0;
const MAGIC_SIZE: usize = DATABASE_MAGIC.len();
const PAGE_SIZE_OFFSET: usize = MAGIC_OFFSET + MAGIC_SIZE;
const PAGE_COUNT_OFFSET: usize = PAGE_SIZE_OFFSET + 2;

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatabaseHeader {
    pub(crate) page_size: u16,
    pub(crate) page_count: u64,
}

impl DatabaseHeader {
    pub(crate) fn new(page_count: u64) -> Self {
        Self { page_size: PAGE_SIZE as u16, page_count }
    }

    pub(crate) fn init_new(page: &mut [u8; PAGE_SIZE]) {
        Self::new(FIRST_DATA_PAGE_ID).write(page);
    }

    pub(crate) fn read(page: &[u8; PAGE_SIZE]) -> StorageResult<Self> {
        if page[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_SIZE] != DATABASE_MAGIC {
            return Err(StorageError::InvalidDatabaseHeader("invalid magic"));
        }

        let page_size = read_u16(page, PAGE_SIZE_OFFSET);
        if page_size != PAGE_SIZE as u16 {
            return Err(StorageError::InvalidDatabaseHeader("invalid page size"));
        }

        Ok(Self { page_size, page_count: read_u64(page, PAGE_COUNT_OFFSET) })
    }

    pub(crate) fn write(&self, page: &mut [u8; PAGE_SIZE]) {
        page.fill(0);
        page[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_SIZE].copy_from_slice(&DATABASE_MAGIC);
        page[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2].copy_from_slice(&self.page_size.to_le_bytes());
        page[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 8]
            .copy_from_slice(&self.page_count.to_le_bytes());
        write_page_checksum(page);
    }

    pub(crate) fn validate(&self, actual_page_count: u64) -> StorageResult<()> {
        if self.page_count == 0 {
            return Err(StorageError::InvalidDatabaseHeader("page count must be at least one"));
        }

        if self.page_count != actual_page_count {
            return Err(StorageError::InvalidDatabaseHeader("page count does not match file size"));
        }

        Ok(())
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_le_bytes(out)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}
