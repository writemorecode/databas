use crate::{
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
const FREELIST_HEAD_OFFSET: usize = PAGE_COUNT_OFFSET + 8;
const FREELIST_PAGE_COUNT_OFFSET: usize = FREELIST_HEAD_OFFSET + 8;

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatabaseHeader {
    pub(crate) page_size: u16,
    pub(crate) page_count: u64,
    pub(crate) freelist_head: PageId,
    pub(crate) freelist_page_count: u64,
}

impl DatabaseHeader {
    pub(crate) fn new(page_count: u64) -> Self {
        Self { page_size: PAGE_SIZE as u16, page_count, freelist_head: 0, freelist_page_count: 0 }
    }

    pub(crate) fn init_new(page: &mut [u8; PAGE_SIZE]) {
        Self::new(FIRST_DATA_PAGE_ID).write(page);
    }

    pub(crate) fn read(page: &[u8; PAGE_SIZE]) -> Result<Self, DatabaseHeaderError> {
        if page[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_SIZE] != DATABASE_MAGIC {
            return Err(DatabaseHeaderError::InvalidMagic);
        }

        let page_size = read_u16(page, PAGE_SIZE_OFFSET);
        if page_size != PAGE_SIZE as u16 {
            return Err(DatabaseHeaderError::InvalidPageSize {
                actual: page_size,
                expected: PAGE_SIZE,
            });
        }

        Ok(Self {
            page_size,
            page_count: read_u64(page, PAGE_COUNT_OFFSET),
            freelist_head: read_u64(page, FREELIST_HEAD_OFFSET),
            freelist_page_count: read_u64(page, FREELIST_PAGE_COUNT_OFFSET),
        })
    }

    pub(crate) fn write(&self, page: &mut [u8; PAGE_SIZE]) {
        page.fill(0);
        page[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_SIZE].copy_from_slice(&DATABASE_MAGIC);
        page[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2].copy_from_slice(&self.page_size.to_le_bytes());
        page[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 8]
            .copy_from_slice(&self.page_count.to_le_bytes());
        page[FREELIST_HEAD_OFFSET..FREELIST_HEAD_OFFSET + 8]
            .copy_from_slice(&self.freelist_head.to_le_bytes());
        page[FREELIST_PAGE_COUNT_OFFSET..FREELIST_PAGE_COUNT_OFFSET + 8]
            .copy_from_slice(&self.freelist_page_count.to_le_bytes());
        write_page_checksum(page);
    }

    pub(crate) fn validate(&self, actual_page_count: u64) -> Result<(), DatabaseHeaderError> {
        if self.page_count == 0 {
            return Err(DatabaseHeaderError::PageCountZero);
        }

        if self.page_count != actual_page_count {
            return Err(DatabaseHeaderError::PageCountMismatch {
                actual: self.page_count,
                expected: actual_page_count,
            });
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatabaseHeaderError {
    #[error("invalid magic")]
    InvalidMagic,
    #[error("invalid page size: expected {expected}, got {actual}")]
    InvalidPageSize { actual: u16, expected: usize },
    #[error("page count must be at least one")]
    PageCountZero,
    #[error("page count does not match file size: expected {expected}, got {actual}")]
    PageCountMismatch { actual: u64, expected: u64 },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_defaults_freelist_to_empty() {
        let header = DatabaseHeader::new(7);
        assert_eq!(header.page_size, PAGE_SIZE as u16);
        assert_eq!(header.page_count, 7);
        assert_eq!(header.freelist_head, 0);
        assert_eq!(header.freelist_page_count, 0);
    }

    #[test]
    fn write_and_read_round_trip_freelist_fields() {
        let header = DatabaseHeader {
            page_size: PAGE_SIZE as u16,
            page_count: 17,
            freelist_head: 9,
            freelist_page_count: 4,
        };
        let mut page = [0u8; PAGE_SIZE];
        header.write(&mut page);

        let decoded = DatabaseHeader::read(&page).unwrap();
        assert_eq!(decoded, header);
    }
}
