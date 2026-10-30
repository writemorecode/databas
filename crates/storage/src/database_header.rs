use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::{PAGE_SIZE, PAGE_SIZE_U16, PageId};

pub const DATABASE_HEADER_PAGE_ID: PageId = 0;

const MAGIC: &[u8; 8] = b"DATABAS\0";
const FORMAT_VERSION: u16 = 3;
const HEADER_LEN: usize = 12;

/// Fixed-format database file header stored on page 0.
pub struct DatabaseHeader;

impl DatabaseHeader {
    pub(crate) fn encode_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(MAGIC);
        page[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        page[10..12].copy_from_slice(&PAGE_SIZE_U16.to_le_bytes());
        page[crate::page_allocator::BITMAP_OFFSET] = 1;
        page
    }

    pub(crate) fn validate_page(page: &[u8; PAGE_SIZE]) -> StorageResult<()> {
        if &page[0..8] != MAGIC {
            let mut actual = [0u8; 8];
            actual.copy_from_slice(&page[0..8]);
            return Err(corrupt_header(CorruptionKind::InvalidDatabaseMagic {
                expected: *MAGIC,
                actual,
            }));
        }

        let version = u16::from_le_bytes([page[8], page[9]]);
        if version != FORMAT_VERSION {
            return Err(corrupt_header(CorruptionKind::UnsupportedDatabaseVersion {
                expected: FORMAT_VERSION,
                actual: version,
            }));
        }

        let page_size = u16::from_le_bytes([page[10], page[11]]) as usize;
        if page_size != PAGE_SIZE {
            return Err(corrupt_header(CorruptionKind::InvalidDatabasePageSize {
                expected: PAGE_SIZE,
                actual: page_size,
            }));
        }

        if page[HEADER_LEN..crate::page_allocator::BITMAP_OFFSET].iter().any(|byte| *byte != 0) {
            return Err(corrupt_header(CorruptionKind::DatabaseHeaderReservedBytesNotZero));
        }

        Ok(())
    }
}

pub fn missing_header() -> StorageError {
    corrupt_header(CorruptionKind::MissingDatabaseHeader)
}

fn corrupt_header(kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::DatabaseFile,
        page_id: Some(DATABASE_HEADER_PAGE_ID),
        kind,
    })
}
