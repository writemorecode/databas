use crate::types::PAGE_SIZE;

pub(crate) const PAGE_CHECKSUM_SIZE: usize = 4;
pub(crate) const PAGE_DATA_END: usize = PAGE_SIZE - PAGE_CHECKSUM_SIZE;

/// Computes CRC32 across the data region of one page.
pub(crate) fn compute_page_checksum(page: &[u8; PAGE_SIZE]) -> u32 {
    crc32_v2::crc32(0, &page[..PAGE_DATA_END])
}

/// Reads the checksum stored in the trailing checksum field.
pub(crate) fn stored_page_checksum(page: &[u8; PAGE_SIZE]) -> u32 {
    let mut bytes = [0u8; PAGE_CHECKSUM_SIZE];
    bytes.copy_from_slice(&page[PAGE_DATA_END..PAGE_SIZE]);
    u32::from_le_bytes(bytes)
}

/// Writes the computed checksum into the trailing checksum field.
pub(crate) fn write_page_checksum(page: &mut [u8; PAGE_SIZE]) {
    let checksum = compute_page_checksum(page);
    page[PAGE_DATA_END..PAGE_SIZE].copy_from_slice(&checksum.to_le_bytes());
}

/// Returns whether the stored and computed checksums match.
pub(crate) fn checksum_matches(page: &[u8; PAGE_SIZE]) -> bool {
    stored_page_checksum(page) == compute_page_checksum(page)
}
