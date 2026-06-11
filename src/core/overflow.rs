//! Overflow-page chain helpers for large B+-tree cell payloads.

use crate::core::{
    PAGE_SIZE, PageId,
    error::StorageResult,
    page::format::{self, OVERFLOW_NEXT_PAGE_ID_SIZE},
    page_cache::PageCache,
};

pub(crate) const OVERFLOW_PAYLOAD_SIZE: usize = PAGE_SIZE - OVERFLOW_NEXT_PAGE_ID_SIZE;

fn write_next_page_id(page: &mut [u8; PAGE_SIZE], next_page_id: Option<PageId>) {
    format::write_optional_u64(page, 0, next_page_id);
}

/// Writes `payload` into a newly allocated overflow chain.
pub(crate) fn write_chain(page_cache: &PageCache, payload: &[u8]) -> StorageResult<Option<PageId>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let mut first_page_id = None;
    let mut previous_page_id = None;

    for chunk in payload.chunks(OVERFLOW_PAYLOAD_SIZE) {
        let (page_id, pin) = page_cache.new_page()?;
        {
            let mut page = pin.write()?;
            page.page_mut().fill(0);
            write_next_page_id(page.page_mut(), None);
            page.page_mut()[OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + chunk.len()]
                .copy_from_slice(chunk);
        }
        drop(pin);

        if first_page_id.is_none() {
            first_page_id = Some(page_id);
        }

        if let Some(previous_page_id) = previous_page_id {
            let previous_pin = page_cache.fetch_page(previous_page_id)?;
            let mut previous_page = previous_pin.write()?;
            write_next_page_id(previous_page.page_mut(), Some(page_id));
        }

        previous_page_id = Some(page_id);
    }

    Ok(first_page_id)
}
