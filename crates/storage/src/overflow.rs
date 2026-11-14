//! Overflow-page chain helpers for large B+-tree cell payloads.

use crate::error::StorageResult;
use crate::page::format;
use crate::page::format::OVERFLOW_NEXT_PAGE_ID_SIZE;
use crate::page_cache::PageCache;
use crate::{PAGE_SIZE, PageId, TxnId};

pub const OVERFLOW_PAYLOAD_SIZE: usize = PAGE_SIZE - OVERFLOW_NEXT_PAGE_ID_SIZE;

/// Writes `payload` into a newly allocated overflow chain.
pub fn write_chain(
    page_cache: &PageCache,
    txn_id: Option<TxnId>,
    payload: &[u8],
) -> StorageResult<Option<PageId>> {
    write_chain_from_slices(page_cache, txn_id, payload, &[])
}

/// Writes two consecutive payload slices without concatenating them in memory.
pub fn write_chain_from_slices(
    page_cache: &PageCache,
    txn_id: Option<TxnId>,
    mut first: &[u8],
    mut second: &[u8],
) -> StorageResult<Option<PageId>> {
    if first.is_empty() && second.is_empty() {
        return Ok(None);
    }

    let mut first_page_id = None;
    let mut previous_page_id = None;
    while !first.is_empty() || !second.is_empty() {
        let (page_id, pin) = page_cache.new_page(txn_id)?;
        {
            let mut page = pin.write(txn_id)?;
            page.page_mut().fill(0);
            format::write_optional_u64(page.page_mut(), 0, None);

            let mut write_offset = OVERFLOW_NEXT_PAGE_ID_SIZE;
            while write_offset < PAGE_SIZE && (!first.is_empty() || !second.is_empty()) {
                let source = if !first.is_empty() { first } else { second };
                let take = source.len().min(PAGE_SIZE - write_offset);
                page.page_mut()[write_offset..write_offset + take].copy_from_slice(&source[..take]);
                write_offset += take;
                if !first.is_empty() {
                    first = &first[take..];
                } else {
                    second = &second[take..];
                }
            }
        }
        drop(pin);

        if first_page_id.is_none() {
            first_page_id = Some(page_id);
        }
        if let Some(previous_page_id) = previous_page_id {
            let previous_pin = page_cache.fetch_page(previous_page_id)?;
            let mut previous_page = previous_pin.write(txn_id)?;
            format::write_optional_u64(previous_page.page_mut(), 0, Some(page_id));
        }
        previous_page_id = Some(page_id);
    }

    Ok(first_page_id)
}
