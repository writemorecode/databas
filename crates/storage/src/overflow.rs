//! Overflow-page chain helpers for large B+-tree cell payloads.

use crate::error::{
    CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult,
};
use crate::page::format;
use crate::page_cache::PageCache;
use crate::types::PAGE_CONTENT_END;
use crate::{PAGE_SIZE, PageId, TxnId};

// Overflow chains use the data-page kind/version tags and next-page field.
const KIND: u8 = 3;
pub const HEADER_SIZE: usize = format::SHARED_HEADER_SIZE;
pub const OVERFLOW_PAYLOAD_SIZE: usize = PAGE_CONTENT_END - HEADER_SIZE;

pub fn initialize(bytes: &mut [u8; PAGE_SIZE]) {
    bytes.fill(0);
    bytes[format::KIND_OFFSET] = KIND;
    bytes[format::VERSION_OFFSET] = format::FORMAT_VERSION;
    format::write_optional_u64(bytes, format::PREV_PAGE_ID_OFFSET, None);
    format::write_optional_u64(bytes, format::NEXT_PAGE_ID_OFFSET, None);
}

/// Validate type/version before following a chain or interpreting its payload.
/// Persistent bytes have already passed checksum verification in the I/O layer.
pub fn next_page_id(page_id: PageId, bytes: &[u8; PAGE_SIZE]) -> StorageResult<Option<PageId>> {
    let error = |kind| {
        StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::OverflowPage,
            page_id: Some(page_id),
            kind,
        })
    };
    if bytes[format::KIND_OFFSET] != KIND {
        return Err(error(CorruptionKind::InvalidPageKind {
            expected: "overflow",
            actual: bytes[format::KIND_OFFSET],
        }));
    }
    if bytes[format::VERSION_OFFSET] != format::FORMAT_VERSION {
        return Err(error(CorruptionKind::InvalidPageVersion {
            expected: format::FORMAT_VERSION,
            actual: bytes[format::VERSION_OFFSET],
        }));
    }
    if bytes[format::SLOT_COUNT_OFFSET..format::PREV_PAGE_ID_OFFSET].iter().any(|b| *b != 0)
        || format::read_optional_u64(bytes, format::PREV_PAGE_ID_OFFSET).is_some()
    {
        return Err(error(CorruptionKind::InvalidOverflowHeader));
    }
    Ok(format::read_optional_u64(bytes, format::NEXT_PAGE_ID_OFFSET))
}

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
        let (page_id, pin) =
            page_cache.new_page(txn_id, crate::page_recovery::PageInitKind::Overflow)?;
        {
            let mut page = pin.write(txn_id)?;

            let mut write_offset = HEADER_SIZE;
            while write_offset < PAGE_CONTENT_END && (!first.is_empty() || !second.is_empty()) {
                let source = if !first.is_empty() { first } else { second };
                let take = source.len().min(PAGE_CONTENT_END - write_offset);
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
            next_page_id(previous_page_id, previous_page.page())?;
            format::write_optional_u64(
                previous_page.page_mut(),
                format::NEXT_PAGE_ID_OFFSET,
                Some(page_id),
            );
        }
        previous_page_id = Some(page_id);
    }

    Ok(first_page_id)
}
