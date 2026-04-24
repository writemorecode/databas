//! Overflow-page chain helpers for large B+-tree cell payloads.

use crate::{
    PAGE_SIZE, PageId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    page::format::{self, NO_OVERFLOW_PAGE_ID, OVERFLOW_NEXT_PAGE_ID_SIZE},
    page_cache::PageCache,
    page_store::PageStore,
};

pub(crate) const OVERFLOW_PAYLOAD_SIZE: usize = PAGE_SIZE - OVERFLOW_NEXT_PAGE_ID_SIZE;

fn write_next_page_id(page: &mut [u8; PAGE_SIZE], next_page_id: Option<PageId>) {
    format::write_optional_u64(page, 0, next_page_id);
}

fn read_next_page_id(page: &[u8; PAGE_SIZE]) -> Option<PageId> {
    format::read_optional_u64(page, 0)
}

fn overflow_corruption(page_id: Option<PageId>, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::OverflowPage,
        page_id,
        kind,
    })
}

/// Writes `payload` into a newly allocated overflow chain.
pub(crate) fn write_chain<S: PageStore>(
    page_cache: &PageCache<S>,
    payload: &[u8],
) -> StorageResult<Option<PageId>> {
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

/// Reads exactly `expected_len` bytes from an overflow chain.
pub(crate) fn read_chain(
    page_cache: &PageCache<impl PageStore>,
    first_page_id: PageId,
    expected_len: usize,
) -> StorageResult<Vec<u8>> {
    let mut page_id = Some(first_page_id);
    let mut payload = Vec::with_capacity(expected_len);

    while payload.len() < expected_len {
        let Some(current_page_id) = page_id else {
            return Err(overflow_corruption(
                None,
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_len,
                    actual: payload.len(),
                },
            ));
        };

        if current_page_id == NO_OVERFLOW_PAGE_ID {
            return Err(overflow_corruption(
                Some(current_page_id),
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_len,
                    actual: payload.len(),
                },
            ));
        }

        let pin = page_cache.fetch_page(current_page_id)?;
        let page = pin.read()?;
        let remaining = expected_len - payload.len();
        let take = remaining.min(OVERFLOW_PAYLOAD_SIZE);
        payload.extend_from_slice(
            &page.page()[OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + take],
        );
        page_id = read_next_page_id(page.page());
    }

    if page_id.is_some() {
        return Err(overflow_corruption(
            page_id,
            CorruptionKind::OverflowChainTooLong { expected: expected_len },
        ));
    }

    Ok(payload)
}

/// Reads the first `expected_len` bytes from an overflow chain.
///
/// Unlike [`read_chain`], this helper does not require the chain to end after
/// the requested bytes. It is used by key comparisons that only need the key
/// suffix from a larger overflow payload.
pub(crate) fn read_chain_prefix(
    page_cache: &PageCache<impl PageStore>,
    first_page_id: PageId,
    expected_len: usize,
) -> StorageResult<Vec<u8>> {
    let mut page_id = Some(first_page_id);
    let mut payload = Vec::with_capacity(expected_len);

    while payload.len() < expected_len {
        let Some(current_page_id) = page_id else {
            return Err(overflow_corruption(
                None,
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_len,
                    actual: payload.len(),
                },
            ));
        };

        if current_page_id == NO_OVERFLOW_PAGE_ID {
            return Err(overflow_corruption(
                Some(current_page_id),
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_len,
                    actual: payload.len(),
                },
            ));
        }

        let pin = page_cache.fetch_page(current_page_id)?;
        let page = pin.read()?;
        let remaining = expected_len - payload.len();
        let take = remaining.min(OVERFLOW_PAYLOAD_SIZE);
        payload.extend_from_slice(
            &page.page()[OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + take],
        );
        page_id = read_next_page_id(page.page());
    }

    Ok(payload)
}
