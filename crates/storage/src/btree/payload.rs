use super::{
    CorruptionComponent, CorruptionError, CorruptionKind, NO_OVERFLOW_PAGE_ID, Ordering, PageCache,
    PageId, StorageError, StorageResult, overflow,
};

type MaterializedLeafCell = (Box<[u8]>, Box<[u8]>);

pub(super) fn cell_corruption(page_id: PageId, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Cell,
        page_id: Some(page_id),
        kind,
    })
}

fn overflow_corruption(page_id: Option<PageId>, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::OverflowPage,
        page_id,
        kind,
    })
}

fn append_overflow_bytes(
    page_cache: &PageCache,
    first_overflow_page_id: PageId,
    payload: &mut Vec<u8>,
    total_payload_len: usize,
) -> StorageResult<Option<PageId>> {
    let initial_len = payload.len();
    let expected_overflow_len = total_payload_len - initial_len;
    let mut page_id = Some(first_overflow_page_id);

    while payload.len() < total_payload_len {
        let Some(current_page_id) = page_id else {
            return Err(overflow_corruption(
                None,
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_overflow_len,
                    actual: payload.len() - initial_len,
                },
            ));
        };

        if current_page_id == NO_OVERFLOW_PAGE_ID {
            return Err(overflow_corruption(
                Some(current_page_id),
                CorruptionKind::OverflowChainTooShort {
                    expected: expected_overflow_len,
                    actual: payload.len() - initial_len,
                },
            ));
        }

        let pin = page_cache.fetch_page(current_page_id)?;
        let page = pin.read()?;
        let next = overflow::next_page_id(current_page_id, page.page())?;
        let remaining = total_payload_len - payload.len();
        let take = remaining.min(overflow::OVERFLOW_PAYLOAD_SIZE);
        payload
            .extend_from_slice(&page.page()[overflow::HEADER_SIZE..overflow::HEADER_SIZE + take]);
        page_id = next;
    }

    Ok(page_id)
}

fn append_overflow_chain_exact(
    page_cache: &PageCache,
    first_overflow_page_id: PageId,
    payload: &mut Vec<u8>,
    total_payload_len: usize,
) -> StorageResult<()> {
    let expected_overflow_len = total_payload_len - payload.len();
    let page_id =
        append_overflow_bytes(page_cache, first_overflow_page_id, payload, total_payload_len)?;
    if page_id.is_some() {
        return Err(overflow_corruption(
            page_id,
            CorruptionKind::OverflowChainTooLong { expected: expected_overflow_len },
        ));
    }

    Ok(())
}

pub(super) fn append_overflow_prefix(
    page_cache: &PageCache,
    first_overflow_page_id: PageId,
    payload: &mut Vec<u8>,
    total_payload_len: usize,
) -> StorageResult<()> {
    append_overflow_bytes(page_cache, first_overflow_page_id, payload, total_payload_len)?;
    Ok(())
}

pub(super) fn materialize_payload(
    page_cache: &PageCache,
    page_id: PageId,
    inline_payload: &[u8],
    first_overflow_page_id: Option<PageId>,
    payload_len: usize,
) -> StorageResult<Vec<u8>> {
    if inline_payload.len() > payload_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(inline_payload);
    match first_overflow_page_id {
        Some(first_overflow_page_id) => append_overflow_chain_exact(
            page_cache,
            first_overflow_page_id,
            &mut payload,
            payload_len,
        )?,
        None if payload.len() != payload_len => {
            return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
        }
        None => {}
    }

    if payload.len() != payload_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }
    Ok(payload)
}

pub(super) fn materialize_leaf_cell(
    page_cache: &PageCache,
    page_id: PageId,
    inline_payload: &[u8],
    first_overflow_page_id: PageId,
    key_len: usize,
    value_len: usize,
) -> StorageResult<MaterializedLeafCell> {
    let mut payload = materialize_payload(
        page_cache,
        page_id,
        inline_payload,
        Some(first_overflow_page_id),
        key_len + value_len,
    )?;
    if payload.len() < key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    let value = payload.split_off(key_len);
    Ok((payload.into_boxed_slice(), value.into_boxed_slice()))
}

pub(super) fn compare_key_prefix(
    page_id: PageId,
    inline_key: &[u8],
    key_len: usize,
    key: &[u8],
) -> StorageResult<Option<Ordering>> {
    if inline_key.len() > key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    if key.len() <= inline_key.len() {
        let ordering = inline_key[..key.len()].cmp(key);
        if ordering != Ordering::Equal {
            return Ok(Some(ordering));
        }
        return Ok(Some(key_len.cmp(&key.len())));
    }

    let ordering = inline_key.cmp(&key[..inline_key.len()]);
    if ordering != Ordering::Equal {
        return Ok(Some(ordering));
    }

    if inline_key.len() == key_len {
        return Ok(Some(Ordering::Less));
    }

    Ok(None)
}

pub(super) fn compare_overflow_key(
    page_cache: &PageCache,
    page_id: PageId,
    key: &[u8],
    inline_key_len: usize,
    first_overflow_page_id: Option<PageId>,
    key_len: usize,
) -> StorageResult<Ordering> {
    if inline_key_len > key_len {
        return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
    }

    let first_overflow_page_id = first_overflow_page_id
        .ok_or_else(|| cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds))?;

    let mut page_id = Some(first_overflow_page_id);
    let mut compared = inline_key_len;
    while compared < key_len {
        let Some(current_page_id) = page_id else {
            return Err(overflow_corruption(
                None,
                CorruptionKind::OverflowChainTooShort {
                    expected: key_len - inline_key_len,
                    actual: compared - inline_key_len,
                },
            ));
        };

        if current_page_id == NO_OVERFLOW_PAGE_ID {
            return Err(overflow_corruption(
                Some(current_page_id),
                CorruptionKind::OverflowChainTooShort {
                    expected: key_len - inline_key_len,
                    actual: compared - inline_key_len,
                },
            ));
        }

        let pin = page_cache.fetch_page(current_page_id)?;
        let page = pin.read()?;
        let next = overflow::next_page_id(current_page_id, page.page())?;
        let remaining = key_len - compared;
        let take = remaining.min(overflow::OVERFLOW_PAYLOAD_SIZE);
        if compared < key.len() {
            let compare_len = (key.len() - compared).min(take);
            let ordering = page.page()[overflow::HEADER_SIZE..overflow::HEADER_SIZE + compare_len]
                .cmp(&key[compared..compared + compare_len]);
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        compared += take;
        page_id = next;
    }

    Ok(key_len.cmp(&key.len()))
}
