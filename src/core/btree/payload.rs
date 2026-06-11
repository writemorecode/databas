use super::*;

type MaterializedLeafCell = (Box<[u8]>, Box<[u8]>);

pub(super) fn cell_corruption(page_id: PageId, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Cell,
        page_id: Some(page_id),
        kind,
    })
}

pub(super) fn overflow_corruption(page_id: Option<PageId>, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::OverflowPage,
        page_id,
        kind,
    })
}

fn read_overflow_next_page_id(page: &[u8; PAGE_SIZE]) -> Option<PageId> {
    page::format::read_optional_u64(page, 0)
}

pub(super) fn write_overflow_chain_from_slices(
    page_cache: &PageCache,
    mut first: &[u8],
    mut second: &[u8],
) -> StorageResult<Option<PageId>> {
    if first.is_empty() && second.is_empty() {
        return Ok(None);
    }

    let mut first_page_id = None;
    let mut previous_page_id = None;
    while !first.is_empty() || !second.is_empty() {
        let (page_id, pin) = page_cache.new_page()?;
        {
            let mut page = pin.write()?;
            page.page_mut().fill(0);
            page::format::write_optional_u64(page.page_mut(), 0, None);

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
            let mut previous_page = previous_pin.write()?;
            page::format::write_optional_u64(previous_page.page_mut(), 0, Some(page_id));
        }
        previous_page_id = Some(page_id);
    }

    Ok(first_page_id)
}

fn append_overflow_chain_exact(
    page_cache: &PageCache,
    first_overflow_page_id: PageId,
    payload: &mut Vec<u8>,
    total_payload_len: usize,
) -> StorageResult<()> {
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
        let remaining = total_payload_len - payload.len();
        let take = remaining.min(overflow::OVERFLOW_PAYLOAD_SIZE);
        payload.extend_from_slice(
            &page.page()[OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + take],
        );
        page_id = read_overflow_next_page_id(page.page());
    }

    if page_id.is_some() {
        return Err(overflow_corruption(
            page_id,
            CorruptionKind::OverflowChainTooLong { expected: expected_overflow_len },
        ));
    }

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

pub(super) fn read_leaf_cell(
    page_cache: &PageCache,
    page_id: PageId,
    slot_index: u16,
) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let pin = page_cache.fetch_page(page_id)?;
    let payload = {
        let page = pin.read()?;
        let leaf = page.open::<Leaf>()?;
        let (key_len, value_len, first_overflow_page_id, inline_range) =
            leaf.cell_payload_parts(slot_index)?;
        let mut payload = materialize_payload(
            page_cache,
            page_id,
            &page.page()[inline_range],
            first_overflow_page_id,
            key_len + value_len,
        )?;
        if payload.len() < key_len {
            return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
        }
        let value = payload.split_off(key_len);
        (payload, value)
    };
    drop(pin);
    Ok(payload)
}

pub(super) fn read_interior_cell(
    page_cache: &PageCache,
    page_id: PageId,
    slot_index: u16,
) -> StorageResult<(PageId, Vec<u8>)> {
    let pin = page_cache.fetch_page(page_id)?;
    let (left_child, key) = {
        let page = pin.read()?;
        let interior = page.open::<Interior>()?;
        let (left_child, key_len, first_overflow_page_id, inline_range) =
            interior.cell_payload_parts(slot_index)?;
        let key = materialize_payload(
            page_cache,
            page_id,
            &page.page()[inline_range],
            first_overflow_page_id,
            key_len,
        )?;
        (left_child, key)
    };
    drop(pin);

    Ok((left_child, key))
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
        let remaining = key_len - compared;
        let take = remaining.min(overflow::OVERFLOW_PAYLOAD_SIZE);
        if compared < key.len() {
            let compare_len = (key.len() - compared).min(take);
            let ordering = page.page()
                [OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + compare_len]
                .cmp(&key[compared..compared + compare_len]);
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        compared += take;
        page_id = read_overflow_next_page_id(page.page());
    }

    Ok(key_len.cmp(&key.len()))
}
