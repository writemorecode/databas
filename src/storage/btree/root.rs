use super::*;

/// Allocates and initializes a brand-new empty raw root leaf page.
pub(crate) fn initialize_empty_root(page_cache: &PageCache) -> StorageResult<PageId> {
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = RawLeaf::<Write<'_>>::initialize(page.page_mut());
    Ok(page_id)
}

/// Validates every B+-tree page reachable from `root_page_id`.
pub(crate) fn validate_tree_page_formats(
    page_cache: &PageCache,
    root_page_id: PageId,
) -> StorageResult<()> {
    let mut pending = vec![root_page_id];
    let mut visited = Vec::new();

    while let Some(page_id) = pending.pop() {
        if visited.contains(&page_id) {
            continue;
        }
        visited.push(page_id);

        let pin = page_cache.fetch_page(page_id)?;
        let page = pin.read()?;
        validate_btree_page_format(page.page(), page_id)?;

        match read_page_kind(page.page(), page_id)? {
            PageKind::RawLeaf => {}
            PageKind::RawInterior => {
                let interior = page.open::<Interior>()?;
                for slot_index in 0..interior.slot_count() {
                    let (left_child, _, _, _) = interior.cell_payload_parts(slot_index)?;
                    pending.push(left_child);
                }
                pending.push(interior.rightmost_child());
            }
        }
    }

    Ok(())
}

fn validate_btree_page_format(bytes: &[u8; PAGE_SIZE], page_id: PageId) -> StorageResult<()> {
    page::validate_btree_page(bytes).map_err(|err| page_error_with_id(err, page_id))
}

fn page_error_with_id(err: PageError, page_id: PageId) -> StorageError {
    let mut err = StorageError::from(err);
    if let StorageError::Corruption(corruption) = &mut err {
        corruption.page_id = Some(page_id);
    }
    err
}

pub(super) fn read_page_kind(bytes: &[u8; PAGE_SIZE], page_id: PageId) -> StorageResult<PageKind> {
    let raw_kind = bytes[KIND_OFFSET];
    PageKind::from_raw(raw_kind).ok_or({
        StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::Page,
            page_id: Some(page_id),
            kind: CorruptionKind::UnknownPageKind { actual: raw_kind },
        })
    })
}

pub(super) fn expect_page_kind(
    bytes: &[u8; PAGE_SIZE],
    page_id: PageId,
    expected: PageKind,
    expected_name: &'static str,
) -> StorageResult<()> {
    let raw_kind = bytes[KIND_OFFSET];
    let actual = read_page_kind(bytes, page_id)?;
    if actual == expected {
        Ok(())
    } else {
        Err(StorageError::Corruption(CorruptionError {
            component: CorruptionComponent::Page,
            page_id: Some(page_id),
            kind: CorruptionKind::InvalidPageKind { expected: expected_name, actual: raw_kind },
        }))
    }
}
