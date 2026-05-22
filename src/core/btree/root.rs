use super::*;

/// Allocates and initializes a brand-new empty raw root leaf page.
pub(crate) fn initialize_empty_root<S: PageStore>(
    page_cache: &PageCache<S>,
) -> StorageResult<PageId> {
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = RawLeaf::<Write<'_>>::initialize(page.page_mut());
    Ok(page_id)
}

/// Verifies that `root_page_id` names a raw leaf or raw interior page.
pub(crate) fn validate_root_page(
    page_cache: &PageCache<impl PageStore>,
    root_page_id: PageId,
) -> StorageResult<()> {
    let pin = page_cache.fetch_page(root_page_id)?;
    let page = pin.read()?;
    match read_page_kind(page.page(), root_page_id)? {
        PageKind::RawLeaf => {
            let _ = page.open::<Leaf>()?;
        }
        PageKind::RawInterior => {
            let _ = page.open::<Interior>()?;
        }
    }
    Ok(())
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
