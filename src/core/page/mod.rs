//! Slotted B+-tree page types and format constants.
//!
//! This module exposes the typed page API used by the storage layer to read and
//! mutate fixed-size on-disk pages. Pages are split by structural node kind:
//! [`Leaf`] pages store raw byte keys and values, while [`Interior`] pages store
//! separator byte keys and child pointers.
//!
//! The main entry point is [`Page`]. A page is parameterized both by access mode
//! ([`Read`] or [`Write`]) and node kind ([`Leaf`] or [`Interior`]). Typed cell
//! views provide access to individual slot entries after lookup.
//!
//! Layout details that are part of the stable page format are re-exported from
//! [`mod@format`], including header sizes, slot entry width, and the current
//! [`FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub(crate) mod format;
mod interior;
mod leaf;

use crate::core::PAGE_SIZE;

/// Page handles, marker types, access traits, and search helpers for typed page access.
pub(crate) use core::{
    BoundResult, Interior, Leaf, NodeMarker, Page, Read, SearchResult, Write, validate_btree_page,
};
/// Errors returned while validating or manipulating encoded pages and cells.
pub(crate) use error::{CellCorruption, PageCorruption, PageError, PageResult};

/// Raw B+-tree leaf page storing byte keys and byte values.
pub(crate) type RawLeaf<A> = Page<A, Leaf>;
/// Raw B+-tree interior page storing separator byte keys and child pointers.
pub(crate) type RawInterior<A> = Page<A, Interior>;

pub(crate) fn is_current_btree_page(bytes: &[u8; PAGE_SIZE]) -> bool {
    format::PageKind::from_raw(bytes[format::KIND_OFFSET]).is_some()
        && bytes[format::VERSION_OFFSET] == format::FORMAT_VERSION
}

pub(crate) fn is_overflow_page(bytes: &[u8; PAGE_SIZE]) -> bool {
    !is_current_btree_page(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::PAGE_SIZE;

    #[test]
    fn leaf_insert_payload_can_be_read() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert_payload_at(0, 5, 5, None, b"alphavalue").unwrap();

        let (key_len, value_len, overflow_page, payload_range) =
            page.cell_payload_parts(0).unwrap();
        assert_eq!(key_len, 5);
        assert_eq!(value_len, 5);
        assert_eq!(overflow_page, None);
        assert_eq!(&page.bytes()[payload_range], b"alphavalue");
    }

    #[test]
    fn page_kind_helpers_classify_btree_and_overflow_pages() {
        let mut leaf_bytes = [0; PAGE_SIZE];
        let _page = Page::<Write<'_>, Leaf>::init(&mut leaf_bytes);
        let overflow_bytes = [0; PAGE_SIZE];

        assert!(is_current_btree_page(&leaf_bytes));
        assert!(!is_overflow_page(&leaf_bytes));
        assert!(!is_current_btree_page(&overflow_bytes));
        assert!(is_overflow_page(&overflow_bytes));
    }

    #[test]
    fn leaf_delete_removes_existing_key() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert_payload_at(0, 5, 5, None, b"alphavalue").unwrap();
        page.delete(b"alpha").unwrap();

        assert!(matches!(page.search(b"alpha").unwrap(), SearchResult::InsertAt(0)));
    }

    #[test]
    fn leaf_delete_rejects_missing_key() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        let result = page.delete(b"missing");

        assert!(matches!(result, Err(PageError::KeyNotFound)));
    }

    #[test]
    fn leaf_insert_payload_returns_page_full_when_free_space_runs_out() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);
        let value = [7; 512];

        for index in 0_u16..20 {
            let key = index.to_be_bytes();
            let mut payload = Vec::from(key);
            payload.extend_from_slice(&value);
            if let Err(error) =
                page.insert_payload_at(index, key.len(), value.len(), None, &payload)
            {
                assert!(matches!(error, PageError::PageFull { .. }));
                return;
            }
        }

        panic!("expected the leaf page to become full");
    }

    #[test]
    fn leaf_insert_payload_rejects_oversized_cell() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        let result = page.insert_payload_at(0, 1, u16::MAX as usize, None, b"");

        assert!(matches!(result, Err(PageError::CellTooLarge { .. })));
    }

    #[test]
    fn interior_insert_payload_can_be_read() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        page.insert_payload_at(0, 7, b"middle".len(), None, b"middle").unwrap();

        let cell = page.cell(0).unwrap();
        assert_eq!(cell.left_child().unwrap(), 7);
    }

    #[test]
    fn interior_cell_mut_replaces_left_child_for_existing_key() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        page.insert_payload_at(0, 7, b"middle".len(), None, b"middle").unwrap();
        let slot_index = 0;
        let mut cell = page.cell_mut(slot_index).unwrap();
        cell.set_left_child(11).unwrap();

        let cell = page.cell(slot_index).unwrap();
        assert_eq!(cell.left_child().unwrap(), 11);
    }

    #[test]
    fn interior_cell_mut_rejects_missing_slot() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        let result = page.cell_mut(0);

        assert!(matches!(result, Err(PageError::InvalidSlotIndex { .. })));
    }

    #[test]
    fn interior_insert_payload_rejects_oversized_cell() {
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        let result = page.insert_payload_at(0, 7, u16::MAX as usize + 1, None, b"");

        assert!(matches!(result, Err(PageError::CellTooLarge { .. })));
    }
}
