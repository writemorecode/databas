//! Slotted B+-tree page types and format constants.
//!
//! This module exposes the typed page API used by the storage layer to read and
//! mutate fixed-size on-disk pages. Pages are split by structural node kind:
//! [`Leaf`] pages store raw byte keys and values, while [`Interior`] pages store
//! separator byte keys and child pointers.
//!
//! The main entry point is [`Page`]. A page is parameterized both by access mode
//! ([`Read`] or [`Write`]) and node kind ([`Leaf`] or [`Interior`]). [`Cell`]
//! and [`CellMut`] provide typed access to individual slot entries after lookup.
//! [`TablePage`] and [`IndexPage`] wrap the raw page API with table- and
//! index-specific key/value interpretation.
//!
//! Layout details that are part of the stable page format are re-exported from
//! [`mod@format`], including header sizes, slot entry width, and the current
//! [`FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub mod format;
mod index;
mod interior;
mod leaf;
mod table;

/// Cell views returned by typed page accessors.
pub use cell::{Cell, CellMut};
/// Page handles, marker types, access traits, and search helpers for typed page access.
pub use core::{Interior, Leaf, NodeMarker, Page, Read, SearchResult, Write};
/// Errors returned while validating or manipulating encoded pages and cells.
pub(crate) use error::{CellCorruption, PageCorruption, PageError, PageResult};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PAGE_SIZE;

    #[test]
    fn leaf_insert_can_be_read_with_lookup() {
        // Verifies that inserting one leaf cell makes it readable by key lookup.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert(b"alpha", b"value").unwrap();

        let cell = page.lookup(b"alpha").unwrap().unwrap();
        assert_eq!(cell.key().unwrap(), b"alpha");
        assert_eq!(cell.value().unwrap(), b"value");
    }

    #[test]
    fn leaf_lookup_reads_inserted_cells_by_key() {
        // Verifies that leaf lookup returns each inserted cell by its key.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert(b"bravo", b"two").unwrap();
        page.insert(b"alpha", b"one").unwrap();
        page.insert(b"charlie", b"three").unwrap();

        assert_eq!(page.lookup(b"alpha").unwrap().unwrap().value().unwrap(), b"one");
        assert_eq!(page.lookup(b"bravo").unwrap().unwrap().value().unwrap(), b"two");
        assert_eq!(page.lookup(b"charlie").unwrap().unwrap().value().unwrap(), b"three");
    }

    #[test]
    fn leaf_update_replaces_value_for_existing_key() {
        // Verifies that updating a leaf cell replaces the value found by lookup.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert(b"alpha", b"old").unwrap();
        page.update(b"alpha", b"new").unwrap();

        let cell = page.lookup(b"alpha").unwrap().unwrap();
        assert_eq!(cell.key().unwrap(), b"alpha");
        assert_eq!(cell.value().unwrap(), b"new");
    }

    #[test]
    fn leaf_delete_removes_existing_key() {
        // Verifies that deleting a leaf cell removes it from key lookup.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert(b"alpha", b"value").unwrap();
        page.delete(b"alpha").unwrap();

        assert!(page.lookup(b"alpha").unwrap().is_none());
    }

    #[test]
    fn leaf_insert_rejects_duplicate_key() {
        // Verifies that inserting an existing leaf key returns a duplicate-key error.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        page.insert(b"alpha", b"first").unwrap();
        let result = page.insert(b"alpha", b"second");

        assert!(matches!(result, Err(PageError::DuplicateKey)));
    }

    #[test]
    fn leaf_update_rejects_missing_key() {
        // Verifies that updating a missing leaf key returns a key-not-found error.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        let result = page.update(b"missing", b"value");

        assert!(matches!(result, Err(PageError::KeyNotFound)));
    }

    #[test]
    fn leaf_delete_rejects_missing_key() {
        // Verifies that deleting a missing leaf key returns a key-not-found error.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);

        let result = page.delete(b"missing");

        assert!(matches!(result, Err(PageError::KeyNotFound)));
    }

    #[test]
    fn leaf_insert_returns_page_full_when_free_space_runs_out() {
        // Verifies that inserting page-sized cumulative data fails with page-full.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);
        let value = [7; 512];

        for index in 0_u16..20 {
            let key = index.to_be_bytes();
            if let Err(error) = page.insert(&key, &value) {
                assert!(matches!(error, PageError::PageFull { .. }));
                return;
            }
        }

        panic!("expected the leaf page to become full");
    }

    #[test]
    fn leaf_insert_rejects_oversized_cell() {
        // Verifies that a single oversized leaf cell is rejected instead of using overflow pages.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf>::init(&mut bytes);
        let value = vec![7; PAGE_SIZE];

        let result = page.insert(b"alpha", &value);

        assert!(matches!(result, Err(PageError::CellTooLarge { .. })));
    }

    #[test]
    fn interior_insert_can_be_read_with_lookup() {
        // Verifies that inserting one interior cell makes it readable by key lookup.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        page.insert(b"middle", 7).unwrap();

        let cell = page.lookup(b"middle").unwrap().unwrap();
        assert_eq!(cell.key().unwrap(), b"middle");
        assert_eq!(cell.left_child().unwrap(), 7);
    }

    #[test]
    fn interior_update_replaces_left_child_for_existing_key() {
        // Verifies that updating an interior cell replaces the child found by lookup.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        page.insert(b"middle", 7).unwrap();
        page.update(b"middle", 11).unwrap();

        let cell = page.lookup(b"middle").unwrap().unwrap();
        assert_eq!(cell.key().unwrap(), b"middle");
        assert_eq!(cell.left_child().unwrap(), 11);
    }

    #[test]
    fn interior_insert_rejects_duplicate_key() {
        // Verifies that inserting an existing interior key returns a duplicate-key error.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        page.insert(b"middle", 7).unwrap();
        let result = page.insert(b"middle", 11);

        assert!(matches!(result, Err(PageError::DuplicateKey)));
    }

    #[test]
    fn interior_update_rejects_missing_key() {
        // Verifies that updating a missing interior key returns a key-not-found error.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);

        let result = page.update(b"missing", 7);

        assert!(matches!(result, Err(PageError::KeyNotFound)));
    }

    #[test]
    fn interior_insert_rejects_oversized_cell() {
        // Verifies that a single oversized interior cell is rejected instead of using overflow pages.
        let mut bytes = [0; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior>::init(&mut bytes, 99);
        let key = [7; PAGE_SIZE];

        let result = page.insert(&key, 7);

        assert!(matches!(result, Err(PageError::CellTooLarge { .. })));
    }
}
