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
//! [`format::FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub mod format;
mod interior;
mod leaf;

use crate::PAGE_SIZE;

/// Page handles, marker types, access traits, and search helpers for typed page access.
pub use core::{
    BoundResult, Interior, Leaf, NodeMarker, Page, Read, SearchResult, Write, validate_btree_page,
};
/// Errors returned while validating or manipulating encoded pages and cells.
pub use error::{CellCorruption, PageCorruption, PageError, PageResult};

/// Raw B+-tree leaf page storing byte keys and byte values.
pub type RawLeaf<A> = Page<A, Leaf>;
/// Raw B+-tree interior page storing separator byte keys and child pointers.
pub type RawInterior<A> = Page<A, Interior>;

pub fn is_current_btree_page(bytes: &[u8; PAGE_SIZE]) -> bool {
    format::PageKind::from_raw(bytes[format::KIND_OFFSET]).is_some()
        && bytes[format::VERSION_OFFSET] == format::FORMAT_VERSION
}

pub fn is_overflow_page(bytes: &[u8; PAGE_SIZE]) -> bool {
    !is_current_btree_page(bytes)
}
