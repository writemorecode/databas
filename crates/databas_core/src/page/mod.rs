//! Slotted B-tree page types and format constants.
//!
//! This module exposes the typed page API used by the storage layer to read and
//! mutate fixed-size on-disk pages. Pages are split into two families:
//! [`Leaf`] pages store `(row_id, payload)` records, while [`Interior`] pages
//! store fixed-size separator cells plus child pointers.
//!
//! The main entry point is [`Page`]. A page is parameterized both by access mode
//! ([`Read`] or [`Write`]) and by node kind ([`Leaf`] or [`Interior`]). This
//! keeps the API zero-copy while still making invalid combinations harder to
//! express.
//!
//! When the concrete page kind is not known ahead of time, use [`AnyPage`] to
//! inspect an already-initialized byte buffer. [`Cell`] provides typed access to
//! individual slot entries after lookup.
//!
//! Layout details that are part of the stable page format are re-exported from
//! [`format`], including header sizes, slot entry width, and the current
//! [`FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub mod format;
mod interior;
mod leaf;

/// A typed view over a single page cell in either a leaf or interior page.
pub use cell::Cell;
/// Page handles, marker types, access traits, and search helpers for typed page access.
pub use core::{
    AnyPage, BoundResult, Interior, Leaf, NodeMarker, Page, PageAccess, PageAccessMut, Read,
    SearchResult, Write,
};
/// Errors returned while validating or manipulating encoded pages and cells.
pub use error::{CellCorruption, PageCorruption, PageError, PageResult};
/// Public page-format constants and layout metadata used by page encoders and decoders.
pub use format::{
    CELL_LENGTH_SIZE, FORMAT_VERSION, INTERIOR_HEADER_SIZE, LEAF_HEADER_SIZE, PageKind,
    RESERVED_FOOTER_SIZE, SHARED_HEADER_SIZE, SLOT_ENTRY_SIZE, USABLE_SPACE_END,
};
