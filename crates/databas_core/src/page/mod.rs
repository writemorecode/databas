//! Slotted B-tree page types and format constants.
//!
//! This module exposes the typed page API used by the storage layer to read and
//! mutate fixed-size on-disk pages. Pages are split across two orthogonal axes:
//! node kind and tree kind. [`Leaf`] pages store records, while [`Interior`]
//! pages store separators and child pointers. [`Table`] pages use row ids as
//! keys; [`Index`] pages use byte-slice keys. Depending on the page kind, cells
//! may also carry a variable-sized payload region.
//!
//! The main entry point is [`Page`]. A page is parameterized both by access mode
//! ([`Read`] or [`Write`]), node kind ([`Leaf`] or [`Interior`]), and tree kind
//! ([`Table`] or [`Index`]). This keeps the API zero-copy while still making
//! invalid combinations harder to express.
//!
//! When the concrete page kind is not known ahead of time, use [`AnyPage`] to
//! inspect an already-initialized byte buffer. [`Cell`] and [`CellMut`] provide
//! typed access to individual slot entries after lookup.
//!
//! Layout details that are part of the stable page format are re-exported from
//! [`mod@format`], including header sizes, slot entry width, and the current
//! [`FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub mod format;
mod index_interior;
mod index_leaf;
mod interior;
mod leaf;

/// Page handles, marker types, access traits, and search helpers for typed page access.
pub use core::{AnyPage, Index, Interior, Leaf, NodeMarker, Page, Read, Table, TreeMarker, Write};
/// Errors returned while validating or manipulating encoded pages and cells.
pub(crate) use error::{CellCorruption, PageCorruption, PageError, PageResult};

/// A typed table leaf page alias.
pub type TableLeafPage<A> = Page<A, Leaf, Table>;
/// A typed table interior page alias.
pub type TableInteriorPage<A> = Page<A, Interior, Table>;
/// A typed index leaf page alias.
pub type IndexLeafPage<A> = Page<A, Leaf, Index>;
/// A typed index interior page alias.
pub type IndexInteriorPage<A> = Page<A, Interior, Index>;
