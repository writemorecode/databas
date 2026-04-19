//! Slotted raw B+-tree page types and format constants.
//!
//! This module exposes the typed page API used by the storage layer to read and
//! mutate fixed-size on-disk pages. Pages are split by structural node kind:
//! [`Leaf`] pages store raw byte keys and values, while [`Interior`] pages store
//! separator byte keys and child pointers.
//!
//! The main entry point is [`Page`]. A page is parameterized both by access mode
//! ([`Read`] or [`Write`]) and node kind ([`Leaf`] or [`Interior`]). [`Cell`]
//! and [`CellMut`] provide typed access to individual slot entries after lookup.
//!
//! Layout details that are part of the stable page format are re-exported from
//! [`mod@format`], including header sizes, slot entry width, and the current
//! [`FORMAT_VERSION`].

mod cell;
mod core;
mod error;
pub mod format;
mod interior;
mod leaf;

/// Cell views returned by typed page accessors.
pub use cell::{Cell, CellMut};
/// Page handles, marker types, access traits, and search helpers for typed page access.
pub use core::{BoundResult, Interior, Leaf, NodeMarker, Page, Read, SearchResult, Write};
/// Errors returned while validating or manipulating encoded pages and cells.
pub(crate) use error::{CellCorruption, PageCorruption, PageError, PageResult};
