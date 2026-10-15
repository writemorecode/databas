use crate::storage::PageId;

use super::{core::Interior, interior};

#[derive(Debug)]
#[doc(hidden)]
pub struct InteriorCellFields {
    left_child: PageId,
}

mod private {
    use super::Interior;

    pub trait Sealed {}

    impl Sealed for Interior {}
}

pub trait CellMarker: private::Sealed {
    #[doc(hidden)]
    type Fields;
}

impl CellMarker for Interior {
    type Fields = InteriorCellFields;
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N: CellMarker> {
    _bytes: &'a [u8],
    fields: N::Fields,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N: CellMarker> {
    bytes: &'a mut [u8],
    fields: N::Fields,
}

impl<'a> Cell<'a, Interior> {
    pub(crate) fn new_interior(bytes: &'a [u8], parts: &interior::InteriorCellParts) -> Self {
        Self { _bytes: bytes, fields: InteriorCellFields { left_child: parts.left_child } }
    }
}

impl<'a> CellMut<'a, Interior> {
    pub(crate) fn new_interior(bytes: &'a mut [u8], parts: &interior::InteriorCellParts) -> Self {
        Self { bytes, fields: InteriorCellFields { left_child: parts.left_child } }
    }
}

impl Cell<'_, Interior> {
    /// Returns the left-child page id referenced by this interior cell.
    pub(crate) fn left_child(&self) -> PageId {
        self.fields.left_child
    }
}

impl CellMut<'_, Interior> {
    /// Updates the left-child page id stored in this interior cell.
    pub(crate) fn set_left_child(&mut self, page_id: PageId) {
        interior::write_left_child(self.bytes, page_id);
        self.fields.left_child = page_id;
    }
}
