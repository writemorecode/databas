use std::ops::Range;

use crate::PageId;

use super::{
    PageResult,
    core::{Interior, Leaf},
    interior, leaf,
};

#[derive(Debug)]
#[doc(hidden)]
pub struct LeafCellFields {
    key_range: Range<usize>,
    value_range: Range<usize>,
}

#[derive(Debug)]
#[doc(hidden)]
pub struct InteriorCellFields {
    key_range: Range<usize>,
    left_child: PageId,
}

mod private {
    use super::{Interior, Leaf};

    pub trait Sealed {}

    impl Sealed for Leaf {}
    impl Sealed for Interior {}
}

pub trait CellMarker: private::Sealed {
    #[doc(hidden)]
    type Fields;
}

impl CellMarker for Leaf {
    type Fields = LeafCellFields;
}

impl CellMarker for Interior {
    type Fields = InteriorCellFields;
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N: CellMarker> {
    bytes: &'a [u8],
    fields: N::Fields,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N: CellMarker> {
    bytes: &'a mut [u8],
    fields: N::Fields,
}

impl<'a> Cell<'a, Leaf> {
    pub(crate) fn new_leaf(bytes: &'a [u8], parts: leaf::LeafCellParts) -> Self {
        Self {
            bytes,
            fields: LeafCellFields { key_range: parts.key_range, value_range: parts.value_range },
        }
    }

    fn bytes_for(&self, range: Range<usize>) -> &[u8] {
        &self.bytes[range]
    }
}

impl<'a> Cell<'a, Interior> {
    pub(crate) fn new_interior(bytes: &'a [u8], parts: interior::InteriorCellParts) -> Self {
        Self {
            bytes,
            fields: InteriorCellFields { key_range: parts.key_range, left_child: parts.left_child },
        }
    }
}

impl<'a> CellMut<'a, Leaf> {
    pub(crate) fn new_leaf(bytes: &'a mut [u8], parts: leaf::LeafCellParts) -> Self {
        Self {
            bytes,
            fields: LeafCellFields { key_range: parts.key_range, value_range: parts.value_range },
        }
    }
}

impl<'a> CellMut<'a, Interior> {
    pub(crate) fn new_interior(bytes: &'a mut [u8], parts: interior::InteriorCellParts) -> Self {
        Self {
            bytes,
            fields: InteriorCellFields { key_range: parts.key_range, left_child: parts.left_child },
        }
    }
}

impl Cell<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.fields.key_range.clone()))
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.fields.value_range.clone()))
    }
}

impl CellMut<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.fields.key_range.clone()])
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.fields.value_range.clone()])
    }

    /// Returns the byte value stored in this leaf cell mutably.
    pub fn value_mut(&mut self) -> PageResult<&mut [u8]> {
        let range = self.fields.value_range.clone();
        Ok(&mut self.bytes[range])
    }
}

impl Cell<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.fields.key_range.clone()])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.fields.left_child)
    }
}

impl CellMut<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.fields.key_range.clone()])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.fields.left_child)
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        interior::write_left_child(self.bytes, page_id);
        self.fields.left_child = page_id;
        Ok(())
    }
}
