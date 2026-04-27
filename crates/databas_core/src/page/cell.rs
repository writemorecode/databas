use core::marker::PhantomData;
use std::ops::Range;

use crate::PageId;

use super::{
    PageResult,
    core::{Interior, Leaf},
    interior, leaf,
};

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N> {
    bytes: &'a [u8],
    key_range: Range<usize>,
    value_range: Option<Range<usize>>,
    left_child: Option<PageId>,
    _marker: PhantomData<N>,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N> {
    bytes: &'a mut [u8],
    key_range: Range<usize>,
    value_range: Option<Range<usize>>,
    left_child: Option<PageId>,
    _marker: PhantomData<N>,
}

impl<'a, N> Cell<'a, N> {
    pub(crate) fn new_leaf(bytes: &'a [u8], parts: leaf::LeafCellParts) -> Self {
        Self {
            bytes,
            key_range: parts.key_range,
            value_range: Some(parts.value_range),
            left_child: None,
            _marker: PhantomData,
        }
    }

    pub(crate) fn new_interior(bytes: &'a [u8], parts: interior::InteriorCellParts) -> Self {
        Self {
            bytes,
            key_range: parts.key_range,
            value_range: None,
            left_child: Some(parts.left_child),
            _marker: PhantomData,
        }
    }

    fn bytes_for(&self, range: Range<usize>) -> &[u8] {
        &self.bytes[range]
    }
}

impl<'a, N> CellMut<'a, N> {
    pub(crate) fn new_leaf(bytes: &'a mut [u8], parts: leaf::LeafCellParts) -> Self {
        Self {
            bytes,
            key_range: parts.key_range,
            value_range: Some(parts.value_range),
            left_child: None,
            _marker: PhantomData,
        }
    }

    pub(crate) fn new_interior(bytes: &'a mut [u8], parts: interior::InteriorCellParts) -> Self {
        Self {
            bytes,
            key_range: parts.key_range,
            value_range: None,
            left_child: Some(parts.left_child),
            _marker: PhantomData,
        }
    }
}

impl Cell<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.key_range.clone()))
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.value_range.clone().expect("leaf cell has a value range")))
    }
}

impl CellMut<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.key_range.clone()])
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.value_range.clone().expect("leaf cell has a value range")])
    }

    /// Returns the byte value stored in this leaf cell mutably.
    pub fn value_mut(&mut self) -> PageResult<&mut [u8]> {
        let range = self.value_range.clone().expect("leaf cell has a value range");
        Ok(&mut self.bytes[range])
    }
}

impl Cell<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.key_range.clone()))
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.left_child.expect("interior cell has a left child"))
    }
}

impl CellMut<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(&self.bytes[self.key_range.clone()])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.left_child.expect("interior cell has a left child"))
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        interior::write_left_child(self.bytes, page_id);
        self.left_child = Some(page_id);
        Ok(())
    }
}
