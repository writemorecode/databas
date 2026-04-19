use core::marker::PhantomData;
use std::ops::Range;

use crate::{PageId, SlotId};

use super::{
    PageResult,
    core::{Interior, Leaf},
    interior, leaf,
};

#[derive(Debug, Clone)]
enum CellMetadata {
    Leaf(leaf::LeafCellParts),
    Interior(interior::InteriorCellParts),
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N> {
    bytes: &'a [u8],
    metadata: CellMetadata,
    slot_index: SlotId,
    _marker: PhantomData<N>,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N> {
    bytes: &'a mut [u8],
    metadata: CellMetadata,
    slot_index: SlotId,
    _marker: PhantomData<N>,
}

impl<'a, N> Cell<'a, N> {
    fn new(bytes: &'a [u8], metadata: CellMetadata, slot_index: SlotId) -> Self {
        Self { bytes, metadata, slot_index, _marker: PhantomData }
    }

    pub(crate) fn new_leaf(
        bytes: &'a [u8],
        parts: leaf::LeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::Leaf(parts), slot_index)
    }

    pub(crate) fn new_interior(
        bytes: &'a [u8],
        parts: interior::InteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::Interior(parts), slot_index)
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    fn leaf_parts(&self) -> &leaf::LeafCellParts {
        match &self.metadata {
            CellMetadata::Leaf(parts) => parts,
            _ => unreachable!("leaf cell metadata mismatch"),
        }
    }

    fn interior_parts(&self) -> &interior::InteriorCellParts {
        match &self.metadata {
            CellMetadata::Interior(parts) => parts,
            _ => unreachable!("interior cell metadata mismatch"),
        }
    }

    fn bytes_for(&self, range: Range<usize>) -> &[u8] {
        &self.bytes[range]
    }
}

impl<'a, N> CellMut<'a, N> {
    fn new(bytes: &'a mut [u8], metadata: CellMetadata, slot_index: SlotId) -> Self {
        Self { bytes, metadata, slot_index, _marker: PhantomData }
    }

    pub(crate) fn new_leaf(
        bytes: &'a mut [u8],
        parts: leaf::LeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::Leaf(parts), slot_index)
    }

    pub(crate) fn new_interior(
        bytes: &'a mut [u8],
        parts: interior::InteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::Interior(parts), slot_index)
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    /// Borrows this mutable cell as an immutable cell view.
    pub fn as_ref(&self) -> Cell<'_, N> {
        Cell::new(self.bytes, self.metadata.clone(), self.slot_index)
    }

    fn leaf_parts(&self) -> &leaf::LeafCellParts {
        match &self.metadata {
            CellMetadata::Leaf(parts) => parts,
            _ => unreachable!("leaf cell metadata mismatch"),
        }
    }

    fn interior_parts(&self) -> &interior::InteriorCellParts {
        match &self.metadata {
            CellMetadata::Interior(parts) => parts,
            _ => unreachable!("interior cell metadata mismatch"),
        }
    }
}

impl Cell<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.leaf_parts().key_range.clone()))
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.leaf_parts().value_range.clone()))
    }
}

impl CellMut<'_, Leaf> {
    /// Returns the byte key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        let range = self.leaf_parts().key_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the byte value stored in this leaf cell.
    pub fn value(&self) -> PageResult<&[u8]> {
        let range = self.leaf_parts().value_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the byte value stored in this leaf cell mutably.
    pub fn value_mut(&mut self) -> PageResult<&mut [u8]> {
        let range = self.leaf_parts().value_range.clone();
        Ok(&mut self.bytes[range])
    }
}

impl Cell<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        Ok(self.bytes_for(self.interior_parts().key_range.clone()))
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.interior_parts().left_child)
    }
}

impl CellMut<'_, Interior> {
    /// Returns the separator key stored in this interior cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        let range = self.interior_parts().key_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.interior_parts().left_child)
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        interior::write_left_child(self.bytes, page_id);
        if let CellMetadata::Interior(parts) = &mut self.metadata {
            parts.left_child = page_id;
        }
        Ok(())
    }
}
