use core::{marker::PhantomData, mem::size_of};

use crate::{PageId, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Interior, Leaf, Table},
    format::CELL_LENGTH_SIZE,
    index_interior, index_leaf, interior, leaf,
};

#[derive(Debug, Clone)]
enum CellMetadata {
    TableLeaf(leaf::LeafCellParts),
    IndexLeaf(index_leaf::IndexLeafCellParts),
    TableInterior(interior::InteriorCellParts),
    IndexInterior(index_interior::IndexInteriorCellParts),
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N, T = Table> {
    bytes: &'a [u8],
    metadata: CellMetadata,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N, T = Table> {
    bytes: &'a mut [u8],
    metadata: CellMetadata,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

fn decode_index_leaf_row_id(bytes: &[u8], slot_index: SlotId) -> PageResult<RowId> {
    let row_id_end = CELL_LENGTH_SIZE + size_of::<RowId>();
    let row_id_bytes = bytes
        .get(CELL_LENGTH_SIZE..row_id_end)
        .ok_or(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall })?;
    let row_id_bytes: [u8; size_of::<RowId>()] = row_id_bytes
        .try_into()
        .map_err(|_| PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall })?;
    Ok(RowId::from_le_bytes(row_id_bytes))
}

impl<'a, N, T> Cell<'a, N, T> {
    fn new(bytes: &'a [u8], metadata: CellMetadata, slot_index: SlotId) -> Self {
        Self { bytes, metadata, slot_index, _marker: PhantomData }
    }

    pub(crate) fn new_table_leaf(
        bytes: &'a [u8],
        parts: leaf::LeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::TableLeaf(parts), slot_index)
    }

    pub(crate) fn new_index_leaf(
        bytes: &'a [u8],
        parts: index_leaf::IndexLeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::IndexLeaf(parts), slot_index)
    }

    pub(crate) fn new_table_interior(
        bytes: &'a [u8],
        parts: interior::InteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::TableInterior(parts), slot_index)
    }

    pub(crate) fn new_index_interior(
        bytes: &'a [u8],
        parts: index_interior::IndexInteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::IndexInterior(parts), slot_index)
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    fn table_leaf_parts(&self) -> &leaf::LeafCellParts {
        match &self.metadata {
            CellMetadata::TableLeaf(parts) => parts,
            _ => unreachable!("table leaf cell metadata mismatch"),
        }
    }

    fn index_leaf_parts(&self) -> &index_leaf::IndexLeafCellParts {
        match &self.metadata {
            CellMetadata::IndexLeaf(parts) => parts,
            _ => unreachable!("index leaf cell metadata mismatch"),
        }
    }

    fn table_interior_parts(&self) -> &interior::InteriorCellParts {
        match &self.metadata {
            CellMetadata::TableInterior(parts) => parts,
            _ => unreachable!("table interior cell metadata mismatch"),
        }
    }

    fn index_interior_parts(&self) -> &index_interior::IndexInteriorCellParts {
        match &self.metadata {
            CellMetadata::IndexInterior(parts) => parts,
            _ => unreachable!("index interior cell metadata mismatch"),
        }
    }
}

impl<'a, N, T> CellMut<'a, N, T> {
    fn new(bytes: &'a mut [u8], metadata: CellMetadata, slot_index: SlotId) -> Self {
        Self { bytes, metadata, slot_index, _marker: PhantomData }
    }

    pub(crate) fn new_table_leaf(
        bytes: &'a mut [u8],
        parts: leaf::LeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::TableLeaf(parts), slot_index)
    }

    pub(crate) fn new_index_leaf(
        bytes: &'a mut [u8],
        parts: index_leaf::IndexLeafCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::IndexLeaf(parts), slot_index)
    }

    pub(crate) fn new_table_interior(
        bytes: &'a mut [u8],
        parts: interior::InteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::TableInterior(parts), slot_index)
    }

    pub(crate) fn new_index_interior(
        bytes: &'a mut [u8],
        parts: index_interior::IndexInteriorCellParts,
        slot_index: SlotId,
    ) -> Self {
        Self::new(bytes, CellMetadata::IndexInterior(parts), slot_index)
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    /// Borrows this mutable cell as an immutable cell view.
    pub fn as_ref(&self) -> Cell<'_, N, T> {
        Cell::new(self.bytes, self.metadata.clone(), self.slot_index)
    }

    fn table_leaf_parts(&self) -> &leaf::LeafCellParts {
        match &self.metadata {
            CellMetadata::TableLeaf(parts) => parts,
            _ => unreachable!("table leaf cell metadata mismatch"),
        }
    }

    fn index_leaf_parts(&self) -> &index_leaf::IndexLeafCellParts {
        match &self.metadata {
            CellMetadata::IndexLeaf(parts) => parts,
            _ => unreachable!("index leaf cell metadata mismatch"),
        }
    }

    fn table_interior_parts(&self) -> &interior::InteriorCellParts {
        match &self.metadata {
            CellMetadata::TableInterior(parts) => parts,
            _ => unreachable!("table interior cell metadata mismatch"),
        }
    }

    fn index_interior_parts(&self) -> &index_interior::IndexInteriorCellParts {
        match &self.metadata {
            CellMetadata::IndexInterior(parts) => parts,
            _ => unreachable!("index interior cell metadata mismatch"),
        }
    }
}

impl Cell<'_, Leaf, Table> {
    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.table_leaf_parts().row_id)
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.table_leaf_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }
}

impl CellMut<'_, Leaf, Table> {
    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.table_leaf_parts().row_id)
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.table_leaf_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the payload bytes stored in this leaf cell mutably.
    pub fn payload_mut(&mut self) -> PageResult<&mut [u8]> {
        let range = self.table_leaf_parts().payload_range.clone();
        Ok(&mut self.bytes[range])
    }
}

impl Cell<'_, Leaf, Index> {
    /// Returns the referenced row id stored in this leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_index_leaf_row_id(self.bytes, self.slot_index)
    }

    /// Returns the variable-sized payload bytes stored in this leaf cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_leaf_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }
}

impl CellMut<'_, Leaf, Index> {
    /// Returns the referenced row id stored in this leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        decode_index_leaf_row_id(self.bytes, self.slot_index)
    }

    /// Returns the variable-sized payload bytes stored in this leaf cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_leaf_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }
}

impl Cell<'_, Interior, Table> {
    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.table_interior_parts().row_id)
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.table_interior_parts().left_child)
    }
}

impl CellMut<'_, Interior, Table> {
    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.table_interior_parts().row_id)
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.table_interior_parts().left_child)
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        interior::write_left_child(self.bytes, page_id);
        if let CellMetadata::TableInterior(parts) = &mut self.metadata {
            parts.left_child = page_id;
        }
        Ok(())
    }
}

impl Cell<'_, Interior, Index> {
    /// Returns the variable-sized payload bytes stored in this interior cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_interior_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.index_interior_parts().left_child)
    }
}

impl CellMut<'_, Interior, Index> {
    /// Returns the variable-sized payload bytes stored in this interior cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_interior_parts().payload_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.index_interior_parts().left_child)
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        index_interior::write_left_child(self.bytes, page_id);
        if let CellMetadata::IndexInterior(parts) = &mut self.metadata {
            parts.left_child = page_id;
        }
        Ok(())
    }
}
