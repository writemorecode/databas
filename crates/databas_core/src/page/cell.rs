use core::{marker::PhantomData, mem::size_of};

use crate::{PageId, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Interior, Leaf, Table},
    format::CELL_LENGTH_SIZE,
    index_interior, index_leaf, table_interior, table_leaf,
};

#[derive(Debug, Clone)]
enum CellMetadata {
    TableLeaf(table_leaf::LeafCellParts),
    IndexLeaf(index_leaf::IndexLeafCellParts),
    TableInterior(table_interior::InteriorCellParts),
    IndexInterior(index_interior::IndexInteriorCellParts),
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N, T> {
    bytes: &'a [u8],
    metadata: CellMetadata,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N, T> {
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
        parts: table_leaf::LeafCellParts,
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
        parts: table_interior::InteriorCellParts,
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

    /// Returns the total encoded size of this cell, including all header fields and payload.
    pub fn encoded_size(&self) -> usize {
        self.bytes.len()
    }

    fn table_leaf_parts(&self) -> &table_leaf::LeafCellParts {
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

    fn table_interior_parts(&self) -> &table_interior::InteriorCellParts {
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
        parts: table_leaf::LeafCellParts,
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
        parts: table_interior::InteriorCellParts,
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

    /// Returns the total encoded size of this cell, including all header fields and payload.
    pub fn encoded_size(&self) -> usize {
        self.bytes.len()
    }

    /// Borrows this mutable cell as an immutable cell view.
    pub fn as_ref(&self) -> Cell<'_, N, T> {
        Cell::new(self.bytes, self.metadata.clone(), self.slot_index)
    }

    fn table_leaf_parts(&self) -> &table_leaf::LeafCellParts {
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

    fn table_interior_parts(&self) -> &table_interior::InteriorCellParts {
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
        table_interior::write_left_child(self.bytes, page_id);
        if let CellMetadata::TableInterior(parts) = &mut self.metadata {
            parts.left_child = page_id;
        }
        Ok(())
    }
}

impl Cell<'_, Interior, Index> {
    /// Returns the separator row id stored in this interior cell payload suffix.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.index_interior_parts().row_id)
    }

    /// Returns the variable-sized payload bytes stored in this interior cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_interior_parts().key_range.clone();
        Ok(&self.bytes[range])
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        Ok(self.index_interior_parts().left_child)
    }
}

impl CellMut<'_, Interior, Index> {
    /// Returns the separator row id stored in this interior cell payload suffix.
    pub fn row_id(&self) -> PageResult<RowId> {
        Ok(self.index_interior_parts().row_id)
    }

    /// Returns the variable-sized payload bytes stored in this interior cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let range = self.index_interior_parts().key_range.clone();
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

#[cfg(test)]
mod tests {
    use crate::{PAGE_SIZE, RowId};

    use super::super::{Index, Interior, Leaf, Page, Table, Write};
    use super::{index_interior, table_interior, table_leaf};

    #[test]
    fn encoded_size_includes_leaf_header_and_payload() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf, Table>::initialize(&mut bytes);
        page.insert(7, b"hello").unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.cell(0).unwrap();

        assert_eq!(cell.encoded_size(), table_leaf::LEAF_CELL_PREFIX_SIZE + b"hello".len());
    }

    #[test]
    fn encoded_size_matches_fixed_size_table_interior_cells() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior, Table>::initialize_with_rightmost(&mut bytes, 9);
        page.insert(42, 3).unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.cell(0).unwrap();

        assert_eq!(cell.encoded_size(), table_interior::INTERIOR_CELL_SIZE);
    }

    #[test]
    fn encoded_size_includes_index_interior_prefix_and_payload() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page =
            Page::<Write<'_>, Interior, Index>::initialize_with_rightmost(&mut bytes, 17);
        page.insert(b"mango", 11, 5).unwrap();

        let page_ref = page.as_ref();
        let cell = page_ref.cell(0).unwrap();

        assert_eq!(
            cell.encoded_size(),
            index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE + b"mango".len() + size_of::<RowId>()
        );
    }

    #[test]
    fn mutable_cell_reports_same_encoded_size() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf, Table>::initialize(&mut bytes);
        page.insert(11, b"abc").unwrap();

        let cell = page.cell_mut(0).unwrap();

        assert_eq!(cell.encoded_size(), table_leaf::LEAF_CELL_PREFIX_SIZE + 3);
        assert_eq!(cell.as_ref().encoded_size(), table_leaf::LEAF_CELL_PREFIX_SIZE + 3);
    }
}
