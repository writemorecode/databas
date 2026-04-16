use core::{marker::PhantomData, ops::Range};

use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Interior, Leaf, Table},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
    index_interior, index_leaf, table_interior, table_leaf,
};

pub(crate) trait CellKind {
    type Header: Clone + core::fmt::Debug;
}

#[derive(Debug, Clone)]
pub(crate) struct TableLeafHeader {
    row_id: RowId,
    payload_range: Range<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexLeafHeader {
    row_id: RowId,
    key_range: Range<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TableInteriorHeader {
    left_child: PageId,
    row_id: RowId,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexInteriorHeader {
    left_child: PageId,
    row_id: RowId,
    key_range: Range<usize>,
}

impl CellKind for (Leaf, Table) {
    type Header = TableLeafHeader;
}

impl CellKind for (Leaf, Index) {
    type Header = IndexLeafHeader;
}

impl CellKind for (Interior, Table) {
    type Header = TableInteriorHeader;
}

impl CellKind for (Interior, Index) {
    type Header = IndexInteriorHeader;
}

fn cell_end(cell_offset: usize, cell_len: usize, slot_index: SlotId) -> PageResult<usize> {
    let cell_end = cell_offset
        .checked_add(cell_len)
        .ok_or(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds })?;
    if cell_end > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }
    Ok(cell_end)
}

fn read_variable_cell_len(
    bytes: &[u8; PAGE_SIZE],
    cell_offset: usize,
    slot_index: SlotId,
    min_len: usize,
) -> PageResult<usize> {
    let cell_len = format::read_u16(bytes, cell_offset) as usize;
    if cell_len < min_len {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }
    cell_end(cell_offset, cell_len, slot_index)?;
    Ok(cell_len)
}

fn slice_cell<'a>(
    bytes: &'a [u8; PAGE_SIZE],
    cell_offset: usize,
    cell_len: usize,
    slot_index: SlotId,
) -> PageResult<&'a [u8]> {
    let cell_end = cell_end(cell_offset, cell_len, slot_index)?;
    bytes
        .get(cell_offset..cell_end)
        .ok_or(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds })
}

fn slice_cell_mut<'a>(
    bytes: &'a mut [u8; PAGE_SIZE],
    cell_offset: usize,
    cell_len: usize,
    slot_index: SlotId,
) -> PageResult<&'a mut [u8]> {
    let cell_end = cell_end(cell_offset, cell_len, slot_index)?;
    bytes
        .get_mut(cell_offset..cell_end)
        .ok_or(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds })
}

pub(crate) fn table_leaf_cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    read_variable_cell_len(bytes, cell_offset, slot_index, table_leaf::LEAF_CELL_PREFIX_SIZE)
}

pub(crate) fn index_leaf_cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    read_variable_cell_len(bytes, cell_offset, slot_index, index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE)
}

pub(crate) fn table_interior_cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    cell_end(cell_offset, table_interior::INTERIOR_CELL_SIZE, slot_index)?;
    let _ = bytes;
    Ok(table_interior::INTERIOR_CELL_SIZE)
}

pub(crate) fn index_interior_cell_len_at(
    bytes: &[u8; PAGE_SIZE],
    slot_index: SlotId,
    cell_offset: usize,
) -> PageResult<usize> {
    read_variable_cell_len(
        bytes,
        cell_offset,
        slot_index,
        index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE + core::mem::size_of::<RowId>(),
    )
}

/// A typed immutable view over a single page cell.
#[derive(Debug)]
pub struct Cell<'a, N, T>
where
    (N, T): CellKind,
{
    bytes: &'a [u8],
    header: <(N, T) as CellKind>::Header,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

/// A typed mutable view over a single page cell.
#[derive(Debug)]
pub struct CellMut<'a, N, T>
where
    (N, T): CellKind,
{
    bytes: &'a mut [u8],
    header: <(N, T) as CellKind>::Header,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

fn decode_index_interior_row_id(bytes: &[u8], slot_index: SlotId) -> PageResult<RowId> {
    let row_id_bytes: [u8; core::mem::size_of::<RowId>()] = bytes
        .try_into()
        .map_err(|_| PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall })?;
    Ok(RowId::from_be_bytes(row_id_bytes))
}

impl<'a, N, T> Cell<'a, N, T>
where
    (N, T): CellKind,
{
    fn from_parts(bytes: &'a [u8], header: <(N, T) as CellKind>::Header, slot_index: SlotId) -> Self
    where
        (N, T): CellKind,
    {
        Self { bytes, header, slot_index, _marker: PhantomData }
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    /// Returns the total encoded size of this cell, including all header fields and payload.
    pub fn encoded_size(&self) -> usize {
        self.bytes.len()
    }
}

impl<'a, N, T> CellMut<'a, N, T>
where
    (N, T): CellKind,
{
    fn from_parts(
        bytes: &'a mut [u8],
        header: <(N, T) as CellKind>::Header,
        slot_index: SlotId,
    ) -> Self
    where
        (N, T): CellKind,
    {
        Self { bytes, header, slot_index, _marker: PhantomData }
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
        Cell::from_parts(self.bytes, self.header.clone(), self.slot_index)
    }
}

impl<'a> Cell<'a, Leaf, Table> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_leaf_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id = format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE);
        let bytes = slice_cell(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            TableLeafHeader { row_id, payload_range: table_leaf::LEAF_CELL_PREFIX_SIZE..cell_len },
            slot_index,
        ))
    }

    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> &[u8] {
        &self.bytes[self.header.payload_range.clone()]
    }
}

impl<'a> CellMut<'a, Leaf, Table> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_leaf_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id = format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE);
        let bytes = slice_cell_mut(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            TableLeafHeader { row_id, payload_range: table_leaf::LEAF_CELL_PREFIX_SIZE..cell_len },
            slot_index,
        ))
    }

    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> &[u8] {
        &self.bytes[self.header.payload_range.clone()]
    }

    /// Returns the payload bytes stored in this leaf cell mutably.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[self.header.payload_range.clone()]
    }
}

impl<'a> Cell<'a, Leaf, Index> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = index_leaf_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id = format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE);
        let bytes = slice_cell(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            IndexLeafHeader {
                row_id,
                key_range: index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE..cell_len,
            },
            slot_index,
        ))
    }

    /// Returns the referenced row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this leaf cell.
    pub fn key(&self) -> &[u8] {
        &self.bytes[self.header.key_range.clone()]
    }
}

impl<'a> CellMut<'a, Leaf, Index> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = index_leaf_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id = format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE);
        let bytes = slice_cell_mut(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            IndexLeafHeader {
                row_id,
                key_range: index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE..cell_len,
            },
            slot_index,
        ))
    }

    /// Returns the referenced row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this leaf cell.
    pub fn key(&self) -> &[u8] {
        &self.bytes[self.header.key_range.clone()]
    }
}

impl<'a> Cell<'a, Interior, Table> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_interior_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let bytes = slice_cell(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            TableInteriorHeader {
                left_child: format::read_u64(page_bytes, cell_offset),
                row_id: format::read_u64(page_bytes, cell_offset + core::mem::size_of::<PageId>()),
            },
            slot_index,
        ))
    }

    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }
}

impl<'a> CellMut<'a, Interior, Table> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_interior_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let left_child = format::read_u64(page_bytes, cell_offset);
        let row_id = format::read_u64(page_bytes, cell_offset + core::mem::size_of::<PageId>());
        let bytes = slice_cell_mut(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(bytes, TableInteriorHeader { left_child, row_id }, slot_index))
    }

    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        table_interior::write_left_child(self.bytes, page_id);
        self.header.left_child = page_id;
    }
}

impl<'a> Cell<'a, Interior, Index> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = index_interior_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id_offset = cell_offset + cell_len - core::mem::size_of::<RowId>();
        let row_id = decode_index_interior_row_id(
            &page_bytes[row_id_offset..row_id_offset + core::mem::size_of::<RowId>()],
            slot_index,
        )?;
        let bytes = slice_cell(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            IndexInteriorHeader {
                left_child: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
                row_id,
                key_range: index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE
                    ..cell_len - core::mem::size_of::<RowId>(),
            },
            slot_index,
        ))
    }

    /// Returns the separator row id stored in this interior cell payload suffix.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this interior cell.
    pub fn key(&self) -> &[u8] {
        &self.bytes[self.header.key_range.clone()]
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }
}

impl<'a> CellMut<'a, Interior, Index> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = index_interior_cell_len_at(page_bytes, slot_index, cell_offset)?;
        let row_id_offset = cell_offset + cell_len - core::mem::size_of::<RowId>();
        let row_id = decode_index_interior_row_id(
            &page_bytes[row_id_offset..row_id_offset + core::mem::size_of::<RowId>()],
            slot_index,
        )?;
        let left_child = format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE);
        let bytes = slice_cell_mut(page_bytes, cell_offset, cell_len, slot_index)?;
        Ok(Self::from_parts(
            bytes,
            IndexInteriorHeader {
                left_child,
                row_id,
                key_range: index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE
                    ..cell_len - core::mem::size_of::<RowId>(),
            },
            slot_index,
        ))
    }

    /// Returns the separator row id stored in this interior cell payload suffix.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this interior cell.
    pub fn key(&self) -> &[u8] {
        &self.bytes[self.header.key_range.clone()]
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }

    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        index_interior::write_left_child(self.bytes, page_id);
        self.header.left_child = page_id;
    }
}
