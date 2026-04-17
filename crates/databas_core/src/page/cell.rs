use core::{marker::PhantomData, mem::size_of};

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

/// Decoded header fields for a table leaf cell.
///
/// Exact on-page layout:
///
/// ```text
/// [cell_len: u16][row_id: u64][payload bytes...]
/// ```
///
/// The fixed-size prefix is `cell_len + row_id`. The payload bytes occupy the
/// remainder of the already-sliced cell buffer after that prefix.
#[derive(Debug, Clone)]
pub(crate) struct TableLeafHeader {
    row_id: RowId,
}

/// Decoded header fields for an index leaf cell.
///
/// Exact on-page layout:
///
/// ```text
/// [cell_len: u16][row_id: u64][key bytes...]
/// ```
///
/// The fixed-size prefix is `cell_len + row_id`. The key bytes occupy the
/// remainder of the already-sliced cell buffer after that prefix.
#[derive(Debug, Clone)]
pub(crate) struct IndexLeafHeader {
    row_id: RowId,
}

/// Decoded header fields for a table interior cell.
///
/// Exact on-page layout:
///
/// ```text
/// [left_child: u64][row_id: u64]
/// ```
///
/// This cell kind is entirely fixed-size, so there is no payload or key range.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TableInteriorHeader {
    left_child: PageId,
    row_id: RowId,
}

/// Decoded header fields for an index interior cell.
///
/// Exact on-page layout:
///
/// ```text
/// [cell_len: u16][left_child: u64][key bytes...][row_id: u64]
/// ```
///
/// This cell has fixed-size fields on both sides of the variable-sized key:
/// the prefix is `cell_len + left_child`, and the suffix is `row_id`.
/// The key bytes occupy the middle region between those fixed-size fields
/// within the already-sliced cell buffer.
#[derive(Debug, Clone)]
pub(crate) struct IndexInteriorHeader {
    left_child: PageId,
    row_id: RowId,
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

pub(super) fn checked_cell_end(
    cell_offset: usize,
    cell_len: usize,
    slot_index: SlotId,
) -> PageResult<usize> {
    let cell_end = cell_offset
        .checked_add(cell_len)
        .ok_or(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds })?;
    if cell_end > USABLE_SPACE_END {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthOutOfBounds });
    }
    Ok(cell_end)
}

pub(super) fn checked_variable_cell_bounds(
    page_bytes: &[u8; PAGE_SIZE],
    cell_offset: usize,
    slot_index: SlotId,
    min_len: usize,
) -> PageResult<(usize, usize)> {
    let cell_len = format::read_u16(page_bytes, cell_offset) as usize;
    if cell_len < min_len {
        return Err(PageError::CorruptCell { slot_index, kind: CellCorruption::LengthTooSmall });
    }
    let cell_end = checked_cell_end(cell_offset, cell_len, slot_index)?;
    Ok((cell_len, cell_end))
}

/// Shared immutable access to a cell-sized byte buffer.
#[derive(Debug, Clone, Copy)]
pub struct CellRead<'a> {
    bytes: &'a [u8],
}

/// Shared mutable access to a cell-sized byte buffer.
#[derive(Debug)]
pub struct CellWrite<'a> {
    bytes: &'a mut [u8],
}

/// Abstraction over cell access modes that can expose immutable bytes.
pub trait CellAccess {
    /// Returns the underlying cell buffer.
    fn bytes(&self) -> &[u8];
}

/// Extension of [`CellAccess`] for access modes that can mutate the cell buffer.
pub trait CellAccessMut: CellAccess {
    /// Returns the underlying cell buffer mutably.
    fn bytes_mut(&mut self) -> &mut [u8];
}

impl CellAccess for CellRead<'_> {
    fn bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl CellAccess for CellWrite<'_> {
    fn bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl CellAccessMut for CellWrite<'_> {
    fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes
    }
}

/// A typed view over a single page cell.
#[derive(Debug)]
pub struct Cell<A, N, T>
where
    (N, T): CellKind,
{
    access: A,
    header: <(N, T) as CellKind>::Header,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

impl<A, N, T> Cell<A, N, T>
where
    A: CellAccess,
    (N, T): CellKind,
{
    fn from_parts(access: A, header: <(N, T) as CellKind>::Header, slot_index: SlotId) -> Self {
        Self { access, header, slot_index, _marker: PhantomData }
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }

    /// Returns the total encoded size of this cell, including all header fields and payload.
    pub fn encoded_size(&self) -> usize {
        self.access.bytes().len()
    }
}

impl<'a> Cell<CellRead<'a>, Leaf, Table> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            table_leaf::LEAF_CELL_PREFIX_SIZE,
        )?;

        let header = TableLeafHeader {
            row_id: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
        };
        Ok(Self::from_parts(
            CellRead { bytes: &page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<'a> Cell<CellWrite<'a>, Leaf, Table> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            table_leaf::LEAF_CELL_PREFIX_SIZE,
        )?;

        let header = TableLeafHeader {
            row_id: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
        };
        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<A> Cell<A, Leaf, Table>
where
    A: CellAccess,
{
    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> &[u8] {
        let cell_len = self.access.bytes().len();
        let bytes = self.access.bytes();
        &bytes[table_leaf::LEAF_CELL_PREFIX_SIZE..cell_len]
    }
}

impl<A> Cell<A, Leaf, Table>
where
    A: CellAccessMut,
{
    /// Returns the payload bytes stored in this leaf cell mutably.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let cell_len = self.access.bytes().len();
        let bytes_mut = self.access.bytes_mut();
        &mut bytes_mut[table_leaf::LEAF_CELL_PREFIX_SIZE..cell_len]
    }
}

impl<'a> Cell<CellRead<'a>, Leaf, Index> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE,
        )?;

        let header = IndexLeafHeader {
            row_id: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
        };
        Ok(Self::from_parts(
            CellRead { bytes: &page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<'a> Cell<CellWrite<'a>, Leaf, Index> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE,
        )?;

        let header = IndexLeafHeader {
            row_id: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
        };
        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<A> Cell<A, Leaf, Index>
where
    A: CellAccess,
{
    /// Returns the referenced row id stored in this leaf cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this leaf cell.
    pub fn key(&self) -> &[u8] {
        let cell_len = self.access.bytes().len();
        let bytes = self.access.bytes();
        &bytes[index_leaf::INDEX_LEAF_CELL_PREFIX_SIZE..cell_len]
    }
}

impl<'a> Cell<CellRead<'a>, Interior, Table> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_interior::INTERIOR_CELL_SIZE;
        let cell_end = cell_offset + cell_len;

        let header = TableInteriorHeader {
            left_child: format::read_u64(page_bytes, cell_offset),
            row_id: format::read_u64(page_bytes, cell_offset + size_of::<PageId>()),
        };
        Ok(Self::from_parts(
            CellRead { bytes: &page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<'a> Cell<CellWrite<'a>, Interior, Table> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_interior::INTERIOR_CELL_SIZE;
        let cell_end = cell_offset + cell_len;

        let header = TableInteriorHeader {
            left_child: format::read_u64(page_bytes, cell_offset),
            row_id: format::read_u64(page_bytes, cell_offset + size_of::<PageId>()),
        };
        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<A> Cell<A, Interior, Table>
where
    A: CellAccess,
{
    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }
}

impl<A> Cell<A, Interior, Table>
where
    A: CellAccessMut,
{
    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        table_interior::write_left_child(self.access.bytes_mut(), page_id);
        self.header.left_child = page_id;
    }
}

impl<'a> Cell<CellRead<'a>, Interior, Index> {
    pub(crate) fn new(
        page_bytes: &'a [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE + size_of::<RowId>(),
        )?;

        let row_id_offset = cell_end - size_of::<RowId>();
        let mut row_id_bytes = [0_u8; size_of::<RowId>()];
        row_id_bytes.copy_from_slice(&page_bytes[row_id_offset..cell_end]);
        let header = IndexInteriorHeader {
            left_child: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
            row_id: RowId::from_be_bytes(row_id_bytes),
        };
        Ok(Self::from_parts(
            CellRead { bytes: &page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<'a> Cell<CellWrite<'a>, Interior, Index> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let (_, cell_end) = checked_variable_cell_bounds(
            page_bytes,
            cell_offset,
            slot_index,
            index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE + size_of::<RowId>(),
        )?;

        let row_id_offset = cell_end - size_of::<RowId>();
        let mut row_id_bytes = [0_u8; size_of::<RowId>()];
        row_id_bytes.copy_from_slice(&page_bytes[row_id_offset..cell_end]);
        let header = IndexInteriorHeader {
            left_child: format::read_u64(page_bytes, cell_offset + CELL_LENGTH_SIZE),
            row_id: RowId::from_be_bytes(row_id_bytes),
        };
        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
            header,
            slot_index,
        ))
    }
}

impl<A> Cell<A, Interior, Index>
where
    A: CellAccess,
{
    /// Returns the separator row id stored in this interior cell payload suffix.
    pub fn row_id(&self) -> RowId {
        self.header.row_id
    }

    /// Returns the variable-sized key bytes stored in this interior cell.
    pub fn key(&self) -> &[u8] {
        let cell_len = self.access.bytes().len();
        let bytes = self.access.bytes();
        &bytes[index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE..cell_len - size_of::<RowId>()]
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        self.header.left_child
    }
}

impl<A> Cell<A, Interior, Index>
where
    A: CellAccessMut,
{
    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        index_interior::write_left_child(self.access.bytes_mut(), page_id);
        self.header.left_child = page_id;
    }
}
