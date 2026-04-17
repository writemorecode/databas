use core::{marker::PhantomData, mem::size_of};

use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Index, Interior, Leaf, Table},
    format::{self, CELL_LENGTH_SIZE, USABLE_SPACE_END},
    index_interior, index_leaf, table_interior, table_leaf,
};

pub(crate) trait CellKind {}

impl CellKind for (Leaf, Table) {}

impl CellKind for (Leaf, Index) {}

impl CellKind for (Interior, Table) {}

impl CellKind for (Interior, Index) {}

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

fn row_id_suffix(bytes: &[u8]) -> RowId {
    RowId::from_be_bytes(
        bytes[bytes.len() - size_of::<RowId>()..]
            .try_into()
            .expect("row id suffix has fixed width"),
    )
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
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

impl<A, N, T> Cell<A, N, T>
where
    A: CellAccess,
    (N, T): CellKind,
{
    fn from_parts(access: A, slot_index: SlotId) -> Self {
        Self { access, slot_index, _marker: PhantomData }
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

        Ok(Self::from_parts(CellRead { bytes: &page_bytes[cell_offset..cell_end] }, slot_index))
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

        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
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
        read_u64_from_slice(self.access.bytes(), CELL_LENGTH_SIZE)
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> &[u8] {
        let cell_len = self.access.bytes().len();
        let bytes = self.access.bytes();
        &bytes[table_leaf::LEAF_CELL_PREFIX_SIZE..cell_len]
    }
}

fn read_u64_from_slice(bytes: &[u8], offset: usize) -> u64 {
    let x = bytes[offset..offset + size_of::<u64>()].as_array().expect("valid cell length");
    u64::from_le_bytes(*x)
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

        Ok(Self::from_parts(CellRead { bytes: &page_bytes[cell_offset..cell_end] }, slot_index))
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

        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
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
        read_u64_from_slice(self.access.bytes(), CELL_LENGTH_SIZE)
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
        let cell_end = checked_cell_end(cell_offset, cell_len, slot_index)?;

        Ok(Self::from_parts(CellRead { bytes: &page_bytes[cell_offset..cell_end] }, slot_index))
    }
}

impl<'a> Cell<CellWrite<'a>, Interior, Table> {
    pub(crate) fn new(
        page_bytes: &'a mut [u8; PAGE_SIZE],
        cell_offset: usize,
        slot_index: SlotId,
    ) -> PageResult<Self> {
        let cell_len = table_interior::INTERIOR_CELL_SIZE;
        let cell_end = checked_cell_end(cell_offset, cell_len, slot_index)?;

        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
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
        read_u64_from_slice(self.access.bytes(), size_of::<PageId>())
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        read_u64_from_slice(self.access.bytes(), 0)
    }
}

impl<A> Cell<A, Interior, Table>
where
    A: CellAccessMut,
{
    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        table_interior::write_left_child(self.access.bytes_mut(), page_id);
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

        Ok(Self::from_parts(CellRead { bytes: &page_bytes[cell_offset..cell_end] }, slot_index))
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

        Ok(Self::from_parts(
            CellWrite { bytes: &mut page_bytes[cell_offset..cell_end] },
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
        row_id_suffix(self.access.bytes())
    }

    /// Returns the variable-sized key bytes stored in this interior cell.
    pub fn key(&self) -> &[u8] {
        let bytes = self.access.bytes();
        &bytes[index_interior::INDEX_INTERIOR_CELL_PREFIX_SIZE..bytes.len() - size_of::<RowId>()]
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageId {
        read_u64_from_slice(self.access.bytes(), CELL_LENGTH_SIZE)
    }
}

impl<A> Cell<A, Interior, Index>
where
    A: CellAccessMut,
{
    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) {
        index_interior::write_left_child(self.access.bytes_mut(), page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Page, Read, Write};
    use super::*;

    #[test]
    fn table_leaf_cell_reads_row_id_and_payload_from_bytes() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf, Table>::init(&mut bytes);
        page.insert(42, b"payload").unwrap();

        let page = Page::<Read<'_>, Leaf, Table>::open(&bytes).unwrap();
        let cell = page.cell(0).unwrap();

        assert_eq!(cell.row_id(), 42);
        assert_eq!(cell.payload(), b"payload");
    }

    #[test]
    fn index_leaf_cell_reads_row_id_and_key_from_bytes() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Leaf, Index>::init(&mut bytes);
        page.insert(b"secondary-key", 17).unwrap();

        let page = Page::<Read<'_>, Leaf, Index>::open(&bytes).unwrap();
        let cell = page.cell(0).unwrap();

        assert_eq!(cell.row_id(), 17);
        assert_eq!(cell.key(), b"secondary-key");
    }

    #[test]
    fn table_interior_left_child_reads_and_writes_cell_bytes() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior, Table>::init(&mut bytes, 99);
        page.insert(42, 7).unwrap();

        {
            let mut cell = page.cell_mut(0).unwrap();
            assert_eq!(cell.left_child(), 7);
            assert_eq!(cell.row_id(), 42);

            cell.set_left_child(11);

            assert_eq!(cell.left_child(), 11);
            assert_eq!(cell.row_id(), 42);
        }

        let page = Page::<Read<'_>, Interior, Table>::open(&bytes).unwrap();
        let cell = page.cell(0).unwrap();
        assert_eq!(cell.left_child(), 11);
        assert_eq!(cell.row_id(), 42);
    }

    #[test]
    fn index_interior_reads_row_id_suffix_and_writes_left_child() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Interior, Index>::init(&mut bytes, 123);
        page.insert(b"separator", 88, 7).unwrap();

        {
            let mut cell = page.cell_mut(0).unwrap();
            assert_eq!(cell.left_child(), 7);
            assert_eq!(cell.key(), b"separator");
            assert_eq!(cell.row_id(), 88);

            cell.set_left_child(19);

            assert_eq!(cell.left_child(), 19);
            assert_eq!(cell.key(), b"separator");
            assert_eq!(cell.row_id(), 88);
        }

        let page = Page::<Read<'_>, Interior, Index>::open(&bytes).unwrap();
        let cell = page.cell(0).unwrap();
        assert_eq!(cell.left_child(), 19);
        assert_eq!(cell.key(), b"separator");
        assert_eq!(cell.row_id(), 88);
    }

    #[test]
    fn table_interior_new_rejects_cells_past_usable_space() {
        let bytes = [0_u8; PAGE_SIZE];
        let cell_offset = USABLE_SPACE_END - table_interior::INTERIOR_CELL_SIZE + 1;

        let err = Cell::<CellRead<'_>, Interior, Table>::new(&bytes, cell_offset, 0).unwrap_err();

        assert_eq!(
            err,
            PageError::CorruptCell { slot_index: 0, kind: CellCorruption::LengthOutOfBounds }
        );
    }
}
