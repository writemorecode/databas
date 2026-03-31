use core::marker::PhantomData;

use crate::{PAGE_SIZE, PageId, RowId, SlotId};

use super::{
    PageResult,
    core::{Index, Interior, Leaf, Page, PageAccess, PageAccessMut, Read, Table, Write},
    index_leaf, interior, leaf,
};

/// A typed view over a single slot entry within a page.
#[derive(Debug)]
pub struct Cell<A, N, T = Table> {
    access: A,
    slot_index: SlotId,
    _marker: PhantomData<(N, T)>,
}

impl<A, N, T> Cell<A, N, T> {
    pub(crate) fn new(access: A, slot_index: SlotId) -> Self {
        Self { access, slot_index, _marker: PhantomData }
    }

    /// Returns the slot index that this cell view refers to.
    pub fn slot_index(&self) -> SlotId {
        self.slot_index
    }
}

impl<A, N, T> Cell<A, N, T>
where
    A: PageAccess,
{
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.access.bytes()
    }
}

impl<A, N, T> Cell<A, N, T>
where
    A: PageAccessMut,
{
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.access.bytes_mut()
    }
}

impl<'a, N, T> Cell<Write<'a>, N, T> {
    /// Borrows this mutable cell as an immutable cell view.
    pub fn as_ref(&self) -> Cell<Read<'_>, N, T> {
        Cell::new(Read { bytes: self.bytes() }, self.slot_index)
    }
}

impl<A> Cell<A, Leaf, Table>
where
    A: PageAccess,
{
    /// Returns the row id stored in this leaf cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        let page = Page::<Read<'_>, Leaf, Table>::open(self.bytes())?;
        Ok(leaf::cell_parts(&page, self.slot_index)?.row_id)
    }

    /// Returns the payload bytes stored in this leaf cell.
    pub fn payload(&self) -> PageResult<&[u8]> {
        let page = Page::<Read<'_>, Leaf, Table>::open(self.bytes())?;
        let parts = leaf::cell_parts(&page, self.slot_index)?;
        Ok(&self.bytes()[parts.payload_start..parts.payload_end])
    }
}

impl<A> Cell<A, Leaf, Table>
where
    A: PageAccessMut,
{
    /// Returns the payload bytes stored in this leaf cell mutably.
    pub fn payload_mut(&mut self) -> PageResult<&mut [u8]> {
        let page = Page::<Read<'_>, Leaf, Table>::open(self.bytes())?;
        let parts = leaf::cell_parts(&page, self.slot_index)?;
        Ok(&mut self.bytes_mut()[parts.payload_start..parts.payload_end])
    }
}

impl<A> Cell<A, Leaf, Index>
where
    A: PageAccess,
{
    /// Returns the indexed key stored in this leaf cell.
    pub fn key(&self) -> PageResult<&[u8]> {
        let page = Page::<Read<'_>, Leaf, Index>::open(self.bytes())?;
        let parts = index_leaf::cell_parts(&page, self.slot_index)?;
        Ok(&self.bytes()[parts.key_start..parts.key_end])
    }

    /// Returns the row reference stored alongside this index key.
    pub fn row_id(&self) -> PageResult<RowId> {
        let page = Page::<Read<'_>, Leaf, Index>::open(self.bytes())?;
        Ok(index_leaf::cell_parts(&page, self.slot_index)?.row_id)
    }
}

impl<A> Cell<A, Interior, Table>
where
    A: PageAccess,
{
    /// Returns the separator row id stored in this interior cell.
    pub fn row_id(&self) -> PageResult<RowId> {
        let page = Page::<Read<'_>, Interior, Table>::open(self.bytes())?;
        Ok(interior::cell_parts(&page, self.slot_index)?.row_id)
    }

    /// Returns the left-child page id referenced by this interior cell.
    pub fn left_child(&self) -> PageResult<PageId> {
        let page = Page::<Read<'_>, Interior, Table>::open(self.bytes())?;
        Ok(interior::cell_parts(&page, self.slot_index)?.left_child)
    }
}

impl<A> Cell<A, Interior, Table>
where
    A: PageAccessMut,
{
    /// Updates the left-child page id stored in this interior cell.
    pub fn set_left_child(&mut self, page_id: PageId) -> PageResult<()> {
        let page = Page::<Read<'_>, Interior, Table>::open(self.bytes())?;
        let parts = interior::cell_parts(&page, self.slot_index)?;
        interior::write_left_child(self.bytes_mut(), parts.cell_offset, page_id);
        Ok(())
    }
}
