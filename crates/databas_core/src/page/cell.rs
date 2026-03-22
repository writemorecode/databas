use core::marker::PhantomData;

use crate::types::{PAGE_SIZE, PageId, RowId};

use super::{
    CellCorruption, PageError, PageResult,
    core::{Interior, Leaf, Page, PageAccess, PageAccessMut, Read, Write},
    leaf,
};

#[derive(Debug)]
pub struct Cell<A, N> {
    access: A,
    slot_index: u16,
    _marker: PhantomData<N>,
}

impl<A, N> Cell<A, N> {
    pub(crate) fn new(access: A, slot_index: u16) -> Self {
        Self { access, slot_index, _marker: PhantomData }
    }

    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }
}

impl<A, N> Cell<A, N>
where
    A: PageAccess,
{
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.access.bytes()
    }
}

impl<A, N> Cell<A, N>
where
    A: PageAccessMut,
{
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.access.bytes_mut()
    }
}

impl<'a, N> Cell<Write<'a>, N> {
    pub fn as_ref(&self) -> Cell<Read<'_>, N> {
        Cell::new(Read { bytes: self.bytes() }, self.slot_index)
    }
}

impl<A> Cell<A, Leaf>
where
    A: PageAccess,
{
    pub fn row_id(&self) -> PageResult<RowId> {
        let page = Page::<Read<'_>, Leaf>::open(self.bytes())?;
        Ok(leaf::cell_parts(&page, self.slot_index)?.row_id)
    }

    pub fn payload(&self) -> PageResult<&[u8]> {
        let page = Page::<Read<'_>, Leaf>::open(self.bytes())?;
        let parts = leaf::cell_parts(&page, self.slot_index)?;
        Ok(&self.bytes()[parts.payload_start..parts.payload_end])
    }
}

impl<A> Cell<A, Leaf>
where
    A: PageAccessMut,
{
    pub fn payload_mut(&mut self) -> PageResult<&mut [u8]> {
        let page = Page::<Read<'_>, Leaf>::open(self.bytes())?;
        let parts = leaf::cell_parts(&page, self.slot_index)?;
        Ok(&mut self.bytes_mut()[parts.payload_start..parts.payload_end])
    }
}

impl<A> Cell<A, Interior>
where
    A: PageAccess,
{
    pub fn row_id(&self) -> PageResult<RowId> {
        Err(PageError::CorruptCell {
            slot_index: self.slot_index,
            kind: CellCorruption::LengthTooSmall,
        })
    }

    pub fn left_child(&self) -> PageResult<PageId> {
        Err(PageError::CorruptCell {
            slot_index: self.slot_index,
            kind: CellCorruption::LengthTooSmall,
        })
    }
}

impl<A> Cell<A, Interior>
where
    A: PageAccessMut,
{
    pub fn set_left_child(&mut self, _page_id: PageId) -> PageResult<()> {
        Err(PageError::CorruptCell {
            slot_index: self.slot_index,
            kind: CellCorruption::LengthTooSmall,
        })
    }
}
