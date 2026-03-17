mod index;
mod layout;
mod table;

use core::marker::PhantomData;

use crate::types::{PAGE_SIZE, RowId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PageTag {
    TableLeaf = 0,
    TableInterior = 1,
    IndexLeaf = 2,
    IndexInterior = 3,
}

impl PageTag {
    pub(crate) fn from_raw(page_type: u8) -> TablePageResult<Self> {
        match page_type {
            0 => Ok(Self::TableLeaf),
            1 => Ok(Self::TableInterior),
            2 => Ok(Self::IndexLeaf),
            3 => Ok(Self::IndexInterior),
            _ => Err(TablePageError::InvalidPageType { page_type }),
        }
    }

    pub(crate) fn raw(self) -> u8 {
        self as u8
    }
}

#[derive(Debug)]
pub(crate) enum Table {}

#[derive(Debug)]
pub(crate) enum Index {}

#[derive(Debug)]
pub(crate) enum Leaf {}

#[derive(Debug)]
pub(crate) enum Interior {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Read<'a> {
    bytes: &'a [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub(crate) struct Write<'a> {
    bytes: &'a mut [u8; PAGE_SIZE],
}

pub(crate) trait PageAccess {
    fn bytes(&self) -> &[u8; PAGE_SIZE];
}

pub(crate) trait PageAccessMut: PageAccess {
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}

impl PageAccess for Read<'_> {
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.bytes
    }
}

impl PageAccess for Write<'_> {
    fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.bytes
    }
}

impl PageAccessMut for Write<'_> {
    fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.bytes
    }
}

#[derive(Debug)]
pub(crate) struct Page<A, F, N> {
    access: A,
    _marker: PhantomData<(F, N)>,
}

#[derive(Debug)]
pub(crate) enum AnyPage<A> {
    TableLeaf(Page<A, Table, Leaf>),
    TableInterior(Page<A, Table, Interior>),
    IndexLeaf(Page<A, Index, Leaf>),
    IndexInterior(Page<A, Index, Interior>),
}

pub(crate) type PageRef<'a> = AnyPage<Read<'a>>;
pub(crate) type PageMut<'a> = AnyPage<Write<'a>>;

impl<A, F, N> Page<A, F, N> {
    pub(super) fn new(access: A) -> Self {
        Self { access, _marker: PhantomData }
    }
}

impl<A, F, N> Page<A, F, N>
where
    A: PageAccess,
{
    pub(super) fn bytes(&self) -> &[u8; PAGE_SIZE] {
        self.access.bytes()
    }

    pub(crate) fn header(&self) -> HeaderView<'_> {
        HeaderView { page: self.bytes() }
    }

    pub(crate) fn page_tag(&self) -> TablePageResult<PageTag> {
        PageTag::from_raw(layout::page_type(self.bytes()))
    }

    pub(crate) fn slot_count(&self) -> u16 {
        layout::cell_count(self.bytes())
    }

    pub(crate) fn free_space(&self) -> TablePageResult<usize> {
        layout::free_space(self.bytes(), layout::spec_for_tag(self.page_tag()?))
    }
}

impl<A, F, N> Page<A, F, N>
where
    A: PageAccessMut,
{
    pub(super) fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.access.bytes_mut()
    }
}

impl<'a, F, N> Page<Write<'a>, F, N> {
    pub(crate) fn as_ref(&self) -> Page<Read<'_>, F, N> {
        Page::new(Read { bytes: self.bytes() })
    }
}

impl<'a> TryFrom<&'a [u8; PAGE_SIZE]> for AnyPage<Read<'a>> {
    type Error = TablePageError;

    fn try_from(bytes: &'a [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        match PageTag::from_raw(layout::page_type(bytes))? {
            PageTag::TableLeaf => {
                Ok(Self::TableLeaf(Page::<Read<'a>, Table, Leaf>::from_bytes(bytes)?))
            }
            PageTag::TableInterior => {
                Ok(Self::TableInterior(Page::<Read<'a>, Table, Interior>::from_bytes(bytes)?))
            }
            PageTag::IndexLeaf => Err(index::unsupported_page_kind(PageTag::IndexLeaf)),
            PageTag::IndexInterior => Err(index::unsupported_page_kind(PageTag::IndexInterior)),
        }
    }
}

impl<'a> TryFrom<&'a mut [u8; PAGE_SIZE]> for AnyPage<Write<'a>> {
    type Error = TablePageError;

    fn try_from(bytes: &'a mut [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        match PageTag::from_raw(layout::page_type(bytes))? {
            PageTag::TableLeaf => {
                Ok(Self::TableLeaf(Page::<Write<'a>, Table, Leaf>::from_bytes(bytes)?))
            }
            PageTag::TableInterior => {
                Ok(Self::TableInterior(Page::<Write<'a>, Table, Interior>::from_bytes(bytes)?))
            }
            PageTag::IndexLeaf => Err(index::unsupported_page_kind(PageTag::IndexLeaf)),
            PageTag::IndexInterior => Err(index::unsupported_page_kind(PageTag::IndexInterior)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct HeaderView<'a> {
    page: &'a [u8; PAGE_SIZE],
}

impl HeaderView<'_> {
    pub(crate) fn page_tag(&self) -> TablePageResult<PageTag> {
        PageTag::from_raw(layout::page_type(self.page))
    }

    pub(crate) fn slot_count(&self) -> u16 {
        layout::cell_count(self.page)
    }

    pub(crate) fn content_start(&self) -> u16 {
        layout::content_start(self.page)
    }

    pub(crate) fn first_freeblock(&self) -> u16 {
        layout::first_freeblock(self.page)
    }

    pub(crate) fn fragmented_free_bytes(&self) -> u8 {
        layout::fragmented_free_bytes(self.page)
    }
}

/// Reads a little-endian `u64` from `bytes` at `offset`.
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TablePageError {
    #[error("invalid page type: {page_type}")]
    InvalidPageType { page_type: u8 },
    #[error("unsupported page kind: {page_tag:?}")]
    UnsupportedPageKind { page_tag: PageTag },
    #[error("corrupt page: {0}")]
    CorruptPage(TablePageCorruptionKind),
    #[error("corrupt cell at slot index {slot_index}")]
    CorruptCell { slot_index: u16 },
    #[error("duplicate row id: {row_id}")]
    DuplicateRowId { row_id: RowId },
    #[error("row id not found: {row_id}")]
    RowIdNotFound { row_id: RowId },
    #[error("cell too large: {len} bytes (max {max})")]
    CellTooLarge { len: usize, max: usize },
    #[error("page full: need {needed} bytes, only {available} bytes available")]
    PageFull { needed: usize, available: usize },
}

pub(crate) type TablePageResult<T> = Result<T, TablePageError>;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TablePageCorruptionKind {
    #[error("invalid cell content start")]
    InvalidCellContentStart,
    #[error("fragmented free byte count exceeds maximum")]
    InvalidFragmentedFreeBytes,
    #[error("slot index out of bounds")]
    SlotIndexOutOfBounds,
    #[error("slot directory overlaps cell content")]
    SlotDirectoryOverlapsCellContent,
    #[error("slot directory exceeds page size")]
    SlotDirectoryExceedsPageSize,
    #[error("invalid freeblock offset")]
    InvalidFreeblockOffset,
    #[error("freeblock too small")]
    FreeblockTooSmall,
    #[error("freeblock chain out of order")]
    FreeblockChainOutOfOrder,
    #[error("adjacent freeblocks")]
    AdjacentFreeblocks,
    #[error("cell too short")]
    CellTooShort,
    #[error("cell payload out of bounds")]
    CellPayloadOutOfBounds,
    #[error("cell content underflow")]
    CellContentUnderflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;

    #[test]
    fn leaf_page_init_and_dispatch_are_consistent() {
        let mut bytes = [0u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();

        assert_eq!(page.page_tag().unwrap(), PageTag::TableLeaf);
        assert_eq!(page.header().page_tag().unwrap(), PageTag::TableLeaf);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.header().slot_count(), 0);
        assert_eq!(page.header().first_freeblock(), 0);
        assert_eq!(page.header().fragmented_free_bytes(), 0);

        page.insert(20, b"twenty").unwrap();
        page.insert(5, b"five").unwrap();
        page.insert(10, b"ten").unwrap();

        let any_page = AnyPage::try_from(&bytes).unwrap();
        match any_page {
            AnyPage::TableLeaf(page) => {
                assert_eq!(page.header().page_tag().unwrap(), PageTag::TableLeaf);
                assert_eq!(page.slot_count(), 3);
                assert_eq!(page.rowid_at(0).unwrap(), 5);
                assert_eq!(page.rowid_at(1).unwrap(), 10);
                assert_eq!(page.rowid_at(2).unwrap(), 20);
            }
            _ => panic!("expected table leaf page"),
        }
    }

    #[test]
    fn leaf_page_supports_basic_crud_and_sorted_lookup() {
        let mut bytes = [0u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Table, Leaf>::init_empty(&mut bytes).unwrap();

        page.insert(20, b"twenty").unwrap();
        page.insert(5, b"five").unwrap();
        page.insert(10, b"ten").unwrap();

        assert_eq!(page.len(), 3);
        assert_eq!(page.rowid_at(0).unwrap(), 5);
        assert_eq!(page.rowid_at(1).unwrap(), 10);
        assert_eq!(page.rowid_at(2).unwrap(), 20);
        assert_eq!(page.search(5).unwrap().unwrap().payload, b"five");
        assert_eq!(page.search(10).unwrap().unwrap().payload, b"ten");
        assert_eq!(page.search(20).unwrap().unwrap().payload, b"twenty");
        assert_eq!(page.search(99).unwrap(), None);

        assert!(matches!(
            page.insert(10, b"duplicate"),
            Err(TablePageError::DuplicateRowId { row_id: 10 })
        ));

        page.update(10, b"ten-updated").unwrap();
        assert_eq!(page.search(10).unwrap().unwrap().payload, b"ten-updated");

        page.delete(5).unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page.rowid_at(0).unwrap(), 10);
        assert_eq!(page.rowid_at(1).unwrap(), 20);
        assert_eq!(page.search(5).unwrap(), None);
    }

    #[test]
    fn interior_page_routes_row_ids_and_dispatches_correctly() {
        let mut bytes = [0u8; PAGE_SIZE];
        let mut page = Page::<Write<'_>, Table, Interior>::init_empty(&mut bytes, 99).unwrap();

        page.insert(20, 2).unwrap();
        page.insert(10, 1).unwrap();
        page.insert(30, 3).unwrap();

        assert_eq!(page.cell_count(), 3);
        assert_eq!(page.rowid_at(0).unwrap(), 10);
        assert_eq!(page.rowid_at(1).unwrap(), 20);
        assert_eq!(page.rowid_at(2).unwrap(), 30);
        assert_eq!(page.child_at(0).unwrap(), 1);
        assert_eq!(page.child_at(1).unwrap(), 2);
        assert_eq!(page.child_at(2).unwrap(), 3);
        assert_eq!(page.rightmost_child(), 99);
        assert_eq!(
            page.search(20).unwrap(),
            Some(table::TableInteriorCell { left_child: 2, row_id: 20 })
        );
        assert_eq!(page.search(25).unwrap(), None);

        assert_eq!(page.child_for_row_id(0).unwrap(), 1);
        assert_eq!(page.child_for_row_id(10).unwrap(), 2);
        assert_eq!(page.child_for_row_id(15).unwrap(), 2);
        assert_eq!(page.child_for_row_id(20).unwrap(), 3);
        assert_eq!(page.child_for_row_id(25).unwrap(), 3);
        assert_eq!(page.child_for_row_id(30).unwrap(), 99);
        assert_eq!(page.child_for_row_id(35).unwrap(), 99);

        let any_page = AnyPage::try_from(&bytes).unwrap();
        match any_page {
            AnyPage::TableInterior(page) => {
                assert_eq!(page.page_tag().unwrap(), PageTag::TableInterior);
                assert_eq!(page.cell_count(), 3);
                assert_eq!(page.rightmost_child(), 99);
            }
            _ => panic!("expected table interior page"),
        }
    }

    #[test]
    fn any_page_rejects_unsupported_index_pages() {
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = PageTag::IndexLeaf.raw();

        assert!(matches!(
            AnyPage::try_from(&bytes),
            Err(TablePageError::UnsupportedPageKind { page_tag: PageTag::IndexLeaf })
        ));
    }
}
