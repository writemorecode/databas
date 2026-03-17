use crate::table_page::{Index, Interior, Leaf, Page, PageTag, TablePageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexLeafCell<'a> {
    pub(crate) _bytes: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexInteriorCell<'a> {
    pub(crate) _bytes: &'a [u8],
}

pub(super) fn unsupported_page_kind(page_tag: PageTag) -> TablePageError {
    TablePageError::UnsupportedPageKind { page_tag }
}

impl<A> Page<A, Index, Leaf> {
    #[allow(dead_code)]
    pub(crate) fn unsupported_kind() -> TablePageError {
        unsupported_page_kind(PageTag::IndexLeaf)
    }
}

impl<A> Page<A, Index, Interior> {
    #[allow(dead_code)]
    pub(crate) fn unsupported_kind() -> TablePageError {
        unsupported_page_kind(PageTag::IndexInterior)
    }
}
