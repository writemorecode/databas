mod interior;
mod layout;
mod leaf;

use crate::types::{PAGE_SIZE, RowId};

pub(crate) use interior::{TableInteriorPageMut, TableInteriorPageRef};
pub(crate) use leaf::{TableLeafPageMut, TableLeafPageRef};

/// Reads a little-endian `u64` from `bytes` at `offset`.
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(out)
}

/// Immutable wrapper for a table page whose concrete kind is discovered at runtime.
#[derive(Debug)]
pub(crate) enum TablePageRef<'a> {
    /// Leaf table page.
    Leaf(TableLeafPageRef<'a>),
    /// Interior table page.
    Interior(TableInteriorPageRef<'a>),
}

impl<'a> TablePageRef<'a> {
    /// Validates and deserializes a page buffer of unknown table-page type.
    pub(crate) fn from_bytes(page: &'a [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        match layout::page_type(page) {
            layout::LEAF_PAGE_TYPE => Ok(Self::Leaf(TableLeafPageRef::from_bytes(page)?)),
            layout::INTERIOR_PAGE_TYPE => {
                Ok(Self::Interior(TableInteriorPageRef::from_bytes(page)?))
            }
            page_type => Err(TablePageError::InvalidPageType { page_type }),
        }
    }
}

/// Mutable wrapper for a table page whose concrete kind is discovered at runtime.
#[derive(Debug)]
pub(crate) enum TablePageMut<'a> {
    /// Leaf table page.
    Leaf(TableLeafPageMut<'a>),
    /// Interior table page.
    Interior(TableInteriorPageMut<'a>),
}

impl<'a> TablePageMut<'a> {
    /// Validates and deserializes a mutable page buffer of unknown table-page type.
    pub(crate) fn from_bytes(page: &'a mut [u8; PAGE_SIZE]) -> TablePageResult<Self> {
        match layout::page_type(page) {
            layout::LEAF_PAGE_TYPE => Ok(Self::Leaf(TableLeafPageMut::from_bytes(page)?)),
            layout::INTERIOR_PAGE_TYPE => {
                Ok(Self::Interior(TableInteriorPageMut::from_bytes(page)?))
            }
            page_type => Err(TablePageError::InvalidPageType { page_type }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TablePageError {
    #[error("invalid page type: {page_type}")]
    InvalidPageType { page_type: u8 },
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
    use crate::table_page::TablePageCorruptionKind;

    fn initialized_leaf_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        {
            let _leaf = TableLeafPageMut::init_empty(&mut page).unwrap();
        }
        page
    }

    fn initialized_interior_page(rightmost_child: u64) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        {
            let _interior = TableInteriorPageMut::init_empty(&mut page, rightmost_child).unwrap();
        }
        page
    }

    #[test]
    fn immutable_unknown_deserialization_detects_leaf_and_kind() {
        let mut page = initialized_leaf_page();
        {
            let mut leaf = TableLeafPageMut::from_bytes(&mut page).unwrap();
            leaf.insert(7, &[1, 2, 3]).unwrap();
        }

        let page_ref = TablePageRef::from_bytes(&page).unwrap();
        match page_ref {
            TablePageRef::Leaf(leaf) => {
                let cell = leaf.search(7).unwrap().unwrap();
                assert_eq!(cell.payload, &[1, 2, 3]);
            }
            TablePageRef::Interior(_) => panic!("expected leaf page"),
        }
    }

    #[test]
    fn immutable_unknown_deserialization_detects_interior_and_kind() {
        let page = initialized_interior_page(99);

        let page_ref = TablePageRef::from_bytes(&page).unwrap();
        match page_ref {
            TablePageRef::Interior(interior) => assert_eq!(interior.rightmost_child(), 99),
            TablePageRef::Leaf(_) => panic!("expected interior page"),
        }
    }

    #[test]
    fn mutable_unknown_deserialization_detects_leaf_and_kind() {
        let mut page = initialized_leaf_page();
        let page_mut = TablePageMut::from_bytes(&mut page).unwrap();
        match page_mut {
            TablePageMut::Leaf(_) => {}
            TablePageMut::Interior(_) => panic!("expected leaf page"),
        }
    }

    #[test]
    fn mutable_unknown_deserialization_detects_interior_and_kind() {
        let mut page = initialized_interior_page(123);
        let page_mut = TablePageMut::from_bytes(&mut page).unwrap();
        match page_mut {
            TablePageMut::Interior(_) => {}
            TablePageMut::Leaf(_) => panic!("expected interior page"),
        }
    }

    #[test]
    fn unknown_deserialization_rejects_invalid_page_type() {
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 255;

        let immutable_err = TablePageRef::from_bytes(&page).unwrap_err();
        assert!(matches!(immutable_err, TablePageError::InvalidPageType { page_type: 255 }));

        let mut mutable_page = page;
        let mutable_err = TablePageMut::from_bytes(&mut mutable_page).unwrap_err();
        assert!(matches!(mutable_err, TablePageError::InvalidPageType { page_type: 255 }));
    }

    #[test]
    fn unknown_deserialization_propagates_corruption_for_valid_leaf_type() {
        let mut page = initialized_leaf_page();
        page[4..6].copy_from_slice(&0u16.to_le_bytes());

        let immutable_err = TablePageRef::from_bytes(&page).unwrap_err();
        assert!(matches!(
            immutable_err,
            TablePageError::CorruptPage(TablePageCorruptionKind::InvalidCellContentStart)
        ));

        let mut mutable_page = page;
        let mutable_err = TablePageMut::from_bytes(&mut mutable_page).unwrap_err();
        assert!(matches!(
            mutable_err,
            TablePageError::CorruptPage(TablePageCorruptionKind::InvalidCellContentStart)
        ));
    }
}
