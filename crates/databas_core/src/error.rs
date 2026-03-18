use thiserror::Error;

use crate::{
    database_header::DatabaseHeaderError,
    disk_manager::DiskManagerError,
    page_cache::PageCacheError,
    table_page::{TablePageCorruptionKind, TablePageError},
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("i/o error")]
    Io(#[source] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(#[source] CorruptionError),
    #[error("constraint violation: {0}")]
    Constraint(#[source] ConstraintError),
    #[error("invalid argument: {0}")]
    InvalidArgument(#[source] InvalidArgumentError),
    #[error("limit exceeded: {0}")]
    LimitExceeded(#[source] LimitExceededError),
    #[error("internal error: {0}")]
    Internal(#[source] InternalError),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{component}: {kind}")]
pub struct CorruptionError {
    pub component: CorruptionComponent,
    pub page_id: Option<u64>,
    pub kind: CorruptionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionComponent {
    DatabaseHeader,
    DiskPage,
    Freelist,
    TablePage,
    TableLeafPage,
    TableInteriorPage,
    BTree,
}

impl std::fmt::Display for CorruptionComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseHeader => write!(f, "database header"),
            Self::DiskPage => write!(f, "disk page"),
            Self::Freelist => write!(f, "freelist"),
            Self::TablePage => write!(f, "table page"),
            Self::TableLeafPage => write!(f, "table leaf page"),
            Self::TableInteriorPage => write!(f, "table interior page"),
            Self::BTree => write!(f, "btree"),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CorruptionKind {
    #[error("invalid file size {size} for page size {page_size}")]
    InvalidFileSize { size: u64, page_size: usize },
    #[error("invalid checksum")]
    InvalidChecksum,
    #[error("invalid header magic")]
    InvalidHeaderMagic,
    #[error("invalid header page size: expected {expected}, got {actual}")]
    InvalidHeaderPageSize { expected: usize, actual: u16 },
    #[error("header page count mismatch: expected {expected}, got {actual}")]
    HeaderPageCountMismatch { expected: u64, actual: u64 },
    #[error("header page count is zero")]
    HeaderPageCountZero,
    #[error("freelist head is zero but free page count is {count}")]
    FreelistCountWithoutHead { count: u64 },
    #[error("freelist head {head} present but free page count is zero")]
    FreelistHeadWithoutCount { head: u64 },
    #[error("freelist page id {page_id} is invalid")]
    InvalidFreelistPageId { page_id: u64 },
    #[error("header page cannot appear in freelist")]
    HeaderPageInFreelist,
    #[error("freelist trunk leaf count {count} exceeds maximum {max}")]
    FreelistLeafCountTooLarge { count: u64, max: usize },

    #[error("invalid checksum on freelist page {page_id}")]
    InvalidFreelistChecksum { page_id: u64 },
    #[error("invalid page type: {page_type}")]
    InvalidPageType { page_type: u8 },
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
    #[error("malformed cell at slot index {slot_index}")]
    MalformedCell { slot_index: u16 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConstraintError {
    #[error("duplicate row id: {row_id}")]
    DuplicateRowId { row_id: u64 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InvalidArgumentError {
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: u64 },
    #[error("page is already free: {page_id}")]
    PageAlreadyFree { page_id: u64 },
    #[error("cannot free pinned page: {page_id}")]
    CannotFreePinnedPage { page_id: u64 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LimitExceededError {
    #[error("cell too large: {len} bytes (max {max})")]
    CellTooLarge { len: usize, max: usize },
    #[error("page full: need {needed} bytes, only {available} bytes available")]
    PageFull { needed: usize, available: usize },
    #[error("cache capacity exhausted")]
    CacheCapacityExhausted,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InternalError {
    #[error("{0}")]
    InvariantViolation(InvariantViolation),
    #[error("unsupported page kind: {page_type}")]
    UnsupportedPageKind { page_type: u8 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error("pinned page during flush: {page_id}")]
    PinnedPageDuringFlush { page_id: u64 },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: u64, frame_id: usize, frame_count: usize },
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<DiskManagerError> for StorageError {
    fn from(err: DiskManagerError) -> Self {
        match err {
            DiskManagerError::Io(err) => Self::Io(err),
            DiskManagerError::InvalidPageId { page_id } => {
                Self::InvalidArgument(InvalidArgumentError::InvalidPageId { page_id })
            }
            DiskManagerError::PageAlreadyFree { page_id } => {
                Self::InvalidArgument(InvalidArgumentError::PageAlreadyFree { page_id })
            }
            DiskManagerError::InvalidFileSize { size } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::DatabaseHeader,
                page_id: Some(0),
                kind: CorruptionKind::InvalidFileSize { size, page_size: crate::types::PAGE_SIZE },
            }),
            DiskManagerError::InvalidPageChecksum { page_id } => {
                Self::Corruption(CorruptionError {
                    component: CorruptionComponent::DiskPage,
                    page_id: Some(page_id),
                    kind: CorruptionKind::InvalidChecksum,
                })
            }
            DiskManagerError::InvalidDatabaseHeader(err) => match err {
                DatabaseHeaderError::InvalidMagic => Self::Corruption(CorruptionError {
                    component: CorruptionComponent::DatabaseHeader,
                    page_id: Some(0),
                    kind: CorruptionKind::InvalidHeaderMagic,
                }),
                DatabaseHeaderError::InvalidPageSize { actual, expected } => {
                    Self::Corruption(CorruptionError {
                        component: CorruptionComponent::DatabaseHeader,
                        page_id: Some(0),
                        kind: CorruptionKind::InvalidHeaderPageSize { expected, actual },
                    })
                }
                DatabaseHeaderError::PageCountZero => Self::Corruption(CorruptionError {
                    component: CorruptionComponent::DatabaseHeader,
                    page_id: Some(0),
                    kind: CorruptionKind::HeaderPageCountZero,
                }),
                DatabaseHeaderError::PageCountMismatch { actual, expected } => {
                    Self::Corruption(CorruptionError {
                        component: CorruptionComponent::DatabaseHeader,
                        page_id: Some(0),
                        kind: CorruptionKind::HeaderPageCountMismatch { expected, actual },
                    })
                }
            },
            DiskManagerError::InvalidFreelist(err) => Self::Corruption(CorruptionError {
                component: CorruptionComponent::Freelist,
                page_id: err.page_id(),
                kind: err.into(),
            }),
        }
    }
}

impl From<PageCacheError> for StorageError {
    fn from(err: PageCacheError) -> Self {
        match err {
            PageCacheError::Disk(err) => err.into(),
            PageCacheError::NoEvictableFrame => {
                Self::LimitExceeded(LimitExceededError::CacheCapacityExhausted)
            }
            PageCacheError::PinnedPage { page_id } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::PinnedPageDuringFlush { page_id },
                ))
            }
            PageCacheError::PinnedPageForFree { page_id } => {
                Self::InvalidArgument(InvalidArgumentError::CannotFreePinnedPage { page_id })
            }
            PageCacheError::InvalidFrameCount { frame_count } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::InvalidFrameCount { frame_count },
                ))
            }
            PageCacheError::CorruptPageTableEntry { page_id, frame_id, frame_count } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::CorruptPageTableEntry { page_id, frame_id, frame_count },
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_free_page_maps_to_invalid_argument() {
        let err = StorageError::from(PageCacheError::PinnedPageForFree { page_id: 7 });
        assert_eq!(
            err.to_string(),
            StorageError::InvalidArgument(InvalidArgumentError::CannotFreePinnedPage {
                page_id: 7
            })
            .to_string()
        );
        assert!(matches!(
            err,
            StorageError::InvalidArgument(InvalidArgumentError::CannotFreePinnedPage {
                page_id: 7
            })
        ));
    }

    #[test]
    fn invalid_freelist_checksum_maps_to_freelist_checksum_corruption() {
        let err = StorageError::from(DiskManagerError::InvalidFreelist(
            crate::disk_manager::FreelistError::InvalidChecksum { page_id: 11 },
        ));
        assert!(matches!(
            err,
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::Freelist,
                page_id: Some(11),
                kind: CorruptionKind::InvalidFreelistChecksum { page_id: 11 },
            })
        ));
    }
}

impl From<TablePageError> for StorageError {
    fn from(err: TablePageError) -> Self {
        match err {
            TablePageError::InvalidPageType { page_type } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::TablePage,
                page_id: None,
                kind: CorruptionKind::InvalidPageType { page_type },
            }),
            TablePageError::CorruptPage(kind) => Self::Corruption(CorruptionError {
                component: CorruptionComponent::TablePage,
                page_id: None,
                kind: kind.into(),
            }),
            TablePageError::UnsupportedPageKind { page_tag } => {
                Self::Internal(InternalError::UnsupportedPageKind { page_type: page_tag.raw() })
            }
            TablePageError::CorruptCell { slot_index } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::TablePage,
                page_id: None,
                kind: CorruptionKind::MalformedCell { slot_index },
            }),
            TablePageError::DuplicateRowId { row_id } => {
                Self::Constraint(ConstraintError::DuplicateRowId { row_id })
            }
            TablePageError::RowIdNotFound { row_id } => {
                panic!("internal-only table-page error escaped public boundary: row id {row_id}")
            }
            TablePageError::CellTooLarge { len, max } => {
                Self::LimitExceeded(LimitExceededError::CellTooLarge { len, max })
            }
            TablePageError::PageFull { needed, available } => {
                Self::LimitExceeded(LimitExceededError::PageFull { needed, available })
            }
        }
    }
}

impl From<TablePageCorruptionKind> for CorruptionKind {
    fn from(kind: TablePageCorruptionKind) -> Self {
        match kind {
            TablePageCorruptionKind::InvalidCellContentStart => Self::InvalidCellContentStart,
            TablePageCorruptionKind::InvalidFragmentedFreeBytes => Self::InvalidFragmentedFreeBytes,
            TablePageCorruptionKind::SlotIndexOutOfBounds => Self::SlotIndexOutOfBounds,
            TablePageCorruptionKind::SlotDirectoryOverlapsCellContent => {
                Self::SlotDirectoryOverlapsCellContent
            }
            TablePageCorruptionKind::SlotDirectoryExceedsPageSize => {
                Self::SlotDirectoryExceedsPageSize
            }
            TablePageCorruptionKind::InvalidFreeblockOffset => Self::InvalidFreeblockOffset,
            TablePageCorruptionKind::FreeblockTooSmall => Self::FreeblockTooSmall,
            TablePageCorruptionKind::FreeblockChainOutOfOrder => Self::FreeblockChainOutOfOrder,
            TablePageCorruptionKind::AdjacentFreeblocks => Self::AdjacentFreeblocks,
            TablePageCorruptionKind::CellTooShort => Self::CellTooShort,
            TablePageCorruptionKind::CellPayloadOutOfBounds => Self::CellPayloadOutOfBounds,
            TablePageCorruptionKind::CellContentUnderflow => Self::CellContentUnderflow,
        }
    }
}
