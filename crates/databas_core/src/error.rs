use std::fmt;
use thiserror::Error;

use crate::{
    page::{CellCorruption, PageCorruption, PageError},
    {PAGE_SIZE, PageId},
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
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

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{}", format_corruption_error(*component, *page_id, kind))]
pub struct CorruptionError {
    pub component: CorruptionComponent,
    pub page_id: Option<PageId>,
    #[source]
    pub kind: CorruptionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionComponent {
    DatabaseFile,
    DiskPage,
    OverflowPage,
    Page,
    LeafPage,
    InteriorPage,
    Cell,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CorruptionKind {
    #[error("invalid file size {size} for page size {page_size}")]
    InvalidFileSize { size: u64, page_size: usize },
    #[error("unknown page kind: raw tag {actual}")]
    UnknownPageKind { actual: u8 },
    #[error("invalid page kind: expected {expected}, got raw tag {actual}")]
    InvalidPageKind { expected: &'static str, actual: u8 },
    #[error("invalid page version: expected {expected}, got {actual}")]
    InvalidPageVersion { expected: u8, actual: u8 },
    #[error("slot directory exceeds usable page space")]
    SlotDirectoryExceedsUsableSpace,
    #[error("content start is outside usable page space")]
    ContentStartOutOfBounds,
    #[error("slot directory overlaps the cell-content region")]
    SlotDirectoryOverlapsContent,
    #[error("reserved footer is not zeroed")]
    ReservedFooterNotZero,
    #[error("fragmented free byte count exceeds the supported maximum")]
    FragmentedFreeBytesTooLarge,
    #[error("freeblock offset points outside the content region")]
    FreeblockOffsetOutOfBounds,
    #[error("freeblock is smaller than the minimum header size")]
    FreeblockTooSmall,
    #[error("freeblock runs past the usable page bounds")]
    FreeblockOutOfBounds,
    #[error("slot offset points outside the cell-content region")]
    SlotOffsetOutOfBounds,
    #[error("cell length prefix runs past the usable page bounds")]
    CellLengthPrefixOutOfBounds,
    #[error("interior cell runs past the usable page bounds")]
    InteriorCellOutOfBounds,
    #[error("cell length is smaller than the minimum header")]
    CellLengthTooSmall,
    #[error("cell length runs past the usable page bounds")]
    CellLengthOutOfBounds,
    #[error("table row-id key has invalid length {actual}")]
    InvalidTableRowIdKeyLength { actual: usize },
    #[error("index row-id value has invalid length {actual}")]
    InvalidIndexRowIdValueLength { actual: usize },
    #[error("overflow chain ended before {expected} bytes could be read")]
    OverflowChainTooShort { expected: usize, actual: usize },
    #[error("overflow chain has extra pages after {expected} bytes were read")]
    OverflowChainTooLong { expected: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConstraintError {
    #[error("duplicate key")]
    DuplicateKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidArgumentError {
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("key not found")]
    KeyNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LimitExceededError {
    #[error("page full: need {needed} bytes, only {available} available")]
    PageFull { needed: usize, available: usize },
    #[error("cell too large: {len} bytes exceeds max {max}")]
    CellTooLarge { len: usize, max: usize },
    #[error("cache capacity exhausted")]
    CacheCapacityExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InternalError {
    #[error("{0}")]
    InvariantViolation(#[source] InvariantViolation),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvariantViolation {
    #[error("pinned page during flush: {page_id}")]
    PinnedPageDuringFlush { page_id: PageId },
    #[error("page {page_id} cannot be borrowed due to an active conflicting borrow")]
    PageBorrowConflict { page_id: PageId },
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
    #[error("invalid slot index {slot_index} for {slot_count} slots")]
    InvalidSlotIndex { slot_index: u16, slot_count: u16 },
}

#[derive(Debug, Error)]
pub(crate) enum DiskManagerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("invalid file size (not multiple of page size): {size}")]
    InvalidFileSize { size: u64 },
}

pub(crate) type DiskManagerResult<T> = Result<T, DiskManagerError>;

#[derive(Debug, Error)]
pub enum PageStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("invalid file size (not multiple of page size): {size}")]
    InvalidFileSize { size: u64 },
}

pub type PageStoreResult<T> = Result<T, PageStoreError>;

#[derive(Debug, Error)]
pub(crate) enum PageCacheError {
    #[error("page store error: {0}")]
    Store(#[from] PageStoreError),
    #[error("no evictable frame available")]
    NoEvictableFrame,
    #[error("page {page_id} is pinned")]
    PinnedPage { page_id: PageId },
    #[error("page {page_id} cannot be borrowed immutably while a mutable borrow is active")]
    PageImmutableBorrowConflict { page_id: PageId },
    #[error("page {page_id} cannot be borrowed mutably while another borrow is active")]
    PageMutableBorrowConflict { page_id: PageId },
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
}

pub(crate) type PageCacheResult<T> = Result<T, PageCacheError>;

impl From<DiskManagerError> for StorageError {
    fn from(err: DiskManagerError) -> Self {
        match err {
            DiskManagerError::Io(err) => Self::Io(err),
            DiskManagerError::InvalidPageId { page_id } => {
                Self::InvalidArgument(InvalidArgumentError::InvalidPageId { page_id })
            }
            DiskManagerError::InvalidFileSize { size } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::DatabaseFile,
                page_id: None,
                kind: CorruptionKind::InvalidFileSize { size, page_size: PAGE_SIZE },
            }),
        }
    }
}

impl From<PageStoreError> for StorageError {
    fn from(err: PageStoreError) -> Self {
        match err {
            PageStoreError::Io(err) => Self::Io(err),
            PageStoreError::InvalidPageId { page_id } => {
                Self::InvalidArgument(InvalidArgumentError::InvalidPageId { page_id })
            }
            PageStoreError::InvalidFileSize { size } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::DatabaseFile,
                page_id: None,
                kind: CorruptionKind::InvalidFileSize { size, page_size: PAGE_SIZE },
            }),
        }
    }
}

impl From<DiskManagerError> for PageStoreError {
    fn from(err: DiskManagerError) -> Self {
        match err {
            DiskManagerError::Io(err) => Self::Io(err),
            DiskManagerError::InvalidPageId { page_id } => Self::InvalidPageId { page_id },
            DiskManagerError::InvalidFileSize { size } => Self::InvalidFileSize { size },
        }
    }
}

impl From<PageCacheError> for StorageError {
    fn from(err: PageCacheError) -> Self {
        match err {
            PageCacheError::Store(err) => err.into(),
            PageCacheError::NoEvictableFrame => {
                Self::LimitExceeded(LimitExceededError::CacheCapacityExhausted)
            }
            PageCacheError::PinnedPage { page_id } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::PinnedPageDuringFlush { page_id },
                ))
            }
            PageCacheError::PageImmutableBorrowConflict { page_id }
            | PageCacheError::PageMutableBorrowConflict { page_id } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::PageBorrowConflict { page_id },
                ))
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

impl From<PageError> for StorageError {
    fn from(err: PageError) -> Self {
        match err {
            PageError::UnknownPageKind { actual } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::Page,
                page_id: None,
                kind: CorruptionKind::UnknownPageKind { actual },
            }),
            PageError::InvalidPageKind { expected, actual } => Self::Corruption(CorruptionError {
                component: match expected.node_kind() {
                    crate::page::format::NodeKind::Leaf => CorruptionComponent::LeafPage,
                    crate::page::format::NodeKind::Interior => CorruptionComponent::InteriorPage,
                },
                page_id: None,
                kind: CorruptionKind::InvalidPageKind {
                    expected: page_kind_name(expected),
                    actual,
                },
            }),
            PageError::InvalidPageVersion { expected, actual } => {
                Self::Corruption(CorruptionError {
                    component: CorruptionComponent::Page,
                    page_id: None,
                    kind: CorruptionKind::InvalidPageVersion { expected, actual },
                })
            }
            PageError::InvalidSlotIndex { slot_index, slot_count } => {
                Self::Internal(InternalError::InvariantViolation(
                    InvariantViolation::InvalidSlotIndex { slot_index, slot_count },
                ))
            }
            PageError::MalformedPage(kind) => Self::Corruption(CorruptionError {
                component: match kind {
                    PageCorruption::InteriorCellOutOfBounds => CorruptionComponent::InteriorPage,
                    _ => CorruptionComponent::Page,
                },
                page_id: None,
                kind: map_page_corruption(kind),
            }),
            PageError::CorruptCell { kind, .. } => Self::Corruption(CorruptionError {
                component: CorruptionComponent::Cell,
                page_id: None,
                kind: map_cell_corruption(kind),
            }),
            PageError::DuplicateKey => Self::Constraint(ConstraintError::DuplicateKey),
            PageError::KeyNotFound => Self::InvalidArgument(InvalidArgumentError::KeyNotFound),
            PageError::PageFull { needed, available } => {
                Self::LimitExceeded(LimitExceededError::PageFull { needed, available })
            }
            PageError::CellTooLarge { len, max } => {
                Self::LimitExceeded(LimitExceededError::CellTooLarge { len, max })
            }
        }
    }
}

fn page_kind_name(kind: crate::page::format::PageKind) -> &'static str {
    match kind {
        crate::page::format::PageKind::RawLeaf => "raw leaf",
        crate::page::format::PageKind::RawInterior => "raw interior",
    }
}

fn map_page_corruption(kind: PageCorruption) -> CorruptionKind {
    match kind {
        PageCorruption::SlotDirectoryExceedsUsableSpace => {
            CorruptionKind::SlotDirectoryExceedsUsableSpace
        }
        PageCorruption::ContentStartOutOfBounds => CorruptionKind::ContentStartOutOfBounds,
        PageCorruption::SlotDirectoryOverlapsContent => {
            CorruptionKind::SlotDirectoryOverlapsContent
        }
        PageCorruption::ReservedFooterNotZero => CorruptionKind::ReservedFooterNotZero,
        PageCorruption::FragmentedFreeBytesTooLarge => CorruptionKind::FragmentedFreeBytesTooLarge,
        PageCorruption::FreeblockOffsetOutOfBounds => CorruptionKind::FreeblockOffsetOutOfBounds,
        PageCorruption::FreeblockTooSmall => CorruptionKind::FreeblockTooSmall,
        PageCorruption::FreeblockOutOfBounds => CorruptionKind::FreeblockOutOfBounds,
        PageCorruption::SlotOffsetOutOfBounds => CorruptionKind::SlotOffsetOutOfBounds,
        PageCorruption::CellLengthPrefixOutOfBounds => CorruptionKind::CellLengthPrefixOutOfBounds,
        PageCorruption::InteriorCellOutOfBounds => CorruptionKind::InteriorCellOutOfBounds,
    }
}

fn map_cell_corruption(kind: CellCorruption) -> CorruptionKind {
    match kind {
        CellCorruption::LengthTooSmall => CorruptionKind::CellLengthTooSmall,
        CellCorruption::LengthOutOfBounds => CorruptionKind::CellLengthOutOfBounds,
        CellCorruption::InvalidTableRowIdKeyLength { actual } => {
            CorruptionKind::InvalidTableRowIdKeyLength { actual }
        }
        CellCorruption::InvalidIndexRowIdValueLength { actual } => {
            CorruptionKind::InvalidIndexRowIdValueLength { actual }
        }
    }
}

impl fmt::Display for CorruptionComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatabaseFile => write!(f, "database file"),
            Self::DiskPage => write!(f, "disk page"),
            Self::OverflowPage => write!(f, "overflow page"),
            Self::Page => write!(f, "page"),
            Self::LeafPage => write!(f, "leaf page"),
            Self::InteriorPage => write!(f, "interior page"),
            Self::Cell => write!(f, "cell"),
        }
    }
}

fn format_corruption_error(
    component: CorruptionComponent,
    page_id: Option<PageId>,
    kind: &CorruptionKind,
) -> String {
    match page_id {
        Some(page_id) => format!("{component} (page {page_id}): {kind}"),
        None => format!("{component}: {kind}"),
    }
}
