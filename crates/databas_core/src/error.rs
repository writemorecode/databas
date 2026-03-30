use std::{error::Error as StdError, fmt};

use crate::{
    page::{CellCorruption, PageCorruption, PageError},
    types::{PAGE_SIZE, PageId, RowId},
};

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Corruption(CorruptionError),
    Constraint(ConstraintError),
    InvalidArgument(InvalidArgumentError),
    LimitExceeded(LimitExceededError),
    Internal(InternalError),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptionError {
    pub component: CorruptionComponent,
    pub page_id: Option<PageId>,
    pub kind: CorruptionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionComponent {
    DatabaseFile,
    DiskPage,
    Page,
    LeafPage,
    InteriorPage,
    Cell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptionKind {
    InvalidFileSize { size: u64, page_size: usize },
    UnknownPageKind { actual: u8 },
    InvalidPageKind { expected: &'static str, actual: u8 },
    InvalidPageVersion { expected: u8, actual: u8 },
    SlotDirectoryExceedsUsableSpace,
    ContentStartOutOfBounds,
    SlotDirectoryOverlapsContent,
    ReservedFooterNotZero,
    FragmentedFreeBytesTooLarge,
    FreeblockOffsetOutOfBounds,
    FreeblockTooSmall,
    FreeblockOutOfBounds,
    SlotOffsetOutOfBounds,
    CellLengthPrefixOutOfBounds,
    InteriorCellOutOfBounds,
    CellLengthTooSmall,
    CellLengthOutOfBounds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintError {
    DuplicateRowId { row_id: RowId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidArgumentError {
    InvalidPageId { page_id: PageId },
    RowIdNotFound { row_id: RowId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitExceededError {
    PageFull { needed: usize, available: usize },
    CellTooLarge { len: usize, max: usize },
    CacheCapacityExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalError {
    InvariantViolation(InvariantViolation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    PinnedPageDuringFlush { page_id: PageId },
    PageBorrowConflict { page_id: PageId },
    InvalidFrameCount { frame_count: usize },
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
    InvalidSlotIndex { slot_index: u16, slot_count: u16 },
}

#[derive(Debug)]
pub(crate) enum DiskManagerError {
    Io(std::io::Error),
    InvalidPageId { page_id: PageId },
    InvalidFileSize { size: u64 },
}

pub(crate) type DiskManagerResult<T> = Result<T, DiskManagerError>;

#[derive(Debug)]
pub(crate) enum PageCacheError {
    Disk(DiskManagerError),
    NoEvictableFrame,
    PinnedPage { page_id: PageId },
    PageImmutableBorrowConflict { page_id: PageId },
    PageMutableBorrowConflict { page_id: PageId },
    InvalidFrameCount { frame_count: usize },
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
}

pub(crate) type PageCacheResult<T> = Result<T, PageCacheError>;

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<std::io::Error> for DiskManagerError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<DiskManagerError> for PageCacheError {
    fn from(err: DiskManagerError) -> Self {
        Self::Disk(err)
    }
}

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
                component: match expected {
                    crate::page::format::PageKind::Leaf => CorruptionComponent::LeafPage,
                    crate::page::format::PageKind::Interior => CorruptionComponent::InteriorPage,
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
            PageError::DuplicateRowId { row_id } => {
                Self::Constraint(ConstraintError::DuplicateRowId { row_id })
            }
            PageError::RowIdNotFound { row_id } => {
                Self::InvalidArgument(InvalidArgumentError::RowIdNotFound { row_id })
            }
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
        crate::page::format::PageKind::Leaf => "leaf",
        crate::page::format::PageKind::Interior => "interior",
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
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Corruption(err) => write!(f, "corruption: {err}"),
            Self::Constraint(err) => write!(f, "constraint violation: {err}"),
            Self::InvalidArgument(err) => write!(f, "invalid argument: {err}"),
            Self::LimitExceeded(err) => write!(f, "limit exceeded: {err}"),
            Self::Internal(err) => write!(f, "internal error: {err}"),
        }
    }
}

impl StdError for StorageError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Corruption(err) => Some(err),
            Self::Constraint(err) => Some(err),
            Self::InvalidArgument(err) => Some(err),
            Self::LimitExceeded(err) => Some(err),
            Self::Internal(err) => Some(err),
        }
    }
}

impl fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.page_id {
            Some(page_id) => write!(f, "{} (page {page_id}): {}", self.component, self.kind),
            None => write!(f, "{}: {}", self.component, self.kind),
        }
    }
}

impl StdError for CorruptionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.kind)
    }
}

impl fmt::Display for CorruptionComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatabaseFile => write!(f, "database file"),
            Self::DiskPage => write!(f, "disk page"),
            Self::Page => write!(f, "page"),
            Self::LeafPage => write!(f, "leaf page"),
            Self::InteriorPage => write!(f, "interior page"),
            Self::Cell => write!(f, "cell"),
        }
    }
}

impl fmt::Display for CorruptionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFileSize { size, page_size } => {
                write!(f, "invalid file size {size} for page size {page_size}")
            }
            Self::UnknownPageKind { actual } => write!(f, "unknown page kind: raw tag {actual}"),
            Self::InvalidPageKind { expected, actual } => {
                write!(f, "invalid page kind: expected {expected}, got raw tag {actual}")
            }
            Self::InvalidPageVersion { expected, actual } => {
                write!(f, "invalid page version: expected {expected}, got {actual}")
            }
            Self::SlotDirectoryExceedsUsableSpace => {
                write!(f, "slot directory exceeds usable page space")
            }
            Self::ContentStartOutOfBounds => {
                write!(f, "content start is outside usable page space")
            }
            Self::SlotDirectoryOverlapsContent => {
                write!(f, "slot directory overlaps the cell-content region")
            }
            Self::ReservedFooterNotZero => write!(f, "reserved footer is not zeroed"),
            Self::FragmentedFreeBytesTooLarge => {
                write!(f, "fragmented free byte count exceeds the supported maximum")
            }
            Self::FreeblockOffsetOutOfBounds => {
                write!(f, "freeblock offset points outside the content region")
            }
            Self::FreeblockTooSmall => {
                write!(f, "freeblock is smaller than the minimum header size")
            }
            Self::FreeblockOutOfBounds => {
                write!(f, "freeblock runs past the usable page bounds")
            }
            Self::SlotOffsetOutOfBounds => {
                write!(f, "slot offset points outside the cell-content region")
            }
            Self::CellLengthPrefixOutOfBounds => {
                write!(f, "cell length prefix runs past the usable page bounds")
            }
            Self::InteriorCellOutOfBounds => {
                write!(f, "interior cell runs past the usable page bounds")
            }
            Self::CellLengthTooSmall => {
                write!(f, "cell length is smaller than the minimum header")
            }
            Self::CellLengthOutOfBounds => {
                write!(f, "cell length runs past the usable page bounds")
            }
        }
    }
}

impl StdError for CorruptionKind {}

impl fmt::Display for ConstraintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateRowId { row_id } => write!(f, "duplicate row id: {row_id}"),
        }
    }
}

impl StdError for ConstraintError {}

impl fmt::Display for InvalidArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPageId { page_id } => write!(f, "invalid page id: {page_id}"),
            Self::RowIdNotFound { row_id } => write!(f, "row id not found: {row_id}"),
        }
    }
}

impl StdError for InvalidArgumentError {}

impl fmt::Display for LimitExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageFull { needed, available } => {
                write!(f, "page full: need {needed} bytes, only {available} available")
            }
            Self::CellTooLarge { len, max } => {
                write!(f, "cell too large: {len} bytes exceeds max {max}")
            }
            Self::CacheCapacityExhausted => write!(f, "cache capacity exhausted"),
        }
    }
}

impl StdError for LimitExceededError {}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvariantViolation(inner) => write!(f, "{inner}"),
        }
    }
}

impl StdError for InternalError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::InvariantViolation(inner) => Some(inner),
        }
    }
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PinnedPageDuringFlush { page_id } => {
                write!(f, "pinned page during flush: {page_id}")
            }
            Self::PageBorrowConflict { page_id } => {
                write!(f, "page {page_id} cannot be borrowed due to an active conflicting borrow")
            }
            Self::InvalidFrameCount { frame_count } => {
                write!(f, "invalid frame count: {frame_count}")
            }
            Self::CorruptPageTableEntry { page_id, frame_id, frame_count } => write!(
                f,
                "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
            ),
            Self::InvalidSlotIndex { slot_index, slot_count } => {
                write!(f, "invalid slot index {slot_index} for {slot_count} slots")
            }
        }
    }
}

impl StdError for InvariantViolation {}

impl fmt::Display for DiskManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::InvalidPageId { page_id } => write!(f, "invalid page id: {page_id}"),
            Self::InvalidFileSize { size } => {
                write!(f, "invalid file size (not multiple of page size): {size}")
            }
        }
    }
}

impl StdError for DiskManagerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::InvalidPageId { .. } | Self::InvalidFileSize { .. } => None,
        }
    }
}

impl fmt::Display for PageCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disk(err) => write!(f, "disk manager error: {err}"),
            Self::NoEvictableFrame => write!(f, "no evictable frame available"),
            Self::PinnedPage { page_id } => write!(f, "page {page_id} is pinned"),
            Self::PageImmutableBorrowConflict { page_id } => {
                write!(
                    f,
                    "page {page_id} cannot be borrowed immutably while a mutable borrow is active"
                )
            }
            Self::PageMutableBorrowConflict { page_id } => {
                write!(
                    f,
                    "page {page_id} cannot be borrowed mutably while another borrow is active"
                )
            }
            Self::InvalidFrameCount { frame_count } => {
                write!(f, "invalid frame count: {frame_count}")
            }
            Self::CorruptPageTableEntry { page_id, frame_id, frame_count } => write!(
                f,
                "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
            ),
        }
    }
}

impl StdError for PageCacheError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Disk(err) => Some(err),
            Self::NoEvictableFrame
            | Self::PinnedPage { .. }
            | Self::PageImmutableBorrowConflict { .. }
            | Self::PageMutableBorrowConflict { .. }
            | Self::InvalidFrameCount { .. }
            | Self::CorruptPageTableEntry { .. } => None,
        }
    }
}
