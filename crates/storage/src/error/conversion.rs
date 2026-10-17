//! Translation from storage-internal failures to the public storage error.

use crate::PAGE_SIZE;
use crate::disk_manager::DiskManagerError;
use crate::error::{
    ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
    InvalidArgumentError, InvariantViolation, LimitExceededError, StorageError,
};
use crate::log_manager::{LogManagerError, LogManagerFlushError};
use crate::page::{CellCorruption, PageCorruption, PageError};
use crate::page_cache::PageCacheError;

impl From<DiskManagerError> for StorageError {
    fn from(error: DiskManagerError) -> Self {
        match error {
            DiskManagerError::Io(error) => Self::Io(error),
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
    fn from(error: PageCacheError) -> Self {
        match error {
            PageCacheError::Storage(error) => *error,
            PageCacheError::NoEvictableFrame => {
                Self::LimitExceeded(LimitExceededError::CacheCapacityExhausted)
            }
            PageCacheError::PinnedPage { page_id } => {
                invariant(InvariantViolation::PinnedPageDuringFlush { page_id })
            }
            PageCacheError::PageImmutableBorrowConflict { page_id }
            | PageCacheError::PageMutableBorrowConflict { page_id } => {
                invariant(InvariantViolation::PageBorrowConflict { page_id })
            }
            PageCacheError::InvalidFrameCount { frame_count } => {
                invariant(InvariantViolation::InvalidFrameCount { frame_count })
            }
            PageCacheError::FrameAllocationFailed { source, .. } => {
                Self::Internal(InternalError::AllocationFailed(source))
            }
            PageCacheError::CorruptPageTableEntry { page_id, frame_id, frame_count } => {
                invariant(InvariantViolation::CorruptPageTableEntry {
                    page_id,
                    frame_id,
                    frame_count,
                })
            }
            PageCacheError::PinCountOverflow { page_id } => {
                invariant(InvariantViolation::PagePinCountOverflow { page_id })
            }
            PageCacheError::Poisoned { lock } => {
                Self::Internal(InternalError::SynchronizationPoisoned { lock })
            }
        }
    }
}

impl From<LogManagerError> for StorageError {
    fn from(error: LogManagerError) -> Self {
        match error {
            LogManagerError::Io(error) => Self::Io(error),
            error => invariant(InvariantViolation::WalLog { message: error.to_string() }),
        }
    }
}

impl From<LogManagerFlushError> for StorageError {
    fn from(error: LogManagerFlushError) -> Self {
        match error {
            LogManagerFlushError::Io(error) => Self::Io(error),
            LogManagerFlushError::LsnNotAppended { requested_lsn, highest_appended_lsn } => {
                invariant(InvariantViolation::WalFlushLsnNotAppended {
                    requested_lsn,
                    highest_appended_lsn,
                })
            }
            error @ LogManagerFlushError::Poisoned => {
                invariant(InvariantViolation::WalLog { message: error.to_string() })
            }
        }
    }
}

impl From<PageError> for StorageError {
    fn from(error: PageError) -> Self {
        match error {
            PageError::UnknownPageKind { actual } => {
                corruption(CorruptionComponent::Page, CorruptionKind::UnknownPageKind { actual })
            }
            PageError::InvalidPageKind { expected, actual } => {
                let component = match expected.node_kind() {
                    crate::page::format::NodeKind::Leaf => CorruptionComponent::LeafPage,
                    crate::page::format::NodeKind::Interior => CorruptionComponent::InteriorPage,
                };
                corruption(
                    component,
                    CorruptionKind::InvalidPageKind { expected: page_kind_name(expected), actual },
                )
            }
            PageError::InvalidPageVersion { expected, actual } => corruption(
                CorruptionComponent::Page,
                CorruptionKind::InvalidPageVersion { expected, actual },
            ),
            PageError::InvalidSlotIndex { slot_index, slot_count } => {
                invariant(InvariantViolation::InvalidSlotIndex { slot_index, slot_count })
            }
            PageError::MalformedPage(kind) => {
                corruption(CorruptionComponent::Page, map_page_corruption(kind))
            }
            PageError::CorruptCell { kind, .. } => {
                corruption(CorruptionComponent::Cell, map_cell_corruption(kind))
            }
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

fn invariant(kind: InvariantViolation) -> StorageError {
    StorageError::Internal(InternalError::InvariantViolation(kind))
}

fn corruption(component: CorruptionComponent, kind: CorruptionKind) -> StorageError {
    StorageError::Corruption(CorruptionError { component, page_id: None, kind })
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
        PageCorruption::OverlappingPageRegions => CorruptionKind::OverlappingPageRegions,
        PageCorruption::SlotOffsetOutOfBounds => CorruptionKind::SlotOffsetOutOfBounds,
        PageCorruption::CellLengthPrefixOutOfBounds => CorruptionKind::CellLengthPrefixOutOfBounds,
    }
}

fn map_cell_corruption(kind: CellCorruption) -> CorruptionKind {
    match kind {
        CellCorruption::LengthTooSmall => CorruptionKind::CellLengthTooSmall,
        CellCorruption::LengthOutOfBounds => CorruptionKind::CellLengthOutOfBounds,
    }
}
