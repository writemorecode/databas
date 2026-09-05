//! Translation from storage-internal failures to the public storage error.

use crate::core::{
    PAGE_SIZE,
    error::{
        ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
        InvalidArgumentError, InvariantViolation, LimitExceededError, StorageError,
    },
};
use crate::storage::{
    disk_manager::DiskManagerError,
    log_manager::{LogManagerError, LogManagerFlushError},
    page::{CellCorruption, PageCorruption, PageError},
    page_cache::PageCacheError,
};

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
                    crate::storage::page::format::NodeKind::Leaf => CorruptionComponent::LeafPage,
                    crate::storage::page::format::NodeKind::Interior => {
                        CorruptionComponent::InteriorPage
                    }
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

fn page_kind_name(kind: crate::storage::page::format::PageKind) -> &'static str {
    match kind {
        crate::storage::page::format::PageKind::RawLeaf => "raw leaf",
        crate::storage::page::format::PageKind::RawInterior => "raw interior",
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
    }
}

fn map_cell_corruption(kind: CellCorruption) -> CorruptionKind {
    match kind {
        CellCorruption::LengthTooSmall => CorruptionKind::CellLengthTooSmall,
        CellCorruption::LengthOutOfBounds => CorruptionKind::CellLengthOutOfBounds,
        CellCorruption::InvalidTableKeyLength { actual } => {
            CorruptionKind::InvalidTableKeyLength { actual }
        }
        CellCorruption::InvalidIndexTableKeyValueLength { actual } => {
            CorruptionKind::InvalidIndexTableKeyValueLength { actual }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::storage::page::format::PageKind;

    #[test]
    fn disk_file_size_error_maps_to_database_corruption() {
        let error = StorageError::from(DiskManagerError::InvalidFileSize { size: 17 });

        assert!(matches!(
            error,
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::DatabaseFile,
                kind: CorruptionKind::InvalidFileSize { size: 17, page_size: PAGE_SIZE },
                ..
            })
        ));
    }

    #[test]
    fn page_kind_error_preserves_node_component() {
        let error = StorageError::from(PageError::InvalidPageKind {
            expected: PageKind::RawLeaf,
            actual: 99,
        });

        assert!(matches!(
            error,
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::LeafPage,
                kind: CorruptionKind::InvalidPageKind { expected: "raw leaf", actual: 99 },
                ..
            })
        ));
    }

    #[test]
    fn page_capacity_error_maps_to_limit_error() {
        let error = StorageError::from(PageError::PageFull { needed: 10, available: 4 });

        assert!(matches!(
            error,
            StorageError::LimitExceeded(LimitExceededError::PageFull { needed: 10, available: 4 })
        ));
    }

    #[test]
    fn wal_format_error_maps_to_internal_invariant() {
        let error = StorageError::from(LogManagerError::InvalidDbFilePath {
            db_file_path: PathBuf::from("database"),
        });

        assert!(matches!(
            error,
            StorageError::Internal(InternalError::InvariantViolation(
                InvariantViolation::WalLog { .. }
            ))
        ));
    }
}
