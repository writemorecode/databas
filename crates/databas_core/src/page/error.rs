use std::{error::Error as StdError, fmt};

use crate::types::RowId;

use super::format::PageKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    InvalidPageKind { expected: PageKind, actual: u8 },
    InvalidPageVersion { expected: u8, actual: u8 },
    InvalidSlotIndex { slot_index: u16, slot_count: u16 },
    MalformedPage(PageCorruption),
    CorruptCell { slot_index: u16, kind: CellCorruption },
    DuplicateKey { key: RowId },
    KeyNotFound { key: RowId },
    PageFull { needed: usize, available: usize },
    CellTooLarge { len: usize, max: usize },
}

pub type PageResult<T> = Result<T, PageError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCorruption {
    SlotDirectoryExceedsUsableSpace,
    ContentStartOutOfBounds,
    SlotDirectoryOverlapsContent,
    ReservedFooterNotZero,
    SlotOffsetOutOfBounds,
    CellLengthPrefixOutOfBounds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellCorruption {
    LengthTooSmall,
    LengthOutOfBounds,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPageKind { expected, actual } => {
                write!(f, "invalid page kind: expected {expected:?}, got raw tag {actual}")
            }
            Self::InvalidPageVersion { expected, actual } => {
                write!(f, "invalid page version: expected {expected}, got {actual}")
            }
            Self::InvalidSlotIndex { slot_index, slot_count } => {
                write!(f, "invalid slot index {slot_index} for {slot_count} slots")
            }
            Self::MalformedPage(kind) => write!(f, "malformed page: {kind}"),
            Self::CorruptCell { slot_index, kind } => {
                write!(f, "corrupt cell at slot {slot_index}: {kind}")
            }
            Self::DuplicateKey { key } => write!(f, "duplicate key: {key}"),
            Self::KeyNotFound { key } => write!(f, "key not found: {key}"),
            Self::PageFull { needed, available } => {
                write!(f, "page full: need {needed} bytes, only {available} available")
            }
            Self::CellTooLarge { len, max } => {
                write!(f, "cell too large: {len} bytes exceeds max {max}")
            }
        }
    }
}

impl StdError for PageError {}

impl fmt::Display for PageCorruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::SlotOffsetOutOfBounds => {
                write!(f, "slot offset points outside the cell-content region")
            }
            Self::CellLengthPrefixOutOfBounds => {
                write!(f, "cell length prefix runs past the usable page bounds")
            }
        }
    }
}

impl fmt::Display for CellCorruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthTooSmall => write!(f, "cell length is smaller than the minimum header"),
            Self::LengthOutOfBounds => write!(f, "cell length runs past the usable page bounds"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_mentions_key_details() {
        let duplicate = PageError::DuplicateKey { key: 42 };
        assert_eq!(duplicate.to_string(), "duplicate key: 42");

        let invalid_kind = PageError::InvalidPageKind { expected: PageKind::Leaf, actual: 9 };
        assert!(invalid_kind.to_string().contains("expected Leaf"));
    }
}
