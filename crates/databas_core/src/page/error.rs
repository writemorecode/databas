use std::{error::Error as StdError, fmt};

use crate::types::{RowId, SlotId};

use super::format::PageKind;

/// Errors returned while validating or modifying encoded pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    /// The encoded page kind tag is not recognized by this format version.
    UnknownPageKind { actual: u8 },
    /// The encoded page kind tag does not match the expected page kind.
    InvalidPageKind { expected: PageKind, actual: u8 },
    /// The encoded page version does not match [`super::FORMAT_VERSION`].
    InvalidPageVersion { expected: u8, actual: u8 },
    /// A requested slot index is out of bounds for the current slot count.
    InvalidSlotIndex { slot_index: SlotId, slot_count: u16 },
    /// The page header or slot directory is structurally invalid.
    MalformedPage(PageCorruption),
    /// A specific cell failed validation.
    CorruptCell { slot_index: SlotId, kind: CellCorruption },
    /// An insert attempted to reuse an existing row id.
    DuplicateKey { key: RowId },
    /// An update targeted a row id that is not present.
    KeyNotFound { key: RowId },
    /// The page has insufficient free space for the requested operation.
    PageFull { needed: usize, available: usize },
    /// A cell encoding is larger than what the page format can represent.
    CellTooLarge { len: usize, max: usize },
}

/// Result type used throughout the page module.
pub type PageResult<T> = Result<T, PageError>;

/// Structural page corruption detected before or during page access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCorruption {
    /// The slot directory would extend past the usable page region.
    SlotDirectoryExceedsUsableSpace,
    /// The content-start offset points beyond usable page space.
    ContentStartOutOfBounds,
    /// The slot directory and cell-content region overlap.
    SlotDirectoryOverlapsContent,
    /// The reserved footer contains non-zero bytes.
    ReservedFooterNotZero,
    /// The fragmented free byte count exceeds the supported maximum.
    FragmentedFreeBytesTooLarge,
    /// A freeblock header does not begin within the live content region.
    FreeblockOffsetOutOfBounds,
    /// A freeblock span is too small to hold its own header.
    FreeblockTooSmall,
    /// A freeblock span runs past the usable page region.
    FreeblockOutOfBounds,
    /// A slot entry points outside the live cell-content region.
    SlotOffsetOutOfBounds,
    /// A cell's length prefix would read past page bounds.
    CellLengthPrefixOutOfBounds,
    /// A fixed-size interior cell would read past page bounds.
    InteriorCellOutOfBounds,
}

/// Cell-level corruption detected while decoding a page cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellCorruption {
    /// The encoded cell length is too small for the minimum required prefix.
    LengthTooSmall,
    /// The encoded cell length runs past the usable page region.
    LengthOutOfBounds,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPageKind { actual } => {
                write!(f, "unknown page kind: raw tag {actual}")
            }
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

        let slot_index: SlotId = 7;
        let invalid_slot = PageError::InvalidSlotIndex { slot_index, slot_count: 3 };
        assert_eq!(invalid_slot.to_string(), "invalid slot index 7 for 3 slots");

        let unknown_kind = PageError::UnknownPageKind { actual: 9 };
        assert!(unknown_kind.to_string().contains("unknown page kind"));

        let invalid_kind = PageError::InvalidPageKind { expected: PageKind::Leaf, actual: 9 };
        assert!(invalid_kind.to_string().contains("expected Leaf"));
    }
}
