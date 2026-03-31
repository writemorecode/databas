use crate::SlotId;
use thiserror::Error;

use super::format::PageKind;

/// Errors returned while validating or modifying encoded pages.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PageError {
    /// The encoded page kind tag is not recognized by this format version.
    #[error("unknown page kind: raw tag {actual}")]
    UnknownPageKind { actual: u8 },
    /// The encoded page kind tag does not match the expected page kind.
    #[error("invalid page kind: expected {expected:?}, got raw tag {actual}")]
    InvalidPageKind { expected: PageKind, actual: u8 },
    /// The encoded page version does not match [`super::FORMAT_VERSION`].
    #[error("invalid page version: expected {expected}, got {actual}")]
    InvalidPageVersion { expected: u8, actual: u8 },
    /// A requested slot index is out of bounds for the current slot count.
    #[error("invalid slot index {slot_index} for {slot_count} slots")]
    InvalidSlotIndex { slot_index: SlotId, slot_count: u16 },
    /// The page header or slot directory is structurally invalid.
    #[error("malformed page: {0}")]
    MalformedPage(#[source] PageCorruption),
    /// A specific cell failed validation.
    #[error("corrupt cell at slot {slot_index}: {kind}")]
    CorruptCell {
        slot_index: SlotId,
        #[source]
        kind: CellCorruption,
    },
    /// An insert attempted to reuse an existing key.
    #[error("duplicate key")]
    DuplicateKey,
    /// An update targeted a key that is not present.
    #[error("key not found")]
    KeyNotFound,
    /// The page has insufficient free space for the requested operation.
    #[error("page full: need {needed} bytes, only {available} available")]
    PageFull { needed: usize, available: usize },
    /// A cell encoding is larger than what the page format can represent.
    #[error("cell too large: {len} bytes exceeds max {max}")]
    CellTooLarge { len: usize, max: usize },
}

/// Result type used throughout the page module.
pub type PageResult<T> = Result<T, PageError>;

/// Structural page corruption detected before or during page access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PageCorruption {
    /// The slot directory would extend past the usable page region.
    #[error("slot directory exceeds usable page space")]
    SlotDirectoryExceedsUsableSpace,
    /// The content-start offset points beyond usable page space.
    #[error("content start is outside usable page space")]
    ContentStartOutOfBounds,
    /// The slot directory and cell-content region overlap.
    #[error("slot directory overlaps the cell-content region")]
    SlotDirectoryOverlapsContent,
    /// The reserved footer contains non-zero bytes.
    #[error("reserved footer is not zeroed")]
    ReservedFooterNotZero,
    /// The fragmented free byte count exceeds the supported maximum.
    #[error("fragmented free byte count exceeds the supported maximum")]
    FragmentedFreeBytesTooLarge,
    /// A freeblock header does not begin within the live content region.
    #[error("freeblock offset points outside the content region")]
    FreeblockOffsetOutOfBounds,
    /// A freeblock span is too small to hold its own header.
    #[error("freeblock is smaller than the minimum header size")]
    FreeblockTooSmall,
    /// A freeblock span runs past the usable page region.
    #[error("freeblock runs past the usable page bounds")]
    FreeblockOutOfBounds,
    /// A slot entry points outside the live cell-content region.
    #[error("slot offset points outside the cell-content region")]
    SlotOffsetOutOfBounds,
    /// A cell's length prefix would read past page bounds.
    #[error("cell length prefix runs past the usable page bounds")]
    CellLengthPrefixOutOfBounds,
    /// A fixed-size interior cell would read past page bounds.
    #[error("interior cell runs past the usable page bounds")]
    InteriorCellOutOfBounds,
}

/// Cell-level corruption detected while decoding a page cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CellCorruption {
    /// The encoded cell length is too small for the minimum required prefix.
    #[error("cell length is smaller than the minimum header")]
    LengthTooSmall,
    /// The encoded cell length runs past the usable page region.
    #[error("cell length runs past the usable page bounds")]
    LengthOutOfBounds,
}
