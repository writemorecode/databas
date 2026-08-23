use std::fmt;

use thiserror::Error;

use crate::core::{PageId, TableKey};

/// Corruption detected in an encoded database component.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{}", format_corruption_error(*component, *page_id, kind))]
pub struct CorruptionError {
    /// Component whose bytes or structure were invalid.
    pub component: CorruptionComponent,
    /// Page containing the corruption, when known.
    pub page_id: Option<PageId>,
    /// Specific validation failure.
    #[source]
    pub kind: CorruptionKind,
}

/// Logical component in which corruption was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionComponent {
    Catalog,
    DatabaseFile,
    DiskPage,
    OverflowPage,
    Page,
    LeafPage,
    InteriorPage,
    Cell,
}

/// Specific malformed on-disk condition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CorruptionKind {
    #[error("invalid file size {size} for page size {page_size}")]
    InvalidFileSize { size: u64, page_size: usize },
    #[error("missing database header")]
    MissingDatabaseHeader,
    #[error("invalid database magic: expected {expected:?}, got {actual:?}")]
    InvalidDatabaseMagic { expected: [u8; 8], actual: [u8; 8] },
    #[error("unsupported database version: expected {expected}, got {actual}")]
    UnsupportedDatabaseVersion { expected: u16, actual: u16 },
    #[error("invalid database page size: expected {expected}, got {actual}")]
    InvalidDatabasePageSize { expected: usize, actual: usize },
    #[error("database header reserved bytes are not zeroed")]
    DatabaseHeaderReservedBytesNotZero,
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
    #[error("table key has invalid length {actual}")]
    InvalidTableKeyLength { actual: usize },
    #[error("index table-key value has invalid length {actual}")]
    InvalidIndexTableKeyValueLength { actual: usize },
    #[error("overflow chain ended before {expected} bytes could be read")]
    OverflowChainTooShort { expected: usize, actual: usize },
    #[error("overflow chain has extra pages after {expected} bytes were read")]
    OverflowChainTooLong { expected: usize },
    #[error("missing system catalog root page {page_id}")]
    MissingSystemCatalogRoot { page_id: PageId },
    #[error("unexpected system catalog root page: expected {expected}, got {actual}")]
    UnexpectedSystemCatalogRoot { expected: PageId, actual: PageId },
    #[error("invalid catalog row in {table}: {reason}")]
    InvalidCatalogRow { table: &'static str, reason: String },
    #[error("invalid table record in {table} key {table_key}: {reason}")]
    InvalidTableRecord { table: String, table_key: TableKey, reason: String },
}

impl fmt::Display for CorruptionComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog => write!(f, "catalog"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corruption_display_includes_component_page_and_kind() {
        let error = CorruptionError {
            component: CorruptionComponent::LeafPage,
            page_id: Some(7),
            kind: CorruptionKind::InvalidPageVersion { expected: 2, actual: 1 },
        };

        assert_eq!(
            error.to_string(),
            "leaf page (page 7): invalid page version: expected 2, got 1"
        );
    }
}
