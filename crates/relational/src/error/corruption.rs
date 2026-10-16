use std::fmt;

use thiserror::Error;

use crate::{PageId, TableKey};

/// Corruption detected in an encoded database component.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{}", format_corruption_error(*component, *page_id, kind))]
pub struct CorruptionError {
    /// Component whose bytes or structure were invalid.
    pub(crate) component: CorruptionComponent,
    /// Page containing the corruption, when known.
    pub(crate) page_id: Option<PageId>,
    /// Specific validation failure.
    #[source]
    pub(crate) kind: CorruptionKind,
}

/// Logical component in which corruption was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionComponent {
    Catalog,
    TableRecord,
    IndexEntry,
}

/// Specific malformed on-disk condition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CorruptionKind {
    #[error("table key has invalid length {actual}")]
    InvalidTableKeyLength { actual: usize },
    #[error("index table-key value has invalid length {actual}")]
    InvalidIndexTableKeyValueLength { actual: usize },
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
            Self::TableRecord => write!(f, "table record"),
            Self::IndexEntry => write!(f, "index entry"),
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
            component: CorruptionComponent::TableRecord,
            page_id: Some(7),
            kind: CorruptionKind::InvalidTableKeyLength { actual: 2 },
        };

        assert_eq!(error.to_string(), "table record (page 7): table key has invalid length 2");
    }
}
