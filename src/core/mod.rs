use std::fmt;

pub(crate) mod access;
pub(crate) mod catalog;
mod catalog_manager;
pub(crate) mod cursor;
pub(crate) mod database;
pub mod error;
pub(crate) mod index_manager;
pub(crate) mod record_manager;
pub(crate) mod tuple;

pub use catalog::{
    ColumnSchema, DataType, IndexColumnSchema, IndexSchema, TableSchema, TupleSchema,
};
pub use cursor::{
    IndexEntry, IndexEntryView, OwnedIndexEntry, OwnedTableRecord, TableRecord, TableRecordView,
};
pub use database::Database;
pub use error::{
    ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
    InvalidArgumentError, LimitExceededError, StorageError, StorageResult,
};
pub use tuple::{EncodedTupleView, Tuple, TupleRef, TupleView, Value, ValueRef};

pub(crate) const PAGE_SIZE: usize = 4096;

pub type PageId = u64;
pub type CatalogId = i32;
pub type TableKey = i32;
pub(crate) type SlotId = u16;

/// Inclusive or exclusive bound over encoded secondary-index B+-tree keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexKeyBound {
    /// The bound includes the stored key bytes.
    Inclusive(Vec<u8>),
    /// The bound excludes the stored key bytes.
    Exclusive(Vec<u8>),
}

impl IndexKeyBound {
    /// Returns the encoded key bytes stored in this bound.
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Inclusive(key) | Self::Exclusive(key) => key,
        }
    }

    /// Returns whether `key` satisfies this lower bound.
    pub(crate) fn contains_lower(&self, key: &[u8]) -> bool {
        match self {
            Self::Inclusive(value) => key >= value,
            Self::Exclusive(value) => key > value,
        }
    }

    /// Returns whether `key` satisfies this upper bound.
    pub(crate) fn contains_upper(&self, key: &[u8]) -> bool {
        match self {
            Self::Inclusive(value) => key <= value,
            Self::Exclusive(value) => key < value,
        }
    }
}

/// Ordered key-byte range for scanning a secondary-index B+-tree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexKeyRange {
    /// Optional lower bound.
    pub lower: Option<IndexKeyBound>,
    /// Optional upper bound.
    pub upper: Option<IndexKeyBound>,
}

impl IndexKeyRange {
    /// Returns whether `key` is inside the range.
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.lower.as_ref().is_none_or(|bound| bound.contains_lower(key))
            && self.upper.as_ref().is_none_or(|bound| bound.contains_upper(key))
    }

    /// Returns whether `key` has moved beyond this range's upper bound.
    pub(crate) fn is_past_upper(&self, key: &[u8]) -> bool {
        self.upper.as_ref().is_some_and(|bound| !bound.contains_upper(key))
    }
}

/// Inclusive or exclusive bound over table primary keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKeyBound {
    /// The bound includes the stored key value.
    Inclusive(TableKey),
    /// The bound excludes the stored key value.
    Exclusive(TableKey),
}

impl TableKeyBound {
    /// Returns the raw key value stored in this bound.
    pub fn value(self) -> TableKey {
        match self {
            Self::Inclusive(value) | Self::Exclusive(value) => value,
        }
    }

    /// Returns whether `key` satisfies this lower bound.
    pub fn contains_lower(self, key: TableKey) -> bool {
        match self {
            Self::Inclusive(value) => key >= value,
            Self::Exclusive(value) => key > value,
        }
    }

    /// Returns whether `key` satisfies this upper bound.
    pub fn contains_upper(self, key: TableKey) -> bool {
        match self {
            Self::Inclusive(value) => key <= value,
            Self::Exclusive(value) => key < value,
        }
    }
}

impl fmt::Display for TableKeyBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inclusive(value) => write!(f, "{value} inclusive"),
            Self::Exclusive(value) => write!(f, "{value} exclusive"),
        }
    }
}

/// Ordered primary-key range for scanning a table B+-tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableKeyRange {
    /// Optional lower bound.
    pub lower: Option<TableKeyBound>,
    /// Optional upper bound.
    pub upper: Option<TableKeyBound>,
}

impl TableKeyRange {
    /// Returns a range with no lower or upper bound.
    pub fn unbounded() -> Self {
        Self::default()
    }

    /// Returns whether `key` is inside the range.
    pub fn contains(self, key: TableKey) -> bool {
        self.lower.is_none_or(|bound| bound.contains_lower(key))
            && self.upper.is_none_or(|bound| bound.contains_upper(key))
    }
}

impl fmt::Display for TableKeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.lower, self.upper) {
            (None, None) => write!(f, "unbounded"),
            (Some(lower), None) => write!(f, "lower={lower}"),
            (None, Some(upper)) => write!(f, "upper={upper}"),
            (Some(lower), Some(upper)) => write!(f, "lower={lower} upper={upper}"),
        }
    }
}
