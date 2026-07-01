use std::fmt;

pub(crate) mod access;
pub(crate) mod btree;
pub(crate) mod catalog;
mod catalog_manager;
pub(crate) mod cursor;
pub(crate) mod database;
pub(crate) mod database_header;
pub(crate) mod disk_manager;
pub mod error;
pub(crate) mod index_manager;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub(crate) mod pager;
pub(crate) mod record_manager;
pub(crate) mod recovery;
pub(crate) mod storage_runtime;
pub(crate) mod transaction_manager;
pub(crate) mod transaction_runtime;
pub(crate) mod tuple;

pub(crate) mod log_manager;

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
