use std::fmt;

/// Identifier of a page in the database file.
pub type PageId = u64;
/// Identifier of an object stored in the system catalog.
pub type CatalogId = i32;
/// Integer primary key used by table B+-trees.
pub type TableKey = i32;
pub(crate) type SlotId = u16;
/// Monotonic identifier for a transaction in the write-ahead log.
pub(crate) type TxnId = u64;
/// Log sequence number assigned to one write-ahead-log record.
pub(crate) type Lsn = u64;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_key_range_respects_inclusive_and_exclusive_bounds() {
        let range = TableKeyRange {
            lower: Some(TableKeyBound::Exclusive(10)),
            upper: Some(TableKeyBound::Inclusive(20)),
        };

        assert!(!range.contains(10));
        assert!(range.contains(11));
        assert!(range.contains(20));
        assert!(!range.contains(21));
    }

    #[test]
    fn index_key_range_respects_bounds_and_detects_upper_end() {
        let range = IndexKeyRange {
            lower: Some(IndexKeyBound::Inclusive(b"b".to_vec())),
            upper: Some(IndexKeyBound::Exclusive(b"d".to_vec())),
        };

        assert!(!range.contains(b"a"));
        assert!(range.contains(b"b"));
        assert!(range.contains(b"c"));
        assert!(!range.contains(b"d"));
        assert!(!range.is_past_upper(b"c"));
        assert!(range.is_past_upper(b"d"));
    }

    #[test]
    fn unbounded_ranges_accept_every_key() {
        assert!(TableKeyRange::unbounded().contains(TableKey::MIN));
        assert!(TableKeyRange::unbounded().contains(TableKey::MAX));
        assert!(IndexKeyRange::default().contains(&[]));
        assert!(!IndexKeyRange::default().is_past_upper(&[]));
    }
}
