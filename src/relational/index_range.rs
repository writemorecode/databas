//! Logical index ranges and their private persisted-key encoding.
use crate::relational::cursor::encode_index_entry_key;
use crate::relational::{
    IndexKeyBound, IndexKeyRange, StorageResult, TableKey, TupleRef, Value, ValueRef,
};
use std::fmt;

/// Inclusive or exclusive logical bound for a secondary-index scan.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexValueBound {
    /// The scan includes this value.
    Inclusive(Value),
    /// The scan excludes this value.
    Exclusive(Value),
}

impl fmt::Display for IndexValueBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inclusive(value) => write!(f, "{value} inclusive"),
            Self::Exclusive(value) => write!(f, "{value} exclusive"),
        }
    }
}

/// Logical range over values in a single-column secondary index.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexValueRange {
    /// Optional lower value bound.
    pub lower: Option<IndexValueBound>,
    /// Optional upper value bound.
    pub upper: Option<IndexValueBound>,
}

impl fmt::Display for IndexValueRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.lower, &self.upper) {
            (None, None) => write!(f, "unbounded"),
            (Some(lower), None) => write!(f, "lower={lower}"),
            (None, Some(upper)) => write!(f, "upper={upper}"),
            (Some(lower), Some(upper)) => write!(f, "lower={lower} upper={upper}"),
        }
    }
}

impl IndexValueBound {
    fn encode(&self, lower: bool) -> StorageResult<IndexKeyBound> {
        let (value, inclusive) = match self {
            Self::Inclusive(v) => (v, true),
            Self::Exclusive(v) => (v, false),
        };
        let key = if lower {
            index_lower_key(value, inclusive)?
        } else {
            index_upper_key(value, inclusive)?
        };
        Ok(if inclusive { IndexKeyBound::Inclusive(key) } else { IndexKeyBound::Exclusive(key) })
    }
}
impl IndexValueRange {
    /// Intersects a lower bound using the index's persisted ordering.
    pub fn intersect_lower(&mut self, value: Value, inclusive: bool) -> StorageResult<()> {
        let candidate = if inclusive {
            IndexValueBound::Inclusive(value)
        } else {
            IndexValueBound::Exclusive(value)
        };
        let current = self.lower.as_ref().map(|v| v.encode(true)).transpose()?;
        if index_lower_is_tighter(current.as_ref(), &candidate.encode(true)?) {
            self.lower = Some(candidate);
        }
        Ok(())
    }
    /// Intersects an upper bound using the index's persisted ordering.
    pub fn intersect_upper(&mut self, value: Value, inclusive: bool) -> StorageResult<()> {
        let candidate = if inclusive {
            IndexValueBound::Inclusive(value)
        } else {
            IndexValueBound::Exclusive(value)
        };
        let current = self.upper.as_ref().map(|v| v.encode(false)).transpose()?;
        if index_upper_is_tighter(current.as_ref(), &candidate.encode(false)?) {
            self.upper = Some(candidate);
        }
        Ok(())
    }
    pub(crate) fn encode(&self) -> StorageResult<IndexKeyRange> {
        Ok(IndexKeyRange {
            lower: self.lower.as_ref().map(|v| v.encode(true)).transpose()?,
            upper: self.upper.as_ref().map(|v| v.encode(false)).transpose()?,
        })
    }
}
/// Encodes the first index entry key allowed by a lower value bound.
fn index_lower_key(value: &Value, inclusive: bool) -> StorageResult<Vec<u8>> {
    let prefix = TupleRef::new(&[ValueRef::from(value)]).to_bytes()?;
    let table_key = if inclusive { TableKey::MIN } else { TableKey::MAX };
    Ok(encode_index_entry_key(&prefix, table_key))
}

/// Encodes the last index entry key allowed by an upper value bound.
fn index_upper_key(value: &Value, inclusive: bool) -> StorageResult<Vec<u8>> {
    let prefix = TupleRef::new(&[ValueRef::from(value)]).to_bytes()?;
    let table_key = if inclusive { TableKey::MAX } else { TableKey::MIN };
    Ok(encode_index_entry_key(&prefix, table_key))
}

/// Reports whether a candidate lower key bound narrows the current bound.
fn index_lower_is_tighter(current: Option<&IndexKeyBound>, candidate: &IndexKeyBound) -> bool {
    match current {
        None => true,
        Some(current) => {
            candidate.key() > current.key()
                || (candidate.key() == current.key()
                    && matches!(candidate, IndexKeyBound::Exclusive(_)))
        }
    }
}

/// Reports whether a candidate upper key bound narrows the current bound.
fn index_upper_is_tighter(current: Option<&IndexKeyBound>, candidate: &IndexKeyBound) -> bool {
    match current {
        None => true,
        Some(current) => {
            candidate.key() < current.key()
                || (candidate.key() == current.key()
                    && matches!(candidate, IndexKeyBound::Exclusive(_)))
        }
    }
}
