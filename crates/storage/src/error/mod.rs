//! Errors exposed by the database facade.

use thiserror::Error;

use crate::PageId;

mod corruption;
mod internal;

pub use corruption::{CorruptionComponent, CorruptionError, CorruptionKind};
pub use internal::{InternalError, InvariantViolation};

/// Top-level error returned by storage and relational operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(#[source] CorruptionError),
    #[error("constraint violation: {0}")]
    Constraint(#[source] ConstraintError),
    #[error("invalid argument: {0}")]
    InvalidArgument(#[source] InvalidArgumentError),
    #[error("limit exceeded: {0}")]
    LimitExceeded(#[source] LimitExceededError),
    #[error("internal error: {0}")]
    Internal(#[source] InternalError),
}

/// Result type returned by storage and relational operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Constraint violation caused by data or schema changes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConstraintError {
    #[error("duplicate key")]
    DuplicateKey,
}

/// Invalid argument supplied to a storage or relational operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidArgumentError {
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("key not found")]
    KeyNotFound,
}

/// Resource or encoding limit exceeded by an operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LimitExceededError {
    #[error("page full: need {needed} bytes, only {available} available")]
    PageFull { needed: usize, available: usize },
    #[error("cell too large: {len} bytes exceeds max {max}")]
    CellTooLarge { len: usize, max: usize },
    #[error("cache capacity exhausted")]
    CacheCapacityExhausted,
}

mod conversion;

impl StorageError {
    /// Whether continued database use is unsafe after this failure.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Corruption(_) | Self::Internal(_))
    }
}
