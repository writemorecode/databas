//! Errors exposed by the database facade.

use std::collections::TryReserveError;
use thiserror::Error;

use crate::relational::PageId;
use crate::relational::lock_manager::LockError;

mod corruption;

pub use crate::storage::error::{InternalError, InvariantViolation, LimitExceededError};
pub use corruption::{CorruptionComponent, CorruptionError, CorruptionKind};

/// Errors returned by relational operations.
#[derive(Debug, Error)]
pub enum RelationalError {
    #[error(transparent)]
    Storage(crate::storage::error::StorageError),
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
    #[error("lock error: {0}")]
    Lock(#[from] LockError),
    #[error("internal error: {0}")]
    Internal(#[source] InternalError),
}

/// Result type returned by storage and relational operations.
pub type RelationalResult<T> = Result<T, RelationalError>;

/// Constraint violation caused by data or schema changes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConstraintError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error("duplicate table name: {name}")]
    DuplicateTableName { name: String },
    #[error("duplicate index name: {name}")]
    DuplicateIndexName { name: String },
    #[error("column {column} does not accept NULL values")]
    NullValue { column: String },
    #[error("column {column} expects {expected:?}, got {actual}")]
    ColumnTypeMismatch {
        column: String,
        expected: crate::relational::DataType,
        actual: &'static str,
    },
}

/// Invalid argument supplied to a storage or relational operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidArgumentError {
    #[error("schema changes require a DDL statement")]
    DdlRequired,
    #[error("invalid page id: {page_id}")]
    InvalidPageId { page_id: PageId },
    #[error("key not found")]
    KeyNotFound,
    #[error("table not found: {name}")]
    TableNotFound { name: String },
    #[error("index not found: {name}")]
    IndexNotFound { name: String },
    #[error("column {column} not found in table {table}")]
    ColumnNotFound { table: String, column: String },
    #[error("index column list cannot be empty")]
    EmptyIndexColumns,
    #[error("table {table} row has {values} values for {columns} columns")]
    TableRowValueCount { table: String, columns: usize, values: usize },
    #[error("invalid primary key for table {table}: {reason}")]
    InvalidPrimaryKey { table: String, reason: String },
    #[error("cannot update primary key column {column} on table {table}")]
    PrimaryKeyUpdate { table: String, column: String },
}

/// Allocation failure encountered while constructing or decoding a tuple.
#[derive(Debug, Error)]
pub enum TupleAllocationError {
    #[error("failed to allocate {value_count} tuple value slots: {source}")]
    Values {
        value_count: usize,
        #[source]
        source: TryReserveError,
    },
}

/// Compatibility names for callers of the original facade.
pub type StorageError = RelationalError;
pub type StorageResult<T> = RelationalResult<T>;

impl RelationalError {
    /// Whether the database must be taken out of service after this failure.
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::Storage(error) => error.is_fatal(),
            Self::Io(_) | Self::Corruption(_) | Self::Internal(_) => true,
            _ => false,
        }
    }
}

impl From<crate::storage::error::StorageError> for RelationalError {
    fn from(error: crate::storage::error::StorageError) -> Self {
        use crate::storage::error::{InvalidArgumentError as A, StorageError as E};
        match error {
            E::Io(error) => Self::Io(error),
            E::Constraint(_) => Self::Constraint(ConstraintError::DuplicateKey),
            E::InvalidArgument(A::KeyNotFound) => {
                Self::InvalidArgument(InvalidArgumentError::KeyNotFound)
            }
            E::LimitExceeded(error) => Self::LimitExceeded(error),
            E::Internal(error) => Self::Internal(error),
            error => Self::Storage(error),
        }
    }
}
