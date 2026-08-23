//! Errors exposed by the database facade.

use std::collections::TryReserveError;
use thiserror::Error;

use crate::core::PageId;

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
    #[error("duplicate table name: {name}")]
    DuplicateTableName { name: String },
    #[error("duplicate index name: {name}")]
    DuplicateIndexName { name: String },
    #[error("column {column} does not accept NULL values")]
    NullValue { column: String },
    #[error("column {column} expects {expected:?}, got {actual}")]
    ColumnTypeMismatch { column: String, expected: crate::core::DataType, actual: &'static str },
}

/// Invalid argument supplied to a storage or relational operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidArgumentError {
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

/// Allocation failure encountered while constructing or decoding a tuple.
#[derive(Debug, Error)]
pub enum TupleAllocationError {
    #[error("failed to allocate {value_count} tuple value slots: {source}")]
    Values {
        value_count: usize,
        #[source]
        source: TryReserveError,
    },
    #[error("failed to allocate {byte_count} tuple string bytes: {source}")]
    StringBytes {
        byte_count: usize,
        #[source]
        source: TryReserveError,
    },
}
