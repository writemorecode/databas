//! SQL-independent tables, indexes, catalog, and transactional access.

pub mod access;
pub(crate) mod catalog;
pub(crate) mod catalog_manager;
pub(crate) mod cursor;
pub(crate) mod database;
pub mod error;
pub(crate) mod index_manager;
pub mod lock_manager;
pub(crate) mod record_manager;
pub mod transaction;
pub(crate) mod tuple;
mod types;

pub use crate::relational::catalog::ColumnSchema;
pub use crate::relational::catalog::DataType;
pub use crate::relational::catalog::IndexColumnSchema;
pub use crate::relational::catalog::IndexSchema;
pub use crate::relational::catalog::TableSchema;
pub use crate::relational::catalog::TupleSchema;
pub use crate::relational::cursor::IndexEntry;
pub use crate::relational::cursor::IndexEntryView;
pub use crate::relational::cursor::OwnedIndexEntry;
pub use crate::relational::cursor::OwnedTableRecord;
pub use crate::relational::cursor::TableRecord;
pub use crate::relational::cursor::TableRecordView;
pub use crate::relational::tuple::Tuple;
pub use crate::relational::tuple::TupleRef;
pub use crate::relational::tuple::TupleView;
pub use crate::relational::tuple::Value;
pub use crate::relational::tuple::ValueRef;
pub use crate::storage::{CheckpointOutcome, PageId, TxnId};

pub use database::{Database, StatementTransactionMode};
pub use error::{
    ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
    InvalidArgumentError, LimitExceededError, RelationalError, RelationalResult, StorageError,
    StorageResult,
};
pub use lock_manager::{LockError, LockMode, TableId};
pub use types::{CatalogId, IndexKeyBound, IndexKeyRange, TableKey, TableKeyBound, TableKeyRange};

mod index_range;
pub use index_range::{IndexValueBound, IndexValueRange};
