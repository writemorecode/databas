//! Public database facade and shared relational primitives.
//!
//! [`Database`] is the facade that assembles the catalog, record, index, and
//! transaction managers. The dependency-neutral identifiers and scan-range
//! types live in the private `types` module and are re-exported here so lower
//! layers can share them without depending on the facade implementation.
//! Storage failures exposed by the facade are defined in [`error`]; conversion
//! from storage-internal failures remains in the storage layer.

pub(crate) mod access;
pub(crate) mod database;
pub mod error;
pub mod lock_manager;
pub(crate) mod transaction;
mod types;

pub use crate::relational::catalog::{
    ColumnSchema, DataType, IndexColumnSchema, IndexSchema, TableSchema, TupleSchema,
};
pub use crate::relational::cursor::{
    IndexEntry, IndexEntryView, OwnedIndexEntry, OwnedTableRecord, TableRecord, TableRecordView,
};
pub use crate::relational::tuple::{EncodedTupleView, Tuple, TupleRef, TupleView, Value, ValueRef};
pub use database::Database;
pub use error::{
    ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
    InvalidArgumentError, LimitExceededError, StorageError, StorageResult,
};
pub(crate) use transaction::Transaction;
pub use types::{
    CatalogId, IndexKeyBound, IndexKeyRange, PageId, TableKey, TableKeyBound, TableKeyRange, TxnId,
};
pub(crate) use types::{Lsn, SlotId};

pub(crate) const PAGE_SIZE_U16: u16 = 4096;
pub(crate) const PAGE_SIZE: usize = PAGE_SIZE_U16 as usize;
