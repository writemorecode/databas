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

pub use crate::catalog::{
    ColumnSchema, DataType, IndexColumnSchema, IndexSchema, TableSchema, TupleSchema,
};
pub use crate::cursor::{
    IndexEntry, IndexEntryView, OwnedIndexEntry, OwnedTableRecord, TableRecord, TableRecordView,
};
pub use crate::storage::{CheckpointOutcome, PageId, TxnId};
pub use crate::tuple::{Tuple, TupleRef, TupleView, Value, ValueRef};

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

mod active_transaction;
pub use active_transaction::ActiveTransaction;

use databas_storage as storage;
#[cfg(all(test, loom))]
#[allow(clippy::unwrap_used)]
mod loom_support;
mod sync;

/// Helpers for in-memory test implementations of the relational access traits.
/// Not part of the production database API.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod testing {
    pub fn transaction_is_active(
        database: &crate::Database,
        id: crate::TxnId,
    ) -> crate::RelationalResult<bool> {
        database.transaction_is_active(id)
    }

    pub use crate::catalog_manager::validate_user_table_schema;
    pub use crate::cursor::encode_index_entry_key;
    pub use crate::index_manager::index_key_from_record;
    pub use crate::record_manager::{table_key_from_values, validate_table_row};
    use crate::{IndexKeyRange, IndexValueRange, RelationalResult};
    pub fn encode_range(range: &IndexValueRange) -> RelationalResult<IndexKeyRange> {
        range.encode()
    }
    pub fn contains(range: &IndexKeyRange, key: &[u8]) -> bool {
        range.contains(key)
    }
}
