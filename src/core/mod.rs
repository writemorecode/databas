//! Compatibility facade. Implementations live in the relational and storage layers.
pub use crate::relational::{
    CatalogId, CheckpointOutcome, ColumnSchema, ConstraintError, CorruptionComponent,
    CorruptionError, CorruptionKind, DataType, Database, IndexColumnSchema, IndexEntry,
    IndexKeyBound, IndexKeyRange, IndexSchema, InternalError, InvalidArgumentError,
    LimitExceededError, LockError, LockMode, OwnedIndexEntry, OwnedTableRecord, PageId,
    StorageError, StorageResult, TableId, TableKey, TableKeyBound, TableKeyRange, TableRecord,
    TableRecordView, TableSchema, Tuple, TupleRef, TupleSchema, TupleView, TxnId, Value, ValueRef,
    error, lock_manager,
};
