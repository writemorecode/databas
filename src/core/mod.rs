pub(crate) mod btree;
pub(crate) mod catalog;
mod catalog_manager;
pub(crate) mod cursor;
pub(crate) mod database;
pub(crate) mod database_header;
pub(crate) mod disk_manager;
pub mod error;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub(crate) mod pager;
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
    IndexCursor, IndexEntry, IndexEntryView, OwnedIndexEntry, OwnedTableRecord, TableCursor,
    TableRecord, TableRecordView,
};
pub use database::Database;
pub use error::{
    ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind, InternalError,
    InvalidArgumentError, LimitExceededError, StorageError, StorageResult,
};
pub use tuple::{EncodedTupleView, Tuple, TupleRef, TupleView, Value, ValueRef};

pub(crate) const PAGE_SIZE: usize = 4096;

pub type PageId = u64;
pub type RowId = u64;
pub(crate) type SlotId = u16;
