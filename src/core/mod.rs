pub mod btree;
pub mod catalog;
mod catalog_manager;
pub mod cursor;
pub mod database;
pub(crate) mod database_header;
pub mod disk_manager;
pub mod error;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub(crate) mod pager;
pub(crate) mod recovery;
pub(crate) mod storage_runtime;
pub(crate) mod transaction_manager;
pub mod tuple;

pub(crate) mod log_manager;

pub use btree::{CursorState, OwnedRecord, Record, RecordView, TreeCursor};
pub use catalog::{
    CatalogError, CatalogObjectKind, ColumnCatalogRow, ColumnSchema, DataType, IndexCatalogRow,
    IndexColumnSchema, IndexSchema, SYS_COLUMNS_ROOT_PAGE_ID, SYS_COLUMNS_TABLE_ID,
    SYS_INDEXES_ROOT_PAGE_ID, SYS_INDEXES_TABLE_ID, SYS_TABLES_ROOT_PAGE_ID, SYS_TABLES_TABLE_ID,
    TableCatalogRow, TableSchema, TupleSchema,
};
pub use cursor::{
    IndexCursor, IndexEntry, IndexEntryRef, TableCursor, TableRecord, TableRecordRef,
};
pub use database::Database;
pub use pager::PagerOptions;
pub use tuple::{EncodedTupleView, Tuple, TupleRef, TupleView, Value, ValueRef};

pub(crate) const PAGE_SIZE: usize = 4096;

pub type PageId = u64;
pub type RowId = u64;
pub(crate) type SlotId = u16;
