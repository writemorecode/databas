//! Durable byte-oriented B+-trees with WAL recovery and physical transactions.
//!
//! Use [`Storage`] to open files and access trees. Physical transactions provide
//! rollback and durability; callers must serialize conflicting tree access for
//! isolation. Raw cursors without a transaction are for exclusive initialization
//! and require an explicit flush. Page, cache, WAL, and runtime implementation
//! details are private.

mod btree;
mod database_header;
mod disk_manager;
mod engine;
pub mod error;
mod log_manager;
mod overflow;
mod page;
mod page_cache;
mod page_replacement;
mod recovery;
mod storage_runtime;
mod transaction_manager;

mod types;
pub use types::{CheckpointOutcome, Lsn, PageId, SlotId, TxnId};
pub(crate) use types::{PAGE_SIZE, PAGE_SIZE_U16};

#[cfg(all(test, loom))]
#[allow(clippy::unwrap_used)]
mod loom_support;
mod sync;
pub use btree::{CursorState, OwnedRecord, Record, RecordView, TreeCursor};
pub use engine::{Storage, StorageOptions};
pub use transaction_manager::TransactionSavepoint;
