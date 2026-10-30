//! Durable byte-oriented B+-trees with WAL recovery and physical transactions.
//!
//! Use [`Storage`] to open files and access trees. Physical transactions provide
//! rollback and durability; callers must serialize conflicting tree access for
//! isolation. Raw cursors without a transaction are for exclusive initialization
//! and require an explicit flush. Page, cache, WAL, and runtime implementation
//! details are private.

mod bitset;
mod btree;
mod database_header;
mod disk_manager;
mod engine;
pub mod error;
pub(crate) mod log_manager;
mod overflow;
mod page;
mod page_allocator;
mod page_cache;
mod page_replacement;
mod recovery;
mod storage_runtime;
mod transaction_manager;

mod types;
pub(crate) use types::SlotId;
pub use types::{CheckpointOutcome, Lsn, PageId, TxnId};
pub(crate) use types::{PAGE_SIZE, PAGE_SIZE_U16};

pub use btree::{CursorState, Record, TreeCursor};
pub use engine::{Storage, StorageOptions};

pub use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
pub use transaction_manager::TransactionSavepoint;
