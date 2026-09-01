//! Durable storage engine for database pages and transactional recovery.
//!
//! The storage layer is organized around four cooperating areas:
//!
//! - [`page`] defines the on-disk page format and typed read/write access.
//! - [`page_cache`] and [`storage`] coordinate cached pages with database-file I/O.
//! - [`btree`] implements byte-oriented B+-tree traversal and mutation.
//! - [`log_manager`], [`transaction_manager`], and [`recovery`] enforce
//!   write-ahead logging and restore committed state after a crash.
//!
//! [`storage_runtime`] owns the concrete disk, log, and transaction managers.
//! `Storage` adds page-cache-aware rollback orchestration for the
//! higher relational and session layers. These runtimes are intentionally
//! crate-private: callers use the database facade rather than assembling
//! storage components directly.

pub(crate) mod btree;
pub(crate) mod database_header;
pub(crate) mod disk_manager;
pub(crate) mod engine;
mod error;
pub(crate) mod log_manager;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub(crate) mod recovery;
pub(crate) mod storage_runtime;
pub(crate) mod transaction_manager;
