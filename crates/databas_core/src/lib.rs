#![allow(dead_code)]

pub mod btree;
pub(crate) mod disk_manager;
pub mod error;
pub(crate) mod memory_page_store;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub(crate) mod page_store;
pub mod pager;

pub use btree::{CursorState, OwnedRecord, Record, RecordView, TreeCursor};
pub use pager::{Pager, PagerOptions};

pub(crate) const PAGE_SIZE: usize = 4096;

pub type PageId = u64;
pub type RowId = u64;
pub(crate) type SlotId = u16;
