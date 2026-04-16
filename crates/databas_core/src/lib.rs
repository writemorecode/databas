#![allow(dead_code)]

pub mod btree;
pub(crate) mod disk_manager;
pub mod error;
pub(crate) mod page;
pub(crate) mod page_cache;
pub(crate) mod page_replacement;
pub mod pager;

pub use btree::{
    CursorState, Index, IndexCursor, IndexEntry, Table, TableCursor, TableRecord, TreeCursor,
    TreeKind,
};
pub use pager::{Pager, PagerOptions};

pub(crate) const PAGE_SIZE: usize = 4096;

pub type PageId = u64;
pub type RowId = u64;
pub(crate) type SlotId = u16;
