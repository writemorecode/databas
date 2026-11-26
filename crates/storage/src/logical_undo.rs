//! The inverse of a completed row edit. The original physical records remain
//! in WAL for redo, and for undo if the edit never reached its completion record.

use crate::{Lsn, PageId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalUndo {
    pub root: PageId,
    pub incarnation: Lsn,
    pub key: Box<[u8]>,
    pub action: Inverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inverse {
    Delete,
    Insert(Box<[u8]>),
    Replace(Box<[u8]>),
}
