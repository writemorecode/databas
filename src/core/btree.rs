//! Foundational byte-oriented B+-tree cursor.
//!
//! This module is intentionally below table and index interpretation. It works
//! only with raw byte keys and raw byte values stored in `RawLeaf` pages
//! and separator byte keys stored in `RawInterior` pages.

use std::{cell::Cell, cmp::Ordering, rc::Rc};

use crate::core::{
    PAGE_SIZE, PageId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
    overflow,
    page::{
        self, BoundResult, Interior, Leaf, PageError, RawInterior, RawLeaf, Read, SearchResult,
        Write,
        format::{
            INTERIOR_CELL_PREFIX_SIZE, KIND_OFFSET, LEAF_CELL_PREFIX_SIZE,
            MAX_INLINE_OVERFLOW_PAYLOAD_BYTES, NO_OVERFLOW_PAGE_ID, OVERFLOW_NEXT_PAGE_ID_SIZE,
            PageKind,
        },
    },
    page_cache::{PageCache, PageWriteGuard, PinGuard},
};

mod mutation;
mod payload;
mod rebalance;
mod record;
mod root;
mod search;
mod split;

#[cfg(test)]
mod tests;

pub use record::{OwnedRecord, Record, RecordView};
pub(crate) use root::{initialize_empty_root, validate_root_page};

#[cfg(test)]
use record::RecordStorage;
#[cfg(test)]
use root::read_page_kind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// The cursor is anchored to a page but not yet to a specific slot.
    Page {
        /// The page currently referenced by the cursor.
        page_id: PageId,
    },
    /// The cursor currently references a slot inside a leaf page.
    Positioned {
        /// The leaf page currently holding the cursor record.
        page_id: PageId,
        /// The slot index within the leaf page.
        slot_index: u16,
    },
    /// The cursor ran past the end of the tree.
    Exhausted,
}

/// Public handle to a single raw B+-tree rooted at `root_page_id`.
#[derive(Clone)]
pub struct TreeCursor {
    page_cache: PageCache,
    root_page_id: Rc<Cell<PageId>>,
    state: CursorState,
}

/// Identifies which child pointer of an interior page led to a descended path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildSlotRef {
    /// The child pointer stored in one interior cell.
    Slot(u16),
    /// The dedicated rightmost child pointer of the page.
    Rightmost,
}

/// One step of the path from the root to a target leaf page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathFrame {
    /// Interior page traversed on the way down.
    page_id: PageId,
    /// Child reference followed from that interior page.
    child_ref: ChildSlotRef,
}

/// Split result that still needs to be inserted into an ancestor page.
#[derive(Debug, Clone)]
struct PendingSplit {
    /// Separator key promoted out of the split page.
    separator: Vec<u8>,
    /// Left child page after the split.
    left_page_id: PageId,
    /// Right child page after the split.
    right_page_id: PageId,
}

/// Temporary description of one leaf cell while rebuilding split pages.
#[derive(Debug, Clone)]
struct LeafSplitCell {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Child pointer plus the maximum key reachable through that child.
#[derive(Debug, Clone)]
struct ChildEntry {
    page_id: PageId,
    max_key: Option<Vec<u8>>,
}

impl LeafSplitCell {
    /// Returns the key length this cell will occupy after the split.
    fn key_len(&self) -> usize {
        self.key.len()
    }

    /// Returns the value length this cell will occupy after the split.
    fn value_len(&self) -> usize {
        self.value.len()
    }

    /// Returns the total encoded size of the cell including fixed fields.
    fn encoded_size(&self) -> usize {
        LEAF_CELL_PREFIX_SIZE + local_payload_len(self.key_len() + self.value_len())
    }

    /// Returns the key bytes from either the page snapshot or owned storage.
    fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value bytes from either the page snapshot or owned storage.
    fn value(&self) -> &[u8] {
        &self.value
    }
}

fn payload_uses_overflow(payload_len: usize) -> bool {
    payload_len > MAX_INLINE_OVERFLOW_PAYLOAD_BYTES
}

fn local_payload_len(payload_len: usize) -> usize {
    if payload_uses_overflow(payload_len) { MAX_INLINE_OVERFLOW_PAYLOAD_BYTES } else { payload_len }
}
