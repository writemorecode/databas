//! Foundational byte-oriented B+-tree cursor.
//!
//! This module is intentionally below table and index interpretation. It works
//! only with raw byte keys and raw byte values stored in `RawLeaf` pages
//! and separator byte keys stored in `RawInterior` pages.

use std::{borrow::Cow, cmp::Ordering};

use crate::sync::{Arc, AtomicU64, Ordering as AtomicOrdering};

use crate::core::{
    PAGE_SIZE, PageId, TxnId,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
};
use crate::storage::{
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
    page_cache::{PageCache, PinGuard},
};

mod mutation;
mod payload;
mod rebalance;
mod rebalance_policy;
mod rebalance_repair;
mod record;
mod root;
mod search;
mod split;

#[cfg(all(test, not(loom)))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod test_support {
    use std::collections::BTreeMap;

    use fastrand::Rng;
    use tempfile::NamedTempFile;

    use super::{root::read_page_kind, *};
    use crate::storage::{disk_manager::DiskManager, storage_runtime::StorageRuntime};

    pub const TARGET_HEIGHT: usize = 4;
    pub const MAX_RECORDS: usize = 50_000;

    pub fn page_cache(cache_frames: usize) -> PageCache {
        let file = NamedTempFile::new().unwrap();
        let disk = DiskManager::new(file.path()).unwrap();
        let runtime = Arc::new(StorageRuntime::new(file.path().to_path_buf(), disk).unwrap());
        PageCache::new(runtime, cache_frames).unwrap()
    }

    pub fn cursor(cache_frames: usize) -> TreeCursor {
        let page_cache = page_cache(cache_frames);
        let root_page_id = initialize_empty_root(&page_cache, None).unwrap();
        TreeCursor::new(page_cache, root_page_id).unwrap()
    }

    pub fn height(cursor: &TreeCursor) -> usize {
        let mut height = 1;
        let mut page_id = cursor.root_page_id();
        loop {
            let pin = cursor.page_cache.fetch_page(page_id).unwrap();
            let page = pin.read().unwrap();
            page_id = match read_page_kind(page.page(), page_id).unwrap() {
                PageKind::RawLeaf => return height,
                PageKind::RawInterior => {
                    let interior = page.open::<Interior>().unwrap();
                    if interior.slot_count() == 0 {
                        interior.rightmost_child()
                    } else {
                        interior.cell(0).unwrap().left_child().unwrap()
                    }
                }
            };
            height += 1;
        }
    }

    pub fn oversized_key(index: u16) -> Vec<u8> {
        let mut key = vec![(index % 251) as u8; PAGE_SIZE + 256];
        key[..2].copy_from_slice(&index.to_be_bytes());
        key
    }

    pub fn record_bytes(record: &Record) -> (Vec<u8>, Vec<u8>) {
        record.with_key_value(|key, value| (key.to_vec(), value.to_vec())).unwrap()
    }

    pub fn scan(cursor: &mut TreeCursor) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut records = BTreeMap::new();
        if !cursor.seek_to_first().unwrap() {
            return records;
        }
        loop {
            let record = cursor.current().unwrap().unwrap();
            let (key, value) = record_bytes(&record);
            if let Some((previous, _)) = records.last_key_value() {
                assert!(previous < &key, "scan keys must be strictly increasing");
            }
            assert!(records.insert(key, value).is_none(), "scan returned a duplicate key");
            if cursor.next_record().unwrap().is_none() {
                return records;
            }
        }
    }

    pub fn assert_matches(cursor: &mut TreeCursor, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for (key, value) in model {
            let record = cursor.get(key).unwrap().expect("model key should be present");
            assert_eq!(record_bytes(&record), (key.clone(), value.clone()));
        }
        assert_eq!(&scan(cursor), model);
    }

    pub fn random_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        rng.fill(&mut bytes);
        bytes
    }

    pub fn random_record(rng: &mut Rng, model: &BTreeMap<Vec<u8>, Vec<u8>>) -> (Vec<u8>, Vec<u8>) {
        loop {
            let key_len = rng.usize(8..=192);
            let key = random_bytes(rng, key_len);
            if !model.contains_key(&key) {
                let value_len = if rng.u8(0..32) == 0 {
                    rng.usize(8..=PAGE_SIZE * 3)
                } else {
                    rng.usize(8..=512)
                };
                return (key, random_bytes(rng, value_len));
            }
        }
    }

    pub fn random_key<'a>(rng: &mut Rng, model: &'a BTreeMap<Vec<u8>, Vec<u8>>) -> &'a Vec<u8> {
        model.keys().nth(rng.usize(0..model.len())).unwrap()
    }
}

#[cfg(all(test, loom))]
#[allow(clippy::unwrap_used)]
mod loom_tests {
    use super::search::advance_mutation_epoch;
    use crate::{
        loom_support::{check_model, thread},
        sync::{Arc, AtomicU64, Ordering},
    };

    #[test]
    fn concurrent_mutations_each_advance_the_shared_epoch() {
        check_model(|| {
            let epoch = Arc::new(AtomicU64::new(0));

            let first_epoch = Arc::clone(&epoch);
            let first = thread::spawn(move || advance_mutation_epoch(&first_epoch));
            let second_epoch = Arc::clone(&epoch);
            let second = thread::spawn(move || advance_mutation_epoch(&second_epoch));

            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(epoch.load(Ordering::Acquire), 2);
        });
    }
}

pub use record::Record;
pub(crate) use root::{initialize_empty_root, validate_tree_page_formats};

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
    root_page_id: Arc<AtomicU64>,
    mutation_epoch: Arc<AtomicU64>,
    txn_id: Option<TxnId>,
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
struct LeafSplitCell<'a> {
    key: Cow<'a, [u8]>,
    value: Cow<'a, [u8]>,
}

/// Child pointer plus the maximum key reachable through that child.
#[derive(Debug, Clone)]
struct ChildEntry {
    page_id: PageId,
    max_key: Option<Vec<u8>>,
}

impl<'a> LeafSplitCell<'a> {
    fn borrowed(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key: Cow::Borrowed(key), value: Cow::Borrowed(value) }
    }

    fn owned(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key: Cow::Owned(key), value: Cow::Owned(value) }
    }

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
        self.key.as_ref()
    }

    /// Returns the value bytes from either the page snapshot or owned storage.
    fn value(&self) -> &[u8] {
        self.value.as_ref()
    }
}

fn payload_uses_overflow(payload_len: usize) -> bool {
    payload_len > MAX_INLINE_OVERFLOW_PAYLOAD_BYTES
}

fn local_payload_len(payload_len: usize) -> usize {
    if payload_uses_overflow(payload_len) { MAX_INLINE_OVERFLOW_PAYLOAD_BYTES } else { payload_len }
}
