use std::fmt;
use std::ops::Range;

use super::payload::{cell_corruption, materialize_leaf_cell};
use super::*;

pub(in crate::core::btree) enum RecordStorage {
    PageResident { pin: PinGuard, key_range: Range<usize>, value_range: Range<usize> },
    Materialized { key: Box<[u8]>, value: Box<[u8]> },
}

/// Raw record returned by tree reads and cursor iteration.
///
/// Records for cells that fit entirely in their leaf page borrow the page
/// through an internal pin and only expose byte slices during accessor
/// callbacks. Records for cells with overflow payload are materialized into
/// fixed-size heap allocations. Use [`OwnedRecord`] when a stable snapshot is
/// needed across later tree mutations.
pub struct Record {
    page_id: PageId,
    slot_index: u16,
    pub(in crate::core::btree) storage: RecordStorage,
}

/// Stable, owned raw record snapshot.
pub struct OwnedRecord {
    key: Box<[u8]>,
    value: Box<[u8]>,
}

/// Borrowed record view valid only for the callback that receives it.
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> RecordView<'a> {
    fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key, value }
    }

    /// Returns the record key bytes.
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    /// Returns the record value bytes.
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Returns the record key and value bytes.
    pub fn key_value(&self) -> (&'a [u8], &'a [u8]) {
        (self.key, self.value)
    }
}

impl Record {
    /// Builds a record view from one raw leaf-page slot.
    pub(crate) fn new(
        page_cache: &PageCache,
        page_id: PageId,
        slot_index: u16,
    ) -> StorageResult<Self> {
        let pin = page_cache.fetch_page(page_id)?;
        let (key_len, value_len, first_overflow_page_id, inline_range) = {
            let page = pin.read()?;
            let leaf = page.open::<Leaf>()?;
            leaf.cell_payload_parts(slot_index)?
        };

        let storage = match first_overflow_page_id {
            None => {
                if inline_range.len() != key_len + value_len {
                    return Err(cell_corruption(page_id, CorruptionKind::CellLengthOutOfBounds));
                }

                let key_start = inline_range.start;
                let value_start = key_start + key_len;
                RecordStorage::PageResident {
                    pin,
                    key_range: key_start..value_start,
                    value_range: value_start..inline_range.end,
                }
            }
            Some(first_overflow_page_id) => {
                let (key, value) = {
                    let page = pin.read()?;
                    materialize_leaf_cell(
                        page_cache,
                        page_id,
                        &page.page()[inline_range],
                        first_overflow_page_id,
                        key_len,
                        value_len,
                    )?
                };
                drop(pin);

                RecordStorage::Materialized { key, value }
            }
        };

        Ok(Self { page_id, slot_index, storage })
    }

    /// Returns the slot index that this record refers to within its leaf page.
    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }

    /// Executes `f` with a borrowed view of this record.
    pub fn with_view<R>(&self, f: impl FnOnce(RecordView<'_>) -> R) -> StorageResult<R> {
        match &self.storage {
            RecordStorage::PageResident { pin, key_range, value_range } => {
                let page = pin.read()?;
                let key = &page.page()[key_range.clone()];
                let value = &page.page()[value_range.clone()];
                Ok(f(RecordView::new(key, value)))
            }
            RecordStorage::Materialized { key, value } => {
                Ok(f(RecordView::new(key.as_ref(), value.as_ref())))
            }
        }
    }

    /// Returns a stable, owned snapshot of this record.
    pub fn to_owned_record(&self) -> StorageResult<OwnedRecord> {
        self.with_key_value(|key, value| OwnedRecord::new(key.into(), value.into()))
    }

    /// Executes `f` with a borrowed view of the record key.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| f(record.key()))
    }

    /// Executes `f` with a borrowed view of the record value.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| f(record.value()))
    }

    /// Executes `f` with borrowed views of the key and value.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> StorageResult<R> {
        self.with_view(|record| {
            let (key, value) = record.key_value();
            f(key, value)
        })
    }
}

impl OwnedRecord {
    fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        Self { key, value }
    }

    /// Executes `f` with a borrowed view of the record key.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.key)
    }

    /// Executes `f` with a borrowed view of the record value.
    pub fn with_value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.value)
    }

    /// Executes `f` with borrowed views of the key and value.
    pub fn with_key_value<R>(&self, f: impl FnOnce(&[u8], &[u8]) -> R) -> R {
        f(&self.key, &self.value)
    }
}

impl fmt::Debug for OwnedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedRecord")
            .field("key_len", &self.key.len())
            .field("value_len", &self.value.len())
            .finish()
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("page_id", &self.page_id)
            .field("slot_index", &self.slot_index)
            .finish_non_exhaustive()
    }
}
