//! Typed table and index cursor wrappers over the raw byte-oriented B+-tree.

use std::fmt;

use crate::{
    RowId,
    btree::{CursorState, OwnedRecord, TreeCursor},
    disk_manager::DiskManager,
    error::StorageResult,
    page::{CellCorruption, PageError},
    page_store::PageStore,
};

const ROW_ID_SIZE: usize = size_of::<RowId>();

fn encode_table_row_id(row_id: RowId) -> [u8; ROW_ID_SIZE] {
    row_id.to_be_bytes()
}

fn decode_table_row_id(key: &[u8]) -> StorageResult<RowId> {
    let bytes: [u8; ROW_ID_SIZE] = key.try_into().map_err(|_| PageError::CorruptCell {
        slot_index: 0,
        kind: CellCorruption::InvalidTableRowIdKeyLength { actual: key.len() },
    })?;
    Ok(RowId::from_be_bytes(bytes))
}

fn encode_index_row_id(row_id: RowId) -> [u8; ROW_ID_SIZE] {
    row_id.to_le_bytes()
}

fn decode_index_row_id(value: &[u8]) -> StorageResult<RowId> {
    let bytes: [u8; ROW_ID_SIZE] = value.try_into().map_err(|_| PageError::CorruptCell {
        slot_index: 0,
        kind: CellCorruption::InvalidIndexRowIdValueLength { actual: value.len() },
    })?;
    Ok(RowId::from_le_bytes(bytes))
}

/// Owned table record returned by [`TableCursor`] lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRecord {
    /// Row id that identifies this table record.
    pub row_id: RowId,
    /// Encoded table record bytes.
    pub record: Box<[u8]>,
}

/// Owned secondary-index entry returned by [`IndexCursor`] lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    /// Encoded secondary-index key bytes.
    pub key: Box<[u8]>,
    /// Table row id referenced by this secondary-index key.
    pub row_id: RowId,
}

/// Typed cursor for a table B+-tree keyed by row id.
#[derive(Clone)]
pub struct TableCursor<S: PageStore = DiskManager> {
    inner: TreeCursor<S>,
}

/// Typed cursor for a secondary-index B+-tree keyed by encoded index bytes.
#[derive(Clone)]
pub struct IndexCursor<S: PageStore = DiskManager> {
    inner: TreeCursor<S>,
}

impl<S: PageStore> TableCursor<S> {
    /// Wraps a raw tree cursor as a table cursor.
    pub(crate) fn new(inner: TreeCursor<S>) -> Self {
        Self { inner }
    }

    /// Consumes this table cursor and returns the raw tree cursor.
    pub fn into_inner(self) -> TreeCursor<S> {
        self.inner
    }

    /// Returns the root page id that anchors this table tree.
    pub fn root_page_id(&self) -> crate::PageId {
        self.inner.root_page_id()
    }

    /// Returns the cursor's current logical state.
    pub fn state(&self) -> CursorState {
        self.inner.state()
    }

    /// Returns the page currently referenced by the cursor, if any.
    pub fn current_page_id(&self) -> Option<crate::PageId> {
        self.inner.current_page_id()
    }

    /// Inserts a table record keyed by `row_id`.
    pub fn insert(&mut self, row_id: RowId, record: &[u8]) -> StorageResult<()> {
        self.inner.insert(&encode_table_row_id(row_id), record)
    }

    /// Looks up a table record by row id.
    pub fn get(&mut self, row_id: RowId) -> StorageResult<Option<TableRecord>> {
        self.inner.get_owned(&encode_table_row_id(row_id))?.map(table_record_from_raw).transpose()
    }

    /// Replaces the encoded record bytes stored for an existing `row_id`.
    pub fn update(&mut self, row_id: RowId, record: &[u8]) -> StorageResult<()> {
        self.inner.update(&encode_table_row_id(row_id), record)
    }

    /// Deletes the table record identified by `row_id`.
    pub fn delete(&mut self, row_id: RowId) -> StorageResult<()> {
        self.inner.delete(&encode_table_row_id(row_id))
    }
}

impl<S: PageStore> IndexCursor<S> {
    /// Wraps a raw tree cursor as an index cursor.
    pub(crate) fn new(inner: TreeCursor<S>) -> Self {
        Self { inner }
    }

    /// Consumes this index cursor and returns the raw tree cursor.
    pub fn into_inner(self) -> TreeCursor<S> {
        self.inner
    }

    /// Returns the root page id that anchors this index tree.
    pub fn root_page_id(&self) -> crate::PageId {
        self.inner.root_page_id()
    }

    /// Returns the cursor's current logical state.
    pub fn state(&self) -> CursorState {
        self.inner.state()
    }

    /// Returns the page currently referenced by the cursor, if any.
    pub fn current_page_id(&self) -> Option<crate::PageId> {
        self.inner.current_page_id()
    }

    /// Inserts an index entry from `key` to `row_id`.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> StorageResult<()> {
        self.inner.insert(key, &encode_index_row_id(row_id))
    }

    /// Looks up an index entry by key.
    pub fn get(&mut self, key: &[u8]) -> StorageResult<Option<IndexEntry>> {
        self.inner.get_owned(key)?.map(index_entry_from_raw).transpose()
    }

    /// Replaces the row id stored for an existing index `key`.
    pub fn update(&mut self, key: &[u8], row_id: RowId) -> StorageResult<()> {
        self.inner.update(key, &encode_index_row_id(row_id))
    }

    /// Deletes the index entry identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.inner.delete(key)
    }
}

impl<S: PageStore> fmt::Debug for TableCursor<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableCursor")
            .field("root_page_id", &self.root_page_id())
            .field("state", &self.state())
            .finish()
    }
}

impl<S: PageStore> fmt::Debug for IndexCursor<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexCursor")
            .field("root_page_id", &self.root_page_id())
            .field("state", &self.state())
            .finish()
    }
}

fn table_record_from_raw(raw: OwnedRecord) -> StorageResult<TableRecord> {
    raw.with_key_value(|key, value| {
        Ok(TableRecord { row_id: decode_table_row_id(key)?, record: value.into() })
    })
}

fn index_entry_from_raw(raw: OwnedRecord) -> StorageResult<IndexEntry> {
    raw.with_key_value(|key, value| {
        Ok(IndexEntry { key: key.into(), row_id: decode_index_row_id(value)? })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PAGE_SIZE,
        btree::initialize_empty_root,
        error::{ConstraintError, InvalidArgumentError, StorageError},
        memory_page_store::MemoryPageStore,
        page_cache::PageCache,
    };

    fn memory_table_cursor(cache_frames: usize) -> TableCursor<MemoryPageStore> {
        let page_cache = PageCache::new(MemoryPageStore::new(), cache_frames).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        TableCursor::new(TreeCursor::new(page_cache, root_page_id))
    }

    fn memory_index_cursor(cache_frames: usize) -> IndexCursor<MemoryPageStore> {
        let page_cache = PageCache::new(MemoryPageStore::new(), cache_frames).unwrap();
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        IndexCursor::new(TreeCursor::new(page_cache, root_page_id))
    }

    #[test]
    fn table_cursor_insert_get_update_delete_round_trips_owned_records() {
        let mut cursor = memory_table_cursor(8);

        cursor.insert(42, b"old record").unwrap();
        assert_eq!(
            cursor.get(42).unwrap(),
            Some(TableRecord { row_id: 42, record: Box::from(&b"old record"[..]) })
        );

        cursor.update(42, b"new record").unwrap();
        assert_eq!(
            cursor.get(42).unwrap(),
            Some(TableRecord { row_id: 42, record: Box::from(&b"new record"[..]) })
        );

        cursor.delete(42).unwrap();
        assert_eq!(cursor.get(42).unwrap(), None);
    }

    #[test]
    fn index_cursor_insert_get_update_delete_round_trips_owned_entries() {
        let mut cursor = memory_index_cursor(8);

        cursor.insert(b"email:ada@example.test", 7).unwrap();
        assert_eq!(
            cursor.get(b"email:ada@example.test").unwrap(),
            Some(IndexEntry { key: Box::from(&b"email:ada@example.test"[..]), row_id: 7 })
        );

        cursor.update(b"email:ada@example.test", 9).unwrap();
        assert_eq!(
            cursor.get(b"email:ada@example.test").unwrap(),
            Some(IndexEntry { key: Box::from(&b"email:ada@example.test"[..]), row_id: 9 })
        );

        cursor.delete(b"email:ada@example.test").unwrap();
        assert_eq!(cursor.get(b"email:ada@example.test").unwrap(), None);
    }

    #[test]
    fn typed_cursors_preserve_duplicate_key_errors() {
        let mut table = memory_table_cursor(8);
        table.insert(1, b"one").unwrap();
        assert!(matches!(
            table.insert(1, b"again"),
            Err(StorageError::Constraint(ConstraintError::DuplicateKey))
        ));

        let mut index = memory_index_cursor(8);
        index.insert(b"key", 1).unwrap();
        assert!(matches!(
            index.insert(b"key", 2),
            Err(StorageError::Constraint(ConstraintError::DuplicateKey))
        ));
    }

    #[test]
    fn typed_cursors_preserve_missing_key_errors() {
        let mut table = memory_table_cursor(8);
        assert!(matches!(
            table.update(404, b"missing"),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));
        assert!(matches!(
            table.delete(404),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));

        let mut index = memory_index_cursor(8);
        assert!(matches!(
            index.update(b"missing", 404),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));
        assert!(matches!(
            index.delete(b"missing"),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));
    }

    #[test]
    fn typed_cursors_preserve_overflow_support() {
        let mut table = memory_table_cursor(16);
        let large_record = vec![0xaa; PAGE_SIZE * 2];
        table.insert(500, &large_record).unwrap();
        assert_eq!(
            table.get(500).unwrap(),
            Some(TableRecord { row_id: 500, record: large_record.into_boxed_slice() })
        );

        let mut index = memory_index_cursor(16);
        let large_key = vec![0xbb; PAGE_SIZE * 2];
        index.insert(&large_key, 900).unwrap();
        assert_eq!(
            index.get(&large_key).unwrap(),
            Some(IndexEntry { key: large_key.into_boxed_slice(), row_id: 900 })
        );
    }
}
