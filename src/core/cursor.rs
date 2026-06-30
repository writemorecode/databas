//! Typed table and index cursor wrappers over the raw byte-oriented B+-tree.

use std::fmt::{self, Display};

use crate::core::{
    TableKey, Tuple,
    btree::{CursorState, OwnedRecord, Record, TreeCursor},
    error::StorageResult,
    page::{CellCorruption, PageError},
};

const TABLE_KEY_SIZE: usize = size_of::<TableKey>();

fn encode_table_key(table_key: TableKey) -> [u8; TABLE_KEY_SIZE] {
    (table_key ^ TableKey::MIN).to_be_bytes()
}

fn decode_table_key(key: &[u8]) -> StorageResult<TableKey> {
    let bytes: [u8; TABLE_KEY_SIZE] = key.try_into().map_err(|_| PageError::CorruptCell {
        slot_index: 0,
        kind: CellCorruption::InvalidTableKeyLength { actual: key.len() },
    })?;
    Ok(TableKey::from_be_bytes(bytes) ^ TableKey::MIN)
}

fn encode_index_table_key(table_key: TableKey) -> [u8; TABLE_KEY_SIZE] {
    encode_table_key(table_key)
}

fn decode_index_table_key(value: &[u8]) -> StorageResult<TableKey> {
    let bytes: [u8; TABLE_KEY_SIZE] = value.try_into().map_err(|_| PageError::CorruptCell {
        slot_index: 0,
        kind: CellCorruption::InvalidIndexTableKeyValueLength { actual: value.len() },
    })?;
    Ok(TableKey::from_be_bytes(bytes) ^ TableKey::MIN)
}

/// Stable, owned table record snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedTableRecord {
    /// Primary-key table key that identifies this table record.
    pub table_key: TableKey,
    /// Encoded table record bytes.
    pub record: Box<[u8]>,
}

impl Display for OwnedTableRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match Tuple::from_bytes(&self.record) {
            Ok(tuple) => {
                for value in &tuple {
                    write!(f, "{value}\t")?;
                }
                Ok(())
            }
            Err(_) => write!(f, "<invalid tuple>"),
        }
    }
}

/// Borrowed table record view valid only for the callback that receives it.
#[derive(Debug, Clone, Copy)]
pub struct TableRecordView<'a> {
    table_key: TableKey,
    record: &'a [u8],
}

impl<'a> TableRecordView<'a> {
    fn new(table_key: TableKey, record: &'a [u8]) -> Self {
        Self { table_key, record }
    }

    /// Returns the primary-key table key that identifies this table record.
    pub fn table_key(&self) -> TableKey {
        self.table_key
    }

    /// Returns the encoded table record bytes.
    pub fn record(&self) -> &'a [u8] {
        self.record
    }
}

/// Stable, owned secondary-index entry snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedIndexEntry {
    /// Encoded secondary-index key bytes.
    pub key: Box<[u8]>,
    /// Table primary-key value referenced by this secondary-index key.
    pub table_key: TableKey,
}

/// Borrowed secondary-index entry view valid only for the callback that receives it.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntryView<'a> {
    key: &'a [u8],
    table_key: TableKey,
}

impl<'a> IndexEntryView<'a> {
    fn new(key: &'a [u8], table_key: TableKey) -> Self {
        Self { key, table_key }
    }

    /// Returns the encoded secondary-index key bytes.
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    /// Returns the table primary-key value referenced by this secondary-index key.
    pub fn table_key(&self) -> TableKey {
        self.table_key
    }
}

/// Table record returned by [`TableCursor`] lookups and cursor iteration.
///
/// Inline records keep the backing page pinned internally and expose bytes only
/// through callbacks. Overflow records may still be materialized by the raw tree.
pub struct TableRecord {
    table_key: TableKey,
    raw: Record,
}

/// Secondary-index entry returned by [`IndexCursor`] lookups and cursor iteration.
///
/// Inline entries keep the backing page pinned internally and expose key bytes
/// only through callbacks. Overflow keys may still be materialized by the raw tree.
pub struct IndexEntry {
    table_key: TableKey,
    raw: Record,
}

/// Typed cursor for a table B+-tree keyed by table key.
#[derive(Clone)]
pub struct TableCursor {
    inner: TreeCursor,
}

/// Typed cursor for a secondary-index B+-tree keyed by encoded index bytes.
#[derive(Clone)]
pub struct IndexCursor {
    inner: TreeCursor,
}

impl TableCursor {
    /// Wraps a raw tree cursor as a table cursor.
    pub(crate) fn new(inner: TreeCursor) -> Self {
        Self { inner }
    }

    /// Returns the root page id that anchors this table tree.
    pub(crate) fn root_page_id(&self) -> crate::core::PageId {
        self.inner.root_page_id()
    }

    /// Returns the cursor's current logical state.
    pub(crate) fn state(&self) -> CursorState {
        self.inner.state()
    }

    /// Resets the cursor back to the table root page.
    pub fn seek_to_root(&mut self) {
        self.inner.seek_to_root();
    }

    /// Positions the cursor on the first table record.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        self.inner.seek_to_first()
    }

    /// Positions the cursor on the first table record whose key is greater than
    /// or equal to `table_key`.
    pub fn seek_to_table_key(&mut self, table_key: TableKey) -> StorageResult<bool> {
        self.inner.seek_to_key(&encode_table_key(table_key))
    }

    /// Reads the currently selected table record, if any.
    pub fn current_record(&self) -> StorageResult<Option<TableRecord>> {
        self.inner.current()?.map(|record| self.table_record_from_raw(record)).transpose()
    }

    /// Reads the currently selected table record as a stable owned snapshot, if any.
    pub fn current_owned_record(&self) -> StorageResult<Option<OwnedTableRecord>> {
        self.inner
            .current_owned()?
            .map(|record| self.owned_table_record_from_raw(record))
            .transpose()
    }

    /// Advances to the next table record in table key order.
    pub fn next_record(&mut self) -> StorageResult<Option<TableRecord>> {
        self.inner.next_record()?.map(|record| self.table_record_from_raw(record)).transpose()
    }

    /// Advances to the next table record and returns a stable owned snapshot.
    pub fn next_owned_record(&mut self) -> StorageResult<Option<OwnedTableRecord>> {
        self.inner
            .next_owned_record()?
            .map(|record| self.owned_table_record_from_raw(record))
            .transpose()
    }

    /// Moves to the previous table record in table key order.
    pub fn prev_record(&mut self) -> StorageResult<Option<TableRecord>> {
        self.inner.prev_record()?.map(|record| self.table_record_from_raw(record)).transpose()
    }

    /// Moves to the previous table record and returns a stable owned snapshot.
    pub fn prev_owned_record(&mut self) -> StorageResult<Option<OwnedTableRecord>> {
        self.inner
            .prev_owned_record()?
            .map(|record| self.owned_table_record_from_raw(record))
            .transpose()
    }

    /// Inserts a table record keyed by `table_key`.
    pub fn insert(&mut self, table_key: TableKey, record: &[u8]) -> StorageResult<()> {
        self.inner.insert(&encode_table_key(table_key), record)
    }

    /// Looks up a table record by table key.
    pub fn get(&mut self, table_key: TableKey) -> StorageResult<Option<OwnedTableRecord>> {
        self.inner
            .get_owned(&encode_table_key(table_key))?
            .map(|record| self.owned_table_record_from_raw(record))
            .transpose()
    }

    /// Looks up a table record by table key and returns a stable owned snapshot.
    pub fn get_owned_record(
        &mut self,
        table_key: TableKey,
    ) -> StorageResult<Option<OwnedTableRecord>> {
        self.get(table_key)
    }

    /// Looks up a table record by table key without eagerly copying page-resident bytes.
    pub fn get_record(&mut self, table_key: TableKey) -> StorageResult<Option<TableRecord>> {
        self.inner
            .get(&encode_table_key(table_key))?
            .map(|record| self.table_record_from_raw(record))
            .transpose()
    }

    /// Replaces the encoded record bytes stored for an existing `table_key`.
    pub fn update(&mut self, table_key: TableKey, record: &[u8]) -> StorageResult<()> {
        self.inner.update(&encode_table_key(table_key), record)
    }

    /// Deletes the table record identified by `table_key`.
    pub fn delete(&mut self, table_key: TableKey) -> StorageResult<()> {
        self.inner.delete(&encode_table_key(table_key))
    }

    fn owned_table_record_from_raw(&self, raw: OwnedRecord) -> StorageResult<OwnedTableRecord> {
        raw.with_key_value(|key, value| {
            Ok(OwnedTableRecord { table_key: decode_table_key(key)?, record: value.into() })
        })
    }

    fn table_record_from_raw(&self, raw: Record) -> StorageResult<TableRecord> {
        let table_key = raw.with_key(decode_table_key)??;
        Ok(TableRecord { table_key, raw })
    }
}

impl IndexCursor {
    /// Wraps a raw tree cursor as an index cursor.
    pub(crate) fn new(inner: TreeCursor) -> Self {
        Self { inner }
    }

    /// Returns the root page id that anchors this index tree.
    pub(crate) fn root_page_id(&self) -> crate::core::PageId {
        self.inner.root_page_id()
    }

    /// Returns the cursor's current logical state.
    pub(crate) fn state(&self) -> CursorState {
        self.inner.state()
    }

    /// Resets the cursor back to the index root page.
    pub fn seek_to_root(&mut self) {
        self.inner.seek_to_root();
    }

    /// Positions the cursor on the first index entry.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        self.inner.seek_to_first()
    }

    /// Positions the cursor on the first index entry whose key is greater than
    /// or equal to `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> StorageResult<bool> {
        self.inner.seek_to_key(key)
    }

    /// Reads the currently selected index entry, if any.
    pub fn current_entry(&self) -> StorageResult<Option<IndexEntry>> {
        self.inner.current()?.map(IndexEntry::try_from).transpose()
    }

    /// Reads the currently selected index entry as a stable owned snapshot, if any.
    pub fn current_owned_entry(&self) -> StorageResult<Option<OwnedIndexEntry>> {
        self.inner.current_owned()?.map(OwnedIndexEntry::try_from).transpose()
    }

    /// Advances to the next index entry in key order.
    pub fn next_entry(&mut self) -> StorageResult<Option<IndexEntry>> {
        self.inner.next_record()?.map(IndexEntry::try_from).transpose()
    }

    /// Advances to the next index entry and returns a stable owned snapshot.
    pub fn next_owned_entry(&mut self) -> StorageResult<Option<OwnedIndexEntry>> {
        self.inner.next_owned_record()?.map(OwnedIndexEntry::try_from).transpose()
    }

    /// Moves to the previous index entry in key order.
    pub fn prev_entry(&mut self) -> StorageResult<Option<IndexEntry>> {
        self.inner.prev_record()?.map(IndexEntry::try_from).transpose()
    }

    /// Moves to the previous index entry and returns a stable owned snapshot.
    pub fn prev_owned_entry(&mut self) -> StorageResult<Option<OwnedIndexEntry>> {
        self.inner.prev_owned_record()?.map(OwnedIndexEntry::try_from).transpose()
    }

    /// Inserts an index entry from `key` to `table_key`.
    pub fn insert(&mut self, key: &[u8], table_key: TableKey) -> StorageResult<()> {
        self.inner.insert(key, &encode_index_table_key(table_key))
    }

    /// Looks up an index entry by key.
    pub fn get(&mut self, key: &[u8]) -> StorageResult<Option<OwnedIndexEntry>> {
        self.inner.get_owned(key)?.map(OwnedIndexEntry::try_from).transpose()
    }

    /// Looks up an index entry by key and returns a stable owned snapshot.
    pub fn get_owned_entry(&mut self, key: &[u8]) -> StorageResult<Option<OwnedIndexEntry>> {
        self.get(key)
    }

    /// Looks up an index entry by key without eagerly copying page-resident bytes.
    pub fn get_entry(&mut self, key: &[u8]) -> StorageResult<Option<IndexEntry>> {
        self.inner.get(key)?.map(IndexEntry::try_from).transpose()
    }

    /// Replaces the table key stored for an existing index `key`.
    pub fn update(&mut self, key: &[u8], table_key: TableKey) -> StorageResult<()> {
        self.inner.update(key, &encode_index_table_key(table_key))
    }

    /// Deletes the index entry identified by `key`.
    pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.inner.delete(key)
    }
}

impl TableRecord {
    /// Returns the primary-key table key that identifies this table record.
    pub fn table_key(&self) -> TableKey {
        self.table_key
    }

    /// Executes `f` with a borrowed table record view.
    pub fn with_view<R>(&self, f: impl FnOnce(TableRecordView<'_>) -> R) -> StorageResult<R> {
        self.raw.with_value(|record| f(TableRecordView::new(self.table_key, record)))
    }

    /// Executes `f` with a borrowed view of the encoded table record bytes.
    pub fn with_record<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.raw.with_value(f)
    }

    /// Returns a stable, owned snapshot of this table record.
    pub fn to_owned_record(&self) -> StorageResult<OwnedTableRecord> {
        self.raw.with_value(|record| OwnedTableRecord {
            table_key: self.table_key,
            record: record.into(),
        })
    }
}

impl IndexEntry {
    /// Returns the table primary-key value referenced by this secondary-index key.
    pub fn table_key(&self) -> TableKey {
        self.table_key
    }

    /// Executes `f` with a borrowed secondary-index entry view.
    pub fn with_view<R>(&self, f: impl FnOnce(IndexEntryView<'_>) -> R) -> StorageResult<R> {
        self.raw.with_key(|key| f(IndexEntryView::new(key, self.table_key)))
    }

    /// Executes `f` with a borrowed view of the encoded secondary-index key bytes.
    pub fn with_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        self.raw.with_key(f)
    }

    /// Returns a stable, owned snapshot of this index entry.
    pub fn to_owned_entry(&self) -> StorageResult<OwnedIndexEntry> {
        self.raw.to_owned_record()?.try_into()
    }
}

impl fmt::Debug for TableCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableCursor")
            .field("root_page_id", &self.root_page_id())
            .field("state", &self.state())
            .finish()
    }
}

impl fmt::Debug for IndexCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexCursor")
            .field("root_page_id", &self.root_page_id())
            .field("state", &self.state())
            .finish()
    }
}

impl fmt::Debug for TableRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRecord")
            .field("table_key", &self.table_key)
            .field("raw", &self.raw)
            .finish()
    }
}

impl fmt::Debug for IndexEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexEntry")
            .field("table_key", &self.table_key)
            .field("raw", &self.raw)
            .finish()
    }
}

impl TryFrom<Record> for IndexEntry {
    type Error = crate::core::error::StorageError;

    fn try_from(raw: Record) -> Result<Self, Self::Error> {
        let table_key = raw.with_value(decode_index_table_key)??;
        Ok(Self { table_key, raw })
    }
}

impl TryFrom<OwnedRecord> for OwnedIndexEntry {
    type Error = crate::core::error::StorageError;

    fn try_from(raw: OwnedRecord) -> Result<Self, Self::Error> {
        raw.with_key_value(|key, value| {
            Ok(Self { key: key.into(), table_key: decode_index_table_key(value)? })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        PAGE_SIZE,
        btree::initialize_empty_root,
        disk_manager::DiskManager,
        error::{ConstraintError, InvalidArgumentError, StorageError},
        page_cache::PageCache,
        storage_runtime::StorageRuntime,
    };

    fn temp_page_cache(cache_frames: usize) -> PageCache {
        let file = NamedTempFile::new().unwrap();
        let disk_manager = DiskManager::new(file.path()).unwrap();
        let runtime =
            Rc::new(StorageRuntime::new(file.path().to_path_buf(), disk_manager).unwrap());
        PageCache::new(runtime, cache_frames).unwrap()
    }

    fn temp_table_cursor(cache_frames: usize) -> TableCursor {
        let page_cache = temp_page_cache(cache_frames);
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        TableCursor::new(TreeCursor::new(page_cache, root_page_id))
    }

    fn temp_index_cursor(cache_frames: usize) -> IndexCursor {
        let page_cache = temp_page_cache(cache_frames);
        let root_page_id = initialize_empty_root(&page_cache).unwrap();
        IndexCursor::new(TreeCursor::new(page_cache, root_page_id))
    }

    #[test]
    fn table_cursor_insert_get_update_delete_round_trips_owned_records() {
        let mut cursor = temp_table_cursor(8);

        cursor.insert(42, b"old record").unwrap();
        assert_eq!(
            cursor.get(42).unwrap(),
            Some(OwnedTableRecord { table_key: 42, record: Box::from(&b"old record"[..]) })
        );

        cursor.update(42, b"new record").unwrap();
        assert_eq!(
            cursor.get(42).unwrap(),
            Some(OwnedTableRecord { table_key: 42, record: Box::from(&b"new record"[..]) })
        );

        cursor.delete(42).unwrap();
        assert_eq!(cursor.get(42).unwrap(), None);
    }

    #[test]
    fn table_cursor_get_record_returns_non_owned_record_view() {
        let mut cursor = temp_table_cursor(8);

        cursor.insert(42, b"record bytes").unwrap();
        let record = cursor.get_record(42).unwrap().expect("record should exist");

        assert_eq!(record.table_key(), 42);
        assert_eq!(record.with_record(|bytes| bytes.to_vec()).unwrap(), b"record bytes");
        assert_eq!(
            record.to_owned_record().unwrap(),
            OwnedTableRecord { table_key: 42, record: Box::from(&b"record bytes"[..]) }
        );
        assert!(cursor.get_record(404).unwrap().is_none());
    }

    #[test]
    fn table_cursor_iterates_without_exposing_raw_tree_cursor() {
        let mut cursor = temp_table_cursor(8);
        cursor.insert(20, b"twenty").unwrap();
        cursor.insert(10, b"ten").unwrap();
        cursor.insert(30, b"thirty").unwrap();

        assert!(cursor.seek_to_first().unwrap());
        let first = cursor.current_record().unwrap().expect("first record should exist");
        assert_eq!(first.table_key(), 10);
        assert_eq!(first.with_record(|bytes| bytes.to_vec()).unwrap(), b"ten");

        assert_eq!(
            cursor.next_owned_record().unwrap(),
            Some(OwnedTableRecord { table_key: 20, record: Box::from(&b"twenty"[..]) })
        );
        assert_eq!(
            cursor.next_owned_record().unwrap(),
            Some(OwnedTableRecord { table_key: 30, record: Box::from(&b"thirty"[..]) })
        );
        assert!(cursor.next_owned_record().unwrap().is_none());

        assert!(cursor.seek_to_table_key(20).unwrap());
        assert_eq!(
            cursor.current_owned_record().unwrap(),
            Some(OwnedTableRecord { table_key: 20, record: Box::from(&b"twenty"[..]) })
        );
        assert_eq!(
            cursor.prev_owned_record().unwrap(),
            Some(OwnedTableRecord { table_key: 10, record: Box::from(&b"ten"[..]) })
        );
    }

    #[test]
    fn index_cursor_insert_get_update_delete_round_trips_owned_entries() {
        let mut cursor = temp_index_cursor(8);

        cursor.insert(b"email:ada@example.test", 7).unwrap();
        assert_eq!(
            cursor.get(b"email:ada@example.test").unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"email:ada@example.test"[..]), table_key: 7 })
        );

        cursor.update(b"email:ada@example.test", 9).unwrap();
        assert_eq!(
            cursor.get(b"email:ada@example.test").unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"email:ada@example.test"[..]), table_key: 9 })
        );

        cursor.delete(b"email:ada@example.test").unwrap();
        assert_eq!(cursor.get(b"email:ada@example.test").unwrap(), None);
    }

    #[test]
    fn index_cursor_get_entry_returns_non_owned_entry_view() {
        let mut cursor = temp_index_cursor(8);

        cursor.insert(b"email:ada@example.test", 7).unwrap();
        let entry =
            cursor.get_entry(b"email:ada@example.test").unwrap().expect("entry should exist");

        assert_eq!(entry.table_key(), 7);
        assert_eq!(entry.with_key(|key| key.to_vec()).unwrap(), b"email:ada@example.test");
        assert_eq!(
            entry.to_owned_entry().unwrap(),
            OwnedIndexEntry { key: Box::from(&b"email:ada@example.test"[..]), table_key: 7 }
        );
        assert!(cursor.get_entry(b"missing").unwrap().is_none());
    }

    #[test]
    fn index_cursor_iterates_without_exposing_raw_tree_cursor() {
        let mut cursor = temp_index_cursor(8);
        cursor.insert(b"bravo", 20).unwrap();
        cursor.insert(b"alpha", 10).unwrap();
        cursor.insert(b"charlie", 30).unwrap();

        assert!(cursor.seek_to_first().unwrap());
        let first = cursor.current_entry().unwrap().expect("first entry should exist");
        assert_eq!(first.table_key(), 10);
        assert_eq!(first.with_key(|key| key.to_vec()).unwrap(), b"alpha");
        assert_eq!(
            first.with_view(|entry| (entry.key().to_vec(), entry.table_key())).unwrap(),
            (b"alpha".to_vec(), 10)
        );

        assert_eq!(
            cursor.next_owned_entry().unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"bravo"[..]), table_key: 20 })
        );
        assert_eq!(
            cursor.next_owned_entry().unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"charlie"[..]), table_key: 30 })
        );
        assert!(cursor.next_owned_entry().unwrap().is_none());

        assert!(cursor.seek_to_key(b"b").unwrap());
        assert_eq!(
            cursor.current_owned_entry().unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"bravo"[..]), table_key: 20 })
        );
        assert_eq!(
            cursor.prev_owned_entry().unwrap(),
            Some(OwnedIndexEntry { key: Box::from(&b"alpha"[..]), table_key: 10 })
        );
    }

    #[test]
    fn typed_cursors_preserve_duplicate_key_errors() {
        let mut table = temp_table_cursor(8);
        table.insert(1, b"one").unwrap();
        assert!(matches!(
            table.insert(1, b"again"),
            Err(StorageError::Constraint(ConstraintError::DuplicateKey))
        ));

        let mut index = temp_index_cursor(8);
        index.insert(b"key", 1).unwrap();
        assert!(matches!(
            index.insert(b"key", 2),
            Err(StorageError::Constraint(ConstraintError::DuplicateKey))
        ));
    }

    #[test]
    fn typed_cursors_preserve_missing_key_errors() {
        let mut table = temp_table_cursor(8);
        assert!(matches!(
            table.update(404, b"missing"),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));
        assert!(matches!(
            table.delete(404),
            Err(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
        ));

        let mut index = temp_index_cursor(8);
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
        let mut table = temp_table_cursor(16);
        let large_record = vec![0xaa; PAGE_SIZE * 2];
        table.insert(500, &large_record).unwrap();
        assert_eq!(
            table.get(500).unwrap(),
            Some(OwnedTableRecord { table_key: 500, record: large_record.into_boxed_slice() })
        );

        let mut index = temp_index_cursor(16);
        let large_key = vec![0xbb; PAGE_SIZE * 2];
        index.insert(&large_key, 900).unwrap();
        assert_eq!(
            index.get(&large_key).unwrap(),
            Some(OwnedIndexEntry { key: large_key.into_boxed_slice(), table_key: 900 })
        );
    }

    #[test]
    fn typed_borrowed_get_preserves_overflow_support() {
        let mut table = temp_table_cursor(16);
        let large_record = vec![0xaa; PAGE_SIZE * 2];
        table.insert(500, &large_record).unwrap();
        let record = table.get_record(500).unwrap().expect("large table record should exist");
        assert_eq!(record.table_key(), 500);
        assert_eq!(record.with_record(|bytes| bytes.to_vec()).unwrap(), large_record);

        let mut index = temp_index_cursor(16);
        let large_key = vec![0xbb; PAGE_SIZE * 2];
        index.insert(&large_key, 900).unwrap();
        let entry = index.get_entry(&large_key).unwrap().expect("large index entry should exist");
        assert_eq!(entry.table_key(), 900);
        assert_eq!(entry.with_key(|key| key.to_vec()).unwrap(), large_key);
    }
}
