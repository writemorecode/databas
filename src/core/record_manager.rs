use crate::core::{
    CorruptionComponent, CorruptionError, CorruptionKind, DataType, IndexEntry, IndexKeyRange,
    IndexSchema, OwnedTableRecord, TableKey, TableKeyBound, TableKeyRange, TableRecord,
    TableSchema, Tuple, Value,
    catalog_manager::CatalogManager,
    cursor::{IndexCursor, TableCursor},
    error::{ConstraintError, InvalidArgumentError, StorageError, StorageResult},
    index_manager::IndexManager,
};

/// Internal manager for table record access and mutation.
#[derive(Clone)]
pub(crate) struct RecordManager {
    catalog: CatalogManager,
    indexes: IndexManager,
}

/// Iterator over records in one table.
pub(crate) struct TableScan {
    cursor: TableCursor,
    range: TableKeyRange,
    initialized: bool,
    done: bool,
}

/// Iterator over table rows referenced by matching secondary-index entries.
pub(crate) struct IndexScan {
    table: TableSchema,
    table_cursor: TableCursor,
    index_cursor: IndexCursor,
    key_range: IndexKeyRange,
    initialized: bool,
    done: bool,
}

impl RecordManager {
    pub(crate) fn new(catalog: CatalogManager, indexes: IndexManager) -> Self {
        Self { catalog, indexes }
    }

    pub(crate) fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        self.scan_table_range(table, TableKeyRange::unbounded())
    }

    pub(crate) fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        Ok(TableScan {
            cursor: self.catalog.table_cursor_by_name(&table.name)?,
            range,
            initialized: false,
            done: false,
        })
    }

    pub(crate) fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan> {
        Ok(IndexScan {
            table: table.clone(),
            table_cursor: self.catalog.table_cursor_by_name(&table.name)?,
            index_cursor: self.catalog.index_cursor_by_name(&index.name)?,
            key_range,
            initialized: false,
            done: false,
        })
    }

    pub(crate) fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;

        let table_key = table_key_from_values(table, &values)?;
        let record = Tuple::new(values).to_bytes()?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.insert(table_key, &record)?;

        let record = OwnedTableRecord { table_key, record: record.into_boxed_slice() };
        self.indexes.insert_index_entries(table, &record)?;
        Ok(record)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.indexes.delete_index_entries(table, record)?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.delete(record.table_key)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;
        let updated_table_key = table_key_from_values(table, &values)?;
        if updated_table_key != record.table_key {
            return Err(StorageError::InvalidArgument(InvalidArgumentError::PrimaryKeyUpdate {
                table: table.name.clone(),
                column: table.row.columns[0].name.clone(),
            }));
        }

        let updated = Tuple::new(values).to_bytes()?;
        let updated =
            OwnedTableRecord { table_key: record.table_key, record: updated.into_boxed_slice() };

        self.indexes.delete_index_entries(table, record)?;
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        table_cursor.update(record.table_key, &updated.record)?;
        self.indexes.insert_index_entries(table, &updated)?;

        Ok(updated)
    }
}

impl Iterator for IndexScan {
    type Item = StorageResult<TableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let entry = if self.initialized {
                self.index_cursor.next_entry()
            } else {
                self.initialized = true;
                self.first_entry()
            };

            match entry {
                Ok(Some(entry)) => {
                    let table_key = entry.table_key();
                    let entry_state = entry.with_key(|key| {
                        if self.key_range.is_past_upper(key) {
                            IndexEntryState::PastUpper
                        } else if self.key_range.contains(key) {
                            IndexEntryState::InRange
                        } else {
                            IndexEntryState::Skip
                        }
                    });
                    match entry_state {
                        Ok(IndexEntryState::PastUpper) => {
                            self.done = true;
                            return None;
                        }
                        Ok(IndexEntryState::Skip) => continue,
                        Ok(IndexEntryState::InRange) => {}
                        Err(error) => {
                            self.done = true;
                            return Some(Err(error));
                        }
                    }

                    match self.table_cursor.get_record(table_key) {
                        Ok(Some(record)) => return Some(Ok(record)),
                        Ok(None) => {
                            self.done = true;
                            return Some(Err(invalid_index_entry(
                                &self.table,
                                table_key,
                                "index entry references missing table row",
                            )));
                        }
                        Err(error) => {
                            self.done = true;
                            return Some(Err(error));
                        }
                    }
                }
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl IndexScan {
    fn first_entry(&mut self) -> StorageResult<Option<IndexEntry>> {
        match self.key_range.lower.as_ref() {
            Some(lower) => {
                if self.index_cursor.seek_to_key(lower.key())? {
                    self.index_cursor.current_entry()
                } else {
                    Ok(None)
                }
            }
            None => {
                if self.index_cursor.seek_to_first()? {
                    self.index_cursor.current_entry()
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl Iterator for TableScan {
    type Item = StorageResult<TableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let record = if self.initialized {
            self.cursor.next_record()
        } else {
            self.initialized = true;
            self.first_record()
        };

        match record {
            Ok(Some(record)) if self.range.contains(record.table_key()) => Some(Ok(record)),
            Ok(Some(_)) => {
                self.done = true;
                None
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

impl TableScan {
    fn first_record(&mut self) -> StorageResult<Option<TableRecord>> {
        match range_start(self.range.lower) {
            RangeStart::At(start_key) => {
                if self.cursor.seek_to_table_key(start_key)? {
                    self.cursor.current_record()
                } else {
                    Ok(None)
                }
            }
            RangeStart::Unbounded => self.cursor.next_record(),
            RangeStart::Empty => Ok(None),
        }
    }
}

enum IndexEntryState {
    PastUpper,
    InRange,
    Skip,
}

enum RangeStart {
    At(TableKey),
    Unbounded,
    Empty,
}

fn range_start(lower: Option<TableKeyBound>) -> RangeStart {
    match lower {
        Some(TableKeyBound::Inclusive(value)) => RangeStart::At(value),
        Some(TableKeyBound::Exclusive(value)) => {
            value.checked_add(1).map_or(RangeStart::Empty, RangeStart::At)
        }
        None => RangeStart::Unbounded,
    }
}

fn validate_table_row(table: &TableSchema, values: &[Value]) -> StorageResult<()> {
    if values.len() != table.row.columns.len() {
        return Err(StorageError::InvalidArgument(InvalidArgumentError::TableRowValueCount {
            table: table.name.clone(),
            columns: table.row.columns.len(),
            values: values.len(),
        }));
    }

    for (column, value) in table.row.columns.iter().zip(values.iter()) {
        if matches!(value, Value::Null) {
            if !column.nullable || column.primary_key {
                return Err(StorageError::Constraint(ConstraintError::NullValue {
                    column: column.name.clone(),
                }));
            }
            continue;
        }

        if !value_matches_data_type(value, column.data_type) {
            return Err(StorageError::Constraint(ConstraintError::ColumnTypeMismatch {
                column: column.name.clone(),
                expected: column.data_type,
                actual: value_type_name(value),
            }));
        }
    }

    Ok(())
}

fn table_key_from_values(table: &TableSchema, values: &[Value]) -> StorageResult<TableKey> {
    match values.first() {
        Some(Value::Integer(value)) => Ok(*value),
        Some(Value::Null) => Err(StorageError::Constraint(ConstraintError::NullValue {
            column: table.row.columns[0].name.clone(),
        })),
        Some(value) => Err(StorageError::Constraint(ConstraintError::ColumnTypeMismatch {
            column: table.row.columns[0].name.clone(),
            expected: DataType::Integer,
            actual: value_type_name(value),
        })),
        None => Err(StorageError::InvalidArgument(InvalidArgumentError::TableRowValueCount {
            table: table.name.clone(),
            columns: table.row.columns.len(),
            values: 0,
        })),
    }
}

fn value_matches_data_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::String(_), DataType::Text)
            | (Value::Boolean(_), DataType::Boolean)
            | (Value::Integer(_), DataType::Integer)
            | (Value::Float(_), DataType::Float)
            | (Value::UnsignedInteger(_), DataType::UnsignedInteger)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::String(_) => "text",
        Value::Boolean(_) => "boolean",
        Value::Integer(_) => "integer",
        Value::Float(_) => "float",
        Value::UnsignedInteger(_) => "unsigned integer",
    }
}

fn invalid_index_entry(table: &TableSchema, table_key: TableKey, reason: &str) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: None,
        kind: CorruptionKind::InvalidTableRecord {
            table: table.name.clone(),
            table_key,
            reason: reason.to_owned(),
        },
    })
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        ColumnSchema, TableKeyBound, TableKeyRange, TupleSchema,
        catalog_manager::CatalogManager,
        cursor::{IndexCursor, encode_index_entry_key},
        index_manager::IndexManager,
        pager::Pager,
    };

    fn open(path: impl AsRef<std::path::Path>) -> StorageResult<(CatalogManager, RecordManager)> {
        let catalog = CatalogManager::from_pager(Pager::open_or_create(path)?)?;
        let indexes = IndexManager::new(catalog.clone());
        let records = RecordManager::new(catalog.clone(), indexes);
        Ok((catalog, records))
    }

    fn users_schema() -> TupleSchema {
        TupleSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "active".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    primary_key: false,
                },
            ],
        }
    }

    fn values(record: &OwnedTableRecord) -> Vec<Value> {
        Tuple::from_bytes(&record.record).unwrap().into_values()
    }

    fn name_key(name: &str) -> Vec<u8> {
        Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap()
    }

    fn name_entry_key(name: &str, table_key: TableKey) -> Vec<u8> {
        encode_index_entry_key(&name_key(name), table_key)
    }

    #[test]
    fn insert_table_row_persists_row_and_updates_secondary_indexes() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let inserted = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();

        let mut users = catalog.table_cursor_by_name("users").unwrap();
        let stored = users.get(1).unwrap().expect("inserted row should be stored");
        let mut index: IndexCursor = catalog.index_cursor_by_name("idx_users_name").unwrap();
        let entry =
            index.get(&name_entry_key("Ada", 1)).unwrap().expect("index should track inserted row");

        assert_eq!(inserted.table_key, 1);
        assert_eq!(
            values(&stored),
            vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)]
        );
        assert_eq!(entry.table_key, 1);
    }

    #[test]
    fn insert_table_row_uses_explicit_primary_key_for_table_and_index_pointers() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        records
            .insert_table_row(
                &table,
                vec![Value::Integer(20), Value::String("Grace".to_owned()), Value::Boolean(true)],
            )
            .unwrap();
        records
            .insert_table_row(
                &table,
                vec![Value::Integer(-5), Value::String("Ada".to_owned()), Value::Boolean(false)],
            )
            .unwrap();

        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(20).unwrap().is_some());
        assert!(users.get(-5).unwrap().is_some());
        assert!(users.seek_to_first().unwrap());
        assert_eq!(users.current_record().unwrap().unwrap().table_key(), -5);

        let mut index: IndexCursor = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert_eq!(index.get(&name_entry_key("Ada", -5)).unwrap().unwrap().table_key, -5);
        assert_eq!(index.get(&name_entry_key("Grace", 20)).unwrap().unwrap().table_key, 20);
    }

    #[test]
    fn insert_table_row_allows_duplicate_secondary_index_values() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(1),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(true),
                ],
            )
            .unwrap();
        records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(2),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(false),
                ],
            )
            .unwrap();

        let mut index = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert_eq!(index.get(&name_entry_key("Engineering", 1)).unwrap().unwrap().table_key, 1);
        assert_eq!(index.get(&name_entry_key("Engineering", 2)).unwrap().unwrap().table_key, 2);
    }

    #[test]
    fn delete_table_row_removes_only_one_duplicate_secondary_index_entry() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let first = records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(1),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(true),
                ],
            )
            .unwrap();
        records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(2),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(false),
                ],
            )
            .unwrap();

        records.delete_table_row(&table, &first).unwrap();

        let mut index = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert!(index.get(&name_entry_key("Engineering", 1)).unwrap().is_none());
        assert_eq!(index.get(&name_entry_key("Engineering", 2)).unwrap().unwrap().table_key, 2);
    }

    #[test]
    fn update_table_row_refreshes_only_one_duplicate_secondary_index_entry() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let indexes = IndexManager::new(catalog.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let first = records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(1),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(true),
                ],
            )
            .unwrap();
        records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(2),
                    Value::String("Engineering".to_owned()),
                    Value::Boolean(false),
                ],
            )
            .unwrap();

        records
            .update_table_row(
                &table,
                &first,
                vec![Value::Integer(1), Value::String("Sales".to_owned()), Value::Boolean(true)],
            )
            .unwrap();

        let mut index = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert!(index.get(&name_entry_key("Engineering", 1)).unwrap().is_none());
        assert_eq!(index.get(&name_entry_key("Engineering", 2)).unwrap().unwrap().table_key, 2);
        assert_eq!(index.get(&name_entry_key("Sales", 1)).unwrap().unwrap().table_key, 1);
    }

    #[test]
    fn scan_table_range_seeks_to_lower_bound_and_stops_at_upper_bound() {
        let file = NamedTempFile::new().unwrap();
        let (_catalog, records) = open(file.path()).unwrap();
        let table = records.catalog.create_table("users", users_schema()).unwrap();

        for id in [-5, 1, 3, 5, 7] {
            records
                .insert_table_row(
                    &table,
                    vec![
                        Value::Integer(id),
                        Value::String(format!("user{id}")),
                        Value::Boolean(true),
                    ],
                )
                .unwrap();
        }

        let range = TableKeyRange {
            lower: Some(TableKeyBound::Exclusive(1)),
            upper: Some(TableKeyBound::Exclusive(7)),
        };
        let keys = records
            .scan_table_range(&table, range)
            .unwrap()
            .map(|record| record.unwrap().table_key())
            .collect::<Vec<_>>();

        assert_eq!(keys, vec![3, 5]);
    }

    #[test]
    fn scan_table_range_with_exclusive_max_lower_is_empty() {
        let file = NamedTempFile::new().unwrap();
        let (_catalog, records) = open(file.path()).unwrap();
        let table = records.catalog.create_table("users", users_schema()).unwrap();
        records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(TableKey::MAX),
                    Value::String("max".to_owned()),
                    Value::Boolean(true),
                ],
            )
            .unwrap();

        let range =
            TableKeyRange { lower: Some(TableKeyBound::Exclusive(TableKey::MAX)), upper: None };
        let rows = records
            .scan_table_range(&table, range)
            .unwrap()
            .collect::<StorageResult<Vec<_>>>()
            .unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn insert_table_row_rejects_duplicate_primary_key() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();

        records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();
        let error = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Grace".to_owned()), Value::Boolean(false)],
            )
            .unwrap_err();

        assert!(matches!(error, StorageError::Constraint(ConstraintError::DuplicateKey)));
    }

    #[test]
    fn update_table_row_rejects_primary_key_change() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();
        let inserted = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();

        let error = records
            .update_table_row(
                &table,
                &inserted,
                vec![Value::Integer(2), Value::String("Ada".to_owned()), Value::Boolean(false)],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::InvalidArgument(InvalidArgumentError::PrimaryKeyUpdate {
                table,
                column,
            }) if table == "users" && column == "id"
        ));
        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_some());
        assert!(users.get(2).unwrap().is_none());
    }

    #[test]
    fn insert_table_row_rejects_null_before_writing_row() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();

        let error = records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Null],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::Constraint(ConstraintError::NullValue { column }) if column == "active"
        ));
        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }

    #[test]
    fn insert_table_row_rejects_wrong_type_before_writing_row() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, records) = open(file.path()).unwrap();
        let table = catalog.create_table("users", users_schema()).unwrap();

        let error = records
            .insert_table_row(
                &table,
                vec![
                    Value::Integer(1),
                    Value::String("Ada".to_owned()),
                    Value::String("yes".to_owned()),
                ],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::Constraint(ConstraintError::ColumnTypeMismatch {
                column,
                expected: DataType::Boolean,
                actual: "text",
            }) if column == "active"
        ));
        let mut users = catalog.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
    }
}
