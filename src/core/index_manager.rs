use crate::core::{
    IndexSchema, OwnedTableRecord, TableKey, TableRecord, TableSchema, Tuple, TupleView, Value,
    catalog_manager::CatalogManager,
    cursor::encode_index_entry_key,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
};

/// Internal manager for secondary-index data maintenance.
#[derive(Clone)]
pub(crate) struct IndexManager {
    catalog: CatalogManager,
}

impl IndexManager {
    pub(crate) fn new(catalog: CatalogManager) -> Self {
        Self { catalog }
    }

    pub(crate) fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        let table = self.catalog.table_schema_by_name(table_name)?;
        let index = self.catalog.create_index(name, table_name, columns)?;
        self.backfill_index(&table, &index)?;
        Ok(index)
    }

    pub(crate) fn insert_index_entries(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        for index in self.catalog.index_schemas_for_table(table)? {
            let key = index_key_from_record(table, &index, record)?;
            let key = encode_index_entry_key(&key, record.table_key);
            let mut index_cursor = self.catalog.index_cursor_by_name(&index.name)?;
            index_cursor.insert(&key, record.table_key)?;
        }

        Ok(())
    }

    pub(crate) fn delete_index_entries(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        for index in self.catalog.index_schemas_for_table(table)? {
            let key = index_key_from_record(table, &index, record)?;
            let key = encode_index_entry_key(&key, record.table_key);
            let mut index_cursor = self.catalog.index_cursor_by_name(&index.name)?;
            index_cursor.delete(&key)?;
        }

        Ok(())
    }

    fn backfill_index(&self, table: &TableSchema, index: &IndexSchema) -> StorageResult<()> {
        let mut table_cursor = self.catalog.table_cursor_by_name(&table.name)?;
        let mut index_cursor = self.catalog.index_cursor_by_name(&index.name)?;

        while let Some(record) = table_cursor.next_record()? {
            let key = index_key_from_table_record(table, index, &record)?;
            let table_key = record.table_key();
            let key = encode_index_entry_key(&key, table_key);
            index_cursor.insert(&key, table_key)?;
        }

        Ok(())
    }
}

fn index_key_from_record(
    table: &TableSchema,
    index: &IndexSchema,
    record: &OwnedTableRecord,
) -> StorageResult<Vec<u8>> {
    index_key_from_record_bytes(table, index, record.table_key, &record.record)
}

fn index_key_from_table_record(
    table: &TableSchema,
    index: &IndexSchema,
    record: &TableRecord,
) -> StorageResult<Vec<u8>> {
    record
        .with_record(|bytes| index_key_from_record_bytes(table, index, record.table_key(), bytes))?
}

fn index_key_from_record_bytes(
    table: &TableSchema,
    index: &IndexSchema,
    table_key: TableKey,
    record: &[u8],
) -> StorageResult<Vec<u8>> {
    let tuple = TupleView::parse(record).map_err(|error| {
        invalid_table_record(table, table_key, format!("invalid tuple bytes: {error}"))
    })?;
    let mut values = Vec::with_capacity(index.columns.len());

    for column in &index.columns {
        let ordinal = column.source_column_ordinal as usize;
        let value = tuple.values().nth(ordinal).ok_or_else(|| {
            invalid_table_record(
                table,
                table_key,
                format!(
                    "index {} references source column ordinal {ordinal}, but row has {} values",
                    index.name,
                    tuple.len()
                ),
            )
        })?;
        values.push(Value::from(value));
    }

    Tuple::new(values).to_bytes().map_err(StorageError::from)
}

fn invalid_table_record(table: &TableSchema, table_key: TableKey, reason: String) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: None,
        kind: CorruptionKind::InvalidTableRecord { table: table.name.clone(), table_key, reason },
    })
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        ColumnSchema, DataType, TupleSchema, Value, catalog_manager::CatalogManager,
        record_manager::RecordManager,
    };
    use crate::storage::pager::Pager;

    fn open(path: impl AsRef<std::path::Path>) -> StorageResult<(CatalogManager, IndexManager)> {
        let catalog = CatalogManager::from_pager(Pager::open_or_create(path)?)?;
        let indexes = IndexManager::new(catalog.clone());
        Ok((catalog, indexes))
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

    fn name_key(name: &str) -> Vec<u8> {
        Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap()
    }

    fn name_entry_key(name: &str, table_key: TableKey) -> Vec<u8> {
        encode_index_entry_key(&name_key(name), table_key)
    }

    #[test]
    fn create_index_backfills_existing_table_rows() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, indexes) = open(file.path()).unwrap();
        let records = RecordManager::new(catalog.clone(), indexes.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
        records
            .insert_table_row(
                &table,
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
            )
            .unwrap();
        records
            .insert_table_row(
                &table,
                vec![Value::Integer(2), Value::String("Grace".to_owned()), Value::Boolean(false)],
            )
            .unwrap();

        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let mut index = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert_eq!(index.get(&name_entry_key("Ada", 1)).unwrap().unwrap().table_key, 1);
        assert_eq!(index.get(&name_entry_key("Grace", 2)).unwrap().unwrap().table_key, 2);
    }

    #[test]
    fn create_index_backfills_duplicate_index_values() {
        let file = NamedTempFile::new().unwrap();
        let (catalog, indexes) = open(file.path()).unwrap();
        let records = RecordManager::new(catalog.clone(), indexes.clone());
        let table = catalog.create_table("users", users_schema()).unwrap();
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

        indexes.create_index("idx_users_name", "users", &["name"]).unwrap();

        let mut index = catalog.index_cursor_by_name("idx_users_name").unwrap();
        assert_eq!(index.get(&name_entry_key("Engineering", 1)).unwrap().unwrap().table_key, 1);
        assert_eq!(index.get(&name_entry_key("Engineering", 2)).unwrap().unwrap().table_key, 2);
    }
}
