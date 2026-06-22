use std::path::Path;

use crate::core::{
    CatalogError, IndexSchema, PageId, RowId, TableCatalogRow, TableRecord, TableSchema, Tuple,
    TupleSchema,
    btree::OwnedRecord,
    catalog::{
        CatalogObjectKind, ColumnCatalogRow, ColumnSchema, IndexCatalogRow, IndexColumnSchema,
        SYS_COLUMNS_ROOT_PAGE_ID, SYS_INDEXES_ROOT_PAGE_ID, SYS_TABLES_ROOT_PAGE_ID,
        system_column_rows, system_table_schemas,
    },
    cursor::{IndexCursor, TableCursor},
    error::{
        ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind,
        InvalidArgumentError, LimitExceededError, StorageError, StorageResult,
    },
    pager::Pager,
};

/// Internal catalog manager for one database file.
///
/// `CatalogManager` owns the low-level pager and manages catalog metadata for
/// table and index B+-trees.
#[derive(Clone)]
pub struct CatalogManager {
    pager: Pager,
}

impl CatalogManager {
    pub(crate) fn from_pager(pager: Pager) -> StorageResult<Self> {
        let manager = Self { pager };
        manager.initialize_or_validate_system_catalog()?;
        manager.validate_page_formats()?;
        Ok(manager)
    }

    /// Creates a catalog manager with default pager options.
    pub(crate) fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::create(path)?;
        Self::from_pager(pager)
    }

    /// Opens a catalog manager, creating an empty database file if needed.
    pub(crate) fn open_or_create(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::open_or_create(path)?;
        Self::from_pager(pager)
    }

    /// Opens a catalog manager with default pager options.
    pub(crate) fn open_existing(path: impl AsRef<Path>) -> StorageResult<Self> {
        let pager = Pager::open(path)?;
        Self::from_pager(pager)
    }

    /// Returns the database-file path associated with this manager.
    pub fn path(&self) -> &Path {
        self.pager.path()
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.pager.flush()
    }

    /// Creates a cataloged table, allocates its root page, and records its columns.
    pub fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        if self.table_catalog_rows()?.iter().any(|row| row.name == name) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateTableName {
                name: name.to_owned(),
            }));
        }

        let table_id = self.next_object_id()?;
        let root_page_id = self.pager.create_table_tree()?.root_page_id();
        let schema =
            TableSchema { table_id, name: name.to_owned(), root_page_id, last_row_id: 0, row };

        self.insert_table_catalog_row(&schema.catalog_row())?;
        for (column_id, (ordinal, column)) in
            (self.next_column_id()?..).zip(schema.row.columns.iter().enumerate())
        {
            let row = ColumnCatalogRow {
                column_id,
                object_kind: CatalogObjectKind::Table,
                object_id: table_id,
                ordinal: ordinal as u64,
                name: column.name.clone(),
                data_type: column.data_type,
                nullable: column.nullable,
                primary_key: column.primary_key,
                source_table_id: None,
                source_column_ordinal: None,
            };
            self.insert_column_catalog_row(&row)?;
        }

        Ok(schema)
    }

    /// Creates a cataloged secondary index over columns from an existing table.
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        if columns.is_empty() {
            return Err(StorageError::InvalidArgument(InvalidArgumentError::EmptyIndexColumns));
        }
        if self.index_catalog_rows()?.iter().any(|row| row.name == name) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateIndexName {
                name: name.to_owned(),
            }));
        }

        let table = self.table_schema_by_name(table_name)?;
        let index_id = self.next_object_id()?;
        let mut index_columns = Vec::new();
        let mut catalog_columns = Vec::new();

        for (column_id, (ordinal, column_name)) in
            (self.next_column_id()?..).zip(columns.iter().enumerate())
        {
            let (source_column_ordinal, source_column) = table
                .row
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == *column_name)
                .ok_or_else(|| {
                    StorageError::InvalidArgument(InvalidArgumentError::ColumnNotFound {
                        table: table.name.clone(),
                        column: (*column_name).to_owned(),
                    })
                })?;
            index_columns.push(IndexColumnSchema {
                source_column_ordinal: source_column_ordinal as u64,
                column: source_column.clone(),
            });
            catalog_columns.push(ColumnCatalogRow {
                column_id,
                object_kind: CatalogObjectKind::Index,
                object_id: index_id,
                ordinal: ordinal as u64,
                name: source_column.name.clone(),
                data_type: source_column.data_type,
                nullable: source_column.nullable,
                primary_key: source_column.primary_key,
                source_table_id: Some(table.table_id),
                source_column_ordinal: Some(source_column_ordinal as u64),
            });
        }

        let root_page_id = self.pager.create_index_tree()?.root_page_id();
        let schema = IndexSchema {
            index_id,
            name: name.to_owned(),
            table_id: table.table_id,
            root_page_id,
            unique: false,
            columns: index_columns,
        };
        self.insert_index_catalog_row(&schema.catalog_row())?;
        for row in catalog_columns {
            self.insert_column_catalog_row(&row)?;
        }
        Ok(schema)
    }

    /// Returns a typed cursor for the cataloged table named `name`.
    pub fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        let schema = self.table_schema_by_name(name)?;
        self.pager.table_cursor(schema.root_page_id)
    }

    /// Returns a typed cursor for the cataloged index named `name`.
    pub fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        let schema = self.index_schema_by_name(name)?;
        self.pager.index_cursor(schema.root_page_id)
    }

    /// Allocates and persists the next row id for a table.
    pub fn allocate_table_row_id(&self, table: &TableSchema) -> StorageResult<RowId> {
        let mut row = self
            .table_catalog_rows()?
            .into_iter()
            .find(|row| row.table_id == table.table_id)
            .ok_or_else(|| {
                StorageError::InvalidArgument(InvalidArgumentError::TableNotFound {
                    name: table.name.clone(),
                })
            })?;
        let next_row_id = row.last_row_id.checked_add(1).ok_or_else(|| {
            StorageError::LimitExceeded(LimitExceededError::RowIdExhausted {
                table: table.name.clone(),
            })
        })?;
        row.last_row_id = next_row_id;
        self.update_table_catalog_row(&row)?;
        Ok(next_row_id)
    }

    fn initialize_or_validate_system_catalog(&self) -> StorageResult<()> {
        match self.pager.opened_page_count() {
            0 => Err(crate::core::database_header::missing_header()),
            1 => self.initialize_system_catalog(),
            2..=3 => Err(missing_system_catalog_root(self.pager.opened_page_count())),
            _ => Ok(()),
        }
    }

    fn initialize_system_root(&self, expected_page_id: PageId) -> StorageResult<()> {
        let actual_page_id = self.pager.create_table_tree()?.root_page_id();
        if actual_page_id == expected_page_id {
            Ok(())
        } else {
            Err(unexpected_system_catalog_root(expected_page_id, actual_page_id))
        }
    }

    fn initialize_system_catalog(&self) -> StorageResult<()> {
        self.initialize_system_root(SYS_TABLES_ROOT_PAGE_ID)?;
        self.initialize_system_root(SYS_INDEXES_ROOT_PAGE_ID)?;
        self.initialize_system_root(SYS_COLUMNS_ROOT_PAGE_ID)?;
        self.seed_system_catalog()
    }

    fn seed_system_catalog(&self) -> StorageResult<()> {
        let mut tables = self.pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID)?;
        let mut columns = self.pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID)?;

        for schema in system_table_schemas() {
            let bytes = schema.catalog_row().to_bytes()?;
            tables.insert(schema.table_id, &bytes)?;
        }

        for row in system_column_rows() {
            let bytes = row.to_bytes()?;
            columns.insert(row.column_id, &bytes)?;
        }

        Ok(())
    }

    fn validate_page_formats(&self) -> StorageResult<()> {
        let system_roots =
            [SYS_TABLES_ROOT_PAGE_ID, SYS_INDEXES_ROOT_PAGE_ID, SYS_COLUMNS_ROOT_PAGE_ID];
        for root_page_id in system_roots {
            self.pager.validate_tree_page_formats(root_page_id)?;
        }

        let mut roots = system_roots.to_vec();
        roots.extend(self.table_catalog_rows()?.into_iter().map(|row| row.root_page_id));
        roots.extend(self.index_catalog_rows()?.into_iter().map(|row| row.root_page_id));
        roots.sort_unstable();
        roots.dedup();

        for root_page_id in roots {
            if system_roots.contains(&root_page_id) {
                continue;
            }
            self.pager.validate_tree_page_formats(root_page_id)?;
        }
        Ok(())
    }

    pub(crate) fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        let table =
            self.table_catalog_rows()?.into_iter().find(|row| row.name == name).ok_or_else(
                || {
                    StorageError::InvalidArgument(InvalidArgumentError::TableNotFound {
                        name: name.to_owned(),
                    })
                },
            )?;
        let mut columns: Vec<_> = self
            .column_catalog_rows()?
            .into_iter()
            .filter(|row| {
                row.object_kind == CatalogObjectKind::Table && row.object_id == table.table_id
            })
            .collect();
        columns.sort_by_key(|row| row.ordinal);

        Ok(TableSchema {
            table_id: table.table_id,
            name: table.name,
            root_page_id: table.root_page_id,
            last_row_id: table.last_row_id,
            row: TupleSchema { columns: columns.into_iter().map(column_schema_from_row).collect() },
        })
    }

    fn index_schema_by_name(&self, name: &str) -> StorageResult<IndexSchema> {
        let index =
            self.index_catalog_rows()?.into_iter().find(|row| row.name == name).ok_or_else(
                || {
                    StorageError::InvalidArgument(InvalidArgumentError::IndexNotFound {
                        name: name.to_owned(),
                    })
                },
            )?;
        let columns: Vec<_> = self
            .column_catalog_rows()?
            .into_iter()
            .filter(|row| {
                row.object_kind == CatalogObjectKind::Index && row.object_id == index.index_id
            })
            .collect();

        Ok(index_schema_from_rows(index, columns))
    }

    pub(crate) fn index_schemas_for_table(
        &self,
        table: &TableSchema,
    ) -> StorageResult<Vec<IndexSchema>> {
        let mut indexes: Vec<_> = self
            .index_catalog_rows()?
            .into_iter()
            .filter(|row| row.table_id == table.table_id)
            .collect();
        indexes.sort_by_key(|row| row.index_id);

        let column_rows = self.column_catalog_rows()?;
        indexes
            .into_iter()
            .map(|index| {
                let columns: Vec<_> = column_rows
                    .iter()
                    .filter(|row| {
                        row.object_kind == CatalogObjectKind::Index
                            && row.object_id == index.index_id
                    })
                    .cloned()
                    .collect();
                Ok(index_schema_from_rows(index, columns))
            })
            .collect()
    }

    fn next_object_id(&self) -> StorageResult<RowId> {
        let max_table_id =
            self.table_catalog_rows()?.into_iter().map(|row| row.table_id).max().unwrap_or(0);
        let max_index_id =
            self.index_catalog_rows()?.into_iter().map(|row| row.index_id).max().unwrap_or(0);
        Ok(max_table_id.max(max_index_id) + 1)
    }

    fn next_column_id(&self) -> StorageResult<RowId> {
        Ok(self.column_catalog_rows()?.into_iter().map(|row| row.column_id).max().unwrap_or(0) + 1)
    }

    fn table_catalog_rows(&self) -> StorageResult<Vec<TableCatalogRow>> {
        self.scan_catalog_table(SYS_TABLES_ROOT_PAGE_ID, "sys_tables", TableCatalogRow::decode)
    }

    fn index_catalog_rows(&self) -> StorageResult<Vec<IndexCatalogRow>> {
        self.scan_catalog_table(SYS_INDEXES_ROOT_PAGE_ID, "sys_indexes", IndexCatalogRow::decode)
    }

    fn column_catalog_rows(&self) -> StorageResult<Vec<ColumnCatalogRow>> {
        self.scan_catalog_table(SYS_COLUMNS_ROOT_PAGE_ID, "sys_columns", ColumnCatalogRow::decode)
    }

    fn scan_catalog_table<T>(
        &self,
        root_page_id: PageId,
        catalog_table_name: &'static str,
        decode: impl Fn(&Tuple) -> Result<T, CatalogError>,
    ) -> StorageResult<Vec<T>> {
        let mut cursor = self.pager.table_cursor(root_page_id)?.into_inner();
        let mut rows = Vec::new();
        if !cursor.seek_to_first()? {
            return Ok(rows);
        }

        let mut record = cursor
            .current_owned()?
            .expect("positioned catalog cursor should have a current record");
        loop {
            rows.push(decode_catalog_record(catalog_table_name, record, &decode)?);
            match cursor.next_owned_record()? {
                Some(next_record) => record = next_record,
                None => break,
            }
        }
        Ok(rows)
    }

    fn insert_table_catalog_row(&self, row: &TableCatalogRow) -> StorageResult<()> {
        self.insert_catalog_row(SYS_TABLES_ROOT_PAGE_ID, row.table_id, &row.encode())
    }

    fn update_table_catalog_row(&self, row: &TableCatalogRow) -> StorageResult<()> {
        let mut cursor = self.pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID)?;
        let bytes = row.encode().to_bytes()?;
        cursor.update(row.table_id, &bytes)
    }

    fn insert_index_catalog_row(&self, row: &IndexCatalogRow) -> StorageResult<()> {
        self.insert_catalog_row(SYS_INDEXES_ROOT_PAGE_ID, row.index_id, &row.encode())
    }

    fn insert_column_catalog_row(&self, row: &ColumnCatalogRow) -> StorageResult<()> {
        self.insert_catalog_row(SYS_COLUMNS_ROOT_PAGE_ID, row.column_id, &row.encode())
    }

    fn insert_catalog_row(
        &self,
        root_page_id: PageId,
        row_id: RowId,
        tuple: &Tuple,
    ) -> StorageResult<()> {
        let mut cursor = self.pager.table_cursor(root_page_id)?;
        let bytes = tuple.to_bytes()?;
        cursor.insert(row_id, &bytes)
    }
}

fn missing_system_catalog_root(page_id: PageId) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: Some(page_id),
        kind: CorruptionKind::MissingSystemCatalogRoot { page_id },
    })
}

fn unexpected_system_catalog_root(expected: PageId, actual: PageId) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: Some(actual),
        kind: CorruptionKind::UnexpectedSystemCatalogRoot { expected, actual },
    })
}

fn decode_catalog_record<T>(
    catalog_table_name: &'static str,
    record: OwnedRecord,
    decode: &impl Fn(&Tuple) -> Result<T, CatalogError>,
) -> StorageResult<T> {
    let record = TableRecord::try_from(record)?;
    let tuple = Tuple::from_bytes(&record.record)
        .map_err(|error| invalid_catalog_row(catalog_table_name, error))?;
    decode(&tuple).map_err(|error| invalid_catalog_row(catalog_table_name, error))
}

fn invalid_catalog_row(table: &'static str, error: impl std::error::Error) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: None,
        kind: CorruptionKind::InvalidCatalogRow { table, reason: error.to_string() },
    })
}

fn column_schema_from_row(row: ColumnCatalogRow) -> ColumnSchema {
    ColumnSchema {
        name: row.name,
        data_type: row.data_type,
        nullable: row.nullable,
        primary_key: row.primary_key,
    }
}

fn index_schema_from_rows(
    index: IndexCatalogRow,
    mut columns: Vec<ColumnCatalogRow>,
) -> IndexSchema {
    columns.sort_by_key(|row| row.ordinal);

    IndexSchema {
        index_id: index.index_id,
        name: index.name,
        table_id: index.table_id,
        root_page_id: index.root_page_id,
        unique: index.unique,
        columns: columns
            .into_iter()
            .map(|row| IndexColumnSchema {
                source_column_ordinal: row.source_column_ordinal.unwrap_or(row.ordinal),
                column: column_schema_from_row(row),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        ColumnCatalogRow, ColumnSchema, DataType, IndexCatalogRow, RowId, Tuple, TupleSchema,
        catalog::{
            SYS_COLUMNS_TABLE_ID, SYS_INDEXES_TABLE_ID, SYS_TABLES_TABLE_ID, TableCatalogRow,
            system_column_rows,
        },
        database_header::DatabaseHeader,
        disk_manager::DiskManager,
    };

    fn open(path: impl AsRef<Path>) -> StorageResult<CatalogManager> {
        CatalogManager::open_or_create(path)
    }

    #[test]
    fn open_new_database_bootstraps_system_catalog_roots_and_rows() {
        let file = NamedTempFile::new().unwrap();
        let manager = open(file.path()).unwrap();

        assert_eq!(manager.pager.create_table_tree().unwrap().root_page_id(), 4);

        let mut tables = manager.pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(
            &mut tables,
            SYS_TABLES_TABLE_ID,
            "sys_tables",
            SYS_TABLES_ROOT_PAGE_ID,
            3,
        );
        assert_table_catalog_row(
            &mut tables,
            SYS_INDEXES_TABLE_ID,
            "sys_indexes",
            SYS_INDEXES_ROOT_PAGE_ID,
            0,
        );
        assert_table_catalog_row(
            &mut tables,
            SYS_COLUMNS_TABLE_ID,
            "sys_columns",
            SYS_COLUMNS_ROOT_PAGE_ID,
            system_column_rows().len() as RowId,
        );

        let mut columns = manager.pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
        for row in system_column_rows() {
            let record =
                columns.get(row.column_id).unwrap().expect("system column row should exist");
            let tuple = Tuple::from_bytes(&record.record).unwrap();
            let actual = crate::core::catalog::ColumnCatalogRow::decode(&tuple).unwrap();
            assert_eq!(actual.column_id, row.column_id);
            assert_eq!(actual.object_kind, row.object_kind);
            assert_eq!(actual.object_id, row.object_id);
            assert_eq!(actual.ordinal, row.ordinal);
            assert_eq!(actual.name, row.name);
            assert_eq!(actual.data_type, row.data_type);
            assert_eq!(actual.nullable, row.nullable);
            assert_eq!(actual.primary_key, row.primary_key);
            assert_eq!(actual.source_table_id, row.source_table_id);
            assert_eq!(actual.source_column_ordinal, row.source_column_ordinal);
        }
    }

    #[test]
    fn open_existing_database_validates_system_catalog() {
        let file = NamedTempFile::new().unwrap();
        {
            let manager = open(file.path()).unwrap();
            manager.flush().unwrap();
        }

        let manager = open(file.path()).unwrap();
        let mut tables = manager.pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(
            &mut tables,
            SYS_TABLES_TABLE_ID,
            "sys_tables",
            SYS_TABLES_ROOT_PAGE_ID,
            3,
        );
    }

    #[test]
    fn open_rejects_partial_system_catalog() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut disk_manager = DiskManager::new(file.path()).unwrap();
            assert_eq!(disk_manager.new_page().unwrap(), 0);
            disk_manager.write_page(0, &DatabaseHeader::encode_page()).unwrap();
            assert_eq!(disk_manager.new_page().unwrap(), 1);
        }

        let error = match open(file.path()) {
            Ok(_) => panic!("partial catalog should be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::Catalog,
                kind: CorruptionKind::MissingSystemCatalogRoot { page_id: 2 },
                ..
            })
        ));
    }

    #[test]
    fn create_table_records_schema_in_catalog() {
        let file = NamedTempFile::new().unwrap();
        let manager = open(file.path()).unwrap();
        let row_schema = users_schema();

        let table = manager.create_table("users", row_schema.clone()).unwrap();

        assert_eq!(table.table_id, 4);
        assert_eq!(table.name, "users");
        assert_eq!(table.root_page_id, 4);
        assert_eq!(table.last_row_id, 0);
        assert_eq!(table.row, row_schema);
        assert_eq!(
            manager.table_cursor_by_name("users").unwrap().root_page_id(),
            table.root_page_id
        );

        let mut tables = manager.pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(&mut tables, table.table_id, "users", table.root_page_id, 0);

        let first_user_column_id = system_column_rows().len() as RowId + 1;
        let mut columns = manager.pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
        assert_column_catalog_row(
            &mut columns,
            ColumnCatalogRow {
                column_id: first_user_column_id,
                object_kind: CatalogObjectKind::Table,
                object_id: table.table_id,
                ordinal: 0,
                name: "id".to_owned(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                source_table_id: None,
                source_column_ordinal: None,
            },
        );
    }

    #[test]
    fn create_index_records_explicit_name_and_source_columns_in_catalog() {
        let file = NamedTempFile::new().unwrap();
        let manager = open(file.path()).unwrap();
        let table = manager.create_table("users", users_schema()).unwrap();

        let index = manager.create_index("idx_users_email", "users", &["email"]).unwrap();

        assert_eq!(index.index_id, 5);
        assert_eq!(index.name, "idx_users_email");
        assert_eq!(index.table_id, table.table_id);
        assert_eq!(index.root_page_id, 5);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].source_column_ordinal, 2);
        assert_eq!(index.columns[0].column.name, "email");
        assert_eq!(
            manager.index_cursor_by_name("idx_users_email").unwrap().root_page_id(),
            index.root_page_id
        );

        let mut indexes = manager.pager.table_cursor(SYS_INDEXES_ROOT_PAGE_ID).unwrap();
        assert_index_catalog_row(
            &mut indexes,
            IndexCatalogRow {
                index_id: index.index_id,
                name: "idx_users_email".to_owned(),
                table_id: table.table_id,
                root_page_id: index.root_page_id,
                unique: false,
            },
        );

        let index_column_id =
            system_column_rows().len() as RowId + table.row.columns.len() as RowId + 1;
        let mut columns = manager.pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
        assert_column_catalog_row(
            &mut columns,
            ColumnCatalogRow {
                column_id: index_column_id,
                object_kind: CatalogObjectKind::Index,
                object_id: index.index_id,
                ordinal: 0,
                name: "email".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
                source_table_id: Some(table.table_id),
                source_column_ordinal: Some(2),
            },
        );
    }

    #[test]
    fn create_index_rejects_unknown_table_column() {
        let file = NamedTempFile::new().unwrap();
        let manager = open(file.path()).unwrap();
        manager.create_table("users", users_schema()).unwrap();

        let error = manager.create_index("idx_users_missing", "users", &["missing"]).unwrap_err();

        assert!(matches!(
            error,
            StorageError::InvalidArgument(InvalidArgumentError::ColumnNotFound {
                table,
                column,
            }) if table == "users" && column == "missing"
        ));
    }

    #[test]
    fn allocate_table_row_id_persists_last_row_id() {
        let file = NamedTempFile::new().unwrap();
        {
            let manager = open(file.path()).unwrap();
            let table = manager.create_table("users", users_schema()).unwrap();

            assert_eq!(manager.allocate_table_row_id(&table).unwrap(), 1);
            assert_eq!(manager.allocate_table_row_id(&table).unwrap(), 2);
            assert_eq!(manager.table_schema_by_name("users").unwrap().last_row_id, 2);
            manager.flush().unwrap();
        }

        let manager = open(file.path()).unwrap();
        let table = manager.table_schema_by_name("users").unwrap();
        assert_eq!(table.last_row_id, 2);
        assert_eq!(manager.allocate_table_row_id(&table).unwrap(), 3);
    }

    fn assert_table_catalog_row(
        tables: &mut TableCursor,
        table_id: RowId,
        name: &str,
        root_page_id: PageId,
        last_row_id: RowId,
    ) {
        let record = tables.get(table_id).unwrap().expect("system table row should exist");
        let tuple = Tuple::from_bytes(&record.record).unwrap();
        assert_eq!(
            TableCatalogRow::decode(&tuple).unwrap(),
            TableCatalogRow { table_id, name: name.to_owned(), root_page_id, last_row_id }
        );
    }

    fn assert_index_catalog_row(indexes: &mut TableCursor, expected: IndexCatalogRow) {
        let record =
            indexes.get(expected.index_id).unwrap().expect("index catalog row should exist");
        let tuple = Tuple::from_bytes(&record.record).unwrap();
        assert_eq!(IndexCatalogRow::decode(&tuple).unwrap(), expected);
    }

    fn assert_column_catalog_row(columns: &mut TableCursor, expected: ColumnCatalogRow) {
        let record =
            columns.get(expected.column_id).unwrap().expect("column catalog row should exist");
        let tuple = Tuple::from_bytes(&record.record).unwrap();
        assert_eq!(ColumnCatalogRow::decode(&tuple).unwrap(), expected);
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
                    name: "email".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
            ],
        }
    }
}
