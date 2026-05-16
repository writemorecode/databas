use std::path::{Path, PathBuf};

use crate::core::{
    CatalogError, IndexSchema, PageId, RowId, TableCatalogRow, TableRecord, TableSchema, Tuple,
    TupleSchema,
    btree::{OwnedRecord, TreeCursor, initialize_empty_root, validate_root_page},
    catalog::{
        CatalogObjectKind, ColumnCatalogRow, ColumnSchema, IndexCatalogRow, IndexColumnSchema,
        SYS_COLUMNS_ROOT_PAGE_ID, SYS_INDEXES_ROOT_PAGE_ID, SYS_TABLES_ROOT_PAGE_ID,
        system_column_rows, system_table_schemas,
    },
    cursor::{IndexCursor, TableCursor},
    disk_manager::DiskManager,
    error::{
        ConstraintError, CorruptionComponent, CorruptionError, CorruptionKind,
        InvalidArgumentError, StorageError, StorageResult,
    },
    page_cache::PageCache,
};

const DEFAULT_PAGE_CACHE_SIZE: usize = 64;

/// Configuration for [`Pager`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PagerOptions {
    /// Number of frames to preallocate in the page cache.
    pub cache_frames: usize,
}

impl Default for PagerOptions {
    fn default() -> Self {
        Self { cache_frames: DEFAULT_PAGE_CACHE_SIZE }
    }
}

/// Storage-engine entry point for one database file.
///
/// `Pager` owns the disk manager and page cache indirectly, and is responsible
/// for producing typed cursors rooted at specific page ids.
#[derive(Clone)]
pub struct Pager {
    path: PathBuf,
    page_cache: PageCache,
    options: PagerOptions,
}

impl Pager {
    /// Opens a pager with default options.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, PagerOptions::default())
    }

    /// Opens a pager with explicit cache settings.
    pub fn open_with_options(path: impl AsRef<Path>, options: PagerOptions) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let disk_manager = DiskManager::new(&path)?;
        let page_count = disk_manager.page_count();
        let page_cache = PageCache::new(disk_manager, options.cache_frames)?;
        let pager = Self { path, page_cache, options };
        pager.initialize_or_validate_system_catalog(page_count)?;
        Ok(pager)
    }

    /// Returns the database-file path associated with this pager.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the options used when this pager was opened.
    pub fn options(&self) -> PagerOptions {
        self.options
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.page_cache.flush_all()?;
        Ok(())
    }

    /// Creates a cataloged table, allocates its root page, and records its columns.
    pub fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        if self.table_catalog_rows()?.iter().any(|row| row.name == name) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateTableName {
                name: name.to_owned(),
            }));
        }

        let table_id = self.next_object_id()?;
        let mut column_id = self.next_column_id()?;
        let root_page_id = self.create_table_tree()?.root_page_id();
        let schema = TableSchema { table_id, name: name.to_owned(), root_page_id, row };

        self.insert_table_catalog_row(&schema.catalog_row())?;
        for (ordinal, column) in schema.row.columns.iter().enumerate() {
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
            column_id += 1;
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
        let mut column_id = self.next_column_id()?;
        let mut index_columns = Vec::new();
        let mut catalog_columns = Vec::new();

        for (ordinal, column_name) in columns.iter().enumerate() {
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
            column_id += 1;
        }

        let root_page_id = self.create_index_tree()?.root_page_id();
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

    /// Creates a new empty table tree and returns a cursor rooted at it.
    pub fn create_table_tree(&self) -> StorageResult<TableCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache)?;
        Ok(TableCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Creates a new empty secondary-index tree and returns a cursor rooted at it.
    pub fn create_index_tree(&self) -> StorageResult<IndexCursor> {
        let root_page_id = initialize_empty_root(&self.page_cache)?;
        Ok(IndexCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Returns a typed cursor for the cataloged table named `name`.
    pub fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        let schema = self.table_schema_by_name(name)?;
        self.table_cursor(schema.root_page_id)
    }

    /// Returns a typed cursor for the cataloged index named `name`.
    pub fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        let schema = self.index_schema_by_name(name)?;
        self.index_cursor(schema.root_page_id)
    }

    /// Returns a typed cursor rooted at an existing table tree.
    pub fn table_cursor(&self, root_page_id: PageId) -> StorageResult<TableCursor> {
        validate_root_page(&self.page_cache, root_page_id)?;
        Ok(TableCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    /// Returns a typed cursor rooted at an existing secondary-index tree.
    pub fn index_cursor(&self, root_page_id: PageId) -> StorageResult<IndexCursor> {
        validate_root_page(&self.page_cache, root_page_id)?;
        Ok(IndexCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id)))
    }

    fn initialize_or_validate_system_catalog(&self, page_count: u64) -> StorageResult<()> {
        match page_count {
            0 => self.initialize_system_catalog(),
            1..=2 => Err(missing_system_catalog_root(page_count)),
            _ => self.validate_system_catalog(),
        }
    }

    fn initialize_system_catalog(&self) -> StorageResult<()> {
        self.initialize_system_root(SYS_TABLES_ROOT_PAGE_ID)?;
        self.initialize_system_root(SYS_INDEXES_ROOT_PAGE_ID)?;
        self.initialize_system_root(SYS_COLUMNS_ROOT_PAGE_ID)?;
        self.seed_system_catalog()
    }

    /// Inserts the built-in system catalog rows into a freshly initialized database.
    fn seed_system_catalog(&self) -> StorageResult<()> {
        let mut tables = self.table_cursor(SYS_TABLES_ROOT_PAGE_ID)?;
        let mut columns = self.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID)?;

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

    fn initialize_system_root(&self, expected_page_id: PageId) -> StorageResult<()> {
        let actual_page_id = initialize_empty_root(&self.page_cache)?;
        if actual_page_id == expected_page_id {
            Ok(())
        } else {
            Err(unexpected_system_catalog_root(expected_page_id, actual_page_id))
        }
    }

    fn validate_system_catalog(&self) -> StorageResult<()> {
        validate_root_page(&self.page_cache, SYS_TABLES_ROOT_PAGE_ID)?;
        validate_root_page(&self.page_cache, SYS_INDEXES_ROOT_PAGE_ID)?;
        validate_root_page(&self.page_cache, SYS_COLUMNS_ROOT_PAGE_ID)?;
        Ok(())
    }

    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
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
        let mut columns: Vec<_> = self
            .column_catalog_rows()?
            .into_iter()
            .filter(|row| {
                row.object_kind == CatalogObjectKind::Index && row.object_id == index.index_id
            })
            .collect();
        columns.sort_by_key(|row| row.ordinal);

        Ok(IndexSchema {
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
        })
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
        decode: impl Fn(&Tuple) -> Result<T, crate::core::CatalogError>,
    ) -> StorageResult<Vec<T>> {
        let mut cursor = TreeCursor::new(self.page_cache.clone(), root_page_id);
        let mut rows = Vec::new();
        if !cursor.seek_to_first()? {
            return Ok(rows);
        }

        loop {
            let record = cursor
                .current_owned()?
                .expect("positioned catalog cursor should have a current record");
            rows.push(decode_catalog_record(catalog_table_name, record, &decode)?);
            if cursor.next_owned_record()?.is_none() {
                break;
            }
        }
        Ok(rows)
    }

    fn insert_table_catalog_row(&self, row: &TableCatalogRow) -> StorageResult<()> {
        self.insert_catalog_row(SYS_TABLES_ROOT_PAGE_ID, row.table_id, &row.encode())
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
        let mut cursor = TableCursor::new(TreeCursor::new(self.page_cache.clone(), root_page_id));
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
    };

    #[test]
    fn open_new_database_bootstraps_system_catalog_roots_and_rows() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open(file.path()).unwrap();

        assert_eq!(pager.create_table_tree().unwrap().root_page_id(), 3);

        let mut tables = pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(
            &mut tables,
            SYS_TABLES_TABLE_ID,
            "sys_tables",
            SYS_TABLES_ROOT_PAGE_ID,
        );
        assert_table_catalog_row(
            &mut tables,
            SYS_INDEXES_TABLE_ID,
            "sys_indexes",
            SYS_INDEXES_ROOT_PAGE_ID,
        );
        assert_table_catalog_row(
            &mut tables,
            SYS_COLUMNS_TABLE_ID,
            "sys_columns",
            SYS_COLUMNS_ROOT_PAGE_ID,
        );

        let mut columns = pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
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
            let pager = Pager::open(file.path()).unwrap();
            pager.flush().unwrap();
        }

        let pager = Pager::open(file.path()).unwrap();
        let mut tables = pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(
            &mut tables,
            SYS_TABLES_TABLE_ID,
            "sys_tables",
            SYS_TABLES_ROOT_PAGE_ID,
        );
    }

    #[test]
    fn open_rejects_partial_system_catalog() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut disk_manager = DiskManager::new(file.path()).unwrap();
            assert_eq!(disk_manager.new_page().unwrap(), 0);
        }

        let error = match Pager::open(file.path()) {
            Ok(_) => panic!("partial catalog should be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            StorageError::Corruption(CorruptionError {
                component: CorruptionComponent::Catalog,
                kind: CorruptionKind::MissingSystemCatalogRoot { page_id: 1 },
                ..
            })
        ));
    }

    #[test]
    fn create_table_records_schema_in_catalog() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::open(file.path()).unwrap();
        let row_schema = users_schema();

        let table = pager.create_table("users", row_schema.clone()).unwrap();

        assert_eq!(table.table_id, 4);
        assert_eq!(table.name, "users");
        assert_eq!(table.root_page_id, 3);
        assert_eq!(table.row, row_schema);
        assert_eq!(pager.table_cursor_by_name("users").unwrap().root_page_id(), table.root_page_id);

        let mut tables = pager.table_cursor(SYS_TABLES_ROOT_PAGE_ID).unwrap();
        assert_table_catalog_row(&mut tables, table.table_id, "users", table.root_page_id);

        let first_user_column_id = system_column_rows().len() as RowId + 1;
        let mut columns = pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
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
        let pager = Pager::open(file.path()).unwrap();
        let table = pager.create_table("users", users_schema()).unwrap();

        let index = pager.create_index("idx_users_email", "users", &["email"]).unwrap();

        assert_eq!(index.index_id, 5);
        assert_eq!(index.name, "idx_users_email");
        assert_eq!(index.table_id, table.table_id);
        assert_eq!(index.root_page_id, 4);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].source_column_ordinal, 2);
        assert_eq!(index.columns[0].column.name, "email");
        assert_eq!(
            pager.index_cursor_by_name("idx_users_email").unwrap().root_page_id(),
            index.root_page_id
        );

        let mut indexes = pager.table_cursor(SYS_INDEXES_ROOT_PAGE_ID).unwrap();
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
        let mut columns = pager.table_cursor(SYS_COLUMNS_ROOT_PAGE_ID).unwrap();
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
        let pager = Pager::open(file.path()).unwrap();
        pager.create_table("users", users_schema()).unwrap();

        let error = pager.create_index("idx_users_missing", "users", &["missing"]).unwrap_err();

        assert!(matches!(
            error,
            StorageError::InvalidArgument(InvalidArgumentError::ColumnNotFound {
                table,
                column,
            }) if table == "users" && column == "missing"
        ));
    }

    fn assert_table_catalog_row(
        tables: &mut TableCursor,
        table_id: RowId,
        name: &str,
        root_page_id: PageId,
    ) {
        let record = tables.get(table_id).unwrap().expect("system table row should exist");
        let tuple = Tuple::from_bytes(&record.record).unwrap();
        assert_eq!(
            TableCatalogRow::decode(&tuple).unwrap(),
            TableCatalogRow { table_id, name: name.to_owned(), root_page_id }
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
