use std::path::{Path, PathBuf};

use crate::core::{
    IndexSchema, PageId, TableSchema, TupleSchema,
    btree::{TreeCursor, initialize_empty_root, validate_root_page},
    catalog_manager::CatalogManager,
    cursor::{IndexCursor, TableCursor},
    disk_manager::DiskManager,
    error::{CorruptionComponent, CorruptionError, CorruptionKind, StorageError, StorageResult},
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
        self.catalog_manager().create_table(name, row)
    }

    /// Creates a cataloged secondary index over columns from an existing table.
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        self.catalog_manager().create_index(name, table_name, columns)
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
        self.catalog_manager().table_cursor_by_name(name)
    }

    /// Returns a typed cursor for the cataloged index named `name`.
    pub fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog_manager().index_cursor_by_name(name)
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

    pub(crate) fn page_cache(&self) -> PageCache {
        self.page_cache.clone()
    }

    fn initialize_or_validate_system_catalog(&self, page_count: u64) -> StorageResult<()> {
        match page_count {
            0 => self.catalog_manager().initialize_system_catalog(),
            1..=2 => Err(missing_system_catalog_root(page_count)),
            _ => self.catalog_manager().validate_system_catalog(),
        }
    }

    fn catalog_manager(&self) -> CatalogManager {
        CatalogManager::new(self.clone())
    }
}

fn missing_system_catalog_root(page_id: PageId) -> StorageError {
    StorageError::Corruption(CorruptionError {
        component: CorruptionComponent::Catalog,
        page_id: Some(page_id),
        kind: CorruptionKind::MissingSystemCatalogRoot { page_id },
    })
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::core::{
        ColumnCatalogRow, ColumnSchema, DataType, IndexCatalogRow, RowId, Tuple,
        catalog::{
            CatalogObjectKind, SYS_COLUMNS_ROOT_PAGE_ID, SYS_COLUMNS_TABLE_ID,
            SYS_INDEXES_ROOT_PAGE_ID, SYS_INDEXES_TABLE_ID, SYS_TABLES_ROOT_PAGE_ID,
            SYS_TABLES_TABLE_ID, TableCatalogRow, system_column_rows,
        },
        error::InvalidArgumentError,
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
