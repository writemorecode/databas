use std::path::Path;

use crate::core::{
    IndexSchema, TableCursor, TableSchema, TupleSchema, catalog_manager::CatalogManager,
    cursor::IndexCursor, error::StorageResult,
};

/// Public database handle for one database file.
#[derive(Clone)]
pub struct Database {
    catalog: CatalogManager,
}

impl Database {
    /// Creates a new database file.
    pub fn create(path: impl AsRef<Path>) -> StorageResult<Self> {
        Ok(Self { catalog: CatalogManager::create(path)? })
    }

    /// Opens an existing database file.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Ok(Self { catalog: CatalogManager::open_existing(path)? })
    }

    /// Returns the database-file path associated with this database.
    pub fn path(&self) -> &Path {
        self.catalog.path()
    }

    /// Flushes all dirty, currently unpinned pages to disk.
    pub fn flush(&self) -> StorageResult<()> {
        self.catalog.flush()
    }

    /// Creates a table and records its schema in the system catalog.
    pub fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.catalog.create_table(name, row)
    }

    /// Creates a secondary index and records its schema in the system catalog.
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        self.catalog.create_index(name, table_name, columns)
    }

    /// Returns the schema for the table named `name`.
    pub fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        self.catalog.table_schema_by_name(name)
    }

    /// Returns a typed cursor for the table named `name`.
    pub fn table_cursor_by_name(&self, name: &str) -> StorageResult<TableCursor> {
        self.catalog.table_cursor_by_name(name)
    }

    /// Returns a typed cursor for the index named `name`.
    pub fn index_cursor_by_name(&self, name: &str) -> StorageResult<IndexCursor> {
        self.catalog.index_cursor_by_name(name)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{NamedTempFile, tempdir};

    use super::*;
    use crate::core::error::{CorruptionError, CorruptionKind, StorageError};

    #[test]
    fn create_initializes_database_that_can_be_opened() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let database = Database::create(&path).unwrap();
        database.flush().unwrap();

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.path(), path);
    }

    #[test]
    fn create_rejects_existing_file() {
        let file = NamedTempFile::new().unwrap();

        assert!(Database::create(file.path()).is_err());
    }

    #[test]
    fn open_rejects_empty_file_without_header() {
        let file = NamedTempFile::new().unwrap();

        assert!(matches!(
            Database::open(file.path()),
            Err(StorageError::Corruption(CorruptionError {
                kind: CorruptionKind::MissingDatabaseHeader,
                ..
            }))
        ));
    }
}
