use crate::core::{
    IndexKeyRange, IndexSchema, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
    error::StorageResult,
};

/// Minimal catalog seam used by the planner and its lightweight tests.
pub(crate) trait CatalogRead {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema>;

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>>;
}

/// Relational operations needed to execute a physical plan.
pub trait RelationalAccess {
    /// Table records in primary-key order, owned independently of storage pages.
    type TableScan: Iterator<Item = StorageResult<OwnedTableRecord>>;
    /// Table records in secondary-index order, owned independently of storage pages.
    type IndexScan: Iterator<Item = StorageResult<OwnedTableRecord>>;

    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema>;
    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema>;
    fn scan_table(&self, table: &TableSchema) -> StorageResult<Self::TableScan>;
    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<Self::TableScan>;
    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        range: IndexKeyRange,
    ) -> StorageResult<Self::IndexScan>;
    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord>;
    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord>;
    fn delete_table_row(&self, table: &TableSchema, record: &OwnedTableRecord)
    -> StorageResult<()>;
}
