use crate::core::{IndexSchema, TableSchema, error::StorageResult};

/// Minimal catalog seam used by the planner and its lightweight tests.
pub(crate) trait CatalogRead {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema>;

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>>;
}
