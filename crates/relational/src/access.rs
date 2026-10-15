use crate::error::RelationalResult;
use crate::{
    IndexSchema, IndexValueRange, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
};

/// Minimal catalog seam used by the planner and its lightweight tests.
pub trait CatalogRead {
    fn table_schema_by_name(&self, name: &str) -> RelationalResult<TableSchema>;

    fn index_schemas_for_table(&self, table: &TableSchema) -> RelationalResult<Vec<IndexSchema>>;
}

/// Relational operations needed to execute a physical plan.
pub trait RelationalAccess {
    /// Table records in primary-key order, owned independently of storage pages.
    type TableScan<'txn>: Iterator<Item = RelationalResult<OwnedTableRecord>>
    where
        Self: 'txn;
    /// Table records in secondary-index order, owned independently of storage pages.
    type IndexScan<'txn>: Iterator<Item = RelationalResult<OwnedTableRecord>>
    where
        Self: 'txn;

    fn create_table(&self, name: &str, row: TupleSchema) -> RelationalResult<TableSchema>;
    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> RelationalResult<IndexSchema>;
    fn scan_table(&self, table: &TableSchema) -> RelationalResult<Self::TableScan<'_>>;
    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> RelationalResult<Self::TableScan<'_>>;
    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        range: IndexValueRange,
    ) -> RelationalResult<Self::IndexScan<'_>>;
    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord>;
    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord>;
    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> RelationalResult<()>;
}
