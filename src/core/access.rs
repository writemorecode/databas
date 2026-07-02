use crate::core::{
    IndexKeyRange, IndexSchema, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
    error::StorageResult,
    record_manager::{IndexScan, TableScan},
};

pub(crate) trait SchemaAccess {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema>;

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>>;
}

pub(crate) trait DdlAccess {
    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema>;

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema>;
}

pub(crate) trait RecordAccess {
    fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan>;

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan>;

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan>;

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord>;

    fn delete_table_row(&self, table: &TableSchema, record: &OwnedTableRecord)
    -> StorageResult<()>;

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord>;
}

pub(crate) trait ExecutionAccess: DdlAccess + RecordAccess {}

impl<T> ExecutionAccess for T where T: DdlAccess + RecordAccess {}
