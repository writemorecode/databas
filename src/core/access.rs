use crate::core::{
    IndexSchema, OwnedTableRecord, TableSchema, TupleSchema, Value, error::StorageResult,
    record_manager::TableScan,
};

pub(crate) trait SchemaAccess {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema>;
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
