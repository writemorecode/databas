//! Transaction-scoped access to relational operations.
//!
//! This type is the concrete boundary between session/execution policy and the
//! relational engine. Transaction identity is retained here even though lower
//! storage mutations still use the legacy ambient transaction during the
//! migration.

use crate::{
    core::{
        IndexKeyRange, IndexSchema, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema,
        Value,
        access::{DdlAccess, RecordAccess},
        error::StorageResult,
    },
    relational::record_manager::{IndexScan, TableScan},
    storage::{log_manager::TxnId, transaction_manager::TransactionSavepoint},
};

use super::Database;

/// An active transaction's concrete relational gateway.
pub(crate) struct Transaction<'db> {
    database: &'db Database,
    txn_id: TxnId,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(database: &'db Database, txn_id: TxnId) -> Self {
        Self { database, txn_id }
    }

    /// Returns the storage transaction identity associated with this gateway.
    pub(crate) fn id(&self) -> TxnId {
        self.txn_id
    }

    pub(crate) fn statement_savepoint(&self) -> StorageResult<TransactionSavepoint> {
        self.database.statement_savepoint(self.txn_id)
    }

    pub(crate) fn is_poisoned(&self) -> StorageResult<bool> {
        self.database.transaction_is_poisoned(self.txn_id)
    }
}

impl DdlAccess for Transaction<'_> {
    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.database.create_table(name, row)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        self.database.create_index(name, table_name, columns)
    }
}

impl RecordAccess for Transaction<'_> {
    fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        self.database.scan_table(table)
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        self.database.scan_table_range(table, range)
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan> {
        self.database.scan_index(table, index, key_range)
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.database.insert_table_row(table, values)
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.database.delete_table_row(table, record)
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.database.update_table_row(table, record, values)
    }
}
