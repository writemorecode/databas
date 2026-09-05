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
        error::{StorageError, StorageResult},
        lock_manager::{LockError, TableId, TableLease},
    },
    relational::{
        index_manager,
        record_manager::{self, IndexScan, TableScan},
    },
    storage::{log_manager::TxnId, transaction_manager::TransactionSavepoint},
};

use super::Database;

/// An active transaction's concrete relational gateway.
pub(crate) struct Transaction<'db> {
    database: &'db Database,
    txn_id: TxnId,
    leases: Vec<TableLease>,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(database: &'db Database, txn_id: TxnId, leases: Vec<TableLease>) -> Self {
        Self { database, txn_id, leases }
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

    fn require_table(&self, table: &TableSchema) -> StorageResult<()> {
        let table_id = TableId::from(table.table_id);
        if self.leases.iter().any(|lease| lease.authorize(self.txn_id, table_id).is_ok()) {
            Ok(())
        } else {
            Err(StorageError::Lock(LockError::LeaseMismatch { txn_id: self.txn_id, table_id }))
        }
    }
}

impl Transaction<'_> {
    pub(crate) fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        self.database.catalog().for_transaction(self.txn_id).create_table(name, row)
    }

    pub(crate) fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        index_manager::create_index(
            &self.database.catalog().for_transaction(self.txn_id),
            name,
            table_name,
            columns,
        )
    }
}

impl Transaction<'_> {
    pub(crate) fn scan_table(&self, table: &TableSchema) -> StorageResult<TableScan> {
        self.require_table(table)?;
        record_manager::scan_table(self.database.catalog(), Some(self.txn_id), table)
    }

    pub(crate) fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        self.require_table(table)?;
        record_manager::scan_table_range(self.database.catalog(), Some(self.txn_id), table, range)
    }

    pub(crate) fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan> {
        self.require_table(table)?;
        record_manager::scan_index(
            self.database.catalog(),
            Some(self.txn_id),
            table,
            index,
            key_range,
        )
    }

    pub(crate) fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.require_table(table)?;
        record_manager::insert_table_row(self.database.catalog(), Some(self.txn_id), table, values)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.require_table(table)?;
        record_manager::delete_table_row(self.database.catalog(), Some(self.txn_id), table, record)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.require_table(table)?;
        record_manager::update_table_row(
            self.database.catalog(),
            Some(self.txn_id),
            table,
            record,
            values,
        )
    }
}
