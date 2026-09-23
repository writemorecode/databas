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
        access::RelationalAccess,
        error::{StorageError, StorageResult},
        lock_manager::{LockError, LockMode, TableId, TableLease},
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

/// Converts page-backed cursor records to owned executor records as they are read.
pub(crate) struct OwnedScan<I>(I);

impl<I> Iterator for OwnedScan<I>
where
    I: Iterator<Item = StorageResult<crate::core::TableRecord>>,
{
    type Item = StorageResult<OwnedTableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|row| row?.to_owned_record())
    }
}

impl RelationalAccess for Transaction<'_> {
    type TableScan = OwnedScan<TableScan>;
    type IndexScan = OwnedScan<IndexScan>;

    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        Transaction::create_table(self, name, row)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        Transaction::create_index(self, name, table_name, columns)
    }

    fn scan_table(&self, table: &TableSchema) -> StorageResult<Self::TableScan> {
        Ok(OwnedScan(Transaction::scan_table(self, table)?))
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<Self::TableScan> {
        Ok(OwnedScan(Transaction::scan_table_range(self, table, range)?))
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        range: IndexKeyRange,
    ) -> StorageResult<Self::IndexScan> {
        Ok(OwnedScan(Transaction::scan_index(self, table, index, range)?))
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        Transaction::insert_table_row(self, table, values)
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        Transaction::update_table_row(self, table, record, values)
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        Transaction::delete_table_row(self, table, record)
    }
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

    fn require_table(&self, table: &TableSchema, mode: LockMode) -> StorageResult<()> {
        let table_id = TableId::from(table.table_id);
        if self.leases.iter().any(|lease| lease.authorize_mode(self.txn_id, table_id, mode).is_ok())
        {
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
        self.require_table(table, LockMode::Shared)?;
        record_manager::scan_table(self.database.catalog(), Some(self.txn_id), table)
    }

    pub(crate) fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<TableScan> {
        self.require_table(table, LockMode::Shared)?;
        record_manager::scan_table_range(self.database.catalog(), Some(self.txn_id), table, range)
    }

    pub(crate) fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: IndexKeyRange,
    ) -> StorageResult<IndexScan> {
        self.require_table(table, LockMode::Shared)?;
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
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::insert_table_row(self.database.catalog(), Some(self.txn_id), table, values)
    }

    pub(crate) fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::delete_table_row(self.database.catalog(), Some(self.txn_id), table, record)
    }

    pub(crate) fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::update_table_row(
            self.database.catalog(),
            Some(self.txn_id),
            table,
            record,
            values,
        )
    }
}
