//! Scoped relational operations. Scans cannot outlive the statement access handle.

use crate::access::RelationalAccess;
use crate::error::{RelationalError, RelationalResult};
use crate::index_manager;
use crate::lock_manager::{LockError, LockMode, TableId, TableLease};
use crate::record_manager;
use crate::record_manager::{IndexScan, TableScan};
use crate::storage::TxnId;
use crate::{
    IndexSchema, IndexValueRange, OwnedTableRecord, TableKeyRange, TableSchema, TupleSchema, Value,
};

use super::Database;

/// An active transaction's concrete relational gateway.
pub struct Transaction<'db> {
    database: &'db Database,
    txn_id: TxnId,
    leases: Vec<TableLease>,
    ddl: bool,
}

/// Converts page-backed cursor records to owned executor records as they are read.
pub struct OwnedScan<'txn, I>(I, std::marker::PhantomData<&'txn ()>);

impl<I> Iterator for OwnedScan<'_, I>
where
    I: Iterator<Item = RelationalResult<crate::TableRecord>>,
{
    type Item = RelationalResult<OwnedTableRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|row| row?.to_owned_record())
    }
}

impl RelationalAccess for Transaction<'_> {
    type TableScan<'txn>
        = OwnedScan<'txn, TableScan>
    where
        Self: 'txn;
    type IndexScan<'txn>
        = OwnedScan<'txn, IndexScan>
    where
        Self: 'txn;

    fn create_table(&self, name: &str, row: TupleSchema) -> RelationalResult<TableSchema> {
        Transaction::create_table(self, name, row)
    }

    fn drop_table(&self, name: &str) -> RelationalResult<()> {
        self.require_ddl()?;
        self.database.catalog().for_transaction(self.txn_id).drop_table(name)
    }

    fn drop_index(&self, name: &str) -> RelationalResult<()> {
        self.require_ddl()?;
        self.database.catalog().for_transaction(self.txn_id).drop_index(name)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> RelationalResult<IndexSchema> {
        Transaction::create_index(self, name, table_name, columns)
    }

    fn scan_table(&self, table: &TableSchema) -> RelationalResult<Self::TableScan<'_>> {
        Ok(OwnedScan(Transaction::scan_table(self, table)?, std::marker::PhantomData))
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> RelationalResult<Self::TableScan<'_>> {
        Ok(OwnedScan(Transaction::scan_table_range(self, table, range)?, std::marker::PhantomData))
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        range: IndexValueRange,
    ) -> RelationalResult<Self::IndexScan<'_>> {
        Ok(OwnedScan(
            Transaction::scan_index(self, table, index, &range)?,
            std::marker::PhantomData,
        ))
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord> {
        Transaction::insert_table_row(self, table, values)
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord> {
        Transaction::update_table_row(self, table, record, values)
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> RelationalResult<()> {
        Transaction::delete_table_row(self, table, record)
    }
}

impl<'db> Transaction<'db> {
    pub(super) fn new(
        database: &'db Database,
        txn_id: TxnId,
        leases: Vec<TableLease>,
        ddl: bool,
    ) -> Self {
        Self { database, txn_id, leases, ddl }
    }

    fn require_ddl(&self) -> RelationalResult<()> {
        if self.ddl {
            Ok(())
        } else {
            Err(RelationalError::InvalidArgument(crate::InvalidArgumentError::DdlRequired))
        }
    }

    fn require_table(&self, table: &TableSchema, mode: LockMode) -> RelationalResult<()> {
        let actual = self.database.catalog().table_schema_by_name(&table.name)?;
        if actual != *table {
            return Err(RelationalError::InvalidArgument(
                crate::InvalidArgumentError::SchemaMismatch { name: table.name.clone() },
            ));
        }
        let table_id = TableId::from(table.table_id);
        if self.leases.iter().any(|lease| lease.authorize_mode(self.txn_id, table_id, mode).is_ok())
        {
            Ok(())
        } else {
            Err(RelationalError::Lock(LockError::LeaseMismatch { txn_id: self.txn_id, table_id }))
        }
    }
}

impl Transaction<'_> {
    fn create_table(&self, name: &str, row: TupleSchema) -> RelationalResult<TableSchema> {
        self.require_ddl()?;
        self.database.catalog().for_transaction(self.txn_id).create_table(name, row)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> RelationalResult<IndexSchema> {
        self.require_ddl()?;
        index_manager::create_index(
            &self.database.catalog().for_transaction(self.txn_id),
            name,
            table_name,
            columns,
        )
    }
}

impl Transaction<'_> {
    fn scan_table(&self, table: &TableSchema) -> RelationalResult<TableScan> {
        self.require_table(table, LockMode::Shared)?;
        record_manager::scan_table(&self.database.catalog().for_transaction(self.txn_id), table)
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> RelationalResult<TableScan> {
        self.require_table(table, LockMode::Shared)?;
        record_manager::scan_table_range(
            &self.database.catalog().for_transaction(self.txn_id),
            table,
            range,
        )
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key_range: &IndexValueRange,
    ) -> RelationalResult<IndexScan> {
        self.require_table(table, LockMode::Shared)?;
        if !self.database.catalog().index_schemas_for_table(table)?.contains(index) {
            return Err(RelationalError::InvalidArgument(
                crate::InvalidArgumentError::SchemaMismatch { name: index.name.clone() },
            ));
        }
        record_manager::scan_index(
            &self.database.catalog().for_transaction(self.txn_id),
            table,
            index,
            key_range.encode()?,
        )
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord> {
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::insert_table_row(
            &self.database.catalog().for_transaction(self.txn_id),
            table,
            values,
        )
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> RelationalResult<()> {
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::delete_table_row(
            &self.database.catalog().for_transaction(self.txn_id),
            table,
            record,
        )
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> RelationalResult<OwnedTableRecord> {
        self.require_table(table, LockMode::Exclusive)?;
        record_manager::update_table_row(
            &self.database.catalog().for_transaction(self.txn_id),
            table,
            record,
            values,
        )
    }
}
