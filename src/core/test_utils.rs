use std::ops::Deref;

use crate::core::{
    Database, IndexSchema, StorageResult, TableId, TableSchema, Transaction, TupleSchema,
    database::StatementTransactionMode,
};

pub(crate) fn create_table(
    database: &Database,
    name: &str,
    row: TupleSchema,
) -> StorageResult<TableSchema> {
    with_ddl_transaction(database, |transaction| transaction.create_table(name, row))
}

pub(crate) fn create_index(
    database: &Database,
    name: &str,
    table_name: &str,
    columns: &[&str],
) -> StorageResult<IndexSchema> {
    with_ddl_transaction(database, |transaction| {
        transaction.create_index(name, table_name, columns)
    })
}

fn with_ddl_transaction<T>(
    database: &Database,
    operation: impl FnOnce(&Transaction<'_>) -> StorageResult<T>,
) -> StorageResult<T> {
    let txn_id = database.begin_statement_transaction(StatementTransactionMode::Ddl)?;
    let transaction = database.transaction(txn_id, Vec::new());
    let result = operation(&transaction);
    drop(transaction);

    match result {
        Ok(value) => {
            database.commit_transaction(txn_id)?;
            Ok(value)
        }
        Err(error) => {
            database.rollback_transaction(txn_id)?;
            Err(error)
        }
    }
}

pub(crate) struct TestTransaction<'db> {
    database: &'db Database,
    transaction: Transaction<'db>,
}

impl<'db> TestTransaction<'db> {
    pub(crate) fn begin(database: &'db Database, table_ids: &[TableId]) -> StorageResult<Self> {
        let txn_id = database.begin_transaction()?;
        let leases = match database.acquire_table_leases(txn_id, table_ids) {
            Ok(leases) => leases,
            Err(error) => {
                database.rollback_transaction(txn_id)?;
                return Err(error);
            }
        };
        let transaction = database.transaction(txn_id, leases);
        Ok(Self { database, transaction })
    }
}

impl<'db> Deref for TestTransaction<'db> {
    type Target = Transaction<'db>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl Drop for TestTransaction<'_> {
    fn drop(&mut self) {
        if self.database.transaction_is_active(self.transaction.id()).unwrap_or(false) {
            self.database.rollback_transaction(self.transaction.id()).unwrap();
        }
    }
}
