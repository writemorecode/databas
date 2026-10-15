//! Transaction lifetime and statement atomicity, independent of SQL.
use crate::error::{InternalError, InvariantViolation};
use crate::transaction::Transaction;
use crate::{
    Database, LockMode, RelationalError, RelationalResult, StatementTransactionMode, TableId, TxnId,
};

/// Owns a physical transaction and its table locks. Drop attempts rollback;
/// use `close` to observe cleanup failures. A failed close is not retried.
pub struct ActiveTransaction<'db> {
    database: &'db Database,
    id: TxnId,
    active: bool,
}
impl<'db> ActiveTransaction<'db> {
    pub(crate) fn new(database: &'db Database, id: TxnId) -> Self {
        Self { database, id, active: true }
    }
    pub fn id(&self) -> TxnId {
        self.id
    }
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Executes an atomic unit of relational work. An error restores its savepoint.
    /// Table locks survive successful and failed statements until transaction end.
    /// Lock-admission failures abort the transaction. Scans borrow the scoped access
    /// handle, so no page-backed scan can escape this callback.
    ///
    /// Owned records can leave the callback:
    /// ```
    /// use databas_relational::{ActiveTransaction, TableSchema, OwnedTableRecord,
    ///     RelationalResult, StatementTransactionMode as Mode, LockMode,
    ///     access::RelationalAccess};
    /// fn read(tx: &mut ActiveTransaction<'_>, table: &TableSchema)
    ///     -> RelationalResult<Vec<OwnedTableRecord>> {
    ///     tx.statement(Mode::Ordinary, &[(table.table_id.into(), LockMode::Shared)], |access| {
    ///         access.scan_table(table)?.collect()
    ///     })
    /// }
    /// ```
    /// A live scan cannot leave the callback:
    /// ```compile_fail
    /// use databas_relational::{ActiveTransaction, TableSchema, RelationalResult,
    ///     StatementTransactionMode as Mode, LockMode, access::RelationalAccess};
    /// fn read(tx: &mut ActiveTransaction<'_>, table: &TableSchema) -> RelationalResult<()> {
    ///     let scan = tx.statement(Mode::Ordinary,
    ///         &[(table.table_id.into(), LockMode::Shared)], |access| access.scan_table(table))?;
    ///     tx.commit()?;
    ///     for record in scan { record?; }
    ///     Ok(())
    /// }
    /// ```
    pub fn statement<T, E: From<RelationalError>>(
        &mut self,
        mode: StatementTransactionMode,
        locks: &[(TableId, LockMode)],
        work: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        if !self.active {
            return Err(inactive().into());
        }
        if mode == StatementTransactionMode::Ddl {
            self.database.acquire_ddl_gate(self.id)?;
        }
        let leases = match self.database.acquire_table_leases(self.id, locks) {
            Ok(leases) => leases,
            Err(error) => {
                self.rollback()?;
                return Err(error.into());
            }
        };
        let savepoint = self.database.statement_savepoint(self.id)?;
        let access =
            self.database.transaction(self.id, leases, mode == StatementTransactionMode::Ddl);
        let result = work(&access);
        drop(access);
        let result = match result {
            Ok(_) if self.database.transaction_is_poisoned(self.id)? => {
                Err(RelationalError::Internal(InternalError::InvariantViolation(
                    InvariantViolation::TransactionPoisoned { txn_id: self.id },
                ))
                .into())
            }
            result => result,
        };
        if result.is_err() {
            self.database.rollback_to_savepoint(savepoint)?;
        }
        result
    }
    pub fn commit(&mut self) -> RelationalResult<()> {
        if !self.active {
            return Err(inactive());
        }
        let result = self.database.commit_transaction(self.id);
        self.active = self.database.transaction_is_active(self.id)?;
        result
    }
    pub fn rollback(&mut self) -> RelationalResult<()> {
        if !self.active {
            return Err(inactive());
        }
        let result = self.database.rollback_transaction(self.id);
        self.active = self.database.transaction_is_active(self.id)?;
        result
    }
    /// Rolls back unfinished work, reporting cleanup failure without a Drop retry.
    pub fn close(mut self) -> RelationalResult<()> {
        let result = if self.active { self.database.rollback_transaction(self.id) } else { Ok(()) };
        self.active = false;
        result
    }
}
impl Drop for ActiveTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.database.rollback_transaction(self.id);
        }
    }
}
fn inactive() -> RelationalError {
    RelationalError::Internal(InternalError::InvariantViolation(
        InvariantViolation::NoActiveTransaction,
    ))
}
