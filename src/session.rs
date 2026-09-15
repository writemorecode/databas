//! SQL session execution and transaction policy.
//!
//! A session is the top-level SQL execution context for one database handle. It
//! dispatches parsed SQL items, keeps explicit transaction state, and preserves
//! the implicit transaction behavior for standalone statements. Read scans use
//! shared table locks; mutations acquire or upgrade to exclusive locks before
//! execution. All locks remain transaction-owned until commit or rollback.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::storage::transaction_manager::TransactionSavepoint;
use crate::{
    core::{
        Database, LockMode, TableId,
        database::StatementTransactionMode,
        error::{InternalError, InvariantViolation, StorageError},
        lock_manager::TableLease,
    },
    error::DatabaseError,
    executor::{ExecutionOutput, Executor},
    planner::{PhysicalPlan, PhysicalPlanNode, Planner},
    sql_parser::parser::{Command, Parser, SqlItem, stmt::Statement},
};

/// Errors raised by session-level transaction control.
#[derive(Debug, Error)]
pub enum SessionError {
    /// `BEGIN` was executed while the session already had an explicit
    /// transaction open.
    #[error("transaction {txn_id} is already active")]
    TransactionAlreadyActive { txn_id: u64 },
    /// `COMMIT` or `ROLLBACK` was executed without an explicit transaction.
    #[error("no active transaction")]
    NoActiveTransaction,
}

/// SQL execution context for a single database handle.
pub struct Session<'db> {
    database: &'db Database,
    active_txn: Option<u64>,
}

impl<'db> Session<'db> {
    /// Creates a new session over `database`.
    pub fn new(database: &'db Database) -> Self {
        Self { database, active_txn: None }
    }

    /// Closes this session, rolling back any open explicit transaction.
    ///
    /// Unlike the best-effort `Drop` fallback, this reports cleanup failures.
    /// A failed rollback is not silently retried during destruction.
    ///
    /// # Errors
    ///
    /// Returns a storage error if rollback cannot complete. Server callers
    /// must treat a failed cleanup as fatal rather than reuse the database.
    pub fn close(mut self) -> Result<(), StorageError> {
        if let Some(txn_id) = self.active_txn.take() {
            self.database.rollback_transaction(txn_id)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn active_transaction_id_for_test(&self) -> Option<u64> {
        self.active_txn
    }

    /// Parses and executes one top-level SQL item.
    pub fn execute_sql<'sql>(
        &mut self,
        sql: &'sql str,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let item = Parser::new(sql).item()?;
        self.execute_item(item)
    }

    /// Executes one parsed SQL item.
    pub fn execute_item<'sql>(
        &mut self,
        item: SqlItem<'sql>,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        match item {
            SqlItem::Statement(statement) => self.execute_statement(statement),
            SqlItem::Command(command) => self.execute_command(command),
        }
    }

    fn execute_statement<'sql>(
        &mut self,
        statement: Statement<'sql>,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let transaction_mode = statement_transaction_mode(&statement);
        let plan = Planner::new(self.database).plan_physical_statement(&statement)?;
        let table_locks = plan_table_locks(&plan);

        if let Some(txn_id) = self.active_txn {
            if transaction_mode == StatementTransactionMode::Ddl {
                self.database.acquire_ddl_gate(txn_id)?;
            }
            let leases = match self.database.acquire_table_leases(txn_id, &table_locks) {
                Ok(leases) => leases,
                Err(error) => {
                    self.database.rollback_transaction(txn_id)?;
                    self.active_txn = None;
                    return Err(error.into());
                }
            };
            self.execute_explicit_transaction_statement(txn_id, leases, plan)
        } else {
            self.execute_implicit_transaction(plan, table_locks, transaction_mode)
        }
    }

    fn execute_command<'sql>(
        &mut self,
        command: Command,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        match command {
            Command::Begin => self.begin_transaction(),
            Command::Commit => self.commit_transaction(),
            Command::Rollback => self.rollback_transaction(),
        }
    }

    fn begin_transaction<'sql>(&mut self) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        if let Some(txn_id) = self.active_txn {
            return Err(SessionError::TransactionAlreadyActive { txn_id }.into());
        }

        let txn_id = self.database.begin_transaction()?;
        self.active_txn = Some(txn_id);
        Ok(ExecutionOutput::CommandOk)
    }

    fn commit_transaction<'sql>(&mut self) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let txn_id = self.active_txn.ok_or(SessionError::NoActiveTransaction)?;
        match self.database.commit_transaction(txn_id) {
            Ok(()) => {
                self.active_txn = None;
                Ok(ExecutionOutput::CommandOk)
            }
            Err(error) => {
                self.sync_active_transaction(txn_id)?;
                Err(error.into())
            }
        }
    }

    fn rollback_transaction<'sql>(&mut self) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let txn_id = self.active_txn.ok_or(SessionError::NoActiveTransaction)?;
        match self.database.rollback_transaction(txn_id) {
            Ok(()) => {
                self.active_txn = None;
                Ok(ExecutionOutput::CommandOk)
            }
            Err(error) => {
                self.sync_active_transaction(txn_id)?;
                Err(error.into())
            }
        }
    }

    fn execute_explicit_transaction_statement<'sql>(
        &mut self,
        txn_id: u64,
        leases: Vec<TableLease>,
        plan: PhysicalPlan,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let transaction = self.database.transaction(txn_id, leases);
        debug_assert_eq!(transaction.id(), txn_id);
        let savepoint = transaction.statement_savepoint()?;
        match Executor::in_transaction(&transaction).execute(plan) {
            Ok(output) => {
                if transaction.is_poisoned()? {
                    return self.rollback_failed_explicit_transaction_statement(
                        savepoint,
                        transaction_poisoned(txn_id).into(),
                    );
                }
                Ok(output)
            }
            Err(error) => {
                self.rollback_failed_explicit_transaction_statement(savepoint, error.into())
            }
        }
    }

    fn rollback_failed_explicit_transaction_statement<'sql>(
        &self,
        savepoint: TransactionSavepoint,
        error: DatabaseError<'sql>,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        match self.database.rollback_to_savepoint(savepoint) {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(rollback_error.into()),
        }
    }

    fn execute_implicit_transaction<'sql>(
        &self,
        plan: PhysicalPlan,
        table_locks: Vec<(TableId, LockMode)>,
        mode: StatementTransactionMode,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let txn_id = self.database.begin_statement_transaction(mode)?;
        let leases = match self.database.acquire_table_leases(txn_id, &table_locks) {
            Ok(leases) => leases,
            Err(error) => {
                self.database.rollback_transaction(txn_id)?;
                return Err(error.into());
            }
        };
        let transaction = self.database.transaction(txn_id, leases);
        match Executor::in_transaction(&transaction).execute(plan) {
            Ok(output) => match self.database.commit_transaction(txn_id) {
                Ok(()) => Ok(output),
                Err(commit_error) => {
                    if self.database.transaction_is_active(txn_id)?
                        && let Err(rollback_error) = self.database.rollback_transaction(txn_id)
                        && !is_no_active_transaction(&rollback_error)
                    {
                        return Err(rollback_error.into());
                    }
                    Err(commit_error.into())
                }
            },
            Err(error) => {
                if let Err(rollback_error) = self.database.rollback_transaction(txn_id) {
                    return Err(rollback_error.into());
                }
                Err(error.into())
            }
        }
    }

    fn sync_active_transaction(&mut self, txn_id: u64) -> Result<(), StorageError> {
        if !self.database.transaction_is_active(txn_id)? {
            self.active_txn = None;
        }
        Ok(())
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if let Some(txn_id) = self.active_txn.take() {
            let _ = self.database.rollback_transaction(txn_id);
        }
    }
}

fn statement_transaction_mode(statement: &Statement<'_>) -> StatementTransactionMode {
    match statement {
        Statement::CreateTable(_) | Statement::CreateIndex(_) => StatementTransactionMode::Ddl,
        _ => StatementTransactionMode::Ordinary,
    }
}

/// Collects the strongest required mode per table in deterministic ID order.
/// Mutation targets are locked exclusively before execution, even when their
/// input is a read scan. Existing shared ownership is upgraded by the manager.
fn plan_table_locks(plan: &PhysicalPlan) -> Vec<(TableId, LockMode)> {
    // EXPLAIN formats its input without executing scans or mutations.
    if matches!(plan.root(), PhysicalPlanNode::Explain { .. }) {
        return Vec::new();
    }
    let mut locks = BTreeMap::new();
    for node in plan.iter() {
        let (table, mode) = match node {
            PhysicalPlanNode::CreateIndex { table, .. }
            | PhysicalPlanNode::InsertValues { table, .. }
            | PhysicalPlanNode::Update { table, .. }
            | PhysicalPlanNode::Delete { table, .. } => (table, LockMode::Exclusive),
            PhysicalPlanNode::FullTableScan { table }
            | PhysicalPlanNode::PrimaryKeyRangeScan { table, .. } => (table, LockMode::Shared),
            PhysicalPlanNode::SecondaryIndexScan { scan } => (&scan.table, LockMode::Shared),
            _ => continue,
        };
        locks
            .entry(TableId::from(table.table_id))
            .and_modify(|held| {
                if mode == LockMode::Exclusive {
                    *held = mode;
                }
            })
            .or_insert(mode);
    }
    locks.into_iter().collect()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::core::access::CatalogRead;

    #[test]
    fn plans_request_the_strongest_table_mode_and_explain_requests_none() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        Session::new(&database)
            .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, value INT);")
            .unwrap();
        let table_id = database.table_schema_by_name("users").unwrap().table_id.into();
        for (sql, mode) in [
            ("SELECT id FROM users;", Some(LockMode::Shared)),
            ("INSERT INTO users (id, value) VALUES (1, 1);", Some(LockMode::Exclusive)),
            ("UPDATE users SET value = 2 WHERE id == 1;", Some(LockMode::Exclusive)),
            ("DELETE FROM users WHERE id == 1;", Some(LockMode::Exclusive)),
            ("CREATE INDEX idx_users_value ON users (value);", Some(LockMode::Exclusive)),
            ("SELECT 1;", None),
            ("EXPLAIN SELECT id FROM users;", None),
            ("EXPLAIN UPDATE users SET value = 2;", None),
        ] {
            let SqlItem::Statement(statement) = Parser::new(sql).item().unwrap() else {
                panic!("expected statement");
            };
            let plan = Planner::new(&database).plan_physical_statement(&statement).unwrap();
            let expected = mode.map(|mode| (table_id, mode)).into_iter().collect::<Vec<_>>();
            assert_eq!(plan_table_locks(&plan), expected, "{sql}");
        }
    }
}

fn is_no_active_transaction(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Internal(InternalError::InvariantViolation(
            InvariantViolation::NoActiveTransaction
        ))
    )
}

fn transaction_poisoned(txn_id: u64) -> StorageError {
    StorageError::Internal(InternalError::InvariantViolation(
        InvariantViolation::TransactionPoisoned { txn_id },
    ))
}
