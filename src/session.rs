//! SQL session execution and transaction policy.
//!
//! A session is the top-level SQL execution context for one database handle. It
//! dispatches parsed SQL items, keeps explicit transaction state, and preserves
//! the implicit transaction behavior for standalone mutating statements.

use thiserror::Error;

use crate::{
    core::{
        Database,
        error::{InternalError, InvariantViolation, StorageError},
        transaction_manager::TransactionSavepoint,
    },
    error::DatabaseError,
    executor::{ExecutionOutput, Executor},
    planner::{PhysicalPlan, Planner},
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
    #[cfg(test)]
    fail_next_savepoint_rollback: bool,
}

impl<'db> Session<'db> {
    /// Creates a new session over `database`.
    pub fn new(database: &'db Database) -> Self {
        Self {
            database,
            active_txn: None,
            #[cfg(test)]
            fail_next_savepoint_rollback: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_savepoint_rollback_for_test(&mut self) {
        self.fail_next_savepoint_rollback = true;
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
        let mutating = statement_is_mutating(&statement);
        let plan = Planner::new(self.database).plan_statement(&statement)?;

        if !mutating {
            return self.execute_plan(plan.physical);
        }

        if let Some(txn_id) = self.active_txn {
            self.execute_explicit_transaction_statement(txn_id, plan.physical)
        } else {
            self.execute_implicit_transaction(plan.physical)
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
                self.sync_active_transaction(txn_id);
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
                self.sync_active_transaction(txn_id);
                Err(error.into())
            }
        }
    }

    fn execute_plan<'sql>(
        &self,
        plan: PhysicalPlan,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        Ok(Executor::new(self.database).execute(plan)?)
    }

    fn execute_explicit_transaction_statement<'sql>(
        &mut self,
        txn_id: u64,
        plan: PhysicalPlan,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let savepoint = self.database.statement_savepoint(txn_id)?;
        match Executor::new(self.database).execute(plan) {
            Ok(output) => {
                if self.database.transaction_is_poisoned(txn_id)? {
                    return self.rollback_failed_explicit_transaction_statement(
                        savepoint,
                        transaction_poisoned(txn_id).into(),
                    );
                }
                Ok(output)
            }
            Err(error) => {
                #[cfg(test)]
                if self.fail_next_savepoint_rollback {
                    self.fail_next_savepoint_rollback = false;
                    self.database.force_next_lsn_exhausted_for_test();
                }

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
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let txn_id = self.database.begin_transaction()?;
        match Executor::new(self.database).execute(plan) {
            Ok(output) => match self.database.commit_transaction(txn_id) {
                Ok(()) => Ok(output),
                Err(commit_error) => {
                    if let Err(rollback_error) = self.database.rollback_transaction(txn_id)
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

    fn sync_active_transaction(&mut self, txn_id: u64) {
        if self.database.active_transaction_id() != Some(txn_id) {
            self.active_txn = None;
        }
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if let Some(txn_id) = self.active_txn.take() {
            let _ = self.database.rollback_transaction(txn_id);
        }
    }
}

fn statement_is_mutating(statement: &Statement<'_>) -> bool {
    match statement {
        Statement::CreateTable(_) | Statement::CreateIndex(_) | Statement::Insert(_) => true,
        Statement::Select(_) | Statement::Explain(_) => false,
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
