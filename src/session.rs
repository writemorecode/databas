//! SQL session execution and transaction policy.
//!
//! A session is the top-level SQL execution context for one database handle. It
//! dispatches parsed SQL items, keeps explicit transaction state, and preserves
//! the implicit transaction behavior for standalone statements. Read scans use
//! shared table locks; mutations acquire or upgrade to exclusive locks before
//! execution. All locks remain transaction-owned until commit or rollback.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::error::DatabaseError;
use crate::executor::{ExecutionOutput, Executor};
use crate::planner::{PhysicalPlan, PhysicalPlanNode, Planner};
use crate::relational::error::StorageError;
use crate::relational::{ActiveTransaction, StatementTransactionMode};
use crate::relational::{Database, LockMode, TableId};
use crate::sql_parser::error::{SQLError, SQLErrorKind};
use crate::sql_parser::parser::stmt::Statement;
use crate::sql_parser::parser::{Command, Parser, SqlItem};

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
    active_txn: Option<ActiveTransaction<'db>>,
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
        if let Some(transaction) = self.active_txn.take() {
            transaction.close()?;
        }
        Ok(())
    }

    /// Whether this session has an explicit transaction open.
    pub(crate) fn has_active_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    /// Parses and executes all SQL items in order, returning the final result.
    /// Earlier row results are drained so their errors are not silently lost.
    pub fn execute_sql<'sql>(
        &mut self,
        sql: &'sql str,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        // Parse the whole request before executing any item, so malformed trailing
        // SQL cannot leave a partially executed transaction behind.
        let items = Parser::new(sql).collect::<Result<Vec<_>, _>>()?;
        let mut items = items.into_iter();
        let Some(first) = items.next() else {
            return Err(SQLError::new(SQLErrorKind::UnexpectedEnd, sql.len()).into());
        };

        let mut output = self.execute_item(first)?;
        for item in items {
            if let ExecutionOutput::Rows { rows, .. } = output {
                for row in rows {
                    row?;
                }
            }
            output = self.execute_item(item)?;
        }
        Ok(output)
    }

    /// Executes one parsed SQL item.
    fn execute_item<'sql>(
        &mut self,
        item: SqlItem<'sql>,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        match item {
            SqlItem::Statement(statement) => self.execute_statement(&statement),
            SqlItem::Command(command) => self.execute_command(command),
        }
    }

    fn execute_statement<'sql>(
        &mut self,
        statement: &Statement<'sql>,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let transaction_mode = statement_transaction_mode(statement);
        let plan = Planner::new(self.database).plan_physical_statement(statement)?;
        let table_locks = plan_table_locks(&plan);

        if let Some(transaction) = self.active_txn.as_mut() {
            let result = transaction.statement(transaction_mode, &table_locks, |access| {
                Executor::in_transaction(access).execute(plan).map_err(DatabaseError::from)
            });
            if !transaction.is_active() {
                self.active_txn = None;
            }
            result
        } else {
            self.execute_implicit_transaction(plan, &table_locks, transaction_mode)
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
        if let Some(transaction) = &self.active_txn {
            return Err(SessionError::TransactionAlreadyActive { txn_id: transaction.id() }.into());
        }
        self.active_txn = Some(self.database.begin(StatementTransactionMode::Ordinary)?);
        Ok(ExecutionOutput::CommandOk)
    }
    fn commit_transaction<'sql>(&mut self) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let transaction = self.active_txn.as_mut().ok_or(SessionError::NoActiveTransaction)?;
        let result = transaction.commit();
        if !transaction.is_active() {
            self.active_txn = None;
        }
        result?;
        Ok(ExecutionOutput::CommandOk)
    }
    fn rollback_transaction<'sql>(&mut self) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let transaction = self.active_txn.as_mut().ok_or(SessionError::NoActiveTransaction)?;
        let result = transaction.rollback();
        if !transaction.is_active() {
            self.active_txn = None;
        }
        result?;
        Ok(ExecutionOutput::CommandOk)
    }
    fn execute_implicit_transaction<'sql>(
        &self,
        plan: PhysicalPlan,
        table_locks: &[(TableId, LockMode)],
        mode: StatementTransactionMode,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        let mut transaction = self.database.begin(mode)?;
        let result = transaction.statement(mode, table_locks, |access| {
            Executor::in_transaction(access).execute(plan).map_err(DatabaseError::from)
        });
        match result {
            Ok(output) => match transaction.commit() {
                Ok(()) => Ok(output),
                Err(error) => {
                    transaction.close()?;
                    Err(error.into())
                }
            },
            Err(error) => {
                transaction.close()?;
                Err(error)
            }
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
            PhysicalPlanNode::FullTableScan { table, .. }
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
    use crate::relational::access::CatalogRead;

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
