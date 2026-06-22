//! Physical query execution.
//!
//! The executor consumes [`PhysicalPlan`] trees produced by the planner and
//! turns them into either a lazy stream of table records or an immediate
//! side-effect result such as rows affected or schema changed. Row-producing
//! operators are deliberately iterator-based: scans, filters, projections,
//! limits, and offsets do their work as the caller pulls rows from the returned
//! [`RowStream`].
//!
//! This module is also responsible for evaluating planned scalar expressions
//! against encoded table records and shaping inserted values into the target
//! table layout before handing the write to storage.

use crate::{
    core::{Database, OwnedTableRecord as TableRecord, Value, error::StorageError},
    planner::PhysicalPlan,
    sql_parser::parser::op::Op,
};

mod expression;

pub use expression::evaluate_expression;
#[cfg(test)]
use expression::record_from_values;
use expression::{
    EvaluationContext, empty_record, evaluate_expressions, evaluate_value, execute_insert_values,
    execute_values, offset_rows,
};

/// Errors that can occur while executing a physical query plan.
///
/// Executor errors cover storage failures, invalid encoded tuple data, row
/// shape mismatches, expression type errors, arithmetic failures, and operator
/// shapes that the current execution engine cannot run yet.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    /// A lower-level storage or catalog operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    /// Encoded tuple bytes could not be parsed into typed values.
    #[error("invalid tuple bytes: {0}")]
    InvalidTuple(#[source] std::io::Error),
    /// A planned column ordinal did not exist in the input tuple.
    #[error("column {column} ordinal {ordinal} is out of bounds for tuple with {len} values")]
    ColumnOrdinalOutOfBounds {
        /// Column name used in diagnostics.
        column: String,
        /// Requested zero-based tuple position.
        ordinal: usize,
        /// Number of values available in the tuple.
        len: usize,
    },
    /// A `WHERE` predicate produced a value other than `TRUE` or `FALSE`.
    #[error("filter predicate evaluated to non-boolean value: {value:?}")]
    NonBooleanPredicate {
        /// Value produced by the predicate expression.
        value: Value,
    },
    /// A unary operator was applied to a value type the executor does not support.
    #[error("unsupported unary expression: {op} {value:?}")]
    UnsupportedUnary {
        /// Operator being evaluated.
        op: Op,
        /// Operand value rejected by the operator.
        value: Value,
    },
    /// A binary operator was applied to value types the executor does not support.
    #[error("unsupported binary expression: {left:?} {op} {right:?}")]
    UnsupportedBinary {
        /// Left operand value.
        left: Value,
        /// Operator being evaluated.
        op: Op,
        /// Right operand value.
        right: Value,
    },
    /// A comparison operator received operands with different value types.
    #[error(
        "type mismatch in comparison {left:?} {op} {right:?}: expected right operand to be {expected}, got {actual}"
    )]
    ComparisonTypeMismatch {
        /// Left operand value that determines the required comparison type.
        left: Value,
        /// Comparison operator being evaluated.
        op: Op,
        /// Right operand value rejected by the comparison.
        right: Value,
        /// Type required for the right operand.
        expected: &'static str,
        /// Type of the rejected right operand.
        actual: &'static str,
    },
    /// A logical expression received a non-boolean operand.
    #[error("{op} expected a boolean operand, got {value:?}")]
    NonBooleanLogicalOperand {
        /// Logical operator being evaluated.
        op: Op,
        /// Operand value rejected by the operator.
        value: Value,
    },
    /// Integer arithmetic overflowed.
    #[error("integer overflow while evaluating operator {op}")]
    IntegerOverflow {
        /// Arithmetic operator whose checked integer operation overflowed.
        op: Op,
    },
    /// A division expression used zero as the divisor.
    #[error("division by zero")]
    DivisionByZero,
    /// A row operator received a non-row-producing child plan.
    #[error("{operator} expected its input plan to return rows")]
    ExpectedRows {
        /// Operator that requested rows from its child.
        operator: &'static str,
    },
    /// The physical operator is planned but not implemented by the executor.
    #[error("{operator} is not supported yet")]
    UnsupportedOperator {
        /// Name of the unsupported physical operator.
        operator: &'static str,
    },
    /// An inserted value row did not match the number of target columns.
    #[error("insert row has {values} values for {columns} columns")]
    InsertColumnValueCount {
        /// Number of target columns in the insert.
        columns: usize,
        /// Number of values supplied by the row.
        values: usize,
    },
}

/// Result type returned by executor operations.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Lazy stream of records produced by a row-returning plan.
///
/// Individual rows can fail while the stream is being consumed, for example if
/// a later scanned page cannot be read or a downstream expression fails for a
/// specific record.
pub type RowStream = Box<dyn Iterator<Item = ExecutorResult<TableRecord>>>;

/// Result of executing one physical plan.
pub enum ExecutionOutput {
    /// Textual physical plan produced by `EXPLAIN`.
    Explain(String),
    /// A lazy stream of result rows.
    Rows {
        /// Result rows yielded on demand.
        rows: RowStream,
    },
    /// Number of table rows changed by a data-modification statement.
    RowsAffected(u64),
    /// A schema-level side effect completed.
    SchemaAffected,
    /// A non-planned command completed.
    CommandOk,
}

impl ExecutionOutput {
    /// Extracts the row stream from this output.
    ///
    /// Row operators call this when they expect their child plan to produce
    /// rows. Non-row outputs become [`ExecutorError::ExpectedRows`] tagged with
    /// the requesting operator name.
    pub fn into_rows(self, operator: &'static str) -> ExecutorResult<RowStream> {
        match self {
            Self::Explain(_) => Err(ExecutorError::ExpectedRows { operator }),
            Self::Rows { rows } => Ok(rows),
            Self::RowsAffected(_) | Self::SchemaAffected | Self::CommandOk => {
                Err(ExecutorError::ExpectedRows { operator })
            }
        }
    }
}

impl std::fmt::Debug for ExecutionOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Explain(plan) => f.debug_tuple("Explain").field(plan).finish(),
            Self::Rows { .. } => f.debug_struct("Rows").field("rows", &"<row stream>").finish(),
            Self::RowsAffected(count) => f.debug_tuple("RowsAffected").field(count).finish(),
            Self::SchemaAffected => f.write_str("SchemaAffected"),
            Self::CommandOk => f.write_str("CommandOk"),
        }
    }
}

impl std::fmt::Display for ExecutionOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionOutput::Explain(plan) => write!(f, "{plan}"),
            ExecutionOutput::Rows { .. } => write!(f, "Query returned rows."),
            ExecutionOutput::RowsAffected(count) => {
                write!(f, "{count} rows affected.")
            }
            ExecutionOutput::SchemaAffected => write!(f, "Schema affected."),
            ExecutionOutput::CommandOk => write!(f, "Command executed."),
        }
    }
}

/// Executes physical query plans against a database handle.
///
/// The executor borrows a [`Database`] and performs catalog, table, and index
/// operations through that handle. It owns no transaction state; mutation
/// ordering is encoded directly in each operator implementation.
pub struct Executor<'db> {
    database: &'db Database,
}

impl<'db> Executor<'db> {
    /// Creates an executor that runs plans against `database`.
    pub fn new(database: &'db Database) -> Self {
        Self { database }
    }

    /// Executes a physical plan and returns its output.
    ///
    /// Row-producing operators return immediately with a lazy [`RowStream`].
    /// The underlying scan, filter, projection, limit, or offset work is then
    /// performed as the caller consumes that stream. DDL and insert operators
    /// perform their side effects before returning.
    pub fn execute(&mut self, plan: PhysicalPlan) -> ExecutorResult<ExecutionOutput> {
        match plan {
            PhysicalPlan::Explain { input } => Ok(ExecutionOutput::Explain(input.to_string())),
            PhysicalPlan::CreateTable { name, schema } => {
                self.database.create_table(&name, schema)?;
                Ok(ExecutionOutput::SchemaAffected)
            }
            PhysicalPlan::CreateIndex { name, table, columns } => {
                let column_names: Vec<&str> = columns.iter().map(|col| col.name.as_str()).collect();
                self.database.create_index(&name, &table.name, &column_names)?;
                Ok(ExecutionOutput::SchemaAffected)
            }
            PhysicalPlan::Values { rows } => execute_values(rows),
            PhysicalPlan::InsertValues { table, columns, values } => {
                execute_insert_values(self.database, table, columns, values)
            }
            PhysicalPlan::OneRow => Ok(ExecutionOutput::Rows {
                rows: Box::new(std::iter::once_with(|| empty_record(0))),
            }),
            PhysicalPlan::FullTableScan { table } => {
                let rows =
                    self.database.scan_table(&table)?.map(|record| record.map_err(Into::into));
                Ok(ExecutionOutput::Rows { rows: Box::new(rows) })
            }
            PhysicalPlan::Filter { input, predicate } => {
                let output_inner = self.execute(*input)?;
                let rows = output_inner.into_rows("FILTER")?.filter_map(move |row| match row {
                    Ok(row) => {
                        let result = EvaluationContext::from_record(&row)
                            .and_then(|context| evaluate_value(&predicate, &context));
                        match result {
                            Ok(Value::Boolean(true)) => Some(Ok(row)),
                            Ok(Value::Boolean(false)) => None,
                            Ok(value) => Some(Err(ExecutorError::NonBooleanPredicate { value })),
                            Err(error) => Some(Err(error)),
                        }
                    }
                    Err(error) => Some(Err(error)),
                });
                Ok(ExecutionOutput::Rows { rows: Box::new(rows) })
            }
            PhysicalPlan::Sort { input: _, terms: _ } => {
                // TODO: Change tuple serialization format to allow value comparison from raw byte slices
                Err(ExecutorError::UnsupportedOperator { operator: "SORT" })
            }
            PhysicalPlan::Project { input, expressions } => {
                let output_inner = self.execute(*input)?;
                let rows = output_inner
                    .into_rows("PROJECT")?
                    .map(move |row| row.and_then(|row| evaluate_expressions(&expressions, &row)));
                Ok(ExecutionOutput::Rows { rows: Box::new(rows) })
            }

            PhysicalPlan::Offset { input, offset } => {
                let output_inner = self.execute(*input)?;
                // TODO: Make `offset` a usize value.
                let offset = offset as usize;
                let rows = offset_rows(output_inner.into_rows("OFFSET")?, offset);
                Ok(ExecutionOutput::Rows { rows })
            }
            PhysicalPlan::Limit { input, limit } => {
                let output_inner = self.execute(*input)?;
                // TODO: Make `limit` a usize value.
                let limit = limit as usize;
                let rows = Box::new(output_inner.into_rows("LIMIT")?.take(limit));
                Ok(ExecutionOutput::Rows { rows })
            }
        }
    }
}

#[cfg(test)]
mod tests;
