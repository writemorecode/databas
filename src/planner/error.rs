//! Errors reported by SQL binding and physical planning.

use thiserror::Error;

use crate::core::error::StorageError;

pub type PlannerResult<T> = Result<T, PlannerError>;

/// Errors that can occur while converting parsed SQL into a plan.
///
/// Planner errors are limited to catalog binding, statement-shape validation,
/// unsupported syntax, and catalog access failures. Runtime errors that depend
/// on row contents are reported by the executor or storage layer instead.
#[derive(Debug, Error)]
pub enum PlannerError {
    /// A statement referenced a table that does not exist in the catalog.
    #[error("table not found: {name}")]
    TableNotFound { name: String },
    /// A qualified column referenced a table that is not an input to the expression.
    #[error("table not in scope: {table}")]
    TableNotInScope { table: String },
    /// A statement referenced a column that is not present in any input relation.
    #[error("column {column} not found")]
    ColumnNotFound { column: String },
    /// An `INSERT` column list named the same column more than once.
    #[error("duplicate insert column: {column}")]
    DuplicateInsertColumn { column: String },
    /// An `UPDATE` assignment list named the same column more than once.
    #[error("duplicate update column: {column}")]
    DuplicateUpdateColumn { column: String },
    /// A `CREATE INDEX` column list named the same column more than once.
    #[error("duplicate index column: {column}")]
    DuplicateIndexColumn { column: String },
    /// An `UPDATE` attempted to modify a primary-key column.
    #[error("cannot update primary key column: {column}")]
    PrimaryKeyUpdate { column: String },
    /// A values row does not provide exactly one value for each target column.
    #[error("insert row has {values} values for {columns} columns")]
    InsertColumnValueCount { columns: usize, values: usize },
    /// The parser accepted a statement kind the planner cannot lower.
    #[error("unsupported statement: {statement}")]
    UnsupportedStatement { statement: String },
    /// The planner cannot lower this expression in the current context.
    #[error("unsupported expression: {expression}")]
    UnsupportedExpression { expression: String },
    /// Aggregate functions are parsed but not yet planned.
    #[error("unsupported aggregate function: {function}")]
    UnsupportedAggregate { function: String },
    /// A wildcard appeared outside the projection list.
    #[error("wildcard is only supported in SELECT projection")]
    UnsupportedWildcardPosition,
    /// A wildcard projection was used without a table to expand against.
    #[error("wildcard projection requires a FROM table")]
    WildcardRequiresTable,
    /// Physical planning found an invalid or reused logical node reference.
    #[error("invalid logical plan")]
    InvalidLogicalPlan,
    /// A bound column was not produced by the physical operator's input.
    #[error("column {column} is not available from the plan input")]
    ColumnNotInInput { column: String },
    /// Physical planning found an insert input shape it cannot execute.
    #[error("invalid insert input: expected VALUES")]
    InvalidInsertInput,
    /// Storage or catalog access failed while planning.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
