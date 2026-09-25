//! Physical query execution.
//!
//! The executor consumes [`PhysicalPlan`] trees produced by the planner and
//! turns them into either a stream of executor rows or an immediate side-effect
//! result such as rows affected or schema changed. Row-producing
//! operators are deliberately iterator-based: scans, filters, projections,
//! limits, and offsets do their work as the caller pulls rows from the returned
//! [`RowStream`].
//!
//! This module is also responsible for evaluating executable scalar expressions
//! against slot-addressed row values and shaping inserted values into the target
//! table layout before handing the write to storage.

use crate::{
    core::{
        CatalogId, OwnedTableRecord, TableKey, Tuple, TupleRef, Value, ValueRef,
        access::RelationalAccess,
        error::{StorageError, StorageResult},
    },
    planner::{NodeId, PhysicalPlan, PhysicalPlanNode, RelationId},
    sql_parser::parser::op::Op,
};

mod expression;

pub use expression::evaluate_expression;
use expression::{
    EvaluationContext, empty_record, evaluate_expressions, evaluate_value, execute_delete,
    execute_insert_values, execute_update, execute_values, offset_rows,
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
    /// A logical evaluator received a non-logical operator.
    #[error("invalid logical operator: {op}")]
    InvalidLogicalOperator { op: Op },
    /// An ordering evaluator received a non-ordering operator.
    #[error("invalid ordering operator: {op}")]
    InvalidOrderingOperator { op: Op },
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
    /// A mutation input row did not retain the target relation's storage locator.
    #[error("row does not contain a locator for relation {relation:?}")]
    MissingRowLocator {
        /// Query-local target relation.
        relation: RelationId,
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

/// Stable storage identity retained alongside an executor row's values.
#[derive(Debug, Clone)]
pub struct RowLocator {
    relation: RelationId,
    table_id: CatalogId,
    record: OwnedTableRecord,
}

impl RowLocator {
    /// Returns the query-local relation this record came from.
    pub fn relation(&self) -> RelationId {
        self.relation
    }

    /// Returns the catalog table containing the record.
    pub fn table_id(&self) -> CatalogId {
        self.table_id
    }

    /// Returns the table key of the source record.
    pub fn table_key(&self) -> TableKey {
        self.record.table_key
    }

    pub(crate) fn record(&self) -> &OwnedTableRecord {
        &self.record
    }
}

/// Row produced by the query executor.
///
/// Values are independent of storage layout and can represent projections or
/// future joined rows. Source records needed by mutations are retained
/// separately as relation-specific [`RowLocator`] values.
#[derive(Debug, Clone)]
pub struct ExecutorRow {
    values: Vec<Value>,
    locators: Vec<RowLocator>,
}

impl ExecutorRow {
    /// Creates a synthetic row with no storage locators.
    pub fn from_values(values: Vec<Value>) -> Self {
        Self { values, locators: Vec::new() }
    }

    fn from_table_record(
        relation: RelationId,
        table_id: CatalogId,
        record: OwnedTableRecord,
    ) -> ExecutorResult<Self> {
        let values =
            Tuple::from_bytes(&record.record).map_err(ExecutorError::InvalidTuple)?.into_values();
        let locator = RowLocator { relation, table_id, record };
        Ok(Self { values, locators: vec![locator] })
    }

    /// Returns this row's values in physical slot order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns all source-row locators retained through relational operators.
    pub fn locators(&self) -> &[RowLocator] {
        &self.locators
    }

    /// Returns the source record for a query-local relation, if present.
    pub fn locator(&self, relation: RelationId) -> Option<&RowLocator> {
        self.locators.iter().find(|locator| locator.relation == relation)
    }

    pub(crate) fn with_values(&self, values: Vec<Value>) -> Self {
        Self { values, locators: self.locators.clone() }
    }

    pub(crate) fn into_with_values(self, values: Vec<Value>) -> Self {
        Self { values, locators: self.locators }
    }

    /// Executes `f` with this row encoded in the storage tuple format.
    pub fn with_record<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let values = self.values.iter().map(ValueRef::from).collect::<Vec<_>>();
        let record = TupleRef::new(&values).to_bytes()?;
        Ok(f(&record))
    }
}

impl std::fmt::Display for ExecutorRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for value in &self.values {
            write!(f, "{value}\t")?;
        }
        Ok(())
    }
}

/// Internal iterator used while an execution operator is being drained.
pub(crate) type RowStream = Box<dyn Iterator<Item = ExecutorResult<ExecutorRow>>>;

fn collect_rows(
    rows: impl Iterator<Item = ExecutorResult<ExecutorRow>>,
) -> Vec<ExecutorResult<ExecutorRow>> {
    rows.collect()
}

/// Result of executing one physical plan.
pub enum ExecutionOutput {
    /// Textual physical plan produced by `EXPLAIN`.
    Explain(String),
    /// Fully materialized result rows owned by the completed execution.
    Rows {
        /// Display names of the result columns, in row-value order.
        columns: Vec<String>,
        /// Rows drained before the statement transaction is finalized.
        rows: Vec<ExecutorResult<ExecutorRow>>,
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
    pub(crate) fn into_rows(self, operator: &'static str) -> ExecutorResult<RowStream> {
        match self {
            Self::Explain(_) => Err(ExecutorError::ExpectedRows { operator }),
            Self::Rows { rows, .. } => Ok(Box::new(rows.into_iter())),
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

/// Executes physical query plans through a relational gateway.
///
/// The executor owns no transaction state; the caller supplies an access handle
/// with the transaction and leases required by the plan.
pub struct Executor<'txn, R>
where
    R: RelationalAccess,
{
    transaction: &'txn R,
}

impl<'txn, R> Executor<'txn, R>
where
    R: RelationalAccess,
{
    /// Creates an executor scoped to an active transaction.
    pub(crate) fn in_transaction(transaction: &'txn R) -> Self {
        Self { transaction }
    }

    /// Executes a physical plan and returns its output.
    ///
    /// Row results are materialized before returning and include the physical
    /// plan's output column names, even when no rows match. Manually built plans
    /// without an output schema return an empty column list.
    pub fn execute(&mut self, plan: impl Into<PhysicalPlan>) -> ExecutorResult<ExecutionOutput> {
        let plan = plan.into();
        if let PhysicalPlanNode::Explain { input } = plan.root() {
            return Ok(ExecutionOutput::Explain(plan.display_node(*input).to_string()));
        }
        let (nodes, root, output_schema) = plan.into_parts();
        let mut nodes = nodes.into_iter().map(Some).collect::<Vec<_>>();
        let mut output = self.execute_node(&mut nodes, root)?;
        if let ExecutionOutput::Rows { columns, .. } = &mut output {
            *columns = output_schema
                .map(|schema| schema.columns.into_iter().map(|column| column.name).collect())
                .unwrap_or_default();
        }
        Ok(output)
    }

    fn execute_node(
        &mut self,
        nodes: &mut [Option<PhysicalPlanNode>],
        node_id: NodeId,
    ) -> ExecutorResult<ExecutionOutput> {
        let Some(node) = nodes.get_mut(node_id.index()).and_then(Option::take) else {
            return Err(ExecutorError::UnsupportedOperator { operator: "INVALID PLAN" });
        };
        match node {
            PhysicalPlanNode::Explain { .. } => {
                Err(ExecutorError::UnsupportedOperator { operator: "NESTED EXPLAIN" })
            }
            PhysicalPlanNode::CreateTable { name, schema } => {
                self.transaction.create_table(&name, schema)?;
                Ok(ExecutionOutput::SchemaAffected)
            }
            PhysicalPlanNode::CreateIndex { name, table, columns } => {
                let column_names: Vec<&str> = columns.iter().map(|col| col.name.as_str()).collect();
                self.transaction.create_index(&name, &table.name, &column_names)?;
                Ok(ExecutionOutput::SchemaAffected)
            }
            PhysicalPlanNode::Values { rows } => execute_values(rows),
            PhysicalPlanNode::InsertValues { table, columns, values } => {
                execute_insert_values(self.transaction, table, columns, values)
            }
            PhysicalPlanNode::Update { relation, table, assignments, input } => {
                let output_inner = self.execute_node(nodes, input)?;
                execute_update(
                    self.transaction,
                    relation,
                    table,
                    assignments,
                    output_inner.into_rows("UPDATE")?,
                )
            }
            PhysicalPlanNode::Delete { relation, table, input } => {
                let output_inner = self.execute_node(nodes, input)?;
                execute_delete(self.transaction, relation, table, output_inner.into_rows("DELETE")?)
            }
            PhysicalPlanNode::OneRow => Ok(ExecutionOutput::Rows {
                columns: Vec::new(),
                rows: collect_rows(std::iter::once_with(|| Ok(empty_record()))),
            }),
            PhysicalPlanNode::FullTableScan { relation, table } => {
                let table_id = table.table_id;
                let rows = self.transaction.scan_table(&table)?.map(move |record| {
                    let record = record.map_err(ExecutorError::from)?;
                    ExecutorRow::from_table_record(relation, table_id, record)
                });
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
            PhysicalPlanNode::PrimaryKeyRangeScan { relation, table, range } => {
                let table_id = table.table_id;
                let rows = self.transaction.scan_table_range(&table, range)?.map(move |record| {
                    let record = record.map_err(ExecutorError::from)?;
                    ExecutorRow::from_table_record(relation, table_id, record)
                });
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
            PhysicalPlanNode::SecondaryIndexScan { scan } => {
                let table_id = scan.table.table_id;
                let relation = scan.relation;
                let rows = self
                    .transaction
                    .scan_index(&scan.table, &scan.index, scan.key_range)?
                    .map(move |record| {
                        let record = record.map_err(ExecutorError::from)?;
                        ExecutorRow::from_table_record(relation, table_id, record)
                    });
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
            PhysicalPlanNode::Filter { input, predicate } => {
                let output_inner = self.execute_node(nodes, input)?;
                let rows = output_inner.into_rows("FILTER")?.filter_map(move |row| match row {
                    Ok(row) => {
                        let context = EvaluationContext::from_row(&row);
                        let result = evaluate_value(&predicate, &context);
                        match result {
                            Ok(Value::Boolean(true)) => Some(Ok(row)),
                            Ok(Value::Boolean(false)) => None,
                            Ok(value) => Some(Err(ExecutorError::NonBooleanPredicate { value })),
                            Err(error) => Some(Err(error)),
                        }
                    }
                    Err(error) => Some(Err(error)),
                });
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
            PhysicalPlanNode::Sort { input: _, terms: _ } => {
                // TODO: Change tuple serialization format to allow value comparison from raw byte slices
                Err(ExecutorError::UnsupportedOperator { operator: "SORT" })
            }
            PhysicalPlanNode::Project { input, expressions } => {
                let output_inner = self.execute_node(nodes, input)?;
                let rows = output_inner
                    .into_rows("PROJECT")?
                    .map(move |row| row.and_then(|row| evaluate_expressions(&expressions, row)));
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }

            PhysicalPlanNode::Offset { input, offset } => {
                let output_inner = self.execute_node(nodes, input)?;
                // TODO: Make `offset` a usize value.
                let offset = offset as usize;
                let rows = offset_rows(output_inner.into_rows("OFFSET")?, offset);
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
            PhysicalPlanNode::Limit { input, limit } => {
                let output_inner = self.execute_node(nodes, input)?;
                // TODO: Make `limit` a usize value.
                let limit = limit as usize;
                let rows = output_inner.into_rows("LIMIT")?.take(limit);
                Ok(ExecutionOutput::Rows { columns: Vec::new(), rows: collect_rows(rows) })
            }
        }
    }
}

#[cfg(test)]
mod tests;
