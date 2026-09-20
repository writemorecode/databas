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
        CatalogId, OwnedTableRecord, TableKey, TableRecord as BorrowedTableRecord, Transaction,
        Tuple, Value,
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
        record: BorrowedTableRecord,
    ) -> ExecutorResult<Self> {
        let record = record.to_owned_record()?;
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

    /// Executes `f` with this row encoded in the storage tuple format.
    pub fn with_record<R>(&self, f: impl FnOnce(&[u8]) -> R) -> StorageResult<R> {
        let record = Tuple::new(self.values.clone()).to_bytes()?;
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
            Self::Rows { rows } => Ok(Box::new(rows.into_iter())),
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

/// Executes physical query plans through a transaction-scoped relational gateway.
///
/// The executor owns no transaction state; the caller supplies an active
/// transaction with the leases required by the plan.
pub struct Executor<'txn, 'db> {
    transaction: &'txn Transaction<'db>,
}

impl<'txn, 'db> Executor<'txn, 'db> {
    /// Creates an executor scoped to an active transaction.
    pub(crate) fn in_transaction(transaction: &'txn Transaction<'db>) -> Self {
        Self { transaction }
    }

    /// Executes a physical plan and returns its output.
    ///
    /// Row-producing operators return immediately with a lazy [`RowStream`].
    /// The underlying scan, filter, projection, limit, or offset work is then
    /// performed as the caller consumes that stream. DDL and insert operators
    /// perform their side effects before returning.
    pub fn execute(&mut self, plan: impl Into<PhysicalPlan>) -> ExecutorResult<ExecutionOutput> {
        let plan = plan.into();
        if let PhysicalPlanNode::Explain { input } = plan.root() {
            return Ok(ExecutionOutput::Explain(plan.display_node(*input).to_string()));
        }
        let (nodes, root) = plan.into_parts();
        let mut nodes = nodes.into_iter().map(Some).collect::<Vec<_>>();
        self.execute_node(&mut nodes, root)
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
                rows: collect_rows(std::iter::once_with(|| Ok(empty_record()))),
            }),
            PhysicalPlanNode::FullTableScan { relation, table } => {
                let table_id = table.table_id;
                let rows = self.transaction.scan_table(&table)?.map(move |record| {
                    let record = record.map_err(ExecutorError::from)?;
                    ExecutorRow::from_table_record(relation, table_id, record)
                });
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
            }
            PhysicalPlanNode::PrimaryKeyRangeScan { relation, table, range } => {
                let table_id = table.table_id;
                let rows = self.transaction.scan_table_range(&table, range)?.map(move |record| {
                    let record = record.map_err(ExecutorError::from)?;
                    ExecutorRow::from_table_record(relation, table_id, record)
                });
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
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
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
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
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
            }
            PhysicalPlanNode::Sort { input: _, terms: _ } => {
                // TODO: Change tuple serialization format to allow value comparison from raw byte slices
                Err(ExecutorError::UnsupportedOperator { operator: "SORT" })
            }
            PhysicalPlanNode::Project { input, expressions } => {
                let output_inner = self.execute_node(nodes, input)?;
                let rows = output_inner
                    .into_rows("PROJECT")?
                    .map(move |row| row.and_then(|row| evaluate_expressions(&expressions, &row)));
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
            }

            PhysicalPlanNode::Offset { input, offset } => {
                let output_inner = self.execute_node(nodes, input)?;
                // TODO: Make `offset` a usize value.
                let offset = offset as usize;
                let rows = offset_rows(output_inner.into_rows("OFFSET")?, offset);
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
            }
            PhysicalPlanNode::Limit { input, limit } => {
                let output_inner = self.execute_node(nodes, input)?;
                // TODO: Make `limit` a usize value.
                let limit = limit as usize;
                let rows = output_inner.into_rows("LIMIT")?.take(limit);
                Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::{
        core::{
            DataType, Database, Tuple, Value,
            error::{ConstraintError, StorageError},
        },
        error::DatabaseError,
        planner::{ExecExpr, PhysicalPlan, PhysicalPlanNode},
        session::Session,
        sql_parser::parser::op::Op,
    };

    fn database() -> (TempDir, Database) {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        (dir, database)
    }

    fn execute<'sql>(
        database: &Database,
        sql: &'sql str,
    ) -> Result<ExecutionOutput, DatabaseError<'sql>> {
        Session::new(database).execute_sql(sql)
    }

    fn row_values(row: &ExecutorRow) -> Vec<Value> {
        row.with_record(|bytes| Tuple::from_bytes(bytes).unwrap().into_values()).unwrap()
    }

    fn try_rows(output: ExecutionOutput) -> ExecutorResult<Vec<Vec<Value>>> {
        output.into_rows("TEST")?.map(|row| row.map(|row| row_values(&row))).collect()
    }

    fn query(database: &Database, sql: &str) -> Vec<Vec<Value>> {
        try_rows(execute(database, sql).unwrap()).unwrap()
    }

    #[test]
    fn values_evaluates_each_literal_row() {
        let (_dir, database) = database();
        let txn_id = database.begin_transaction().unwrap();
        let transaction = database.transaction(txn_id, Vec::new());
        let plan = PhysicalPlan::new(PhysicalPlanNode::Values {
            rows: vec![
                vec![ExecExpr::Literal(Value::Integer(10))],
                vec![ExecExpr::Literal(Value::Integer(20))],
            ],
        });

        let rows = Executor::in_transaction(&transaction)
            .execute(plan)
            .unwrap()
            .into_rows("TEST")
            .unwrap()
            .collect::<ExecutorResult<Vec<_>>>()
            .unwrap();

        assert_eq!(
            rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![vec![Value::Integer(10)], vec![Value::Integer(20)],]
        );

        drop(transaction);
        database.rollback_transaction(txn_id).unwrap();
    }

    #[test]
    fn select_without_from_evaluates_arithmetic() {
        let (_dir, database) = database();

        assert_eq!(
            query(&database, "SELECT 7 + 5, 7 - 5, 7 * 5, 10 / 5, -5, 1.5 + 0.5;"),
            vec![vec![
                Value::Integer(12),
                Value::Integer(2),
                Value::Integer(35),
                Value::Integer(2),
                Value::Integer(-5),
                Value::Float(2.0),
            ]]
        );
    }

    #[test]
    fn select_without_from_evaluates_boolean_expressions() {
        let (_dir, database) = database();

        assert_eq!(
            query(
                &database,
                "SELECT NOT FALSE, 1 < 2, 2 <= 2, 3 > 2, 3 >= 3, \
                 1 == 1, 1 != 2, TRUE AND TRUE, FALSE OR TRUE;",
            ),
            vec![vec![Value::Boolean(true); 9]]
        );
    }

    #[test]
    fn boolean_expressions_short_circuit() {
        let (_dir, database) = database();

        assert_eq!(
            query(&database, "SELECT FALSE AND 1 / 0 == 0, TRUE OR 1 / 0 == 0;"),
            vec![vec![Value::Boolean(false), Value::Boolean(true)]]
        );
    }

    #[test]
    fn invalid_expressions_return_precise_errors() {
        let (_dir, database) = database();

        assert!(matches!(
            try_rows(execute(&database, "SELECT 1 / 0;").unwrap()),
            Err(ExecutorError::DivisionByZero)
        ));
        assert!(matches!(
            try_rows(execute(&database, "SELECT 2147483647 + 1;").unwrap()),
            Err(ExecutorError::IntegerOverflow { op: Op::Add })
        ));
        assert!(matches!(
            try_rows(execute(&database, "SELECT 1 + 1.0;").unwrap()),
            Err(ExecutorError::UnsupportedBinary { .. })
        ));
        assert!(matches!(
            try_rows(execute(&database, "SELECT 1 == 1.0;").unwrap()),
            Err(ExecutorError::ComparisonTypeMismatch { .. })
        ));
        assert!(matches!(
            try_rows(execute(&database, "SELECT TRUE AND 1;").unwrap()),
            Err(ExecutorError::NonBooleanLogicalOperand { .. })
        ));
    }

    #[test]
    fn select_scans_filters_and_projects_rows() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
            .unwrap();
        execute(
            &database,
            "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 20), (2, 'Grace', 10), (3, 'Linus', 30);",
        )
        .unwrap();

        assert_eq!(
            query(
                &database,
                "SELECT name, score + 1 FROM users WHERE score > 10 AND name != 'Linus';"
            ),
            vec![vec![Value::String("Ada".into()), Value::Integer(21)]]
        );
    }

    #[test]
    fn select_supports_table_aliases_in_projection_and_filter() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
            .unwrap();
        execute(
            &database,
            "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 20), (2, 'Grace', 10), (3, 'Linus', 30);",
        )
        .unwrap();

        assert_eq!(
            query(&database, "SELECT u.name, u.score + 1 FROM users AS u WHERE u.score >= 20;"),
            vec![
                vec![Value::String("Ada".into()), Value::Integer(21)],
                vec![Value::String("Linus".into()), Value::Integer(31)],
            ]
        );
    }

    #[test]
    fn filter_rejects_non_boolean_predicates() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY);").unwrap();
        execute(&database, "INSERT INTO users (id) VALUES (1);").unwrap();

        assert!(matches!(
            try_rows(execute(&database, "SELECT id FROM users WHERE id;").unwrap()),
            Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
        ));
    }

    #[test]
    fn primary_and_secondary_index_scans_return_matching_rows() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
            .unwrap();
        execute(
            &database,
            "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 10), (2, 'Grace', 20), (3, 'Linus', 20), (4, 'Margaret', 30);",
        )
        .unwrap();
        execute(&database, "CREATE INDEX users_score ON users (score);").unwrap();
        execute(&database, "INSERT INTO users (id, name, score) VALUES (5, 'Ken', 20);").unwrap();

        assert_eq!(
            query(&database, "SELECT id FROM users WHERE id >= 2 AND id < 4;"),
            vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
        );
        assert_eq!(
            query(&database, "SELECT id FROM users WHERE score == 20 AND id > 2;"),
            vec![vec![Value::Integer(3)], vec![Value::Integer(5)]]
        );
        assert_eq!(
            query(&database, "SELECT id FROM users WHERE score >= 20 AND score < 30;"),
            vec![vec![Value::Integer(2)], vec![Value::Integer(3)], vec![Value::Integer(5)],]
        );
    }

    #[test]
    fn limit_and_offset_select_the_requested_window() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE numbers (id INT PRIMARY KEY);").unwrap();
        execute(&database, "INSERT INTO numbers (id) VALUES (1), (2), (3), (4);").unwrap();

        assert_eq!(
            query(&database, "SELECT id FROM numbers LIMIT 2 OFFSET 1;"),
            vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
        );
        assert!(query(&database, "SELECT id FROM numbers OFFSET 10;").is_empty());
        assert!(query(&database, "SELECT id FROM numbers LIMIT 0;").is_empty());
    }

    #[test]
    fn insert_maps_columns_to_the_table_schema() {
        let (_dir, database) = database();
        execute(
            &database,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT NULLABLE);",
        )
        .unwrap();

        assert!(matches!(
            execute(&database, "INSERT INTO users (name, id) VALUES ('Ada', 1);").unwrap(),
            ExecutionOutput::RowsAffected(1)
        ));
        assert_eq!(
            query(&database, "SELECT id, name, score FROM users;"),
            vec![vec![Value::Integer(1), Value::String("Ada".into()), Value::Null]]
        );
    }

    #[test]
    fn insert_enforces_nullability_and_column_types() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();

        assert!(matches!(
            execute(&database, "INSERT INTO users (id) VALUES (1);"),
            Err(DatabaseError::Executor(ExecutorError::Storage(StorageError::Constraint(
                ConstraintError::NullValue { column }
            )))) if column == "name"
        ));
        assert!(matches!(
            execute(&database, "INSERT INTO users (id, name) VALUES ('two', 'Grace');"),
            Err(DatabaseError::Executor(ExecutorError::Storage(StorageError::Constraint(
                ConstraintError::ColumnTypeMismatch {
                    column,
                    expected: DataType::Integer,
                    actual: "text",
                }
            )))) if column == "id"
        ));
        assert!(query(&database, "SELECT id FROM users;").is_empty());
    }

    #[test]
    fn failed_multi_row_insert_is_atomic() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();

        assert!(
            execute(&database, "INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 99);").is_err()
        );
        assert!(query(&database, "SELECT id FROM users;").is_empty());
    }

    #[test]
    fn update_evaluates_assignments_and_refreshes_indexes() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
            .unwrap();
        execute(&database, "CREATE INDEX users_name ON users (name);").unwrap();
        execute(
            &database,
            "INSERT INTO users (id, name, score) VALUES (1, 'Ada', 10), (2, 'Grace', 20);",
        )
        .unwrap();

        assert!(matches!(
            execute(
                &database,
                "UPDATE users SET name = 'Linus', score = score + 1 WHERE name == 'Ada';",
            )
            .unwrap(),
            ExecutionOutput::RowsAffected(1)
        ));
        assert!(query(&database, "SELECT id FROM users WHERE name == 'Ada';").is_empty());
        assert_eq!(
            query(&database, "SELECT id, score FROM users WHERE name == 'Linus';"),
            vec![vec![Value::Integer(1), Value::Integer(11)]]
        );
    }

    #[test]
    fn failed_multi_row_update_is_atomic() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, score INT);").unwrap();
        execute(&database, "INSERT INTO users (id, score) VALUES (1, 10), (2, 20);").unwrap();

        assert!(matches!(
            execute(&database, "UPDATE users SET score = 10 / (2 - id);"),
            Err(DatabaseError::Executor(ExecutorError::DivisionByZero))
        ));
        assert_eq!(
            query(&database, "SELECT score FROM users;"),
            vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]
        );
    }

    #[test]
    fn delete_removes_matching_rows_and_index_entries() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();
        execute(&database, "CREATE INDEX users_name ON users (name);").unwrap();
        execute(
            &database,
            "INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Ada'), (3, 'Grace');",
        )
        .unwrap();

        assert!(matches!(
            execute(&database, "DELETE FROM users WHERE name == 'Ada';").unwrap(),
            ExecutionOutput::RowsAffected(2)
        ));
        assert!(query(&database, "SELECT id FROM users WHERE name == 'Ada';").is_empty());
        assert_eq!(
            query(&database, "SELECT id FROM users WHERE name == 'Grace';"),
            vec![vec![Value::Integer(3)]]
        );
    }

    #[test]
    fn mutations_do_not_skip_rows_when_btree_pages_change() {
        let (_dir, database) = database();
        execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();
        let values = (1..=40)
            .map(|id| format!("({id}, '{}')", "x".repeat(40)))
            .collect::<Vec<_>>()
            .join(", ");
        execute(&database, &format!("INSERT INTO users (id, name) VALUES {values};")).unwrap();

        let large_name = "y".repeat(500);
        assert!(matches!(
            execute(&database, &format!("UPDATE users SET name = '{large_name}';")).unwrap(),
            ExecutionOutput::RowsAffected(40)
        ));
        assert!(matches!(
            execute(&database, "DELETE FROM users;").unwrap(),
            ExecutionOutput::RowsAffected(40)
        ));
        assert!(query(&database, "SELECT id FROM users;").is_empty());
    }
}
