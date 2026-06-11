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
//! against encoded table records, validating inserted rows against catalog
//! schemas, backfilling newly-created secondary indexes, and keeping existing
//! secondary indexes up to date when new rows are inserted.

use crate::{
    core::{
        DataType, Database, IndexSchema, TableRecord, TableSchema, Tuple, TupleView, Value,
        error::{InternalError, InvariantViolation, StorageError},
    },
    planner::{BoundColumn, PhysicalPlan, PlannedExpression},
    sql_parser::parser::op::Op,
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
    /// An inserted row left a non-nullable column as `NULL`.
    #[error("column {column} does not accept NULL values")]
    InsertNullConstraint {
        /// Column that rejected `NULL`.
        column: String,
    },
    /// An inserted value did not match its target column type.
    #[error("column {column} expects {expected:?}, got {actual:?}")]
    InsertTypeMismatch {
        /// Target column name.
        column: String,
        /// Column type recorded in the catalog.
        expected: DataType,
        /// Value rejected by the column.
        actual: Value,
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
            Self::RowsAffected(_) | Self::SchemaAffected => {
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
                self.execute_mutating_statement(|database| {
                    database.create_table(&name, schema)?;
                    Ok(ExecutionOutput::SchemaAffected)
                })
            }
            PhysicalPlan::CreateIndex { name, table, columns } => {
                self.execute_mutating_statement(|database| {
                    let column_names: Vec<&str> =
                        columns.iter().map(|col| col.name.as_str()).collect();
                    let index = database.create_index(&name, &table.name, &column_names)?;
                    backfill_index(database, &table, &index)?;
                    Ok(ExecutionOutput::SchemaAffected)
                })
            }
            PhysicalPlan::Values { rows } => execute_values(rows),
            PhysicalPlan::InsertValues { table, columns, values } => self
                .execute_mutating_statement(|database| {
                    execute_insert_values(database, table, columns, values)
                }),
            PhysicalPlan::OneRow => Ok(ExecutionOutput::Rows {
                rows: Box::new(std::iter::once_with(|| empty_record(0))),
            }),
            PhysicalPlan::FullTableScan { table } => {
                let table_cursor = self.database.table_cursor_by_name(&table.name)?;
                let mut tree_cursor = table_cursor.into_inner();
                let mut done = false;
                let rows = std::iter::from_fn(move || {
                    if done {
                        return None;
                    }

                    match tree_cursor.next_owned_record() {
                        Ok(Some(record)) => {
                            Some(TableRecord::try_from(record).map_err(ExecutorError::Storage))
                        }
                        Ok(None) => {
                            done = true;
                            None
                        }
                        Err(error) => {
                            done = true;
                            Some(Err(error.into()))
                        }
                    }
                });
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

    fn execute_mutating_statement(
        &self,
        execute: impl FnOnce(&Database) -> ExecutorResult<ExecutionOutput>,
    ) -> ExecutorResult<ExecutionOutput> {
        let txn_id = self.database.begin_transaction()?;
        match execute(self.database) {
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
                Err(error)
            }
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

/// Evaluates one planned scalar expression against a record.
///
/// The result is represented as a single-column [`TableRecord`] that preserves
/// the input record's row id. This is primarily useful for tests and callers
/// that need expression evaluation without building a full projection plan.
pub fn evaluate_expression(
    expression: &PlannedExpression,
    record: &TableRecord,
) -> ExecutorResult<TableRecord> {
    evaluate_expressions(std::slice::from_ref(expression), record)
}

/// Executes a `VALUES` plan as a stream of evaluated literal rows.
///
/// Each values row is evaluated against an empty synthetic record. The row's
/// position in the `VALUES` list becomes the result row id.
fn execute_values(rows: Vec<Vec<PlannedExpression>>) -> ExecutorResult<ExecutionOutput> {
    let rows = rows.into_iter().enumerate().map(|(row_id, expressions)| {
        let input = empty_record(row_id as u64)?;
        evaluate_expressions(&expressions, &input)
    });
    Ok(ExecutionOutput::Rows { rows: Box::new(rows) })
}

/// Skips rows from a child stream while still surfacing skipped-row errors.
///
/// SQL `OFFSET` cannot silently swallow errors from rows it discards: if the
/// child stream fails before enough rows have been skipped, the offset operator
/// yields that error and continues counting it as one consumed input row.
fn offset_rows(mut rows: RowStream, mut remaining: usize) -> RowStream {
    Box::new(std::iter::from_fn(move || {
        while remaining > 0 {
            match rows.next()? {
                Ok(_) => remaining -= 1,
                Err(error) => {
                    remaining -= 1;
                    return Some(Err(error));
                }
            }
        }

        rows.next()
    }))
}

/// Executes an `INSERT ... VALUES` plan.
///
/// Each value row is evaluated, expanded into the target table layout, checked
/// against nullability and type constraints, inserted into the table B+-tree,
/// and then mirrored into every existing secondary index for that table.
fn execute_insert_values(
    database: &Database,
    table: TableSchema,
    columns: Vec<BoundColumn>,
    values: Vec<Vec<PlannedExpression>>,
) -> ExecutorResult<ExecutionOutput> {
    let mut cursor = database.table_cursor_by_name(&table.name)?;
    let indexes = database.index_schemas_for_table(&table)?;
    let mut affected = 0;

    for expressions in values {
        if expressions.len() != columns.len() {
            return Err(ExecutorError::InsertColumnValueCount {
                columns: columns.len(),
                values: expressions.len(),
            });
        }

        let input = empty_record(0)?;
        let input_context = EvaluationContext::from_record(&input)?;
        let mut row_values = vec![Value::Null; table.row.columns.len()];
        for (column, expression) in columns.iter().zip(expressions.iter()) {
            let len = row_values.len();
            let value = evaluate_value(expression, &input_context)?;
            let slot = row_values.get_mut(column.ordinal).ok_or_else(|| {
                ExecutorError::ColumnOrdinalOutOfBounds {
                    column: column.name.clone(),
                    ordinal: column.ordinal,
                    len,
                }
            })?;
            *slot = value;
        }
        validate_insert_row(&table, &row_values)?;

        let record = record_bytes_from_values(row_values)?;
        let row_id = database.allocate_table_row_id(&table)?;
        cursor.insert(row_id, &record)?;
        let record = TableRecord { row_id, record: record.into_boxed_slice() };
        insert_index_entries(database, &indexes, &record)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Validates one fully-shaped table row against the table schema.
///
/// Missing target columns are represented as [`Value::Null`] before this
/// function runs, so omitted non-nullable and primary-key columns are rejected
/// by the same logic as explicit `NULL` values.
fn validate_insert_row(table: &TableSchema, values: &[Value]) -> ExecutorResult<()> {
    for (column, value) in table.row.columns.iter().zip(values.iter()) {
        if matches!(value, Value::Null) {
            if !column.nullable || column.primary_key {
                return Err(ExecutorError::InsertNullConstraint { column: column.name.clone() });
            }
            continue;
        }

        if !value_matches_data_type(value, column.data_type) {
            return Err(ExecutorError::InsertTypeMismatch {
                column: column.name.clone(),
                expected: column.data_type,
                actual: value.clone(),
            });
        }
    }

    Ok(())
}

/// Inserts secondary-index entries for a newly inserted table record.
///
/// Index keys are reconstructed from the encoded table record so this code path
/// uses the same key derivation as index backfill.
fn insert_index_entries(
    database: &Database,
    indexes: &[IndexSchema],
    record: &TableRecord,
) -> ExecutorResult<()> {
    for index in indexes {
        let key = index_key_from_record(index, record)?;
        let mut index_cursor = database.index_cursor_by_name(&index.name)?;
        index_cursor.insert(&key, record.row_id)?;
    }

    Ok(())
}

/// Populates a newly-created secondary index from existing table rows.
fn backfill_index(
    database: &Database,
    table: &TableSchema,
    index: &IndexSchema,
) -> ExecutorResult<()> {
    let table_cursor = database.table_cursor_by_name(&table.name)?;
    let mut table_cursor = table_cursor.into_inner();
    let mut index_cursor = database.index_cursor_by_name(&index.name)?;

    while let Some(record) = table_cursor.next_owned_record()? {
        let record = TableRecord::try_from(record).map_err(ExecutorError::Storage)?;
        let key = index_key_from_record(index, &record)?;
        index_cursor.insert(&key, record.row_id)?;
    }

    Ok(())
}

/// Builds the encoded key tuple for an index entry.
///
/// Index column metadata records the ordinal of each source table column. The
/// key is an encoded tuple containing those source values in index-column order.
fn index_key_from_record(index: &IndexSchema, record: &TableRecord) -> ExecutorResult<Vec<u8>> {
    let context = EvaluationContext::from_record(record)?;
    let values = index
        .columns
        .iter()
        .map(|column| context.value_at(column.source_column_ordinal as usize, &column.column.name))
        .collect::<ExecutorResult<Vec<_>>>()?;
    record_bytes_from_values(values)
}

/// Returns whether a non-null value can be stored in a column of `data_type`.
fn value_matches_data_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::String(_), DataType::Text)
            | (Value::Boolean(_), DataType::Boolean)
            | (Value::Integer(_), DataType::Integer)
            | (Value::Float(_), DataType::Float)
            | (Value::UnsignedInteger(_), DataType::UnsignedInteger)
    )
}

/// Evaluates a projection list against one input record.
fn evaluate_expressions(
    expressions: &[PlannedExpression],
    record: &TableRecord,
) -> ExecutorResult<TableRecord> {
    let context = EvaluationContext::from_record(record)?;
    evaluate_expressions_in_context(expressions, &context)
}

/// Evaluates expressions using an already-parsed tuple context.
fn evaluate_expressions_in_context(
    expressions: &[PlannedExpression],
    context: &EvaluationContext<'_>,
) -> ExecutorResult<TableRecord> {
    let values = expressions
        .iter()
        .map(|expression| evaluate_value(expression, context))
        .collect::<ExecutorResult<Vec<_>>>()?;
    record_from_values(context.row_id, values)
}

/// Evaluates a scalar expression to one typed value.
///
/// Logical `AND` and `OR` are short-circuited here before evaluating the right
/// operand, so expressions like `FALSE AND (1 / 0)` do not report division by
/// zero.
fn evaluate_value(
    expression: &PlannedExpression,
    context: &EvaluationContext<'_>,
) -> ExecutorResult<Value> {
    match expression {
        PlannedExpression::Literal(value) => Ok(value.clone()),
        PlannedExpression::Column(column) => context.evaluate_column(column),
        PlannedExpression::Unary { op, expr } => {
            let value = evaluate_value(expr, context)?;
            evaluate_unary(*op, value)
        }
        PlannedExpression::Binary { left, op, right } => {
            let left = evaluate_value(left, context)?;
            if matches!(op, Op::And | Op::Or) {
                return evaluate_logical_binary(left, *op, right, context);
            }
            let right = evaluate_value(right, context)?;
            evaluate_binary(left, *op, right)
        }
    }
}

/// Parsed view of an input record used while evaluating expressions.
///
/// The context keeps the original row id so projected records preserve their
/// source identity, and it keeps a zero-copy tuple view so column references can
/// read typed values by ordinal.
struct EvaluationContext<'a> {
    row_id: u64,
    tuple: TupleView<'a>,
}

impl<'a> EvaluationContext<'a> {
    /// Parses the encoded tuple bytes from `record`.
    fn from_record(record: &'a TableRecord) -> ExecutorResult<Self> {
        let tuple = TupleView::parse(&record.record).map_err(ExecutorError::InvalidTuple)?;
        Ok(Self { row_id: record.row_id, tuple })
    }

    /// Reads the value for a planner-bound column reference.
    fn evaluate_column(&self, column: &BoundColumn) -> ExecutorResult<Value> {
        self.value_at(column.ordinal, &column.name)
    }

    /// Reads the value at `ordinal`, using `column_name` for diagnostics.
    fn value_at(&self, ordinal: usize, column_name: &str) -> ExecutorResult<Value> {
        let len = self.tuple.len();
        self.tuple.values().nth(ordinal).map(Value::from).ok_or_else(|| {
            ExecutorError::ColumnOrdinalOutOfBounds { column: column_name.to_owned(), ordinal, len }
        })
    }
}

/// Evaluates a unary operator against one value.
fn evaluate_unary(op: Op, value: Value) -> ExecutorResult<Value> {
    match (op, value) {
        (Op::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (Op::Sub, Value::Integer(value)) => {
            value.checked_neg().map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Op::Sub, Value::Float(value)) => Ok(Value::Float(-value)),
        (op, value) => Err(ExecutorError::UnsupportedUnary { op, value }),
    }
}

/// Evaluates a non-short-circuiting binary operator.
fn evaluate_binary(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    match op {
        Op::And | Op::Or => evaluate_eager_boolean_binary(left, op, right),
        Op::Add | Op::Sub | Op::Mul | Op::Div => evaluate_arithmetic(left, op, right),
        Op::EqualsEquals | Op::NotEquals => evaluate_equality(left, op, right),
        Op::LessThan | Op::GreaterThan | Op::LessThanOrEqual | Op::GreaterThanOrEqual => {
            evaluate_ordering(left, op, right)
        }
        Op::Not => Err(ExecutorError::UnsupportedBinary { left, op, right }),
    }
}

/// Evaluates short-circuiting boolean `AND` and `OR`.
fn evaluate_logical_binary(
    left: Value,
    op: Op,
    right: &PlannedExpression,
    context: &EvaluationContext<'_>,
) -> ExecutorResult<Value> {
    match (left, op) {
        (Value::Boolean(false), Op::And) => Ok(Value::Boolean(false)),
        (Value::Boolean(true), Op::Or) => Ok(Value::Boolean(true)),
        (Value::Boolean(_), op @ (Op::And | Op::Or)) => {
            let right = evaluate_value(right, context)?;
            match right {
                Value::Boolean(right) => Ok(Value::Boolean(right)),
                value => Err(ExecutorError::NonBooleanLogicalOperand { op, value }),
            }
        }
        (value, op @ (Op::And | Op::Or)) => {
            Err(ExecutorError::NonBooleanLogicalOperand { op, value })
        }
        (_, _) => unreachable!("evaluate_logical_binary only accepts logical operators"),
    }
}

/// Evaluates boolean operators when both operands have already been evaluated.
fn evaluate_eager_boolean_binary(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    match (left, op, right) {
        (Value::Boolean(left), Op::And, Value::Boolean(right)) => Ok(Value::Boolean(left && right)),
        (Value::Boolean(left), Op::Or, Value::Boolean(right)) => Ok(Value::Boolean(left || right)),
        (left, op, right) => Err(ExecutorError::UnsupportedBinary { left, op, right }),
    }
}

/// Evaluates arithmetic over supported numeric value pairs.
fn evaluate_arithmetic(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    match (left, op, right) {
        (Value::Integer(left), Op::Add, Value::Integer(right)) => {
            left.checked_add(right).map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Value::Integer(left), Op::Sub, Value::Integer(right)) => {
            left.checked_sub(right).map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Value::Integer(left), Op::Mul, Value::Integer(right)) => {
            left.checked_mul(right).map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Value::Integer(_), Op::Div, Value::Integer(0)) => Err(ExecutorError::DivisionByZero),
        (Value::Integer(left), Op::Div, Value::Integer(right)) => {
            left.checked_div(right).map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Value::Float(left), Op::Add, Value::Float(right)) => Ok(Value::Float(left + right)),
        (Value::Float(left), Op::Sub, Value::Float(right)) => Ok(Value::Float(left - right)),
        (Value::Float(left), Op::Mul, Value::Float(right)) => Ok(Value::Float(left * right)),
        (Value::Float(_), Op::Div, Value::Float(0.0)) => Err(ExecutorError::DivisionByZero),
        (Value::Float(left), Op::Div, Value::Float(right)) => Ok(Value::Float(left / right)),
        (left, op, right) => Err(ExecutorError::UnsupportedBinary { left, op, right }),
    }
}

/// Evaluates equality and inequality for same-type values.
fn evaluate_equality(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    match (&left, &right) {
        (Value::Null, Value::Null)
        | (Value::String(_), Value::String(_))
        | (Value::Boolean(_), Value::Boolean(_))
        | (Value::Integer(_), Value::Integer(_))
        | (Value::Float(_), Value::Float(_))
        | (Value::UnsignedInteger(_), Value::UnsignedInteger(_)) => {
            let equal = left == right;
            Ok(Value::Boolean(if matches!(op, Op::EqualsEquals) { equal } else { !equal }))
        }
        _ => Err(ExecutorError::UnsupportedBinary { left, op, right }),
    }
}

/// Evaluates ordering comparisons for same-type ordered values.
fn evaluate_ordering(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    let result = match (&left, &right) {
        (Value::String(left), Value::String(right)) => compare_ordered(left, op, right),
        (Value::Boolean(left), Value::Boolean(right)) => compare_ordered(left, op, right),
        (Value::Integer(left), Value::Integer(right)) => compare_ordered(left, op, right),
        (Value::Float(left), Value::Float(right)) => compare_ordered(left, op, right),
        (Value::UnsignedInteger(left), Value::UnsignedInteger(right)) => {
            compare_ordered(left, op, right)
        }
        _ => return Err(ExecutorError::UnsupportedBinary { left, op, right }),
    };
    Ok(Value::Boolean(result))
}

/// Applies an ordering operator to values with a Rust [`PartialOrd`] relation.
fn compare_ordered<T: PartialOrd>(left: &T, op: Op, right: &T) -> bool {
    match op {
        Op::LessThan => left < right,
        Op::GreaterThan => left > right,
        Op::LessThanOrEqual => left <= right,
        Op::GreaterThanOrEqual => left >= right,
        _ => unreachable!("compare_ordered only accepts ordering operators"),
    }
}

/// Builds an encoded empty record with the provided row id.
fn empty_record(row_id: u64) -> ExecutorResult<TableRecord> {
    record_from_values(row_id, Vec::new())
}

/// Builds a table record from owned typed values.
fn record_from_values(row_id: u64, values: Vec<Value>) -> ExecutorResult<TableRecord> {
    let record = record_bytes_from_values(values)?;
    Ok(TableRecord { row_id, record: record.into_boxed_slice() })
}

/// Serializes typed values using the tuple storage format.
fn record_bytes_from_values(values: Vec<Value>) -> ExecutorResult<Vec<u8>> {
    Tuple::new(values).to_bytes().map_err(ExecutorError::InvalidTuple)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        core::{ColumnSchema, DataType, PAGE_SIZE, TupleSchema},
        planner::{BoundColumn, Planner},
        sql_parser::parser::Parser,
    };

    fn record(row_id: u64, values: Vec<Value>) -> TableRecord {
        record_from_values(row_id, values).unwrap()
    }

    fn values(record: &TableRecord) -> Vec<Value> {
        Tuple::from_bytes(&record.record).unwrap().into_values()
    }

    fn collect_rows(output: ExecutionOutput) -> ExecutorResult<Vec<TableRecord>> {
        output.into_rows("TEST")?.collect()
    }

    fn execute_sql(database: &Database, sql: &str) -> ExecutorResult<ExecutionOutput> {
        let statement = Parser::new(sql).stmt().unwrap();
        let plan = Planner::new(database).plan_statement(&statement).unwrap();
        Executor::new(database).execute(plan.physical)
    }

    fn bound(name: &str, ordinal: usize, data_type: DataType) -> BoundColumn {
        BoundColumn { table: "users".to_owned(), name: name.to_owned(), ordinal, data_type }
    }

    fn insert_values_plan(
        database: &Database,
        columns: Vec<BoundColumn>,
        rows: Vec<Vec<Value>>,
    ) -> PhysicalPlan {
        PhysicalPlan::InsertValues {
            table: database.table_schema_by_name("users").unwrap(),
            columns,
            values: rows
                .into_iter()
                .map(|row| row.into_iter().map(PlannedExpression::Literal).collect())
                .collect(),
        }
    }

    fn users_schema() -> TupleSchema {
        TupleSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "active".to_owned(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    primary_key: false,
                },
            ],
        }
    }

    fn insert_many_users_sql(count: u64) -> String {
        let mut sql = String::from("INSERT INTO users (id, name, active) VALUES ");
        for id in 1..=count {
            if id > 1 {
                sql.push_str(", ");
            }
            write!(&mut sql, "({id}, 'user{id}', TRUE)").unwrap();
        }
        sql.push(';');
        sql
    }

    fn assert_user_row(database: &Database, row_id: u64, expected_name: &str) {
        let mut users = database.table_cursor_by_name("users").unwrap();
        let row = users.get(row_id).unwrap().expect("user row should exist");
        assert_eq!(
            values(&row),
            vec![
                Value::Integer(row_id as i32),
                Value::String(expected_name.to_owned()),
                Value::Boolean(true),
            ]
        );
    }

    fn assert_name_index_entry(database: &Database, name: &str, row_id: u64) {
        let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
        let key = Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap();
        let entry = index.get(&key).unwrap().expect("index entry should exist");
        assert_eq!(entry.row_id, row_id);
    }

    #[test]
    fn single_literal_expression_produces_one_column_record() {
        let input = record(7, vec![Value::Integer(1)]);
        let output = evaluate_expression(
            &PlannedExpression::Literal(Value::String("Ada".to_owned())),
            &input,
        )
        .unwrap();

        assert_eq!(output.row_id, 7);
        assert_eq!(values(&output), vec![Value::String("Ada".to_owned())]);
    }

    #[test]
    fn single_column_expression_reads_bound_ordinal() {
        let input = record(
            8,
            vec![Value::Integer(1), Value::String("Grace".to_owned()), Value::Boolean(true)],
        );
        let output = evaluate_expression(
            &PlannedExpression::Column(bound("name", 1, DataType::Text)),
            &input,
        )
        .unwrap();

        assert_eq!(output.row_id, 8);
        assert_eq!(values(&output), vec![Value::String("Grace".to_owned())]);
    }

    #[test]
    fn project_evaluates_multiple_expressions_in_order() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![vec![
                    PlannedExpression::Literal(Value::Integer(4)),
                    PlannedExpression::Literal(Value::Integer(5)),
                ]],
            }),
            expressions: vec![
                PlannedExpression::Column(bound("right", 1, DataType::Integer)),
                PlannedExpression::Binary {
                    left: Box::new(PlannedExpression::Column(bound("left", 0, DataType::Integer))),
                    op: Op::Add,
                    right: Box::new(PlannedExpression::Column(bound(
                        "right",
                        1,
                        DataType::Integer,
                    ))),
                },
            ],
        };

        let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, 0);
        assert_eq!(values(&rows[0]), vec![Value::Integer(5), Value::Integer(9)]);
    }

    #[test]
    fn filter_keeps_only_rows_with_true_predicate() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![
                    vec![
                        PlannedExpression::Literal(Value::Integer(1)),
                        PlannedExpression::Literal(Value::Boolean(true)),
                    ],
                    vec![
                        PlannedExpression::Literal(Value::Integer(2)),
                        PlannedExpression::Literal(Value::Boolean(false)),
                    ],
                ],
            }),
            predicate: PlannedExpression::Column(bound("active", 1, DataType::Boolean)),
        };

        let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(values(&rows[0]), vec![Value::Integer(1), Value::Boolean(true)]);
    }

    #[test]
    fn filter_rejects_non_boolean_predicate() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
            }),
            predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
        };

        assert!(matches!(
            collect_rows(executor.execute(plan).unwrap()),
            Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
        ));
    }

    #[test]
    fn limit_does_not_evaluate_rows_beyond_limit() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![
                    vec![PlannedExpression::Literal(Value::Integer(1))],
                    vec![PlannedExpression::Binary {
                        left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
                        op: Op::Div,
                        right: Box::new(PlannedExpression::Literal(Value::Integer(0))),
                    }],
                ],
            }),
            limit: 1,
        };

        let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(values(&rows[0]), vec![Value::Integer(1)]);
    }

    #[test]
    fn limit_larger_than_child_rows_returns_all_rows() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![
                    vec![PlannedExpression::Literal(Value::Integer(1))],
                    vec![PlannedExpression::Literal(Value::Integer(2))],
                ],
            }),
            limit: u32::MAX,
        };

        let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(values(&rows[0]), vec![Value::Integer(1)]);
        assert_eq!(values(&rows[1]), vec![Value::Integer(2)]);
    }

    #[test]
    fn limit_rejects_non_row_child() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::CreateTable {
                name: "users".to_owned(),
                schema: users_schema(),
            }),
            limit: 1,
        };

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::ExpectedRows { operator: "LIMIT" })
        ));
    }

    #[test]
    fn offset_reports_errors_from_skipped_rows() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Offset {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::Values {
                    rows: vec![
                        vec![PlannedExpression::Literal(Value::Integer(1))],
                        vec![PlannedExpression::Literal(Value::Integer(2))],
                    ],
                }),
                predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
            }),
            offset: 1,
        };

        assert!(matches!(
            collect_rows(executor.execute(plan).unwrap()),
            Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
        ));
    }

    #[test]
    fn offset_larger_than_child_rows_returns_no_rows() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Offset {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![
                    vec![PlannedExpression::Literal(Value::Integer(1))],
                    vec![PlannedExpression::Literal(Value::Integer(2))],
                ],
            }),
            offset: u32::MAX,
        };

        let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn offset_rejects_non_row_child() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Offset {
            input: Box::new(PhysicalPlan::CreateTable {
                name: "users".to_owned(),
                schema: users_schema(),
            }),
            offset: 1,
        };

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::ExpectedRows { operator: "OFFSET" })
        ));
    }

    #[test]
    fn filter_propagates_child_row_errors() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::Values {
                    rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
                }),
                predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
            }),
            predicate: PlannedExpression::Literal(Value::Boolean(true)),
        };

        assert!(matches!(
            collect_rows(executor.execute(plan).unwrap()),
            Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
        ));
    }

    #[test]
    fn project_propagates_child_row_errors() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::Values {
                    rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
                }),
                predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
            }),
            expressions: vec![PlannedExpression::Literal(Value::Integer(2))],
        };

        assert!(matches!(
            collect_rows(executor.execute(plan).unwrap()),
            Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
        ));
    }

    #[test]
    fn row_operator_rejects_non_row_child() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::CreateTable {
                name: "users".to_owned(),
                schema: users_schema(),
            }),
            expressions: vec![PlannedExpression::Literal(Value::Integer(1))],
        };

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::ExpectedRows { operator: "PROJECT" })
        ));
    }

    #[test]
    fn sort_returns_unsupported_error_instead_of_panicking() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let mut executor = Executor::new(&database);
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Values { rows: Vec::new() }),
            terms: Vec::new(),
        };

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::UnsupportedOperator { operator: "SORT" })
        ));
    }

    #[test]
    fn evaluates_arithmetic_comparison_boolean_and_unary_expressions() {
        let input = record(9, Vec::new());
        let expression = PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Unary {
                op: Op::Not,
                expr: Box::new(PlannedExpression::Binary {
                    left: Box::new(PlannedExpression::Literal(Value::Integer(2))),
                    op: Op::GreaterThan,
                    right: Box::new(PlannedExpression::Literal(Value::Integer(3))),
                }),
            }),
            op: Op::And,
            right: Box::new(PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Binary {
                    left: Box::new(PlannedExpression::Literal(Value::Integer(2))),
                    op: Op::Mul,
                    right: Box::new(PlannedExpression::Literal(Value::Integer(4))),
                }),
                op: Op::EqualsEquals,
                right: Box::new(PlannedExpression::Literal(Value::Integer(8))),
            }),
        };

        let output = evaluate_expression(&expression, &input).unwrap();

        assert_eq!(values(&output), vec![Value::Boolean(true)]);
    }

    #[test]
    fn boolean_expressions_short_circuit() {
        let input = record(9, Vec::new());
        let divide_by_zero = PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            op: Op::Div,
            right: Box::new(PlannedExpression::Literal(Value::Integer(0))),
        };
        let false_and_error = PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Literal(Value::Boolean(false))),
            op: Op::And,
            right: Box::new(divide_by_zero.clone()),
        };
        let true_or_error = PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Literal(Value::Boolean(true))),
            op: Op::Or,
            right: Box::new(divide_by_zero),
        };

        let false_and_output = evaluate_expression(&false_and_error, &input).unwrap();
        let true_or_output = evaluate_expression(&true_or_error, &input).unwrap();

        assert_eq!(values(&false_and_output), vec![Value::Boolean(false)]);
        assert_eq!(values(&true_or_output), vec![Value::Boolean(true)]);
    }

    #[test]
    fn invalid_type_combinations_return_executor_errors() {
        let input = record(10, Vec::new());
        let expression = PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            op: Op::Add,
            right: Box::new(PlannedExpression::Literal(Value::Float(1.0))),
        };

        assert!(matches!(
            evaluate_expression(&expression, &input),
            Err(ExecutorError::UnsupportedBinary {
                left: Value::Integer(1),
                op: Op::Add,
                right: Value::Float(1.0),
            })
        ));
    }

    #[test]
    fn select_with_projection_and_filter_executes_end_to_end() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();
        let mut users = database.table_cursor_by_name("users").unwrap();
        users
            .insert(
                1,
                &Tuple::new(vec![
                    Value::Integer(1),
                    Value::String("Ada".to_owned()),
                    Value::Boolean(true),
                ])
                .to_bytes()
                .unwrap(),
            )
            .unwrap();
        users
            .insert(
                2,
                &Tuple::new(vec![
                    Value::Integer(2),
                    Value::String("Grace".to_owned()),
                    Value::Boolean(false),
                ])
                .to_bytes()
                .unwrap(),
            )
            .unwrap();

        let statement = Parser::new("SELECT name FROM users WHERE id == 1;").stmt().unwrap();
        let plan = Planner::new(&database).plan_statement(&statement).unwrap();
        let mut executor = Executor::new(&database);

        let rows = collect_rows(executor.execute(plan.physical).unwrap()).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, 1);
        assert_eq!(values(&rows[0]), vec![Value::String("Ada".to_owned())]);
    }

    #[test]
    fn insert_values_allocates_row_ids_and_persists_rows() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();

        let statement = Parser::new(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
        )
        .stmt()
        .unwrap();
        let plan = Planner::new(&database).plan_statement(&statement).unwrap();
        let mut executor = Executor::new(&database);

        let output = executor.execute(plan.physical).unwrap();

        assert!(matches!(output, ExecutionOutput::RowsAffected(2)));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 2);

        let mut users = database.table_cursor_by_name("users").unwrap();
        let first = users.get(1).unwrap().expect("first inserted row should exist");
        let second = users.get(2).unwrap().expect("second inserted row should exist");
        assert_eq!(
            values(&first),
            vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true),]
        );
        assert_eq!(
            values(&second),
            vec![Value::Integer(2), Value::String("Grace".to_owned()), Value::Boolean(false),]
        );
    }

    #[test]
    fn insert_values_rejects_omitted_non_nullable_columns() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();

        let plan = insert_values_plan(
            &database,
            vec![bound("id", 0, DataType::Integer), bound("name", 1, DataType::Text)],
            vec![vec![Value::Integer(1), Value::String("Ada".to_owned())]],
        );
        let mut executor = Executor::new(&database);

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::InsertNullConstraint { column }) if column == "active"
        ));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
    }

    #[test]
    fn insert_values_rejects_values_with_wrong_type() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();

        let statement =
            Parser::new("INSERT INTO users (id, name, active) VALUES ('one', 'Ada', TRUE);")
                .stmt()
                .unwrap();
        let plan = Planner::new(&database).plan_statement(&statement).unwrap();
        let mut executor = Executor::new(&database);

        assert!(matches!(
            executor.execute(plan.physical),
            Err(ExecutorError::InsertTypeMismatch {
                column,
                expected: DataType::Integer,
                actual: Value::String(value),
            }) if column == "id" && value == "one"
        ));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
    }

    #[test]
    fn insert_values_rejects_null_for_non_nullable_columns() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();

        let plan = insert_values_plan(
            &database,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![vec![Value::Integer(1), Value::Null, Value::Boolean(true)]],
        );
        let mut executor = Executor::new(&database);

        assert!(matches!(
            executor.execute(plan),
            Err(ExecutorError::InsertNullConstraint { column }) if column == "name"
        ));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
    }

    #[test]
    fn failed_insert_does_not_advance_last_row_id() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();
        let mut executor = Executor::new(&database);

        let valid = insert_values_plan(
            &database,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)]],
        );
        executor.execute(valid).unwrap();
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 1);

        let invalid = insert_values_plan(
            &database,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![vec![
                Value::Integer(2),
                Value::String("Grace".to_owned()),
                Value::String("yes".to_owned()),
            ]],
        );

        assert!(matches!(
            executor.execute(invalid),
            Err(ExecutorError::InsertTypeMismatch {
                column,
                expected: DataType::Boolean,
                actual: Value::String(value),
            }) if column == "active" && value == "yes"
        ));
        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 1);

        let mut users = database.table_cursor_by_name("users").unwrap();
        assert!(users.get(2).unwrap().is_none());
    }

    #[test]
    fn failed_multi_row_insert_rolls_back_rows_already_inserted_in_statement() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();
        let mut executor = Executor::new(&database);

        let invalid = insert_values_plan(
            &database,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
                vec![
                    Value::Integer(2),
                    Value::String("Grace".to_owned()),
                    Value::String("yes".to_owned()),
                ],
            ],
        );

        assert!(matches!(
            executor.execute(invalid),
            Err(ExecutorError::InsertTypeMismatch {
                column,
                expected: DataType::Boolean,
                actual: Value::String(value),
            }) if column == "active" && value == "yes"
        ));

        assert_eq!(database.table_schema_by_name("users").unwrap().last_row_id, 0);
        let mut users = database.table_cursor_by_name("users").unwrap();
        assert!(users.get(1).unwrap().is_none());
        assert!(users.get(2).unwrap().is_none());
    }

    #[test]
    fn create_index_backfills_existing_table_rows() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();
        let insert = Parser::new(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
        )
        .stmt()
        .unwrap();
        let insert_plan = Planner::new(&database).plan_statement(&insert).unwrap();
        let mut executor = Executor::new(&database);
        executor.execute(insert_plan.physical).unwrap();

        let create_index =
            Parser::new("CREATE INDEX idx_users_name ON users (name);").stmt().unwrap();
        let create_index_plan = Planner::new(&database).plan_statement(&create_index).unwrap();
        executor.execute(create_index_plan.physical).unwrap();

        let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
        let key = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();
        let entry = index.get(&key).unwrap().expect("index entry should be backfilled");

        assert_eq!(entry.row_id, 1);
    }

    #[test]
    fn insert_values_updates_existing_secondary_indexes() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        database.create_table("users", users_schema()).unwrap();
        let create_index =
            Parser::new("CREATE INDEX idx_users_name ON users (name);").stmt().unwrap();
        let create_index_plan = Planner::new(&database).plan_statement(&create_index).unwrap();
        let mut executor = Executor::new(&database);
        executor.execute(create_index_plan.physical).unwrap();

        let insert = Parser::new("INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
            .stmt()
            .unwrap();
        let insert_plan = Planner::new(&database).plan_statement(&insert).unwrap();
        executor.execute(insert_plan.physical).unwrap();

        let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
        let key = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();
        let entry = index.get(&key).unwrap().expect("index entry should track inserted row");

        assert_eq!(entry.row_id, 1);
    }

    #[test]
    fn committed_create_table_recovers_from_wal_after_crash_without_database_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.flush().unwrap();

        execute_sql(&database, "CREATE TABLE recovered (id INT PRIMARY KEY, name TEXT);").unwrap();
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();
        let schema = reopened.table_schema_by_name("recovered").unwrap();

        assert_eq!(schema.name, "recovered");
        assert_eq!(schema.row.columns.len(), 2);
        assert_eq!(schema.row.columns[0].name, "id");
        assert_eq!(schema.row.columns[1].name, "name");
        reopened.table_cursor_by_name("recovered").unwrap();
    }

    #[test]
    fn committed_create_index_backfill_recovers_from_wal_after_crash_without_database_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.flush().unwrap();

        execute_sql(
            &database,
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
        )
        .unwrap();
        database.flush().unwrap();
        execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();

        assert_name_index_entry(&reopened, "Ada", 1);
        assert_name_index_entry(&reopened, "Grace", 2);
    }

    #[test]
    fn committed_large_insert_with_btree_splits_recovers_from_wal_after_crash() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.flush().unwrap();

        execute_sql(&database, &insert_many_users_sql(500)).unwrap();
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();

        assert_eq!(reopened.table_schema_by_name("users").unwrap().last_row_id, 500);
        assert_user_row(&reopened, 1, "user1");
        assert_user_row(&reopened, 250, "user250");
        assert_user_row(&reopened, 500, "user500");
    }

    #[test]
    fn committed_overflow_insert_recovers_from_wal_after_crash() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.flush().unwrap();
        let large_name = "x".repeat(PAGE_SIZE * 3);
        let insert =
            format!("INSERT INTO users (id, name, active) VALUES (1, '{large_name}', TRUE);");

        execute_sql(&database, &insert).unwrap();
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();
        let mut users = reopened.table_cursor_by_name("users").unwrap();
        let row = users.get(1).unwrap().expect("overflow row should recover from WAL");

        assert_eq!(
            values(&row),
            vec![Value::Integer(1), Value::String(large_name), Value::Boolean(true)]
        );
    }

    #[test]
    fn failed_indexed_multi_row_insert_rolls_back_after_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.create_index("idx_users_name", "users", &["name"]).unwrap();
        database.flush().unwrap();

        let invalid = insert_values_plan(
            &database,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![
                vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)],
                vec![
                    Value::Integer(2),
                    Value::String("Grace".to_owned()),
                    Value::String("yes".to_owned()),
                ],
            ],
        );

        assert!(matches!(
            Executor::new(&database).execute(invalid),
            Err(ExecutorError::InsertTypeMismatch {
                column,
                expected: DataType::Boolean,
                actual: Value::String(value),
            }) if column == "active" && value == "yes"
        ));
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();
        let mut users = reopened.table_cursor_by_name("users").unwrap();
        let mut index = reopened.index_cursor_by_name("idx_users_name").unwrap();
        let ada = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();

        assert_eq!(reopened.table_schema_by_name("users").unwrap().last_row_id, 0);
        assert!(users.get(1).unwrap().is_none());
        assert!(index.get(&ada).unwrap().is_none());
    }

    #[test]
    fn uncommitted_flushed_insert_is_undone_during_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.create_index("idx_users_name", "users", &["name"]).unwrap();
        database.flush().unwrap();
        let txn_id = database.begin_transaction().unwrap();
        let table = database.table_schema_by_name("users").unwrap();

        execute_insert_values(
            &database,
            table,
            vec![
                bound("id", 0, DataType::Integer),
                bound("name", 1, DataType::Text),
                bound("active", 2, DataType::Boolean),
            ],
            vec![vec![
                PlannedExpression::Literal(Value::Integer(1)),
                PlannedExpression::Literal(Value::String("Ada".to_owned())),
                PlannedExpression::Literal(Value::Boolean(true)),
            ]],
        )
        .unwrap();
        database.flush().unwrap();
        assert_eq!(txn_id, 1);
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();
        let mut users = reopened.table_cursor_by_name("users").unwrap();
        let mut index = reopened.index_cursor_by_name("idx_users_name").unwrap();
        let ada = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();

        assert_eq!(reopened.table_schema_by_name("users").unwrap().last_row_id, 0);
        assert!(users.get(1).unwrap().is_none());
        assert!(index.get(&ada).unwrap().is_none());
    }

    #[test]
    fn create_without_explicit_flush_does_not_promise_catalog_durability() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();

        std::mem::forget(database);

        assert!(Database::open(&path).is_err());
    }

    #[test]
    fn committed_insert_recovers_from_wal_after_crash_without_database_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        database.flush().unwrap();
        let insert = Parser::new("INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
            .stmt()
            .unwrap();
        let insert_plan = Planner::new(&database).plan_statement(&insert).unwrap();
        {
            let mut executor = Executor::new(&database);
            executor.execute(insert_plan.physical).unwrap();
        }
        std::mem::forget(database);

        let reopened = Database::open(&path).unwrap();
        let mut users = reopened.table_cursor_by_name("users").unwrap();
        let row = users.get(1).unwrap().expect("committed row should recover from WAL");

        assert_eq!(
            values(&row),
            vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true),]
        );
    }

    #[test]
    fn select_without_from_executes_through_one_row_and_project() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let statement = Parser::new("SELECT 1 + 2;").stmt().unwrap();
        let plan = Planner::new(&database).plan_statement(&statement).unwrap();
        let mut executor = Executor::new(&database);

        let rows = collect_rows(executor.execute(plan.physical).unwrap()).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, 0);
        assert_eq!(values(&rows[0]), vec![Value::Integer(3)]);
    }
}
