use crate::{
    core::{TableSchema, Transaction, Tuple, Value},
    planner::{BoundColumn, ExecColumn, ExecExpr, RelationId, UpdateAssignment},
    sql_parser::parser::op::Op,
};

use super::{ExecutionOutput, ExecutorError, ExecutorResult, ExecutorRow, RowStream, collect_rows};

/// Evaluates one executable scalar expression against a row.
///
/// The result contains one value and preserves any source locators carried by
/// the input row.
pub fn evaluate_expression(
    expression: &ExecExpr,
    record: &ExecutorRow,
) -> ExecutorResult<ExecutorRow> {
    evaluate_values(std::slice::from_ref(expression), record)
        .map(|values| record.with_values(values))
}

/// Executes a `VALUES` plan as a stream of evaluated literal rows.
pub(super) fn execute_values(rows: Vec<Vec<ExecExpr>>) -> ExecutorResult<ExecutionOutput> {
    let rows = rows.into_iter().map(|expressions| {
        let input = empty_record();
        evaluate_values(&expressions, &input).map(ExecutorRow::from_values)
    });
    Ok(ExecutionOutput::Rows { rows: collect_rows(rows) })
}

/// Skips rows from a child stream while still surfacing skipped-row errors.
///
/// SQL `OFFSET` cannot silently swallow errors from rows it discards: if the
/// child stream fails before enough rows have been skipped, the offset operator
/// yields that error and continues counting it as one consumed input row.
pub(super) fn offset_rows(mut rows: RowStream, mut remaining: usize) -> RowStream {
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
/// Each value row is evaluated, expanded into the target table layout, and then
/// handed to storage for validation and insertion.
pub(super) fn execute_insert_values(
    transaction: &Transaction<'_>,
    table: TableSchema,
    columns: Vec<BoundColumn>,
    values: Vec<Vec<ExecExpr>>,
) -> ExecutorResult<ExecutionOutput> {
    let mut affected = 0;

    for expressions in values {
        if expressions.len() != columns.len() {
            return Err(ExecutorError::InsertColumnValueCount {
                columns: columns.len(),
                values: expressions.len(),
            });
        }

        let input = empty_record();
        let input_context = EvaluationContext::from_row(&input);
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
        transaction.insert_table_row(&table, row_values)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Executes an `UPDATE` plan by consuming and mutating one target row at a time.
pub(super) fn execute_update(
    transaction: &Transaction<'_>,
    relation: RelationId,
    table: TableSchema,
    assignments: Vec<UpdateAssignment>,
    target_rows: RowStream,
) -> ExecutorResult<ExecutionOutput> {
    let mut affected = 0;

    for row in target_rows {
        let row = row?;
        let owned_row =
            row.locator(relation).ok_or(ExecutorError::MissingRowLocator { relation })?.record();
        let context = EvaluationContext::from_row(&row);
        let mut values = Tuple::from_bytes(&owned_row.record)
            .map_err(ExecutorError::InvalidTuple)?
            .into_values();

        for assignment in &assignments {
            let len = values.len();
            let value = evaluate_value(&assignment.expression, &context)?;
            let slot = values.get_mut(assignment.column.ordinal).ok_or_else(|| {
                ExecutorError::ColumnOrdinalOutOfBounds {
                    column: assignment.column.name.clone(),
                    ordinal: assignment.column.ordinal,
                    len,
                }
            })?;
            *slot = value;
        }

        transaction.update_table_row(&table, owned_row, values)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Executes a `DELETE` plan by consuming and deleting one target row at a time.
pub(super) fn execute_delete(
    transaction: &Transaction<'_>,
    relation: RelationId,
    table: TableSchema,
    target_rows: RowStream,
) -> ExecutorResult<ExecutionOutput> {
    let mut affected = 0;

    for row in target_rows {
        let row = row?;
        let owned_row =
            row.locator(relation).ok_or(ExecutorError::MissingRowLocator { relation })?.record();

        transaction.delete_table_row(&table, owned_row)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Evaluates a projection list against one input row.
pub(super) fn evaluate_expressions(
    expressions: &[ExecExpr],
    record: ExecutorRow,
) -> ExecutorResult<ExecutorRow> {
    let values = evaluate_values(expressions, &record)?;
    Ok(record.into_with_values(values))
}

fn evaluate_values(expressions: &[ExecExpr], record: &ExecutorRow) -> ExecutorResult<Vec<Value>> {
    let context = EvaluationContext::from_row(record);
    expressions.iter().map(|expression| evaluate_value(expression, &context)).collect()
}

/// Evaluates a scalar expression to one typed value.
///
/// Logical `AND` and `OR` are short-circuited here before evaluating the right
/// operand, so expressions like `FALSE AND (1 / 0)` do not report division by
/// zero.
pub(super) fn evaluate_value(
    expression: &ExecExpr,
    context: &EvaluationContext<'_>,
) -> ExecutorResult<Value> {
    match expression {
        ExecExpr::Literal(value) => Ok(value.clone()),
        ExecExpr::Column(column) => context.evaluate_column(column),
        ExecExpr::Unary { op, expr } => {
            let value = evaluate_value(expr, context)?;
            evaluate_unary(*op, value)
        }
        ExecExpr::Binary { left, op, right } => {
            let left = evaluate_value(left, context)?;
            if matches!(op, Op::And | Op::Or) {
                return evaluate_logical_binary(left, *op, right, context);
            }
            let right = evaluate_value(right, context)?;
            evaluate_binary(left, *op, right)
        }
    }
}

/// Borrowed view of an executor row used while evaluating expressions.
pub(super) struct EvaluationContext<'a> {
    values: &'a [Value],
}

impl<'a> EvaluationContext<'a> {
    pub(super) fn from_row(row: &'a ExecutorRow) -> Self {
        Self { values: row.values() }
    }

    /// Reads the value for a planner-bound column reference.
    fn evaluate_column(&self, column: &ExecColumn) -> ExecutorResult<Value> {
        self.value_at(column.slot, &column.name)
    }

    /// Reads the value at `ordinal`, using `column_name` for diagnostics.
    fn value_at(&self, ordinal: usize, column_name: &str) -> ExecutorResult<Value> {
        let len = self.values.len();
        self.values.get(ordinal).cloned().ok_or_else(|| ExecutorError::ColumnOrdinalOutOfBounds {
            column: column_name.to_owned(),
            ordinal,
            len,
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
    right: &ExecExpr,
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
        (_, op) => Err(ExecutorError::InvalidLogicalOperator { op }),
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
        _ => Err(comparison_type_mismatch(left, op, right)),
    }
}

/// Evaluates ordering comparisons for same-type ordered values.
fn evaluate_ordering(left: Value, op: Op, right: Value) -> ExecutorResult<Value> {
    let result = match (&left, &right) {
        (Value::String(left), Value::String(right)) => compare_ordered(left, op, right)?,
        (Value::Boolean(left), Value::Boolean(right)) => compare_ordered(left, op, right)?,
        (Value::Integer(left), Value::Integer(right)) => compare_ordered(left, op, right)?,
        (Value::Float(left), Value::Float(right)) => compare_ordered(left, op, right)?,
        (Value::UnsignedInteger(left), Value::UnsignedInteger(right)) => {
            compare_ordered(left, op, right)?
        }
        _ => return Err(comparison_type_mismatch(left, op, right)),
    };
    Ok(Value::Boolean(result))
}

fn comparison_type_mismatch(left: Value, op: Op, right: Value) -> ExecutorError {
    ExecutorError::ComparisonTypeMismatch {
        expected: value_type_name(&left),
        actual: value_type_name(&right),
        left,
        op,
        right,
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::String(_) => "text",
        Value::Boolean(_) => "boolean",
        Value::Integer(_) => "integer",
        Value::Float(_) => "float",
        Value::UnsignedInteger(_) => "unsigned integer",
    }
}

/// Applies an ordering operator to values with a Rust [`PartialOrd`] relation.
fn compare_ordered<T: PartialOrd>(left: &T, op: Op, right: &T) -> ExecutorResult<bool> {
    match op {
        Op::LessThan => Ok(left < right),
        Op::GreaterThan => Ok(left > right),
        Op::LessThanOrEqual => Ok(left <= right),
        Op::GreaterThanOrEqual => Ok(left >= right),
        op => Err(ExecutorError::InvalidOrderingOperator { op }),
    }
}

/// Builds an empty synthetic row.
pub(super) fn empty_record() -> ExecutorRow {
    ExecutorRow::from_values(Vec::new())
}
