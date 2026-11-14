use crate::planner::{BoundColumn, ExecColumn, ExecExpr, RelationId, UpdateAssignment};
use crate::relational::access::RelationalAccess;
use crate::relational::{TableSchema, Tuple, Value};
use crate::scalar::{evaluate_binary, evaluate_unary};

use super::{ExecutionOutput, ExecutorError, ExecutorResult, ExecutorRow};

/// Executes a `VALUES` plan as a stream of evaluated literal rows.
pub(super) fn execute_values(
    rows: Vec<Vec<ExecExpr>>,
) -> impl Iterator<Item = ExecutorResult<ExecutorRow>> {
    rows.into_iter().map(|expressions| {
        let input = empty_record();
        evaluate_values(&expressions, &input).map(ExecutorRow::from_values)
    })
}

/// Skips rows from a child stream while still surfacing skipped-row errors.
///
/// SQL `OFFSET` cannot silently swallow errors from rows it discards: if the
/// child stream fails before enough rows have been skipped, the offset operator
/// yields that error and continues counting it as one consumed input row.
pub(super) fn offset_rows(
    mut rows: impl Iterator<Item = ExecutorResult<ExecutorRow>>,
    mut remaining: usize,
) -> impl Iterator<Item = ExecutorResult<ExecutorRow>> {
    std::iter::from_fn(move || {
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
    })
}

/// Executes an `INSERT ... VALUES` plan.
///
/// Each value row is evaluated, expanded into the target table layout, and then
/// handed to storage for validation and insertion.
pub(super) fn execute_insert_values(
    transaction: &impl RelationalAccess,
    table: &TableSchema,
    columns: &[BoundColumn],
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
        transaction.insert_table_row(table, row_values)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Executes an `UPDATE` plan by consuming and mutating one target row at a time.
pub(super) fn execute_update(
    transaction: &impl RelationalAccess,
    relation: RelationId,
    table: &TableSchema,
    assignments: &[UpdateAssignment],
    target_rows: impl Iterator<Item = ExecutorResult<ExecutorRow>>,
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

        for assignment in assignments {
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

        transaction.update_table_row(table, owned_row, values)?;
        affected += 1;
    }

    Ok(ExecutionOutput::RowsAffected(affected))
}

/// Executes a `DELETE` plan by consuming and deleting one target row at a time.
pub(super) fn execute_delete(
    transaction: &impl RelationalAccess,
    relation: RelationId,
    table: &TableSchema,
    target_rows: impl Iterator<Item = ExecutorResult<ExecutorRow>>,
) -> ExecutorResult<ExecutionOutput> {
    let mut affected = 0;

    for row in target_rows {
        let row = row?;
        let owned_row =
            row.locator(relation).ok_or(ExecutorError::MissingRowLocator { relation })?.record();

        transaction.delete_table_row(table, owned_row)?;
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
            evaluate_binary(left, *op, || evaluate_value(right, context))
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

/// Builds an empty synthetic row.
pub(super) fn empty_record() -> ExecutorRow {
    ExecutorRow::from_values(Vec::new())
}
