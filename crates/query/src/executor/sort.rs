use std::cmp::Ordering;
use std::io;

use crate::planner::SortTerm;
use crate::relational::Value;
use crate::sql_parser::parser::stmt::select::Ordering as SortDirection;

use super::expression::value_type_name;
use super::{ExecutorError, ExecutorResult, ExecutorRow};

/// Buffers the input and orders it by the requested slot. Return input failures
/// before making the sorted rows available to a downstream window.
pub(super) fn sort_rows(
    rows: impl Iterator<Item = ExecutorResult<ExecutorRow>>,
    term: &SortTerm,
) -> ExecutorResult<Vec<ExecutorRow>> {
    let slot = term.column.slot;
    let mut sorted = Vec::new();
    for row in rows {
        let row = row?;
        let key =
            row.values().get(slot).ok_or_else(|| ExecutorError::ColumnOrdinalOutOfBounds {
                column: term.column.name.clone(),
                ordinal: slot,
                len: row.values().len(),
            })?;
        if matches!(key, Value::Float(value) if value.is_nan()) {
            return Err(ExecutorError::InvalidTuple(io::Error::new(
                io::ErrorKind::InvalidData,
                "NaN tuple float",
            )));
        }
        sorted.push(row);
    }

    let descending = matches!(term.direction, SortDirection::Descending);
    sorted.sort_unstable_by(|left, right| {
        let ordering = compare_values(&left.values()[slot], &right.values()[slot]);
        if descending { ordering.reverse() } else { ordering }
    });
    Ok(sorted)
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => {
            // NaN is rejected before sorting. Partial comparison treats signed zeros as equal.
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (Value::UnsignedInteger(left), Value::UnsignedInteger(right)) => left.cmp(right),
        // SQL columns have a single non-null type. Keep a total order for synthetic rows.
        (left, right) => value_type_name(left).cmp(value_type_name(right)),
    }
}

#[cfg(test)]
mod tests {
    use super::{Ordering, Value, compare_values};

    #[test]
    fn compare_values_orders_supported_numeric_and_boolean_keys() {
        let ordered_pairs = [
            (Value::Boolean(false), Value::Boolean(true)),
            (Value::Integer(-10), Value::Integer(2)),
            (Value::UnsignedInteger(2), Value::UnsignedInteger(u64::MAX)),
            (Value::Float(f32::NEG_INFINITY), Value::Float(-1.5)),
            (Value::Float(-1.5), Value::Float(0.0)),
            (Value::Float(0.0), Value::Float(f32::INFINITY)),
        ];

        for (low, high) in ordered_pairs {
            assert_eq!(compare_values(&low, &high), Ordering::Less);
            assert_eq!(compare_values(&high, &low), Ordering::Greater);
        }
        assert_eq!(compare_values(&Value::Float(-0.0), &Value::Float(0.0)), Ordering::Equal);
    }
}
