//! Scalar operations shared by constant folding and row execution.

use crate::executor::{ExecutorError, ExecutorResult};
use crate::relational::Value;
use crate::sql_parser::parser::op::Op;

/// Evaluates a unary operator against one value.
pub fn evaluate_unary(op: Op, value: Value) -> ExecutorResult<Value> {
    match (op, value) {
        (Op::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (Op::Sub, Value::Integer(value)) => {
            value.checked_neg().map(Value::Integer).ok_or(ExecutorError::IntegerOverflow { op })
        }
        (Op::Sub, Value::Float(value)) => Ok(Value::Float(-value)),
        (op, value) => Err(ExecutorError::UnsupportedUnary { op, value }),
    }
}

/// Evaluates a binary operator, deferring the right operand for short-circuiting.
pub fn evaluate_binary(
    left: Value,
    op: Op,
    right: impl FnOnce() -> ExecutorResult<Value>,
) -> ExecutorResult<Value> {
    match op {
        Op::And | Op::Or => match (op, left) {
            (Op::And, Value::Boolean(false)) => Ok(Value::Boolean(false)),
            (Op::Or, Value::Boolean(true)) => Ok(Value::Boolean(true)),
            (_, Value::Boolean(_)) => match right()? {
                Value::Boolean(value) => Ok(Value::Boolean(value)),
                value => Err(ExecutorError::NonBooleanLogicalOperand { op, value }),
            },
            (_, value) => Err(ExecutorError::NonBooleanLogicalOperand { op, value }),
        },
        Op::Add | Op::Sub | Op::Mul | Op::Div => evaluate_arithmetic(left, op, right()?),
        Op::EqualsEquals | Op::NotEquals => evaluate_equality(left, op, right()?),
        Op::LessThan | Op::GreaterThan | Op::LessThanOrEqual | Op::GreaterThanOrEqual => {
            evaluate_ordering(left, op, right()?)
        }
        Op::Not => Err(ExecutorError::UnsupportedBinary { left, op, right: right()? }),
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

pub fn value_type_name(value: &Value) -> &'static str {
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
