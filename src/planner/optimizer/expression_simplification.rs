//! Simplification of scalar expressions in logical plans.

use crate::{
    core::{DataType, Value},
    sql_parser::parser::op::Op,
};

use super::super::{BoundExpr, LogicalPlan, LogicalPlanNode};

/// Simplifies every scalar expression without changing the plan shape.
pub(super) fn optimize(logical: LogicalPlan) -> LogicalPlan {
    let (nodes, root) = logical.into_parts();
    let nodes = nodes.into_iter().map(simplify_node).collect();
    LogicalPlan::from_parts(nodes, root)
}

/// Simplifies scalar expressions carried by a single logical operator.
fn simplify_node(node: LogicalPlanNode) -> LogicalPlanNode {
    match node {
        LogicalPlanNode::Values { rows, output } => LogicalPlanNode::Values {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(simplify_expression).collect())
                .collect(),
            output,
        },
        LogicalPlanNode::Update { relation, table, assignments, input } => {
            LogicalPlanNode::Update {
                relation,
                table,
                assignments: assignments
                    .into_iter()
                    .map(|mut assignment| {
                        assignment.expression = simplify_expression(assignment.expression);
                        assignment
                    })
                    .collect(),
                input,
            }
        }
        LogicalPlanNode::Filter { input, predicate, output } => {
            LogicalPlanNode::Filter { input, predicate: simplify_expression(predicate), output }
        }
        LogicalPlanNode::Project { input, expressions, output } => LogicalPlanNode::Project {
            input,
            expressions: expressions.into_iter().map(simplify_expression).collect(),
            output,
        },
        LogicalPlanNode::Join { left, right, join_type, predicate, output } => {
            LogicalPlanNode::Join {
                left,
                right,
                join_type,
                predicate: simplify_expression(predicate),
                output,
            }
        }
        node => node,
    }
}

/// Recursively simplifies an expression tree.
fn simplify_expression(expression: BoundExpr) -> BoundExpr {
    match expression {
        BoundExpr::Literal(_) | BoundExpr::Column(_) => expression,
        BoundExpr::Unary { op, expr } => {
            let expr = simplify_expression(*expr);
            simplify_unary(op, expr)
        }
        BoundExpr::Binary { left, op, right } => {
            let left = simplify_expression(*left);
            let right = simplify_expression(*right);
            simplify_binary(left, op, right)
        }
    }
}

/// Folds a unary operation or applies safe boolean/comparison rewrites.
fn simplify_unary(op: Op, expression: BoundExpr) -> BoundExpr {
    if let BoundExpr::Literal(value) = &expression
        && let Some(value) = evaluate_unary(op, value)
    {
        return BoundExpr::Literal(value);
    }

    if op == Op::Not {
        if let BoundExpr::Unary { op: Op::Not, expr } = expression {
            if expr.data_type() == Some(DataType::Boolean) {
                return *expr;
            }
            return BoundExpr::Unary { op, expr: Box::new(BoundExpr::Unary { op: Op::Not, expr }) };
        }
        if let Some(inverted) = invert_comparison(&expression) {
            return inverted;
        }
    }

    BoundExpr::Unary { op, expr: Box::new(expression) }
}

/// Folds literal binary operations and applies boolean identity rewrites.
fn simplify_binary(left: BoundExpr, op: Op, right: BoundExpr) -> BoundExpr {
    if let (BoundExpr::Literal(left_value), BoundExpr::Literal(right_value)) = (&left, &right)
        && let Some(value) = evaluate_binary(left_value, op, right_value)
    {
        return BoundExpr::Literal(value);
    }

    match (&left, op, &right) {
        (BoundExpr::Literal(Value::Boolean(false)), Op::And, _) => {
            return BoundExpr::Literal(Value::Boolean(false));
        }
        (BoundExpr::Literal(Value::Boolean(true)), Op::Or, _) => {
            return BoundExpr::Literal(Value::Boolean(true));
        }
        (BoundExpr::Literal(Value::Boolean(true)), Op::And, _)
        | (BoundExpr::Literal(Value::Boolean(false)), Op::Or, _)
            if right.data_type() == Some(DataType::Boolean) =>
        {
            return right;
        }
        (_, Op::And, BoundExpr::Literal(Value::Boolean(true)))
        | (_, Op::Or, BoundExpr::Literal(Value::Boolean(false)))
            if left.data_type() == Some(DataType::Boolean) =>
        {
            return left;
        }
        _ => {}
    }

    BoundExpr::Binary { left: Box::new(left), op, right: Box::new(right) }
}

/// Returns the comparison equivalent to negating a supported predicate.
fn invert_comparison(expression: &BoundExpr) -> Option<BoundExpr> {
    let BoundExpr::Binary { left, op, right } = expression else {
        return None;
    };
    let op = match op {
        Op::EqualsEquals => Op::NotEquals,
        Op::NotEquals => Op::EqualsEquals,
        Op::LessThan
            if left.data_type() != Some(DataType::Float)
                && right.data_type() != Some(DataType::Float) =>
        {
            Op::GreaterThanOrEqual
        }
        Op::GreaterThan
            if left.data_type() != Some(DataType::Float)
                && right.data_type() != Some(DataType::Float) =>
        {
            Op::LessThanOrEqual
        }
        Op::LessThanOrEqual
            if left.data_type() != Some(DataType::Float)
                && right.data_type() != Some(DataType::Float) =>
        {
            Op::GreaterThan
        }
        Op::GreaterThanOrEqual
            if left.data_type() != Some(DataType::Float)
                && right.data_type() != Some(DataType::Float) =>
        {
            Op::LessThan
        }
        _ => return None,
    };
    Some(BoundExpr::Binary { left: left.clone(), op, right: right.clone() })
}

/// Evaluates a supported unary operator on a literal value.
fn evaluate_unary(op: Op, value: &Value) -> Option<Value> {
    match (op, value) {
        (Op::Not, Value::Boolean(value)) => Some(Value::Boolean(!value)),
        (Op::Sub, Value::Integer(value)) => value.checked_neg().map(Value::Integer),
        (Op::Sub, Value::Float(value)) => Some(Value::Float(-value)),
        _ => None,
    }
}

/// Evaluates a supported binary operator on two literal values.
fn evaluate_binary(left: &Value, op: Op, right: &Value) -> Option<Value> {
    match op {
        Op::And | Op::Or => evaluate_boolean(left, op, right),
        Op::Add | Op::Sub | Op::Mul | Op::Div => evaluate_arithmetic(left, op, right),
        Op::EqualsEquals | Op::NotEquals => evaluate_equality(left, op, right),
        Op::LessThan | Op::GreaterThan | Op::LessThanOrEqual | Op::GreaterThanOrEqual => {
            evaluate_ordering(left, op, right)
        }
        Op::Not => None,
    }
}

/// Evaluates boolean conjunction or disjunction when both operands are boolean.
fn evaluate_boolean(left: &Value, op: Op, right: &Value) -> Option<Value> {
    let (Value::Boolean(left), Value::Boolean(right)) = (left, right) else {
        return None;
    };
    Some(Value::Boolean(match op {
        Op::And => *left && *right,
        Op::Or => *left || *right,
        _ => return None,
    }))
}

/// Evaluates supported same-type integer or floating-point arithmetic.
fn evaluate_arithmetic(left: &Value, op: Op, right: &Value) -> Option<Value> {
    match (left, op, right) {
        (Value::Integer(left), Op::Add, Value::Integer(right)) => {
            left.checked_add(*right).map(Value::Integer)
        }
        (Value::Integer(left), Op::Sub, Value::Integer(right)) => {
            left.checked_sub(*right).map(Value::Integer)
        }
        (Value::Integer(left), Op::Mul, Value::Integer(right)) => {
            left.checked_mul(*right).map(Value::Integer)
        }
        (Value::Integer(left), Op::Div, Value::Integer(right)) => {
            left.checked_div(*right).map(Value::Integer)
        }
        (Value::Float(left), Op::Add, Value::Float(right)) => Some(Value::Float(left + right)),
        (Value::Float(left), Op::Sub, Value::Float(right)) => Some(Value::Float(left - right)),
        (Value::Float(left), Op::Mul, Value::Float(right)) => Some(Value::Float(left * right)),
        (Value::Float(left), Op::Div, Value::Float(right)) if *right != 0.0 => {
            Some(Value::Float(left / right))
        }
        _ => None,
    }
}

/// Evaluates equality or inequality for compatible literal types.
fn evaluate_equality(left: &Value, op: Op, right: &Value) -> Option<Value> {
    let equal = match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Boolean(left), Value::Boolean(right)) => left == right,
        (Value::Integer(left), Value::Integer(right)) => left == right,
        (Value::Float(left), Value::Float(right)) => left == right,
        (Value::UnsignedInteger(left), Value::UnsignedInteger(right)) => left == right,
        _ => return None,
    };
    Some(Value::Boolean(if op == Op::EqualsEquals { equal } else { !equal }))
}

/// Evaluates an ordering comparison for compatible literal types.
fn evaluate_ordering(left: &Value, op: Op, right: &Value) -> Option<Value> {
    macro_rules! compare {
        ($left:expr, $right:expr) => {
            match op {
                Op::LessThan => $left < $right,
                Op::GreaterThan => $left > $right,
                Op::LessThanOrEqual => $left <= $right,
                Op::GreaterThanOrEqual => $left >= $right,
                _ => return None,
            }
        };
    }
    let result = match (left, right) {
        (Value::String(left), Value::String(right)) => compare!(left, right),
        (Value::Boolean(left), Value::Boolean(right)) => compare!(left, right),
        (Value::Integer(left), Value::Integer(right)) => compare!(left, right),
        (Value::Float(left), Value::Float(right)) => compare!(left, right),
        (Value::UnsignedInteger(left), Value::UnsignedInteger(right)) => compare!(left, right),
        _ => return None,
    };
    Some(Value::Boolean(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary(left: BoundExpr, op: Op, right: BoundExpr) -> BoundExpr {
        BoundExpr::Binary { left: Box::new(left), op, right: Box::new(right) }
    }

    #[test]
    fn folds_literal_expression_trees() {
        let expression = binary(
            binary(
                BoundExpr::Literal(Value::Integer(1)),
                Op::Add,
                BoundExpr::Literal(Value::Integer(2)),
            ),
            Op::EqualsEquals,
            BoundExpr::Literal(Value::Integer(3)),
        );

        assert_eq!(simplify_expression(expression), BoundExpr::Literal(Value::Boolean(true)));
    }

    #[test]
    fn leaves_failing_literal_expressions_for_runtime_evaluation() {
        let expression = binary(
            BoundExpr::Literal(Value::Integer(1)),
            Op::Div,
            BoundExpr::Literal(Value::Integer(0)),
        );

        assert_eq!(simplify_expression(expression.clone()), expression);
    }

    #[test]
    fn preserves_short_circuiting_when_simplifying_booleans() {
        let division_by_zero = binary(
            BoundExpr::Literal(Value::Integer(1)),
            Op::Div,
            BoundExpr::Literal(Value::Integer(0)),
        );
        let expression =
            binary(BoundExpr::Literal(Value::Boolean(false)), Op::And, division_by_zero);

        assert_eq!(simplify_expression(expression), BoundExpr::Literal(Value::Boolean(false)));
    }
}
