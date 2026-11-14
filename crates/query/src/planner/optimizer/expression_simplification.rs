//! Simplification of scalar expressions in logical plans.

use crate::relational::{DataType, Value};
use crate::scalar::{evaluate_binary, evaluate_unary};
use crate::sql_parser::parser::op::Op;

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
        && let Ok(value) = evaluate_unary(op, value.clone())
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
        && let Ok(value) = evaluate_binary(left_value.clone(), op, || Ok(right_value.clone()))
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
