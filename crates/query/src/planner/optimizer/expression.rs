//! Shared expression-tree helpers for logical optimizer passes.

use crate::sql_parser::parser::op::Op;

use super::super::{BoundColumn, BoundExpr};

/// Returns the columns referenced by an expression in traversal order.
pub(super) fn referenced_columns(expression: &BoundExpr) -> Vec<&BoundColumn> {
    let mut columns = Vec::new();
    collect_referenced_columns(expression, &mut columns);
    columns
}

/// Recursively appends all column references found in an expression.
fn collect_referenced_columns<'expression>(
    expression: &'expression BoundExpr,
    columns: &mut Vec<&'expression BoundColumn>,
) {
    match expression {
        BoundExpr::Literal(_) => {}
        BoundExpr::Column(column) => columns.push(column),
        BoundExpr::Unary { expr, .. } => collect_referenced_columns(expr, columns),
        BoundExpr::Binary { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
    }
}

/// Splits a conjunction into its individual conjunct expressions.
pub(super) fn conjuncts(expression: BoundExpr) -> Vec<BoundExpr> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expression, &mut conjuncts);
    conjuncts
}

/// Appends the leaves of an `AND` tree to the conjunct list.
fn flatten_conjuncts(expression: BoundExpr, conjuncts: &mut Vec<BoundExpr>) {
    match expression {
        BoundExpr::Binary { left, op: Op::And, right } => {
            flatten_conjuncts(*left, conjuncts);
            flatten_conjuncts(*right, conjuncts);
        }
        expression => conjuncts.push(expression),
    }
}

/// Combines expressions into a left-associated conjunction, or returns `None` for an empty list.
pub(super) fn combine_conjuncts(conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    conjuncts.into_iter().reduce(|left, right| BoundExpr::Binary {
        left: Box::new(left),
        op: Op::And,
        right: Box::new(right),
    })
}
