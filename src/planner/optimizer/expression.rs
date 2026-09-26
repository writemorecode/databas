//! Shared expression-tree helpers for logical optimizer passes.

use crate::sql_parser::parser::op::Op;

use super::super::{BoundColumn, BoundExpr};

pub(super) fn referenced_columns(expression: &BoundExpr) -> Vec<&BoundColumn> {
    let mut columns = Vec::new();
    collect_referenced_columns(expression, &mut columns);
    columns
}

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

pub(super) fn conjuncts(expression: BoundExpr) -> Vec<BoundExpr> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expression, &mut conjuncts);
    conjuncts
}

fn flatten_conjuncts(expression: BoundExpr, conjuncts: &mut Vec<BoundExpr>) {
    match expression {
        BoundExpr::Binary { left, op: Op::And, right } => {
            flatten_conjuncts(*left, conjuncts);
            flatten_conjuncts(*right, conjuncts);
        }
        expression => conjuncts.push(expression),
    }
}

pub(super) fn combine_conjuncts(conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    conjuncts.into_iter().reduce(|left, right| BoundExpr::Binary {
        left: Box::new(left),
        op: Op::And,
        right: Box::new(right),
    })
}
