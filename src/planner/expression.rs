//! Catalog-bound scalar expressions shared by logical and physical plans.

use std::fmt;

use crate::{
    core::{DataType, Value},
    sql_parser::parser::{op::Op, stmt::select::Ordering},
};

/// Expression after literal conversion and column binding.
///
/// Planned expressions are the scalar language shared by filters, projections,
/// update assignments, and insert values. Identifiers have already been
/// resolved into [`BoundColumn`] values, and parser literals have already been
/// converted into storage [`Value`]s.
///
/// The planner does not type-check every operator combination. It records the
/// bound expression tree and leaves value-dependent type errors, such as adding
/// incompatible values or evaluating a non-boolean predicate, to execution.
#[derive(Debug, Clone, PartialEq)]
pub enum PlannedExpression {
    /// Constant storage value.
    Literal(Value),
    /// Reference to a bound table column.
    Column(BoundColumn),
    /// Unary operator applied to a planned expression.
    Unary { op: Op, expr: Box<PlannedExpression> },
    /// Binary operator applied to two planned expressions.
    Binary { left: Box<PlannedExpression>, op: Op, right: Box<PlannedExpression> },
}

impl fmt::Display for PlannedExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannedExpression::Literal(value) => write!(f, "{value}"),
            PlannedExpression::Column(column) => write!(f, "{column}"),
            PlannedExpression::Unary { op, expr } => write!(f, "{op}{expr}"),
            PlannedExpression::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
        }
    }
}

/// Catalog column reference resolved during planning.
///
/// A bound column is deliberately redundant: it stores display names for
/// diagnostics and plan formatting, plus the row ordinal and data type needed by
/// the executor. `ordinal` is the zero-based position of the column in the table
/// row schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumn {
    /// Name of the table that owns this column.
    pub table: String,
    /// Column name.
    pub name: String,
    /// Zero-based column position in the table row.
    pub ordinal: usize,
    /// Storage type recorded for the column.
    pub data_type: DataType,
}

impl fmt::Display for BoundColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.name)
    }
}

/// One bound column assignment from an `UPDATE ... SET` clause.
///
/// The target column has already been checked for existence, duplicate
/// assignment, and primary-key immutability. The expression is evaluated against
/// the original row when the update executes.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAssignment {
    /// Target column to overwrite.
    pub column: BoundColumn,
    /// Expression evaluated against the original row.
    pub expression: PlannedExpression,
}

impl fmt::Display for UpdateAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.column, self.expression)
    }
}

/// One bound column and optional direction from an `ORDER BY` clause.
///
/// Only simple column sort keys are represented today. A missing direction
/// means SQL omitted `ASC` or `DESC`; consumers should treat that as their
/// default ascending order.
#[derive(Debug, Clone, PartialEq)]
pub struct SortTerm {
    /// Column used as the sort key.
    pub column: BoundColumn,
    /// Direction specified by SQL, or `None` when the query omitted one.
    pub direction: Option<Ordering>,
}

impl fmt::Display for SortTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.column)?;
        if let Some(direction) = &self.direction {
            write!(f, " {direction}")?;
        }
        Ok(())
    }
}
