//! Scalar expressions used by bound logical and executable physical plans.

use std::fmt;

use crate::{
    core::{DataType, Value},
    sql_parser::parser::{op::Op, stmt::select::Ordering},
};

use super::RelationId;

/// Catalog column reference resolved during SQL binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumn {
    /// Query-local identity of the table occurrence that owns this column.
    pub relation: RelationId,
    /// Table name retained for diagnostics and plan formatting.
    pub table: String,
    /// Column name.
    pub name: String,
    /// Zero-based position in the source table row.
    pub ordinal: usize,
    /// Catalog type of the column.
    pub data_type: DataType,
}

impl fmt::Display for BoundColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.name)
    }
}

/// Scalar expression after catalog binding but before physical slot assignment.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    /// Constant storage value.
    Literal(Value),
    /// Reference to a query-local bound column.
    Column(BoundColumn),
    /// Unary operator applied to a bound expression.
    Unary { op: Op, expr: Box<BoundExpr> },
    /// Binary operator applied to two bound expressions.
    Binary { left: Box<BoundExpr>, op: Op, right: Box<BoundExpr> },
}

impl BoundExpr {
    pub(crate) fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Literal(value) => value_data_type(value),
            Self::Column(column) => Some(column.data_type),
            Self::Unary { op: Op::Not, .. } => Some(DataType::Boolean),
            Self::Unary { expr, .. } => expr.data_type(),
            Self::Binary {
                op:
                    Op::And
                    | Op::Or
                    | Op::EqualsEquals
                    | Op::NotEquals
                    | Op::LessThan
                    | Op::GreaterThan
                    | Op::LessThanOrEqual
                    | Op::GreaterThanOrEqual,
                ..
            } => Some(DataType::Boolean),
            Self::Binary { left, .. } => left.data_type(),
        }
    }
}

impl fmt::Display for BoundExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(value) => write!(f, "{value}"),
            Self::Column(column) => write!(f, "{column}"),
            Self::Unary { op, expr } => write!(f, "{op}{expr}"),
            Self::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
        }
    }
}

/// Input slot selected for an executable expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecColumn {
    /// Zero-based slot in the physical operator's input row.
    pub slot: usize,
    /// Qualified source name retained for diagnostics and `EXPLAIN`.
    pub name: String,
    /// Statically known slot type.
    pub data_type: DataType,
}

impl fmt::Display for ExecColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Scalar expression after physical input slots have been assigned.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecExpr {
    /// Constant storage value.
    Literal(Value),
    /// Reference to an input row slot.
    Column(ExecColumn),
    /// Unary operator applied to an executable expression.
    Unary { op: Op, expr: Box<ExecExpr> },
    /// Binary operator applied to two executable expressions.
    Binary { left: Box<ExecExpr>, op: Op, right: Box<ExecExpr> },
}

impl fmt::Display for ExecExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(value) => write!(f, "{value}"),
            Self::Column(column) => write!(f, "{column}"),
            Self::Unary { op, expr } => write!(f, "{op}{expr}"),
            Self::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
        }
    }
}

/// One bound column assignment from an `UPDATE ... SET` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundUpdateAssignment {
    /// Target column to overwrite.
    pub column: BoundColumn,
    /// Expression evaluated against the original row.
    pub expression: BoundExpr,
}

/// One executable update assignment.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAssignment {
    /// Target catalog column.
    pub column: BoundColumn,
    /// Slot-resolved expression evaluated against the original row.
    pub expression: ExecExpr,
}

impl fmt::Display for UpdateAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.column, self.expression)
    }
}

/// One bound `ORDER BY` term.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSortTerm {
    /// Bound source column used as the sort key.
    pub column: BoundColumn,
    /// Requested direction, or `None` for the SQL default.
    pub direction: Option<Ordering>,
}

/// One executable `ORDER BY` term.
#[derive(Debug, Clone, PartialEq)]
pub struct SortTerm {
    /// Input slot used as the sort key.
    pub column: ExecColumn,
    /// Requested direction, or `None` for the SQL default.
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

fn value_data_type(value: &Value) -> Option<DataType> {
    match value {
        Value::Null => None,
        Value::Integer(_) => Some(DataType::Integer),
        Value::Float(_) => Some(DataType::Float),
        Value::String(_) => Some(DataType::Text),
        Value::Boolean(_) => Some(DataType::Boolean),
        Value::UnsignedInteger(_) => Some(DataType::UnsignedInteger),
    }
}
