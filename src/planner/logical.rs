//! Catalog-bound logical plan representation.

use crate::core::{TableSchema, TupleSchema};

use super::{BoundColumn, PlannedExpression, SortTerm, UpdateAssignment};

/// Catalog-bound relational representation of a parsed SQL statement.
///
/// Logical plans are still independent of any concrete scan strategy. A
/// [`LogicalPlan::TableScan`] means "rows from this table"; deciding whether
/// those rows come from a full scan, primary-key range, or secondary index is a
/// physical-planning concern.
///
/// Every table or column reference in this enum is already bound to catalog
/// metadata. Children are boxed so select, update, and delete statements can be
/// represented as recursive operator trees.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Return the physical plan for an input statement without executing it.
    Explain { input: Box<LogicalPlan> },
    /// Create a table with the provided tuple schema.
    CreateTable { name: String, schema: TupleSchema },
    /// Create a secondary index over bound columns from an existing table.
    CreateIndex { name: String, table: TableSchema, columns: Vec<BoundColumn> },
    /// Literal rows, usually produced by an `INSERT ... VALUES` statement.
    ///
    /// The current planner accepts only literal expressions in insert values, so
    /// this node is side-effect free and independent of table input.
    Values { rows: Vec<Vec<PlannedExpression>> },
    /// Insert rows from an input plan into bound table columns.
    ///
    /// The input is currently expected to be [`LogicalPlan::Values`] during
    /// physical planning.
    Insert { table: TableSchema, columns: Vec<BoundColumn>, input: Box<LogicalPlan> },
    /// Update rows in a table selected by an input plan.
    ///
    /// Assignment targets are bound and checked for duplicate names before this
    /// node is built. Primary-key columns are rejected here because changing
    /// them would require moving table records.
    Update { table: TableSchema, assignments: Vec<UpdateAssignment>, input: Box<LogicalPlan> },
    /// Delete rows from a table selected by an input plan.
    Delete { table: TableSchema, input: Box<LogicalPlan> },
    /// Synthetic single-row input used for projection-only selects without a
    /// `FROM` clause.
    OneRow,
    /// Read every row from a catalog table.
    TableScan { table: TableSchema },
    /// Keep only rows for which the predicate evaluates truthfully.
    ///
    /// Physical planning may use part of this predicate to choose a narrower
    /// table access path. Any remaining predicate is preserved as a filter.
    Filter { input: Box<LogicalPlan>, predicate: PlannedExpression },
    /// Order input rows by one or more columns.
    Sort { input: Box<LogicalPlan>, terms: Vec<SortTerm> },
    /// Produce output expressions from each input row.
    Project { input: Box<LogicalPlan>, expressions: Vec<PlannedExpression> },
    /// Skip the first `offset` input rows.
    Offset { input: Box<LogicalPlan>, offset: u32 },
    /// Emit at most `limit` input rows.
    Limit { input: Box<LogicalPlan>, limit: u32 },
}
