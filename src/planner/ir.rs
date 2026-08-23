use std::fmt;

use thiserror::Error;

use super::*;

pub type PlannerResult<T> = Result<T, PlannerError>;

/// Complete planning result for one SQL statement.
///
/// A plan keeps both representations because they answer different questions:
/// the logical plan is the catalog-bound statement shape, while the physical
/// plan is the exact operator tree consumed by the executor. Tests often assert
/// both to verify that binding and access-path selection agree.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// Catalog-bound statement representation before physical operator
    /// selection.
    pub logical: LogicalPlan,
    /// Executable operator tree selected from the logical plan.
    pub physical: PhysicalPlan,
}

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

/// Executable operator tree selected by the planner.
///
/// Physical plans mirror the executor's available operators. They are still
/// declarative data, but table access and mutation shapes have been made
/// concrete enough for execution:
///
/// - table scans become full scans, primary-key range scans, or secondary-index
///   scans;
/// - `INSERT ... VALUES` becomes [`PhysicalPlan::InsertValues`]; and
/// - row-shaping operators such as filters, projections, limits, offsets, and
///   sorts keep their input subtrees.
///
/// [`fmt::Display`] renders this tree in the same shape returned by `EXPLAIN`.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    /// Return the formatted input plan without executing it.
    Explain {
        /// Plan to describe.
        input: Box<PhysicalPlan>,
    },
    /// Execute a catalog table creation.
    CreateTable {
        /// Table name to create.
        name: String,
        /// Row schema for the new table.
        schema: TupleSchema,
    },
    /// Execute a catalog secondary-index creation.
    CreateIndex {
        /// Index name to create.
        name: String,
        /// Table whose rows the index covers.
        table: TableSchema,
        /// Bound table columns that form the index key.
        columns: Vec<BoundColumn>,
    },
    /// Produce literal rows.
    Values {
        /// Planned expressions for each literal row.
        rows: Vec<Vec<PlannedExpression>>,
    },
    /// Insert literal values into bound table columns.
    InsertValues {
        /// Target table.
        table: TableSchema,
        /// Target columns in value order.
        columns: Vec<BoundColumn>,
        /// Literal value rows to insert.
        values: Vec<Vec<PlannedExpression>>,
    },
    /// Update rows from a table selected by an input operator.
    Update {
        /// Target table.
        table: TableSchema,
        /// Bound column assignments.
        assignments: Vec<UpdateAssignment>,
        /// Row-producing operator that yields target table records.
        input: Box<PhysicalPlan>,
    },
    /// Delete rows from a table selected by an input operator.
    Delete {
        /// Target table.
        table: TableSchema,
        /// Row-producing operator that yields target table records.
        input: Box<PhysicalPlan>,
    },
    /// Produce exactly one empty row.
    ///
    /// This is the row source for `SELECT` statements without a `FROM` clause.
    OneRow,
    /// Scan all rows from a table.
    FullTableScan {
        /// Table to scan.
        table: TableSchema,
    },
    /// Scan rows from a table whose primary key falls in a bounded range.
    ///
    /// The planner emits this for compatible comparisons against the first
    /// integer primary-key column. If only part of the `WHERE` predicate can be
    /// expressed as a key range, the range scan is wrapped in a
    /// [`PhysicalPlan::Filter`] for the residual expression.
    PrimaryKeyRangeScan {
        /// Table to scan.
        table: TableSchema,
        /// Primary-key range to scan.
        range: TableKeyRange,
    },
    /// Scan rows from a table through a secondary-index key range.
    ///
    /// This is selected for predicates on a compatible single-column secondary
    /// index. The scan produces candidate table rows; the original predicate is
    /// still applied by a surrounding [`PhysicalPlan::Filter`] so the executor
    /// preserves SQL semantics when the access range is only an approximation.
    SecondaryIndexScan {
        /// Scan metadata and key bounds.
        scan: Box<SecondaryIndexScanPlan>,
    },
    /// Filter rows from an input physical operator.
    Filter {
        /// Input operator.
        input: Box<PhysicalPlan>,
        /// Predicate evaluated for each input row.
        predicate: PlannedExpression,
    },
    /// Sort rows from an input physical operator.
    Sort {
        /// Input operator.
        input: Box<PhysicalPlan>,
        /// Sort keys in priority order.
        terms: Vec<SortTerm>,
    },
    /// Evaluate expressions for each input row.
    Project {
        /// Input operator.
        input: Box<PhysicalPlan>,
        /// Output expressions in result-column order.
        expressions: Vec<PlannedExpression>,
    },
    /// Skip input rows before producing output.
    Offset {
        /// Input operator.
        input: Box<PhysicalPlan>,
        /// Number of rows to skip.
        offset: u32,
    },
    /// Stop after producing a bounded number of rows.
    Limit {
        /// Input operator.
        input: Box<PhysicalPlan>,
        /// Maximum number of rows to emit.
        limit: u32,
    },
}

/// Metadata needed to scan a table through a secondary index.
///
/// Secondary indexes store encoded index keys, while diagnostics and `EXPLAIN`
/// output should stay close to SQL values. This struct therefore keeps both the
/// human-readable [`IndexValueRange`] and the encoded [`IndexKeyRange`] that the
/// storage layer scans.
#[derive(Debug, Clone, PartialEq)]
pub struct SecondaryIndexScanPlan {
    /// Table to fetch rows from.
    pub table: TableSchema,
    /// Secondary index to scan.
    pub index: IndexSchema,
    /// Bound table column matched by the index lookup.
    pub column: BoundColumn,
    /// Human-readable indexed value range.
    pub value_range: IndexValueRange,
    /// Encoded index-key range to scan.
    pub key_range: IndexKeyRange,
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_physical_plan(self, f, "", true, true)
    }
}

fn format_physical_plan(
    plan: &PhysicalPlan,
    f: &mut fmt::Formatter<'_>,
    prefix: &str,
    is_last: bool,
    is_root: bool,
) -> fmt::Result {
    if !is_root {
        write!(f, "\n{}{} ", prefix, if is_last { "`-" } else { "|-" })?;
    }
    write!(f, "{}", physical_plan_label(plan))?;

    if let Some(input) = physical_plan_input(plan) {
        let child_prefix = if is_root {
            String::new()
        } else {
            format!("{}{}", prefix, if is_last { "   " } else { "|  " })
        };
        format_physical_plan(input, f, &child_prefix, true, false)?;
    }

    Ok(())
}

fn physical_plan_input(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match plan {
        PhysicalPlan::Explain { input }
        | PhysicalPlan::Update { input, .. }
        | PhysicalPlan::Delete { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Offset { input, .. }
        | PhysicalPlan::Limit { input, .. } => Some(input),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::InsertValues { .. }
        | PhysicalPlan::OneRow
        | PhysicalPlan::FullTableScan { .. }
        | PhysicalPlan::PrimaryKeyRangeScan { .. }
        | PhysicalPlan::SecondaryIndexScan { .. } => None,
    }
}

fn physical_plan_label(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Explain { .. } => "Explain".to_owned(),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable table={name}"),
        PhysicalPlan::CreateIndex { name, table, columns } => format!(
            "CreateIndex index={name} table={} columns=[{}]",
            table.name,
            display_list(columns)
        ),
        PhysicalPlan::Values { rows } => format!("Values rows={}", rows.len()),
        PhysicalPlan::InsertValues { table, columns, values } => format!(
            "InsertValues table={} columns=[{}] rows={}",
            table.name,
            display_list(columns),
            values.len()
        ),
        PhysicalPlan::Update { table, assignments, .. } => {
            format!("Update table={} assignments=[{}]", table.name, display_list(assignments))
        }
        PhysicalPlan::Delete { table, .. } => format!("Delete table={}", table.name),
        PhysicalPlan::OneRow => "OneRow".to_owned(),
        PhysicalPlan::FullTableScan { table } => format!("FullTableScan table={}", table.name),
        PhysicalPlan::PrimaryKeyRangeScan { table, range } => {
            format!("PrimaryKeyRangeScan table={} range=[{}]", table.name, range)
        }
        PhysicalPlan::SecondaryIndexScan { scan } => format!(
            "SecondaryIndexScan table={} index={} column={} range=[{}]",
            scan.table.name, scan.index.name, scan.column, scan.value_range
        ),
        PhysicalPlan::Filter { predicate, .. } => format!("Filter predicate={predicate}"),
        PhysicalPlan::Sort { terms, .. } => format!("Sort terms=[{}]", display_list(terms)),
        PhysicalPlan::Project { expressions, .. } => {
            format!("Project expressions=[{}]", display_list(expressions))
        }
        PhysicalPlan::Offset { offset, .. } => format!("Offset offset={offset}"),
        PhysicalPlan::Limit { limit, .. } => format!("Limit limit={limit}"),
    }
}

fn display_list<T: fmt::Display>(values: &[T]) -> String {
    values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

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

/// Inclusive or exclusive display bound for a secondary-index scan value.
///
/// These bounds are used for formatted plans and tests. The storage-facing byte
/// bounds live in [`IndexKeyBound`] values inside [`SecondaryIndexScanPlan`].
#[derive(Debug, Clone, PartialEq)]
pub enum IndexValueBound {
    /// The scan includes this value.
    Inclusive(Value),
    /// The scan excludes this value.
    Exclusive(Value),
}

impl fmt::Display for IndexValueBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inclusive(value) => write!(f, "{value} inclusive"),
            Self::Exclusive(value) => write!(f, "{value} exclusive"),
        }
    }
}

/// Human-readable range over values in a single-column secondary index.
///
/// This mirrors the encoded [`IndexKeyRange`] selected for storage, but keeps
/// the original SQL value type visible for debugging and `EXPLAIN` output.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexValueRange {
    /// Optional lower value bound.
    pub lower: Option<IndexValueBound>,
    /// Optional upper value bound.
    pub upper: Option<IndexValueBound>,
}

impl fmt::Display for IndexValueRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.lower, &self.upper) {
            (None, None) => write!(f, "unbounded"),
            (Some(lower), None) => write!(f, "lower={lower}"),
            (None, Some(upper)) => write!(f, "upper={upper}"),
            (Some(lower), Some(upper)) => write!(f, "lower={lower} upper={upper}"),
        }
    }
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

/// Errors that can occur while converting parsed SQL into a plan.
///
/// Planner errors are limited to catalog binding, statement-shape validation,
/// unsupported syntax, and catalog access failures. Runtime errors that depend
/// on row contents are reported by the executor or storage layer instead.
#[derive(Debug, Error)]
pub enum PlannerError {
    /// A statement referenced a table that does not exist in the catalog.
    #[error("table not found: {name}")]
    TableNotFound { name: String },
    /// A statement referenced a column that is not present in the bound table.
    #[error("column {column} not found")]
    ColumnNotFound { column: String },
    /// An `INSERT` column list named the same column more than once.
    #[error("duplicate insert column: {column}")]
    DuplicateInsertColumn { column: String },
    /// An `UPDATE` assignment list named the same column more than once.
    #[error("duplicate update column: {column}")]
    DuplicateUpdateColumn { column: String },
    /// A `CREATE INDEX` column list named the same column more than once.
    #[error("duplicate index column: {column}")]
    DuplicateIndexColumn { column: String },
    /// An `UPDATE` attempted to modify a primary-key column.
    #[error("cannot update primary key column: {column}")]
    PrimaryKeyUpdate { column: String },
    /// A values row does not provide exactly one value for each target column.
    #[error("insert row has {values} values for {columns} columns")]
    InsertColumnValueCount { columns: usize, values: usize },
    /// The parser accepted a statement kind the planner cannot lower.
    #[error("unsupported statement: {statement}")]
    UnsupportedStatement { statement: String },
    /// The planner cannot lower this expression in the current context.
    #[error("unsupported expression: {expression}")]
    UnsupportedExpression { expression: String },
    /// Aggregate functions are parsed but not yet planned.
    #[error("unsupported aggregate function: {function}")]
    UnsupportedAggregate { function: String },
    /// A wildcard appeared outside the projection list.
    #[error("wildcard is only supported in SELECT projection")]
    UnsupportedWildcardPosition,
    /// A wildcard projection was used without a table to expand against.
    #[error("wildcard projection requires a FROM table")]
    WildcardRequiresTable,
    /// Physical planning found an insert input shape it cannot execute.
    #[error("invalid insert input: expected VALUES")]
    InvalidInsertInput,
    /// Storage or catalog access failed while planning.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
