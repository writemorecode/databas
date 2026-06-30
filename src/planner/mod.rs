//! SQL query planning.
//!
//! The planner lowers parsed SQL statements into a [`Plan`] that contains both
//! a catalog-bound [`LogicalPlan`] and the executable [`PhysicalPlan`]. Planning
//! resolves table and column names against the database catalog, expands
//! wildcard projections, validates statement shapes, and records enough schema
//! metadata for execution to read or write typed tuples.
//!
//! This planner is intentionally conservative. It preserves the parsed query
//! shape for most relational operators and currently chooses only simple
//! physical operators such as full table scans and values-backed inserts.

use std::{collections::HashSet, fmt};

use thiserror::Error;

use crate::{
    core::{
        ColumnSchema, DataType, Database, TableSchema, TupleSchema, Value,
        access::SchemaAccess,
        error::{InvalidArgumentError, StorageError},
    },
    sql_parser::{
        NumberKind,
        parser::{
            expr::{Expression, Literal},
            op::Op,
            stmt::{
                Statement,
                create_index::CreateIndexQuery,
                create_table::CreateTableQuery,
                delete::DeleteQuery,
                insert::InsertQuery,
                select::{Ordering, SelectQuery},
                update::UpdateQuery,
            },
        },
    },
};

/// Result type returned by query planning operations.
pub type PlannerResult<T> = Result<T, PlannerError>;

/// Complete planning result for one SQL statement.
///
/// The logical plan is useful for validation, tests, and future optimization
/// passes. The physical plan is the tree consumed by the executor.
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
/// Logical plans describe what the statement means after name binding and basic
/// validation, but before choosing concrete access methods. Children are stored
/// in `Box`es so the plan can form recursive operator trees.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Return the physical plan for an input statement without executing it.
    Explain { input: Box<LogicalPlan> },
    /// Create a table with the provided tuple schema.
    CreateTable { name: String, schema: TupleSchema },
    /// Create a secondary index over bound columns from an existing table.
    CreateIndex { name: String, table: TableSchema, columns: Vec<BoundColumn> },
    /// Literal rows, usually produced by an `INSERT ... VALUES` statement.
    Values { rows: Vec<Vec<PlannedExpression>> },
    /// Insert rows from an input plan into bound table columns.
    Insert { table: TableSchema, columns: Vec<BoundColumn>, input: Box<LogicalPlan> },
    /// Update rows in a table selected by an input plan.
    Update { table: TableSchema, assignments: Vec<UpdateAssignment>, input: Box<LogicalPlan> },
    /// Delete rows from a table selected by an input plan.
    Delete { table: TableSchema, input: Box<LogicalPlan> },
    /// Synthetic single-row input used for projection-only selects without a
    /// `FROM` clause.
    OneRow,
    /// Read every row from a catalog table.
    TableScan { table: TableSchema },
    /// Keep only rows for which the predicate evaluates truthfully.
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
/// Physical plans mirror the current executor's available operators. Today this
/// mostly maps logical operators directly, with a few concrete choices such as
/// [`PhysicalPlan::FullTableScan`] for table access and
/// [`PhysicalPlan::InsertValues`] for `INSERT ... VALUES`.
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
    OneRow,
    /// Scan all rows from a table.
    FullTableScan {
        /// Table to scan.
        table: TableSchema,
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
        | PhysicalPlan::FullTableScan { .. } => None,
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
/// Bound column expressions carry catalog metadata and row ordinals, so the
/// executor can evaluate them without doing name resolution again.
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
/// `ordinal` is the zero-based position of the column in the table row schema.
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

/// Normalized sort direction.
///
/// This enum is available for consumers that need an executor-level direction
/// independent of the parser's `ORDER BY` syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Sort smaller values before larger values.
    Ascending,
    /// Sort larger values before smaller values.
    Descending,
}

/// Errors that can occur while converting parsed SQL into a plan.
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

/// Planner bound to a database catalog.
///
/// A planner borrows a [`Database`] so it can resolve table schemas and bind
/// column references. It does not mutate the catalog; DDL statements are only
/// represented as plan nodes until executed.
pub struct Planner<'db> {
    schema: &'db dyn SchemaAccess,
}

impl<'db> Planner<'db> {
    /// Creates a planner that resolves names through `database`.
    pub fn new(database: &'db Database) -> Self {
        Self { schema: database }
    }

    /// Plans one parsed SQL statement.
    ///
    /// This first builds a [`LogicalPlan`] with catalog-bound names, then
    /// converts it into a [`PhysicalPlan`] that can be passed to the executor.
    pub fn plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<Plan> {
        let logical = self.logical_plan_statement(statement)?;
        let physical = self.physical_plan(&logical)?;
        Ok(Plan { logical, physical })
    }

    fn logical_plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<LogicalPlan> {
        match statement {
            Statement::Explain(statement) => self.plan_explain(statement),
            Statement::CreateTable(query) => self.plan_create_table(query),
            Statement::CreateIndex(query) => self.plan_create_index(query),
            Statement::Insert(query) => self.plan_insert(query),
            Statement::Update(query) => self.plan_update(query),
            Statement::Delete(query) => self.plan_delete(query),
            Statement::Select(query) => self.plan_select(query),
        }
    }

    fn plan_explain(&self, statement: &Statement<'_>) -> PlannerResult<LogicalPlan> {
        match statement {
            Statement::Select(query) => {
                Ok(LogicalPlan::Explain { input: Box::new(self.plan_select(query)?) })
            }
            statement => {
                Err(PlannerError::UnsupportedStatement { statement: statement.to_string() })
            }
        }
    }

    fn plan_create_table(&self, query: &CreateTableQuery<'_>) -> PlannerResult<LogicalPlan> {
        Ok(LogicalPlan::CreateTable {
            name: query.table_name.to_owned(),
            schema: TupleSchema::from_create_table_query(query),
        })
    }

    fn plan_create_index(&self, query: &CreateIndexQuery<'_>) -> PlannerResult<LogicalPlan> {
        let table = self.table_schema(query.table_name)?;
        let mut seen = HashSet::new();
        let mut columns = Vec::new();

        for column in &query.columns.0 {
            if !seen.insert(*column) {
                return Err(PlannerError::DuplicateIndexColumn { column: (*column).to_owned() });
            }
            columns.push(bind_column(&table, column)?);
        }

        Ok(LogicalPlan::CreateIndex { name: query.index_name.to_owned(), table, columns })
    }

    fn plan_insert(&self, query: &InsertQuery<'_>) -> PlannerResult<LogicalPlan> {
        let table = self.table_schema(query.table)?;
        let mut seen = HashSet::new();
        let mut columns = Vec::new();

        for column in &query.columns.0 {
            if !seen.insert(*column) {
                return Err(PlannerError::DuplicateInsertColumn { column: (*column).to_owned() });
            }
            columns.push(bind_column(&table, column)?);
        }

        let mut rows = Vec::new();
        for row in &query.values.0 {
            if row.0.len() != columns.len() {
                return Err(PlannerError::InsertColumnValueCount {
                    columns: columns.len(),
                    values: row.0.len(),
                });
            }
            rows.push(
                row.0
                    .iter()
                    .map(|expr| literal_expression(expr).ok_or_else(|| unsupported_expr(expr)))
                    .collect::<PlannerResult<Vec<_>>>()?,
            );
        }

        Ok(LogicalPlan::Insert { table, columns, input: Box::new(LogicalPlan::Values { rows }) })
    }

    fn plan_delete(&self, query: &DeleteQuery<'_>) -> PlannerResult<LogicalPlan> {
        let table = self.table_schema(query.table)?;
        let mut input = LogicalPlan::TableScan { table: table.clone() };

        if let Some(predicate) = &query.where_clause {
            input = LogicalPlan::Filter {
                input: Box::new(input),
                predicate: self.bind_expression(predicate, Some(&table))?,
            };
        }

        Ok(LogicalPlan::Delete { table, input: Box::new(input) })
    }

    fn plan_update(&self, query: &UpdateQuery<'_>) -> PlannerResult<LogicalPlan> {
        let table = self.table_schema(query.table)?;
        let mut seen = HashSet::new();
        let mut assignments = Vec::new();

        for assignment in &query.assignments.0 {
            if !seen.insert(assignment.column) {
                return Err(PlannerError::DuplicateUpdateColumn {
                    column: assignment.column.to_owned(),
                });
            }
            let column = bind_column(&table, assignment.column)?;
            if table.row.columns[column.ordinal].primary_key {
                return Err(PlannerError::PrimaryKeyUpdate { column: column.name });
            }
            assignments.push(UpdateAssignment {
                column,
                expression: self.bind_expression(&assignment.expression, Some(&table))?,
            });
        }

        let mut input = LogicalPlan::TableScan { table: table.clone() };
        if let Some(predicate) = &query.where_clause {
            input = LogicalPlan::Filter {
                input: Box::new(input),
                predicate: self.bind_expression(predicate, Some(&table))?,
            };
        }

        Ok(LogicalPlan::Update { table, assignments, input: Box::new(input) })
    }

    fn plan_select(&self, query: &SelectQuery<'_>) -> PlannerResult<LogicalPlan> {
        let table = query.table.map(|name| self.table_schema(name)).transpose()?;
        let mut plan = match &table {
            Some(table) => LogicalPlan::TableScan { table: table.clone() },
            None => LogicalPlan::OneRow,
        };

        if let Some(predicate) = &query.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: self.bind_expression(predicate, table.as_ref())?,
            };
        }

        if let Some(order_by) = &query.order_by {
            let terms = order_by
                .terms
                .iter()
                .map(|term| {
                    let table = table.as_ref().ok_or_else(|| PlannerError::ColumnNotFound {
                        column: term.column.to_owned(),
                    })?;
                    Ok(SortTerm {
                        column: bind_column(table, term.column)?,
                        direction: term.order.clone(),
                    })
                })
                .collect::<PlannerResult<Vec<_>>>()?;
            plan = LogicalPlan::Sort { input: Box::new(plan), terms };
        }

        let expressions = self.bind_projection(&query.columns.0, table.as_ref())?;
        plan = LogicalPlan::Project { input: Box::new(plan), expressions };

        if let Some(offset) = query.offset {
            plan = LogicalPlan::Offset { input: Box::new(plan), offset };
        }

        if let Some(limit) = query.limit {
            plan = LogicalPlan::Limit { input: Box::new(plan), limit };
        }

        Ok(plan)
    }

    fn physical_plan(&self, logical: &LogicalPlan) -> PlannerResult<PhysicalPlan> {
        match logical {
            LogicalPlan::Explain { input } => {
                Ok(PhysicalPlan::Explain { input: Box::new(self.physical_plan(input)?) })
            }
            LogicalPlan::CreateTable { name, schema } => {
                Ok(PhysicalPlan::CreateTable { name: name.clone(), schema: schema.clone() })
            }
            LogicalPlan::CreateIndex { name, table, columns } => Ok(PhysicalPlan::CreateIndex {
                name: name.clone(),
                table: table.clone(),
                columns: columns.clone(),
            }),
            LogicalPlan::Values { rows } => Ok(PhysicalPlan::Values { rows: rows.clone() }),
            LogicalPlan::Insert { table, columns, input } => match input.as_ref() {
                LogicalPlan::Values { rows } => Ok(PhysicalPlan::InsertValues {
                    table: table.clone(),
                    columns: columns.clone(),
                    values: rows.clone(),
                }),
                _ => Err(PlannerError::InvalidInsertInput),
            },
            LogicalPlan::Update { table, assignments, input } => Ok(PhysicalPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                input: Box::new(self.physical_plan(input)?),
            }),
            LogicalPlan::Delete { table, input } => Ok(PhysicalPlan::Delete {
                table: table.clone(),
                input: Box::new(self.physical_plan(input)?),
            }),
            LogicalPlan::OneRow => Ok(PhysicalPlan::OneRow),
            LogicalPlan::TableScan { table } => {
                Ok(PhysicalPlan::FullTableScan { table: table.clone() })
            }
            LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter {
                input: Box::new(self.physical_plan(input)?),
                predicate: predicate.clone(),
            }),
            LogicalPlan::Sort { input, terms } => Ok(PhysicalPlan::Sort {
                input: Box::new(self.physical_plan(input)?),
                terms: terms.clone(),
            }),
            LogicalPlan::Project { input, expressions } => Ok(PhysicalPlan::Project {
                input: Box::new(self.physical_plan(input)?),
                expressions: expressions.clone(),
            }),
            LogicalPlan::Offset { input, offset } => Ok(PhysicalPlan::Offset {
                input: Box::new(self.physical_plan(input)?),
                offset: *offset,
            }),
            LogicalPlan::Limit { input, limit } => Ok(PhysicalPlan::Limit {
                input: Box::new(self.physical_plan(input)?),
                limit: *limit,
            }),
        }
    }

    fn bind_projection(
        &self,
        expressions: &[Expression<'_>],
        table: Option<&TableSchema>,
    ) -> PlannerResult<Vec<PlannedExpression>> {
        let mut bound = Vec::new();
        for expression in expressions {
            match expression {
                Expression::Wildcard => {
                    let table = table.ok_or(PlannerError::WildcardRequiresTable)?;
                    bound.extend(table.row.columns.iter().enumerate().map(|(ordinal, column)| {
                        PlannedExpression::Column(bound_column(table, ordinal, column))
                    }));
                }
                _ => bound.push(self.bind_expression(expression, table)?),
            }
        }
        Ok(bound)
    }

    fn bind_expression(
        &self,
        expression: &Expression<'_>,
        table: Option<&TableSchema>,
    ) -> PlannerResult<PlannedExpression> {
        match expression {
            Expression::Literal(literal) => Ok(PlannedExpression::Literal(Value::from(literal))),
            Expression::Identifier(column) => match table {
                Some(table) => bind_column(table, column).map(PlannedExpression::Column),
                None => Err(PlannerError::ColumnNotFound { column: (*column).to_owned() }),
            },
            Expression::UnaryOp((op, expr)) => Ok(PlannedExpression::Unary {
                op: *op,
                expr: Box::new(self.bind_expression(expr, table)?),
            }),
            Expression::BinaryOp((left, op, right)) => Ok(PlannedExpression::Binary {
                left: Box::new(self.bind_expression(left, table)?),
                op: *op,
                right: Box::new(self.bind_expression(right, table)?),
            }),
            Expression::Wildcard => Err(PlannerError::UnsupportedWildcardPosition),
            Expression::AggregateFunction(aggregate) => {
                Err(PlannerError::UnsupportedAggregate { function: aggregate.kind.to_string() })
            }
        }
    }

    fn table_schema(&self, name: &str) -> PlannerResult<TableSchema> {
        self.schema.table_schema_by_name(name).map_err(|error| match error {
            StorageError::InvalidArgument(InvalidArgumentError::TableNotFound { name }) => {
                PlannerError::TableNotFound { name }
            }
            other => PlannerError::Storage(other),
        })
    }
}

fn bind_column(table: &TableSchema, column: &str) -> PlannerResult<BoundColumn> {
    table
        .row
        .columns
        .iter()
        .enumerate()
        .find(|(_, schema)| schema.name == column)
        .map(|(ordinal, schema)| bound_column(table, ordinal, schema))
        .ok_or_else(|| PlannerError::ColumnNotFound { column: column.to_owned() })
}

fn bound_column(table: &TableSchema, ordinal: usize, column: &ColumnSchema) -> BoundColumn {
    BoundColumn {
        table: table.name.clone(),
        name: column.name.clone(),
        ordinal,
        data_type: column.data_type,
    }
}

fn literal_expression(expression: &Expression<'_>) -> Option<PlannedExpression> {
    match expression {
        Expression::Literal(literal) => Some(PlannedExpression::Literal(Value::from(literal))),
        _ => None,
    }
}

impl From<&Literal<'_>> for Value {
    fn from(literal: &Literal) -> Self {
        match literal {
            Literal::String(value) => Value::String((*value).to_owned()),
            Literal::Number(NumberKind::Integer(value)) => Value::Integer(*value),
            Literal::Number(NumberKind::Float(value)) => Value::Float(*value),
            Literal::Boolean(value) => Value::Boolean(*value),
        }
    }
}

fn unsupported_expr(expression: &Expression<'_>) -> PlannerError {
    match expression {
        Expression::AggregateFunction(aggregate) => {
            PlannerError::UnsupportedAggregate { function: aggregate.kind.to_string() }
        }
        Expression::Wildcard => PlannerError::UnsupportedWildcardPosition,
        _ => PlannerError::UnsupportedExpression { expression: expression.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{
        core::{ColumnSchema, DataType},
        sql_parser::parser::Parser,
    };

    fn parse(sql: &str) -> Statement<'_> {
        Parser::new(sql).stmt().unwrap()
    }

    fn users_schema() -> TupleSchema {
        TupleSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "age".to_owned(),
                    data_type: DataType::Integer,
                    nullable: true,
                    primary_key: false,
                },
            ],
        }
    }

    fn database_with_users() -> (tempfile::TempDir, Database) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let database = Database::create(&path).unwrap();
        database.create_table("users", users_schema()).unwrap();
        (dir, database)
    }

    #[test]
    fn create_table_produces_logical_and_physical_create_table_plans() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("test.db")).unwrap();
        let planner = Planner::new(&database);
        let statement =
            parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT NULLABLE);");

        let plan = planner.plan_statement(&statement).unwrap();

        let expected_schema = users_schema();
        assert_eq!(
            plan.logical,
            LogicalPlan::CreateTable { name: "users".to_owned(), schema: expected_schema.clone() }
        );
        assert_eq!(
            plan.physical,
            PhysicalPlan::CreateTable { name: "users".to_owned(), schema: expected_schema }
        );
    }

    #[test]
    fn create_index_binds_table_and_index_columns() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("CREATE INDEX idx_users_name_age ON users (name, age);");

        let plan = planner.plan_statement(&statement).unwrap();

        let expected_columns = vec![
            bound("users", "name", 1, DataType::Text),
            bound("users", "age", 2, DataType::Integer),
        ];
        assert_eq!(
            plan.logical,
            LogicalPlan::CreateIndex {
                name: "idx_users_name_age".to_owned(),
                table: database.table_schema_by_name("users").unwrap(),
                columns: expected_columns.clone(),
            }
        );
        assert_eq!(
            plan.physical,
            PhysicalPlan::CreateIndex {
                name: "idx_users_name_age".to_owned(),
                table: database.table_schema_by_name("users").unwrap(),
                columns: expected_columns,
            }
        );
    }

    #[test]
    fn insert_binds_table_columns_and_values() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace');");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Insert { table, columns, input } = &plan.logical else {
            panic!("expected logical insert plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert_eq!(
            columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(
            input.as_ref(),
            &LogicalPlan::Values {
                rows: vec![
                    vec![
                        PlannedExpression::Literal(Value::Integer(1)),
                        PlannedExpression::Literal(Value::String("Ada".to_owned())),
                    ],
                    vec![
                        PlannedExpression::Literal(Value::Integer(2)),
                        PlannedExpression::Literal(Value::String("Grace".to_owned())),
                    ],
                ],
            }
        );

        let PhysicalPlan::InsertValues { table, columns, values } = &plan.physical else {
            panic!("expected physical insert values plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert_eq!(
            columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn update_all_plans_full_table_scan_under_update() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("UPDATE users SET name = 'Ada';");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Update { table, assignments, input } = &plan.logical else {
            panic!("expected logical update plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column, bound("users", "name", 1, DataType::Text));
        assert_eq!(
            assignments[0].expression,
            PlannedExpression::Literal(Value::String("Ada".to_owned()))
        );
        assert!(
            matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users")
        );

        let PhysicalPlan::Update { table, assignments, input } = &plan.physical else {
            panic!("expected physical update plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert_eq!(assignments.len(), 1);
        assert!(
            matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
        );
    }

    #[test]
    fn update_where_binds_filter_and_assignment_column_refs() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("UPDATE users SET age = age + 1 WHERE id == 1;");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Update { assignments, input, .. } = &plan.logical else {
            panic!("expected logical update plan: {plan:?}");
        };
        assert_eq!(
            assignments,
            &[UpdateAssignment {
                column: bound("users", "age", 2, DataType::Integer),
                expression: PlannedExpression::Binary {
                    left: Box::new(PlannedExpression::Column(bound(
                        "users",
                        "age",
                        2,
                        DataType::Integer
                    ))),
                    op: Op::Add,
                    right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
                },
            }]
        );

        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter below update: {plan:?}");
        };
        assert!(
            matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users")
        );
        assert_eq!(
            predicate,
            &PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Column(bound(
                    "users",
                    "id",
                    0,
                    DataType::Integer
                ))),
                op: Op::EqualsEquals,
                right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            }
        );

        let PhysicalPlan::Update { input, .. } = &plan.physical else {
            panic!("expected physical update plan: {plan:?}");
        };
        assert!(matches!(input.as_ref(), PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn update_rejects_duplicate_assignment_columns() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("UPDATE users SET name = 'Ada', name = 'Grace';");

        assert!(matches!(
            planner.plan_statement(&statement),
            Err(PlannerError::DuplicateUpdateColumn { column }) if column == "name"
        ));
    }

    #[test]
    fn delete_all_plans_full_table_scan_under_delete() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("DELETE FROM users;");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Delete { table, input } = &plan.logical else {
            panic!("expected logical delete plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert!(
            matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users")
        );

        let PhysicalPlan::Delete { table, input } = &plan.physical else {
            panic!("expected physical delete plan: {plan:?}");
        };
        assert_eq!(table.name, "users");
        assert!(
            matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
        );
    }

    #[test]
    fn delete_where_binds_column_refs_in_filter() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("DELETE FROM users WHERE id == 1;");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Delete { input, .. } = &plan.logical else {
            panic!("expected logical delete plan: {plan:?}");
        };
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter below delete: {plan:?}");
        };
        assert!(
            matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users")
        );
        assert_eq!(
            predicate,
            &PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Column(bound(
                    "users",
                    "id",
                    0,
                    DataType::Integer
                ))),
                op: Op::EqualsEquals,
                right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            }
        );

        let PhysicalPlan::Delete { input, .. } = &plan.physical else {
            panic!("expected physical delete plan: {plan:?}");
        };
        assert!(matches!(input.as_ref(), PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn select_star_projects_all_columns_over_full_table_scan() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("SELECT * FROM users;");

        let plan = planner.plan_statement(&statement).unwrap();

        let PhysicalPlan::Project { input, expressions } = &plan.physical else {
            panic!("expected physical project plan: {plan:?}");
        };
        assert_eq!(
            expressions
                .iter()
                .map(|expr| match expr {
                    PlannedExpression::Column(column) => column.name.as_str(),
                    other => panic!("expected column expression, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            ["id", "name", "age"]
        );
        assert!(
            matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
        );
    }

    #[test]
    fn select_where_binds_column_refs_in_filter_and_projection() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("SELECT name FROM users WHERE id == 1;");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Project { input, expressions } = &plan.logical else {
            panic!("expected logical project plan: {plan:?}");
        };
        assert_eq!(
            expressions,
            &[PlannedExpression::Column(bound("users", "name", 1, DataType::Text))]
        );

        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter below project: {plan:?}");
        };
        assert!(
            matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users")
        );
        assert_eq!(
            predicate,
            &PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Column(bound(
                    "users",
                    "id",
                    0,
                    DataType::Integer
                ))),
                op: Op::EqualsEquals,
                right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            }
        );
    }

    #[test]
    fn explain_select_wraps_planned_select() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("EXPLAIN SELECT name FROM users WHERE id == 1;");

        let plan = planner.plan_statement(&statement).unwrap();

        let LogicalPlan::Explain { input } = &plan.logical else {
            panic!("expected logical explain plan: {plan:?}");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));

        let PhysicalPlan::Explain { input } = &plan.physical else {
            panic!("expected physical explain plan: {plan:?}");
        };
        let PhysicalPlan::Project { input, expressions } = input.as_ref() else {
            panic!("expected explained select project plan: {plan:?}");
        };
        assert_eq!(
            expressions,
            &[PlannedExpression::Column(bound("users", "name", 1, DataType::Text))]
        );
        assert!(matches!(input.as_ref(), PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn select_order_limit_offset_preserves_operator_order() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);
        let statement = parse("SELECT name FROM users ORDER BY id LIMIT 10 OFFSET 5;");

        let plan = planner.plan_statement(&statement).unwrap();

        let PhysicalPlan::Limit { input, limit } = &plan.physical else {
            panic!("expected limit root: {plan:?}");
        };
        assert_eq!(*limit, 10);
        let PhysicalPlan::Offset { input, offset } = input.as_ref() else {
            panic!("expected offset under limit: {plan:?}");
        };
        assert_eq!(*offset, 5);
        let PhysicalPlan::Project { input, .. } = input.as_ref() else {
            panic!("expected project under offset: {plan:?}");
        };
        let PhysicalPlan::Sort { input, terms } = input.as_ref() else {
            panic!("expected sort under project: {plan:?}");
        };
        assert_eq!(
            terms,
            &[SortTerm { column: bound("users", "id", 0, DataType::Integer), direction: None }]
        );
        assert!(
            matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
        );
    }

    #[test]
    fn reports_semantic_planning_errors() {
        let (_dir, database) = database_with_users();
        let planner = Planner::new(&database);

        assert!(matches!(
            planner.plan_statement(&parse("SELECT * FROM missing;")),
            Err(PlannerError::TableNotFound { name }) if name == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT missing FROM users;")),
            Err(PlannerError::ColumnNotFound { column }) if column == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("DELETE FROM missing;")),
            Err(PlannerError::TableNotFound { name }) if name == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("DELETE FROM users WHERE missing == 1;")),
            Err(PlannerError::ColumnNotFound { column }) if column == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT id FROM users WHERE * == id;")),
            Err(PlannerError::UnsupportedWildcardPosition)
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT *;")),
            Err(PlannerError::WildcardRequiresTable)
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT COUNT(*) FROM users;")),
            Err(PlannerError::UnsupportedAggregate { function }) if function == "COUNT"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("INSERT INTO users (id, id) VALUES (1, 2);")),
            Err(PlannerError::DuplicateInsertColumn { column }) if column == "id"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("INSERT INTO users (id, name) VALUES (1);")),
            Err(PlannerError::InsertColumnValueCount { columns: 2, values: 1 })
        ));
        assert!(matches!(
            planner.plan_statement(&parse("CREATE INDEX idx_missing ON users (missing);")),
            Err(PlannerError::ColumnNotFound { column }) if column == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("CREATE INDEX idx_duplicate ON users (name, name);")),
            Err(PlannerError::DuplicateIndexColumn { column }) if column == "name"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("EXPLAIN INSERT INTO users (id) VALUES (1);")),
            Err(PlannerError::UnsupportedStatement { statement }) if statement.starts_with("INSERT")
        ));
    }

    fn bound(table: &str, name: &str, ordinal: usize, data_type: DataType) -> BoundColumn {
        BoundColumn { table: table.to_owned(), name: name.to_owned(), ordinal, data_type }
    }
}
