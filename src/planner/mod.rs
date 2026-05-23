use std::collections::HashSet;

use thiserror::Error;

use crate::{
    core::{
        ColumnSchema, DataType, Database, TableSchema, TupleSchema, Value,
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
                insert::InsertQuery,
                select::{Ordering, SelectQuery},
            },
        },
    },
};

pub type PlannerResult<T> = Result<T, PlannerError>;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub logical: LogicalPlan,
    pub physical: PhysicalPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    CreateTable { name: String, schema: TupleSchema },
    CreateIndex { name: String, table: TableSchema, columns: Vec<BoundColumn> },
    Values { rows: Vec<Vec<PlannedExpression>> },
    Insert { table: TableSchema, columns: Vec<BoundColumn>, input: Box<LogicalPlan> },
    OneRow,
    TableScan { table: TableSchema },
    Filter { input: Box<LogicalPlan>, predicate: PlannedExpression },
    Sort { input: Box<LogicalPlan>, terms: Vec<SortTerm> },
    Project { input: Box<LogicalPlan>, expressions: Vec<PlannedExpression> },
    Offset { input: Box<LogicalPlan>, offset: u32 },
    Limit { input: Box<LogicalPlan>, limit: u32 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    CreateTable {
        name: String,
        schema: TupleSchema,
    },
    CreateIndex {
        name: String,
        table: TableSchema,
        columns: Vec<BoundColumn>,
    },
    Values {
        rows: Vec<Vec<PlannedExpression>>,
    },
    InsertValues {
        table: TableSchema,
        columns: Vec<BoundColumn>,
        values: Vec<Vec<PlannedExpression>>,
    },
    OneRow,
    FullTableScan {
        table: TableSchema,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: PlannedExpression,
    },
    Sort {
        input: Box<PhysicalPlan>,
        terms: Vec<SortTerm>,
    },
    Project {
        input: Box<PhysicalPlan>,
        expressions: Vec<PlannedExpression>,
    },
    Offset {
        input: Box<PhysicalPlan>,
        offset: u32,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: u32,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlannedExpression {
    Literal(Value),
    Column(BoundColumn),
    Unary { op: Op, expr: Box<PlannedExpression> },
    Binary { left: Box<PlannedExpression>, op: Op, right: Box<PlannedExpression> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumn {
    pub table: String,
    pub name: String,
    pub ordinal: usize,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortTerm {
    pub expression: PlannedExpression,
    pub direction: Option<Ordering>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("table not found: {name}")]
    TableNotFound { name: String },
    #[error("column {column} not found")]
    ColumnNotFound { column: String },
    #[error("duplicate insert column: {column}")]
    DuplicateInsertColumn { column: String },
    #[error("duplicate index column: {column}")]
    DuplicateIndexColumn { column: String },
    #[error("insert row has {values} values for {columns} columns")]
    InsertColumnValueCount { columns: usize, values: usize },
    #[error("unsupported statement: {statement}")]
    UnsupportedStatement { statement: String },
    #[error("unsupported expression: {expression}")]
    UnsupportedExpression { expression: String },
    #[error("unsupported aggregate function: {function}")]
    UnsupportedAggregate { function: String },
    #[error("wildcard is only supported in SELECT projection")]
    UnsupportedWildcardPosition,
    #[error("wildcard projection requires a FROM table")]
    WildcardRequiresTable,
    #[error("invalid insert input: expected VALUES")]
    InvalidInsertInput,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub struct Planner<'db> {
    database: &'db Database,
}

impl<'db> Planner<'db> {
    pub fn new(database: &'db Database) -> Self {
        Self { database }
    }

    pub fn plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<Plan> {
        let logical = self.logical_plan_statement(statement)?;
        let physical = self.physical_plan(&logical)?;
        Ok(Plan { logical, physical })
    }

    fn logical_plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<LogicalPlan> {
        match statement {
            Statement::CreateTable(query) => self.plan_create_table(query),
            Statement::CreateIndex(query) => self.plan_create_index(query),
            Statement::Insert(query) => self.plan_insert(query),
            Statement::Select(query) => self.plan_select(query),
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
                .0
                .iter()
                .map(|expr| {
                    Ok(SortTerm {
                        expression: self.bind_expression(expr, table.as_ref())?,
                        direction: order_by.order.clone(),
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
        self.database.table_schema_by_name(name).map_err(|error| match error {
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
            &[SortTerm {
                expression: PlannedExpression::Column(bound("users", "id", 0, DataType::Integer)),
                direction: None,
            }]
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
    }

    fn bound(table: &str, name: &str, ordinal: usize, data_type: DataType) -> BoundColumn {
        BoundColumn { table: table.to_owned(), name: name.to_owned(), ordinal, data_type }
    }
}
