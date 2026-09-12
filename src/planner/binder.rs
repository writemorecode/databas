//! SQL name binding, statement validation, and logical plan construction.

use std::collections::HashSet;

use crate::{
    core::{
        ColumnSchema, TableSchema, TupleSchema, Value,
        access::CatalogRead,
        error::{InvalidArgumentError, StorageError},
    },
    sql_parser::{
        NumberKind,
        parser::{
            expr::{Expression, Literal},
            stmt::{
                Statement, create_index::CreateIndexQuery, create_table::CreateTableQuery,
                delete::DeleteQuery, insert::InsertQuery, select::SelectQuery, update::UpdateQuery,
            },
        },
    },
};

use super::{
    BoundColumn, LogicalPlan, PlannedExpression, PlannerError, PlannerResult, SortTerm,
    UpdateAssignment,
};

/// Binds parsed SQL syntax to catalog metadata and builds a logical plan.
pub(super) struct Binder<'catalog> {
    catalog: &'catalog dyn CatalogRead,
}

impl<'catalog> Binder<'catalog> {
    pub(super) fn new(catalog: &'catalog dyn CatalogRead) -> Self {
        Self { catalog }
    }

    pub(super) fn bind(&self, statement: &Statement<'_>) -> PlannerResult<LogicalPlan> {
        self.logical_plan_statement(statement)
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
            Statement::Update(query) => {
                Ok(LogicalPlan::Explain { input: Box::new(self.plan_update(query)?) })
            }
            Statement::Delete(query) => {
                Ok(LogicalPlan::Explain { input: Box::new(self.plan_delete(query)?) })
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
        self.catalog.table_schema_by_name(name).map_err(|error| match error {
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
