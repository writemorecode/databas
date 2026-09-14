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
            expr::{ColumnReference, Expression, Literal},
            stmt::{
                Statement, create_index::CreateIndexQuery, create_table::CreateTableQuery,
                delete::DeleteQuery, insert::InsertQuery, select::SelectQuery, update::UpdateQuery,
            },
        },
    },
};

use super::{
    BoundColumn, LogicalPlan, LogicalPlanNode, PlannedExpression, PlannerError, PlannerResult,
    SortTerm, UpdateAssignment,
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
        let mut nodes = Vec::new();
        let root = self.bind_statement(statement, &mut nodes)?;
        Ok(LogicalPlan::from_parts(nodes, root))
    }

    fn bind_statement(
        &self,
        statement: &Statement<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        match statement {
            Statement::Explain(statement) => self.plan_explain(statement, nodes),
            Statement::CreateTable(query) => self.plan_create_table(query, nodes),
            Statement::CreateIndex(query) => self.plan_create_index(query, nodes),
            Statement::Insert(query) => self.plan_insert(query, nodes),
            Statement::Update(query) => self.plan_update(query, nodes),
            Statement::Delete(query) => self.plan_delete(query, nodes),
            Statement::Select(query) => self.plan_select(query, nodes),
        }
    }

    fn plan_explain(
        &self,
        statement: &Statement<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        let input = match statement {
            Statement::Select(query) => self.plan_select(query, nodes)?,
            Statement::Update(query) => self.plan_update(query, nodes)?,
            Statement::Delete(query) => self.plan_delete(query, nodes)?,
            statement => {
                return Err(PlannerError::UnsupportedStatement {
                    statement: statement.to_string(),
                });
            }
        };
        Ok(push_logical_node(nodes, LogicalPlanNode::Explain { input }))
    }

    fn plan_create_table(
        &self,
        query: &CreateTableQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        Ok(push_logical_node(
            nodes,
            LogicalPlanNode::CreateTable {
                name: query.table_name.to_owned(),
                schema: TupleSchema::from_create_table_query(query),
            },
        ))
    }

    fn plan_create_index(
        &self,
        query: &CreateIndexQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        let table = self.table_schema(query.table_name)?;
        let mut seen = HashSet::new();
        let mut columns = Vec::new();

        for column in &query.columns.0 {
            if !seen.insert(*column) {
                return Err(PlannerError::DuplicateIndexColumn { column: (*column).to_owned() });
            }
            columns.push(bind_column(&table, column)?);
        }

        Ok(push_logical_node(
            nodes,
            LogicalPlanNode::CreateIndex { name: query.index_name.to_owned(), table, columns },
        ))
    }

    fn plan_insert(
        &self,
        query: &InsertQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
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

        let input = push_logical_node(nodes, LogicalPlanNode::Values { rows });
        Ok(push_logical_node(nodes, LogicalPlanNode::Insert { table, columns, input }))
    }

    fn plan_delete(
        &self,
        query: &DeleteQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        let table = self.table_schema(query.table)?;
        let mut input =
            push_logical_node(nodes, LogicalPlanNode::TableScan { table: table.clone() });

        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, Some(&table))?;
            input = push_logical_node(nodes, LogicalPlanNode::Filter { input, predicate });
        }

        Ok(push_logical_node(nodes, LogicalPlanNode::Delete { table, input }))
    }

    fn plan_update(
        &self,
        query: &UpdateQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
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

        let mut input =
            push_logical_node(nodes, LogicalPlanNode::TableScan { table: table.clone() });
        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, Some(&table))?;
            input = push_logical_node(nodes, LogicalPlanNode::Filter { input, predicate });
        }

        Ok(push_logical_node(nodes, LogicalPlanNode::Update { table, assignments, input }))
    }

    fn plan_select(
        &self,
        query: &SelectQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<usize> {
        let table = query.table.map(|name| self.table_schema(name)).transpose()?;
        let mut input = match &table {
            Some(table) => {
                push_logical_node(nodes, LogicalPlanNode::TableScan { table: table.clone() })
            }
            None => push_logical_node(nodes, LogicalPlanNode::OneRow),
        };

        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, table.as_ref())?;
            input = push_logical_node(nodes, LogicalPlanNode::Filter { input, predicate });
        }

        if let Some(order_by) = &query.order_by {
            let terms = order_by
                .terms
                .iter()
                .map(|term| {
                    Ok(SortTerm {
                        column: bind_column_reference(table.as_ref(), &term.column)?,
                        direction: term.order.clone(),
                    })
                })
                .collect::<PlannerResult<Vec<_>>>()?;
            input = push_logical_node(nodes, LogicalPlanNode::Sort { input, terms });
        }

        let expressions = self.bind_projection(&query.columns.0, table.as_ref())?;
        input = push_logical_node(nodes, LogicalPlanNode::Project { input, expressions });

        if let Some(offset) = query.offset {
            input = push_logical_node(nodes, LogicalPlanNode::Offset { input, offset });
        }

        if let Some(limit) = query.limit {
            input = push_logical_node(nodes, LogicalPlanNode::Limit { input, limit });
        }

        Ok(input)
    }

    fn bind_projection(
        &self,
        expressions: &[Expression<'_>],
        source_table: Option<&TableSchema>,
    ) -> PlannerResult<Vec<PlannedExpression>> {
        let mut bound = Vec::new();
        for expression in expressions {
            match expression {
                Expression::Wildcard => {
                    let source_table = source_table.ok_or(PlannerError::WildcardRequiresTable)?;
                    bound.extend(source_table.row.columns.iter().enumerate().map(
                        |(ordinal, column)| {
                            PlannedExpression::Column(bound_column(source_table, ordinal, column))
                        },
                    ));
                }
                _ => bound.push(self.bind_expression(expression, source_table)?),
            }
        }
        Ok(bound)
    }

    fn bind_expression(
        &self,
        expression: &Expression<'_>,
        source_table: Option<&TableSchema>,
    ) -> PlannerResult<PlannedExpression> {
        match expression {
            Expression::Literal(literal) => Ok(PlannedExpression::Literal(Value::from(literal))),
            Expression::ColumnReference(reference) => {
                bind_column_reference(source_table, reference).map(PlannedExpression::Column)
            }
            Expression::UnaryOp((op, expr)) => Ok(PlannedExpression::Unary {
                op: *op,
                expr: Box::new(self.bind_expression(expr, source_table)?),
            }),
            Expression::BinaryOp((left, op, right)) => Ok(PlannedExpression::Binary {
                left: Box::new(self.bind_expression(left, source_table)?),
                op: *op,
                right: Box::new(self.bind_expression(right, source_table)?),
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

fn push_logical_node(nodes: &mut Vec<LogicalPlanNode>, node: LogicalPlanNode) -> usize {
    let index = nodes.len();
    nodes.push(node);
    index
}

fn bind_column_reference(
    source_table: Option<&TableSchema>,
    reference: &ColumnReference<'_>,
) -> PlannerResult<BoundColumn> {
    let Some(source_table) = source_table else {
        return match reference.table {
            Some(table) => Err(PlannerError::TableNotInScope { table: table.to_owned() }),
            None => Err(PlannerError::ColumnNotFound { column: reference.column.to_owned() }),
        };
    };
    if let Some(qualifier) = reference.table
        && qualifier != source_table.name
    {
        return Err(PlannerError::TableNotInScope { table: qualifier.to_owned() });
    }
    bind_column(source_table, reference.column)
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
