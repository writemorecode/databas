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
    BoundColumn, BoundExpr, BoundSortTerm, BoundUpdateAssignment, LogicalPlan, LogicalPlanNode,
    NodeId, PlanSchema, PlannerError, PlannerResult, RelationId,
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
    ) -> PlannerResult<NodeId> {
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
    ) -> PlannerResult<NodeId> {
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
    ) -> PlannerResult<NodeId> {
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
    ) -> PlannerResult<NodeId> {
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
    ) -> PlannerResult<NodeId> {
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

        let input = push_logical_node(
            nodes,
            LogicalPlanNode::Values { rows, output: PlanSchema::default() },
        );
        Ok(push_logical_node(nodes, LogicalPlanNode::Insert { table, columns, input }))
    }

    fn plan_delete(
        &self,
        query: &DeleteQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let table = self.table_schema(query.table)?;
        let relation = RelationId::new(0);
        let scope = BindingScope::for_table(&table);
        let output = PlanSchema::for_table(relation, &table);
        let mut input = push_logical_node(
            nodes,
            LogicalPlanNode::TableScan { relation, table: table.clone(), output: output.clone() },
        );

        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, &scope)?;
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Filter { input, predicate, output: output.clone() },
            );
        }

        Ok(push_logical_node(nodes, LogicalPlanNode::Delete { relation, table, input }))
    }

    fn plan_update(
        &self,
        query: &UpdateQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let table = self.table_schema(query.table)?;
        let relation = RelationId::new(0);
        let scope = BindingScope::for_table(&table);
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
            assignments.push(BoundUpdateAssignment {
                column,
                expression: self.bind_expression(&assignment.expression, &scope)?,
            });
        }

        let output = PlanSchema::for_table(relation, &table);
        let mut input = push_logical_node(
            nodes,
            LogicalPlanNode::TableScan { relation, table: table.clone(), output: output.clone() },
        );
        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, &scope)?;
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Filter { input, predicate, output: output.clone() },
            );
        }

        Ok(push_logical_node(
            nodes,
            LogicalPlanNode::Update { relation, table, assignments, input },
        ))
    }

    fn plan_select(
        &self,
        query: &SelectQuery<'_>,
        nodes: &mut Vec<LogicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let table =
            query.table.as_ref().map(|reference| self.table_schema(reference.name)).transpose()?;
        let relation = RelationId::new(0);
        let source = table.as_ref().zip(query.table.as_ref()).map(|(table, reference)| {
            RelationBinding { table, qualifier: reference.qualifier(), relation }
        });
        let mut scope = BindingScope::default();
        if let Some(source) = source {
            scope.add_relation(source)?;
        }
        let mut input = match source {
            Some(source) => push_logical_node(
                nodes,
                LogicalPlanNode::TableScan {
                    relation,
                    table: source.table.clone(),
                    output: PlanSchema::for_qualified_table(
                        relation,
                        source.table,
                        source.qualifier,
                    ),
                },
            ),
            None => {
                push_logical_node(nodes, LogicalPlanNode::OneRow { output: PlanSchema::default() })
            }
        };
        let mut output = source.map_or_else(PlanSchema::default, |source| {
            PlanSchema::for_qualified_table(relation, source.table, source.qualifier)
        });

        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, &scope)?;
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Filter { input, predicate, output: output.clone() },
            );
        }

        if let Some(order_by) = &query.order_by {
            let terms = order_by
                .terms
                .iter()
                .map(|term| {
                    Ok(BoundSortTerm {
                        column: bind_column_reference(&scope, &term.column)?,
                        direction: term.order.clone(),
                    })
                })
                .collect::<PlannerResult<Vec<_>>>()?;
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Sort { input, terms, output: output.clone() },
            );
        }

        let expressions = self.bind_projection(&query.columns.0, &scope)?;
        output = PlanSchema::for_expressions(&expressions);
        input = push_logical_node(
            nodes,
            LogicalPlanNode::Project { input, expressions, output: output.clone() },
        );

        if let Some(offset) = query.offset {
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Offset { input, offset, output: output.clone() },
            );
        }

        if let Some(limit) = query.limit {
            input = push_logical_node(
                nodes,
                LogicalPlanNode::Limit { input, limit, output: output.clone() },
            );
        }

        Ok(input)
    }

    fn bind_projection(
        &self,
        expressions: &[Expression<'_>],
        scope: &BindingScope<'_>,
    ) -> PlannerResult<Vec<BoundExpr>> {
        let mut bound = Vec::new();
        for expression in expressions {
            match expression {
                Expression::Wildcard => {
                    if scope.relations.is_empty() {
                        return Err(PlannerError::WildcardRequiresTable);
                    }
                    for source in &scope.relations {
                        bound.extend(source.table.row.columns.iter().enumerate().map(
                            |(ordinal, column)| {
                                BoundExpr::Column(bound_scoped_column(*source, ordinal, column))
                            },
                        ));
                    }
                }
                _ => bound.push(self.bind_expression(expression, scope)?),
            }
        }
        Ok(bound)
    }

    fn bind_expression(
        &self,
        expression: &Expression<'_>,
        scope: &BindingScope<'_>,
    ) -> PlannerResult<BoundExpr> {
        match expression {
            Expression::Literal(literal) => Ok(BoundExpr::Literal(Value::from(literal))),
            Expression::ColumnReference(reference) => {
                bind_column_reference(scope, reference).map(BoundExpr::Column)
            }
            Expression::UnaryOp((op, expr)) => {
                Ok(BoundExpr::Unary { op: *op, expr: Box::new(self.bind_expression(expr, scope)?) })
            }
            Expression::BinaryOp((left, op, right)) => Ok(BoundExpr::Binary {
                left: Box::new(self.bind_expression(left, scope)?),
                op: *op,
                right: Box::new(self.bind_expression(right, scope)?),
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

fn push_logical_node(nodes: &mut Vec<LogicalPlanNode>, node: LogicalPlanNode) -> NodeId {
    let id = NodeId::new(nodes.len());
    nodes.push(node);
    id
}

/// One table occurrence visible while binding a statement.
#[derive(Clone, Copy)]
struct RelationBinding<'a> {
    table: &'a TableSchema,
    qualifier: &'a str,
    relation: RelationId,
}

/// Ordered relation namespace visible at one point in a statement.
#[derive(Default)]
struct BindingScope<'a> {
    relations: Vec<RelationBinding<'a>>,
}

impl<'a> BindingScope<'a> {
    fn for_table(table: &'a TableSchema) -> Self {
        Self { relations: vec![RelationBinding::for_table(table)] }
    }

    fn add_relation(&mut self, relation: RelationBinding<'a>) -> PlannerResult<()> {
        if self.relations.iter().any(|existing| existing.qualifier == relation.qualifier) {
            return Err(PlannerError::DuplicateTableQualifier {
                qualifier: relation.qualifier.to_owned(),
            });
        }
        self.relations.push(relation);
        Ok(())
    }
}

impl<'a> RelationBinding<'a> {
    fn for_table(table: &'a TableSchema) -> Self {
        Self { table, qualifier: &table.name, relation: RelationId::new(0) }
    }
}

fn bind_column_reference(
    scope: &BindingScope<'_>,
    reference: &ColumnReference<'_>,
) -> PlannerResult<BoundColumn> {
    if let Some(qualifier) = reference.table {
        let source = scope
            .relations
            .iter()
            .find(|source| source.qualifier == qualifier)
            .ok_or_else(|| PlannerError::TableNotInScope { table: qualifier.to_owned() })?;
        return find_relation_column(*source, reference.column)
            .ok_or_else(|| PlannerError::ColumnNotFound { column: reference.column.to_owned() });
    }

    let mut matches =
        scope.relations.iter().filter_map(|source| find_relation_column(*source, reference.column));
    let column = matches
        .next()
        .ok_or_else(|| PlannerError::ColumnNotFound { column: reference.column.to_owned() })?;
    if matches.next().is_some() {
        return Err(PlannerError::AmbiguousColumn { column: reference.column.to_owned() });
    }
    Ok(column)
}

fn find_relation_column(source: RelationBinding<'_>, column: &str) -> Option<BoundColumn> {
    source
        .table
        .row
        .columns
        .iter()
        .enumerate()
        .find(|(_, schema)| schema.name == column)
        .map(|(ordinal, schema)| bound_scoped_column(source, ordinal, schema))
}

fn bound_scoped_column(
    source: RelationBinding<'_>,
    ordinal: usize,
    column: &ColumnSchema,
) -> BoundColumn {
    BoundColumn {
        relation: source.relation,
        table: source.qualifier.to_owned(),
        name: column.name.clone(),
        ordinal,
        data_type: column.data_type,
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
        relation: RelationId::new(0),
        table: table.name.clone(),
        name: column.name.clone(),
        ordinal,
        data_type: column.data_type,
    }
}

fn literal_expression(expression: &Expression<'_>) -> Option<BoundExpr> {
    match expression {
        Expression::Literal(literal) => Some(BoundExpr::Literal(Value::from(literal))),
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
    use super::*;
    use crate::core::{DataType, TupleSchema};

    fn table(table_id: i32, name: &str, columns: &[&str]) -> TableSchema {
        TableSchema {
            table_id,
            name: name.into(),
            root_page_id: u64::try_from(table_id).unwrap(),
            row: TupleSchema {
                columns: columns
                    .iter()
                    .map(|name| ColumnSchema {
                        name: (*name).into(),
                        data_type: DataType::Integer,
                        nullable: false,
                        primary_key: false,
                    })
                    .collect(),
            },
        }
    }

    #[test]
    fn binding_scope_resolves_qualified_and_unique_unqualified_columns() {
        let users = table(1, "users", &["id", "name"]);
        let orders = table(2, "orders", &["id", "user_id", "total"]);
        let mut scope = BindingScope::default();
        scope
            .add_relation(RelationBinding {
                table: &users,
                qualifier: "u",
                relation: RelationId::new(0),
            })
            .unwrap();
        scope
            .add_relation(RelationBinding {
                table: &orders,
                qualifier: "o",
                relation: RelationId::new(1),
            })
            .unwrap();

        let name = bind_column_reference(&scope, &ColumnReference { table: None, column: "name" })
            .unwrap();
        assert_eq!((name.relation, name.ordinal, &*name.table), (RelationId::new(0), 1, "u"));

        let total =
            bind_column_reference(&scope, &ColumnReference { table: Some("o"), column: "total" })
                .unwrap();
        assert_eq!((total.relation, total.ordinal, &*total.table), (RelationId::new(1), 2, "o"));
    }

    #[test]
    fn binding_scope_rejects_ambiguous_columns_and_hidden_table_names() {
        let users = table(1, "users", &["id", "name"]);
        let orders = table(2, "orders", &["id", "user_id"]);
        let scope = BindingScope {
            relations: vec![
                RelationBinding { table: &users, qualifier: "u", relation: RelationId::new(0) },
                RelationBinding { table: &orders, qualifier: "o", relation: RelationId::new(1) },
            ],
        };

        assert!(matches!(
            bind_column_reference(&scope, &ColumnReference { table: None, column: "id" }),
            Err(PlannerError::AmbiguousColumn { column }) if column == "id"
        ));
        assert!(matches!(
            bind_column_reference(
                &scope,
                &ColumnReference { table: Some("users"), column: "id" },
            ),
            Err(PlannerError::TableNotInScope { table }) if table == "users"
        ));
    }

    #[test]
    fn binding_scope_rejects_duplicate_relation_qualifiers() {
        let users = table(1, "users", &["id"]);
        let orders = table(2, "orders", &["id"]);
        let mut scope = BindingScope::default();
        scope
            .add_relation(RelationBinding {
                table: &users,
                qualifier: "source",
                relation: RelationId::new(0),
            })
            .unwrap();

        assert!(matches!(
            scope.add_relation(RelationBinding {
                table: &orders,
                qualifier: "source",
                relation: RelationId::new(1),
            }),
            Err(PlannerError::DuplicateTableQualifier { qualifier }) if qualifier == "source"
        ));
    }
}
