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
        let base_table =
            query.table.as_ref().map(|reference| self.table_schema(reference.name)).transpose()?;
        // Resolve all joined tables before borrowing their schemas into the binding scope.
        let joined_tables = query
            .joins
            .iter()
            .map(|join| self.table_schema(join.table.name))
            .collect::<PlannerResult<Vec<_>>>()?;

        let base_relation = RelationId::new(0);
        let base_binding =
            base_table.as_ref().zip(query.table.as_ref()).map(|(table, reference)| {
                RelationBinding { table, qualifier: reference.qualifier(), relation: base_relation }
            });
        let mut scope = BindingScope::default();
        if let Some(binding) = base_binding {
            scope.add_relation(binding)?;
        }

        let (mut current_input, mut current_output) = match base_binding {
            Some(binding) => {
                let output = PlanSchema::for_qualified_table(
                    binding.relation,
                    binding.table,
                    binding.qualifier,
                );
                let scan = push_logical_node(
                    nodes,
                    LogicalPlanNode::TableScan {
                        relation: binding.relation,
                        table: binding.table.clone(),
                        output: output.clone(),
                    },
                );
                (scan, output)
            }
            None => {
                let output = PlanSchema::default();
                let input =
                    push_logical_node(nodes, LogicalPlanNode::OneRow { output: output.clone() });
                (input, output)
            }
        };

        for (join_index, (join, joined_table)) in query.joins.iter().zip(&joined_tables).enumerate()
        {
            let relation_number =
                u32::try_from(join_index + 1).map_err(|_error| PlannerError::TooManyRelations)?;
            let right_binding = RelationBinding {
                table: joined_table,
                qualifier: join.table.qualifier(),
                relation: RelationId::new(relation_number),
            };
            scope.add_relation(right_binding)?;
            let join_predicate = self.bind_expression(&join.condition, &scope)?;

            let right_output = PlanSchema::for_qualified_table(
                right_binding.relation,
                right_binding.table,
                right_binding.qualifier,
            );
            let right_scan = push_logical_node(
                nodes,
                LogicalPlanNode::TableScan {
                    relation: right_binding.relation,
                    table: right_binding.table.clone(),
                    output: right_output.clone(),
                },
            );
            let join_output = PlanSchema::join(current_output, right_output);
            current_input = push_logical_node(
                nodes,
                LogicalPlanNode::Join {
                    left: current_input,
                    right: right_scan,
                    join_type: join.join_type,
                    predicate: join_predicate,
                    output: join_output.clone(),
                },
            );
            current_output = join_output;
        }

        if let Some(predicate) = &query.where_clause {
            let predicate = self.bind_expression(predicate, &scope)?;
            current_input = push_logical_node(
                nodes,
                LogicalPlanNode::Filter {
                    input: current_input,
                    predicate,
                    output: current_output.clone(),
                },
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
            current_input = push_logical_node(
                nodes,
                LogicalPlanNode::Sort {
                    input: current_input,
                    terms,
                    output: current_output.clone(),
                },
            );
        }

        let expressions = self.bind_projection(&query.columns.0, &scope)?;
        current_output = PlanSchema::for_expressions(&expressions);
        current_input = push_logical_node(
            nodes,
            LogicalPlanNode::Project {
                input: current_input,
                expressions,
                output: current_output.clone(),
            },
        );

        if let Some(offset) = query.offset {
            current_input = push_logical_node(
                nodes,
                LogicalPlanNode::Offset {
                    input: current_input,
                    offset,
                    output: current_output.clone(),
                },
            );
        }

        if let Some(limit) = query.limit {
            current_input = push_logical_node(
                nodes,
                LogicalPlanNode::Limit {
                    input: current_input,
                    limit,
                    output: current_output.clone(),
                },
            );
        }

        Ok(current_input)
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
    use crate::{
        core::{DataType, IndexSchema, TupleSchema},
        planner::Planner,
        sql_parser::parser::{Parser, op::Op, stmt::select::JoinType},
    };

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

    struct TestCatalog {
        tables: Vec<TableSchema>,
    }

    impl CatalogRead for TestCatalog {
        fn table_schema_by_name(&self, name: &str) -> Result<TableSchema, StorageError> {
            self.tables.iter().find(|table| table.name == name).cloned().ok_or_else(|| {
                StorageError::InvalidArgument(InvalidArgumentError::TableNotFound {
                    name: name.to_owned(),
                })
            })
        }

        fn index_schemas_for_table(
            &self,
            _table: &TableSchema,
        ) -> Result<Vec<IndexSchema>, StorageError> {
            Ok(Vec::new())
        }
    }

    fn parse(sql: &str) -> Statement<'_> {
        Parser::new(sql).stmt().expect("test SQL should parse")
    }

    fn bind_select(catalog: &TestCatalog, sql: &str) -> PlannerResult<LogicalPlan> {
        Binder::new(catalog).bind(&parse(sql))
    }

    fn scoped_column(
        table: &TableSchema,
        qualifier: &str,
        relation: RelationId,
        column: &str,
    ) -> BoundColumn {
        find_relation_column(RelationBinding { table, qualifier, relation }, column)
            .expect("test column should exist")
    }

    #[test]
    fn select_join_builds_bound_scans_join_and_following_operators() {
        let users = table(1, "users", &["id", "name"]);
        let orders = table(2, "orders", &["id", "user_id", "total"]);
        let catalog = TestCatalog { tables: vec![users.clone(), orders.clone()] };
        let got = bind_select(
            &catalog,
            "SELECT u.name, o.total FROM users AS u \
             JOIN orders AS o ON u.id == o.user_id \
             WHERE o.total > 10 ORDER BY u.name;",
        )
        .unwrap();

        let users_relation = RelationId::new(0);
        let orders_relation = RelationId::new(1);
        let users_id = scoped_column(&users, "u", users_relation, "id");
        let users_name = scoped_column(&users, "u", users_relation, "name");
        let orders_user_id = scoped_column(&orders, "o", orders_relation, "user_id");
        let orders_total = scoped_column(&orders, "o", orders_relation, "total");
        let users_output = PlanSchema::for_qualified_table(users_relation, &users, "u");
        let orders_output = PlanSchema::for_qualified_table(orders_relation, &orders, "o");

        let mut expected = LogicalPlan::new(LogicalPlanNode::TableScan {
            relation: users_relation,
            table: users,
            output: users_output.clone(),
        });
        let users_scan = expected.root_id();
        let orders_scan = expected.push(LogicalPlanNode::TableScan {
            relation: orders_relation,
            table: orders,
            output: orders_output.clone(),
        });
        let join_output = PlanSchema::join(users_output, orders_output);
        let join = expected.push(LogicalPlanNode::Join {
            left: users_scan,
            right: orders_scan,
            join_type: JoinType::Inner,
            predicate: BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(users_id)),
                op: Op::EqualsEquals,
                right: Box::new(BoundExpr::Column(orders_user_id)),
            },
            output: join_output.clone(),
        });
        let filter = expected.push(LogicalPlanNode::Filter {
            input: join,
            predicate: BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(orders_total.clone())),
                op: Op::GreaterThan,
                right: Box::new(BoundExpr::Literal(Value::Integer(10))),
            },
            output: join_output.clone(),
        });
        let sort = expected.push(LogicalPlanNode::Sort {
            input: filter,
            terms: vec![BoundSortTerm { column: users_name.clone(), direction: None }],
            output: join_output,
        });
        let expressions = vec![BoundExpr::Column(users_name), BoundExpr::Column(orders_total)];
        let projection_output = PlanSchema::for_expressions(&expressions);
        expected.push(LogicalPlanNode::Project {
            input: sort,
            expressions,
            output: projection_output,
        });

        assert_eq!(got, expected);
    }

    #[test]
    fn multiple_joins_are_left_deep_and_receive_distinct_relation_ids() {
        let users = table(1, "users", &["id"]);
        let orders = table(2, "orders", &["user_id", "product_id"]);
        let products = table(3, "products", &["id", "name"]);
        let catalog = TestCatalog { tables: vec![users.clone(), orders.clone(), products.clone()] };
        let got = bind_select(
            &catalog,
            "SELECT p.name FROM users AS u \
             JOIN orders AS o ON u.id == o.user_id \
             JOIN products AS p ON o.product_id == p.id;",
        )
        .unwrap();

        let users_relation = RelationId::new(0);
        let orders_relation = RelationId::new(1);
        let products_relation = RelationId::new(2);
        let users_id = scoped_column(&users, "u", users_relation, "id");
        let orders_user_id = scoped_column(&orders, "o", orders_relation, "user_id");
        let orders_product_id = scoped_column(&orders, "o", orders_relation, "product_id");
        let products_id = scoped_column(&products, "p", products_relation, "id");
        let products_name = scoped_column(&products, "p", products_relation, "name");
        let users_output = PlanSchema::for_qualified_table(users_relation, &users, "u");
        let orders_output = PlanSchema::for_qualified_table(orders_relation, &orders, "o");
        let products_output = PlanSchema::for_qualified_table(products_relation, &products, "p");

        let mut expected = LogicalPlan::new(LogicalPlanNode::TableScan {
            relation: users_relation,
            table: users,
            output: users_output.clone(),
        });
        let users_scan = expected.root_id();
        let orders_scan = expected.push(LogicalPlanNode::TableScan {
            relation: orders_relation,
            table: orders,
            output: orders_output.clone(),
        });
        let first_join_output = PlanSchema::join(users_output, orders_output);
        let first_join = expected.push(LogicalPlanNode::Join {
            left: users_scan,
            right: orders_scan,
            join_type: JoinType::Inner,
            predicate: BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(users_id)),
                op: Op::EqualsEquals,
                right: Box::new(BoundExpr::Column(orders_user_id)),
            },
            output: first_join_output.clone(),
        });
        let products_scan = expected.push(LogicalPlanNode::TableScan {
            relation: products_relation,
            table: products,
            output: products_output.clone(),
        });
        let second_join_output = PlanSchema::join(first_join_output, products_output);
        let second_join = expected.push(LogicalPlanNode::Join {
            left: first_join,
            right: products_scan,
            join_type: JoinType::Inner,
            predicate: BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(orders_product_id)),
                op: Op::EqualsEquals,
                right: Box::new(BoundExpr::Column(products_id)),
            },
            output: second_join_output,
        });
        let expressions = vec![BoundExpr::Column(products_name)];
        let projection_output = PlanSchema::for_expressions(&expressions);
        expected.push(LogicalPlanNode::Project {
            input: second_join,
            expressions,
            output: projection_output,
        });

        assert_eq!(got, expected);
    }

    #[test]
    fn self_join_wildcard_preserves_each_aliased_relation() {
        let employees = table(1, "employees", &["id", "manager_id"]);
        let catalog = TestCatalog { tables: vec![employees.clone()] };
        let got = bind_select(
            &catalog,
            "SELECT * FROM employees AS employee \
             JOIN employees AS manager ON employee.manager_id == manager.id;",
        )
        .unwrap();

        let employee_relation = RelationId::new(0);
        let manager_relation = RelationId::new(1);
        let employee_id = scoped_column(&employees, "employee", employee_relation, "id");
        let employee_manager_id =
            scoped_column(&employees, "employee", employee_relation, "manager_id");
        let manager_id = scoped_column(&employees, "manager", manager_relation, "id");
        let manager_manager_id =
            scoped_column(&employees, "manager", manager_relation, "manager_id");
        let employee_output =
            PlanSchema::for_qualified_table(employee_relation, &employees, "employee");
        let manager_output =
            PlanSchema::for_qualified_table(manager_relation, &employees, "manager");

        let mut expected = LogicalPlan::new(LogicalPlanNode::TableScan {
            relation: employee_relation,
            table: employees.clone(),
            output: employee_output.clone(),
        });
        let employee_scan = expected.root_id();
        let manager_scan = expected.push(LogicalPlanNode::TableScan {
            relation: manager_relation,
            table: employees,
            output: manager_output.clone(),
        });
        let join_output = PlanSchema::join(employee_output, manager_output);
        let join = expected.push(LogicalPlanNode::Join {
            left: employee_scan,
            right: manager_scan,
            join_type: JoinType::Inner,
            predicate: BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(employee_manager_id.clone())),
                op: Op::EqualsEquals,
                right: Box::new(BoundExpr::Column(manager_id.clone())),
            },
            output: join_output,
        });
        let expressions = vec![
            BoundExpr::Column(employee_id),
            BoundExpr::Column(employee_manager_id),
            BoundExpr::Column(manager_id),
            BoundExpr::Column(manager_manager_id),
        ];
        let projection_output = PlanSchema::for_expressions(&expressions);
        expected.push(LogicalPlanNode::Project {
            input: join,
            expressions,
            output: projection_output,
        });

        assert_eq!(got, expected);
    }

    #[test]
    fn join_condition_cannot_reference_a_later_relation() {
        let catalog = TestCatalog {
            tables: vec![
                table(1, "users", &["id"]),
                table(2, "orders", &["id"]),
                table(3, "products", &["id"]),
            ],
        };

        assert!(matches!(
            bind_select(
                &catalog,
                "SELECT u.id FROM users AS u \
                 JOIN orders AS o ON p.id == o.id \
                 JOIN products AS p ON o.id == p.id;",
            ),
            Err(PlannerError::TableNotInScope { table }) if table == "p"
        ));
    }

    #[test]
    fn select_join_rejects_a_missing_joined_table() {
        let catalog = TestCatalog { tables: vec![table(1, "users", &["id"])] };

        assert!(matches!(
            bind_select(
                &catalog,
                "SELECT users.id FROM users JOIN missing ON users.id == missing.id;",
            ),
            Err(PlannerError::TableNotFound { name }) if name == "missing"
        ));
    }

    #[test]
    fn select_join_rejects_an_ambiguous_unqualified_column() {
        let catalog = TestCatalog {
            tables: vec![table(1, "users", &["id"]), table(2, "orders", &["id", "user_id"])],
        };

        assert!(matches!(
            bind_select(
                &catalog,
                "SELECT id FROM users JOIN orders ON users.id == orders.user_id;",
            ),
            Err(PlannerError::AmbiguousColumn { column }) if column == "id"
        ));
    }

    #[test]
    fn select_join_rejects_duplicate_qualifiers() {
        let catalog = TestCatalog {
            tables: vec![table(1, "users", &["id"]), table(2, "orders", &["user_id"])],
        };

        assert!(matches!(
            bind_select(
                &catalog,
                "SELECT source.id FROM users AS source \
                 JOIN orders AS source ON source.id == source.user_id;",
            ),
            Err(PlannerError::DuplicateTableQualifier { qualifier }) if qualifier == "source"
        ));
    }

    #[test]
    fn physical_planning_rejects_logical_joins_until_they_are_supported() {
        let catalog = TestCatalog {
            tables: vec![table(1, "users", &["id"]), table(2, "orders", &["user_id"])],
        };
        let statement =
            parse("SELECT users.id FROM users JOIN orders ON users.id == orders.user_id;");

        assert!(matches!(
            Planner::with_schema(&catalog).plan_statement(&statement),
            Err(PlannerError::UnsupportedJoin)
        ));
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
