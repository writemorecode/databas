use super::*;

/// Planner bound to a database catalog.
///
/// A planner borrows catalog access so it can resolve table schemas, discover
/// indexes, and bind column references. It does not mutate the catalog or
/// perform writes. Even DDL and DML statements are represented only as plan
/// nodes until an executor runs the physical plan.
pub struct Planner<'db> {
    schema: &'db dyn CatalogRead,
}

impl<'db> Planner<'db> {
    /// Creates a planner that resolves names through `database`.
    ///
    /// The database is borrowed for catalog reads only. The returned planner can
    /// be reused for multiple statements as long as the borrowed database lives.
    pub fn new(database: &'db Database) -> Self {
        Self::with_schema(database)
    }

    /// Creates a planner over an arbitrary catalog implementation.
    ///
    /// Keeping this constructor crate-visible lets focused tests and other
    /// internal planning clients provide lightweight schema access without
    /// constructing a storage-backed database.
    pub(crate) fn with_schema(schema: &'db dyn CatalogRead) -> Self {
        Self { schema }
    }

    /// Plans one parsed SQL statement.
    ///
    /// This first builds a [`LogicalPlan`] with catalog-bound names, then
    /// converts it into a [`PhysicalPlan`] that can be passed to the executor.
    /// The method performs catalog lookups and access-path selection, but does
    /// not execute the statement or mutate database state.
    pub fn plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<Plan> {
        let logical = self.logical_plan_statement(statement)?;
        let physical = self.physical_plan(logical.clone())?;
        Ok(Plan { logical, physical })
    }

    /// Plans one parsed SQL statement directly into an executable physical plan.
    ///
    /// This is the production execution path: the intermediate logical plan is
    /// consumed during physical planning instead of being retained for
    /// inspection.
    pub fn plan_physical_statement(
        &self,
        statement: &Statement<'_>,
    ) -> PlannerResult<PhysicalPlan> {
        let logical = self.logical_plan_statement(statement)?;
        self.physical_plan(logical)
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

    fn physical_plan(&self, logical: LogicalPlan) -> PlannerResult<PhysicalPlan> {
        self.physical_plan_with_secondary_indexes(logical, true)
    }

    fn physical_plan_with_secondary_indexes(
        &self,
        logical: LogicalPlan,
        allow_secondary_index_scans: bool,
    ) -> PlannerResult<PhysicalPlan> {
        match logical {
            LogicalPlan::Explain { input } => Ok(PhysicalPlan::Explain {
                input: Box::new(
                    self.physical_plan_with_secondary_indexes(*input, allow_secondary_index_scans)?,
                ),
            }),
            LogicalPlan::CreateTable { name, schema } => {
                Ok(PhysicalPlan::CreateTable { name, schema })
            }
            LogicalPlan::CreateIndex { name, table, columns } => {
                Ok(PhysicalPlan::CreateIndex { name, table, columns })
            }
            LogicalPlan::Values { rows } => Ok(PhysicalPlan::Values { rows }),
            LogicalPlan::Insert { table, columns, input } => match *input {
                LogicalPlan::Values { rows } => {
                    Ok(PhysicalPlan::InsertValues { table, columns, values: rows })
                }
                _ => Err(PlannerError::InvalidInsertInput),
            },
            LogicalPlan::Update { table, assignments, input } => Ok(PhysicalPlan::Update {
                table,
                assignments,
                input: Box::new(self.physical_plan_with_secondary_indexes(*input, false)?),
            }),
            LogicalPlan::Delete { table, input } => Ok(PhysicalPlan::Delete {
                table,
                input: Box::new(self.physical_plan_with_secondary_indexes(*input, false)?),
            }),
            LogicalPlan::OneRow => Ok(PhysicalPlan::OneRow),
            LogicalPlan::TableScan { table } => Ok(PhysicalPlan::FullTableScan { table }),
            LogicalPlan::Filter { input, predicate } => match *input {
                LogicalPlan::TableScan { table } => {
                    match primary_key_range_predicate(&table, &predicate) {
                        Some(range_predicate) => {
                            let scan = PhysicalPlan::PrimaryKeyRangeScan {
                                table,
                                range: range_predicate.range,
                            };
                            match range_predicate.residual {
                                Some(residual) => Ok(PhysicalPlan::Filter {
                                    input: Box::new(scan),
                                    predicate: residual,
                                }),
                                None => Ok(scan),
                            }
                        }
                        None if allow_secondary_index_scans => {
                            match self.secondary_index_predicate(&table, &predicate)? {
                                Some(index_predicate) => Ok(PhysicalPlan::Filter {
                                    input: Box::new(PhysicalPlan::SecondaryIndexScan {
                                        scan: Box::new(SecondaryIndexScanPlan {
                                            table,
                                            index: index_predicate.index,
                                            column: index_predicate.column,
                                            value_range: index_predicate.value_range,
                                            key_range: index_predicate.key_range,
                                        }),
                                    }),
                                    predicate,
                                }),
                                None => Ok(PhysicalPlan::Filter {
                                    input: Box::new(PhysicalPlan::FullTableScan { table }),
                                    predicate,
                                }),
                            }
                        }
                        None => Ok(PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::FullTableScan { table }),
                            predicate,
                        }),
                    }
                }
                input => Ok(PhysicalPlan::Filter {
                    input: Box::new(self.physical_plan_with_secondary_indexes(
                        input,
                        allow_secondary_index_scans,
                    )?),
                    predicate,
                }),
            },
            LogicalPlan::Sort { input, terms } => Ok(PhysicalPlan::Sort {
                input: Box::new(
                    self.physical_plan_with_secondary_indexes(*input, allow_secondary_index_scans)?,
                ),
                terms,
            }),
            LogicalPlan::Project { input, expressions } => Ok(PhysicalPlan::Project {
                input: Box::new(
                    self.physical_plan_with_secondary_indexes(*input, allow_secondary_index_scans)?,
                ),
                expressions,
            }),
            LogicalPlan::Offset { input, offset } => Ok(PhysicalPlan::Offset {
                input: Box::new(
                    self.physical_plan_with_secondary_indexes(*input, allow_secondary_index_scans)?,
                ),
                offset,
            }),
            LogicalPlan::Limit { input, limit } => Ok(PhysicalPlan::Limit {
                input: Box::new(
                    self.physical_plan_with_secondary_indexes(*input, allow_secondary_index_scans)?,
                ),
                limit,
            }),
        }
    }

    fn secondary_index_predicate(
        &self,
        table: &TableSchema,
        predicate: &PlannedExpression,
    ) -> PlannerResult<Option<IndexPredicate>> {
        let indexes = self.schema.index_schemas_for_table(table)?;
        Ok(secondary_index_predicate(table, predicate, &indexes))
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

#[derive(Debug, Clone, PartialEq)]
struct RangePredicate {
    range: TableKeyRange,
    residual: Option<PlannedExpression>,
}

#[derive(Debug, Clone, PartialEq)]
struct IndexPredicate {
    index: IndexSchema,
    column: BoundColumn,
    value_range: IndexValueRange,
    key_range: IndexKeyRange,
}

fn primary_key_range_predicate(
    table: &TableSchema,
    predicate: &PlannedExpression,
) -> Option<RangePredicate> {
    let primary_key = table.row.columns.first()?;
    if !primary_key.primary_key || primary_key.data_type != DataType::Integer {
        return None;
    }

    range_predicate_from_expression(table, predicate)
}

fn secondary_index_predicate(
    table: &TableSchema,
    predicate: &PlannedExpression,
    indexes: &[IndexSchema],
) -> Option<IndexPredicate> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(predicate, &mut conjuncts);

    for conjunct in &conjuncts {
        let Some(candidate) = secondary_index_comparison(table, conjunct, indexes) else {
            continue;
        };
        let mut value_range = IndexValueRange::default();
        let mut key_range = IndexKeyRange::default();

        for conjunct in &conjuncts {
            if let Some(comparison) =
                index_comparison_for_column(table, conjunct, &candidate.column)
            {
                combine_index_ranges(
                    &mut value_range,
                    &mut key_range,
                    comparison.value,
                    comparison.kind,
                )?;
            }
        }

        return Some(IndexPredicate {
            index: candidate.index,
            column: candidate.column,
            value_range,
            key_range,
        });
    }

    None
}

fn flatten_conjuncts<'a>(
    expression: &'a PlannedExpression,
    conjuncts: &mut Vec<&'a PlannedExpression>,
) {
    match expression {
        PlannedExpression::Binary { left, op: Op::And, right } => {
            flatten_conjuncts(left, conjuncts);
            flatten_conjuncts(right, conjuncts);
        }
        _ => conjuncts.push(expression),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexComparisonKind {
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

struct IndexComparison<'a> {
    column: BoundColumn,
    value: &'a Value,
    kind: IndexComparisonKind,
}

struct SecondaryIndexCandidate {
    index: IndexSchema,
    column: BoundColumn,
}

fn secondary_index_comparison(
    table: &TableSchema,
    comparison: &PlannedExpression,
    indexes: &[IndexSchema],
) -> Option<SecondaryIndexCandidate> {
    let comparison = index_comparison(table, comparison)?;
    let index =
        indexes.iter().find(|index| exact_single_column_index(index, table, &comparison.column))?;
    Some(SecondaryIndexCandidate { index: index.clone(), column: comparison.column })
}

fn index_comparison_for_column<'a>(
    table: &TableSchema,
    comparison: &'a PlannedExpression,
    column: &BoundColumn,
) -> Option<IndexComparison<'a>> {
    let comparison = index_comparison(table, comparison)?;
    (comparison.column == *column).then_some(comparison)
}

fn index_comparison<'a>(
    table: &TableSchema,
    comparison: &'a PlannedExpression,
) -> Option<IndexComparison<'a>> {
    let PlannedExpression::Binary { left, op, right } = comparison else {
        return None;
    };
    index_comparison_from_operands(table, left, *op, right)
        .or_else(|| index_comparison_from_reversed_operands(table, left, *op, right))
}

fn index_comparison_from_operands<'a>(
    table: &TableSchema,
    column: &PlannedExpression,
    op: Op,
    value: &'a PlannedExpression,
) -> Option<IndexComparison<'a>> {
    let PlannedExpression::Column(column) = column else {
        return None;
    };
    let PlannedExpression::Literal(value) = value else {
        return None;
    };
    if column.table != table.name || !value_matches_data_type(value, column.data_type) {
        return None;
    }

    let kind = match op {
        Op::EqualsEquals => IndexComparisonKind::Equals,
        Op::GreaterThan => IndexComparisonKind::GreaterThan,
        Op::GreaterThanOrEqual => IndexComparisonKind::GreaterThanOrEqual,
        Op::LessThan => IndexComparisonKind::LessThan,
        Op::LessThanOrEqual => IndexComparisonKind::LessThanOrEqual,
        Op::And | Op::Or | Op::NotEquals | Op::Not | Op::Add | Op::Sub | Op::Mul | Op::Div => {
            return None;
        }
    };

    if !secondary_index_comparison_is_order_compatible(column.data_type, kind) {
        return None;
    }

    Some(IndexComparison { column: column.clone(), value, kind })
}

fn index_comparison_from_reversed_operands<'a>(
    table: &TableSchema,
    value: &'a PlannedExpression,
    op: Op,
    column: &PlannedExpression,
) -> Option<IndexComparison<'a>> {
    let reversed = reverse_comparison_op(op)?;
    index_comparison_from_operands(table, column, reversed, value)
}

fn reverse_comparison_op(op: Op) -> Option<Op> {
    match op {
        Op::EqualsEquals => Some(Op::EqualsEquals),
        Op::GreaterThan => Some(Op::LessThan),
        Op::GreaterThanOrEqual => Some(Op::LessThanOrEqual),
        Op::LessThan => Some(Op::GreaterThan),
        Op::LessThanOrEqual => Some(Op::GreaterThanOrEqual),
        Op::And | Op::Or | Op::NotEquals | Op::Not | Op::Add | Op::Sub | Op::Mul | Op::Div => None,
    }
}

fn secondary_index_comparison_is_order_compatible(
    data_type: DataType,
    kind: IndexComparisonKind,
) -> bool {
    kind == IndexComparisonKind::Equals || data_type != DataType::Text
}

fn combine_index_ranges(
    value_range: &mut IndexValueRange,
    key_range: &mut IndexKeyRange,
    value: &Value,
    kind: IndexComparisonKind,
) -> Option<()> {
    match kind {
        IndexComparisonKind::Equals => {
            combine_index_lower_bound(value_range, key_range, value.clone(), true)?;
            combine_index_upper_bound(value_range, key_range, value.clone(), true)?;
        }
        IndexComparisonKind::GreaterThan => {
            combine_index_lower_bound(value_range, key_range, value.clone(), false)?;
        }
        IndexComparisonKind::GreaterThanOrEqual => {
            combine_index_lower_bound(value_range, key_range, value.clone(), true)?;
        }
        IndexComparisonKind::LessThan => {
            combine_index_upper_bound(value_range, key_range, value.clone(), false)?;
        }
        IndexComparisonKind::LessThanOrEqual => {
            combine_index_upper_bound(value_range, key_range, value.clone(), true)?;
        }
    }
    Some(())
}

fn combine_index_lower_bound(
    value_range: &mut IndexValueRange,
    key_range: &mut IndexKeyRange,
    value: Value,
    inclusive: bool,
) -> Option<()> {
    let key = index_lower_key(&value, inclusive)?;
    let value_bound = if inclusive {
        IndexValueBound::Inclusive(value)
    } else {
        IndexValueBound::Exclusive(value)
    };
    let key_bound =
        if inclusive { IndexKeyBound::Inclusive(key) } else { IndexKeyBound::Exclusive(key) };

    if index_lower_is_tighter(key_range.lower.as_ref(), &key_bound) {
        value_range.lower = Some(value_bound);
        key_range.lower = Some(key_bound);
    }
    Some(())
}

fn combine_index_upper_bound(
    value_range: &mut IndexValueRange,
    key_range: &mut IndexKeyRange,
    value: Value,
    inclusive: bool,
) -> Option<()> {
    let key = index_upper_key(&value, inclusive)?;
    let value_bound = if inclusive {
        IndexValueBound::Inclusive(value)
    } else {
        IndexValueBound::Exclusive(value)
    };
    let key_bound =
        if inclusive { IndexKeyBound::Inclusive(key) } else { IndexKeyBound::Exclusive(key) };

    if index_upper_is_tighter(key_range.upper.as_ref(), &key_bound) {
        value_range.upper = Some(value_bound);
        key_range.upper = Some(key_bound);
    }
    Some(())
}

fn index_lower_key(value: &Value, inclusive: bool) -> Option<Vec<u8>> {
    let prefix = Tuple::new(vec![value.clone()]).to_bytes().ok()?;
    let table_key = if inclusive { TableKey::MIN } else { TableKey::MAX };
    Some(encode_index_entry_key(&prefix, table_key))
}

fn index_upper_key(value: &Value, inclusive: bool) -> Option<Vec<u8>> {
    let prefix = Tuple::new(vec![value.clone()]).to_bytes().ok()?;
    let table_key = if inclusive { TableKey::MAX } else { TableKey::MIN };
    Some(encode_index_entry_key(&prefix, table_key))
}

fn index_lower_is_tighter(current: Option<&IndexKeyBound>, candidate: &IndexKeyBound) -> bool {
    match current {
        None => true,
        Some(current) => {
            candidate.key() > current.key()
                || (candidate.key() == current.key()
                    && matches!(candidate, IndexKeyBound::Exclusive(_)))
        }
    }
}

fn index_upper_is_tighter(current: Option<&IndexKeyBound>, candidate: &IndexKeyBound) -> bool {
    match current {
        None => true,
        Some(current) => {
            candidate.key() < current.key()
                || (candidate.key() == current.key()
                    && matches!(candidate, IndexKeyBound::Exclusive(_)))
        }
    }
}

fn exact_single_column_index(
    index: &IndexSchema,
    table: &TableSchema,
    column: &BoundColumn,
) -> bool {
    index.table_id == table.table_id
        && index.columns.len() == 1
        && index.columns[0].source_column_ordinal == column.ordinal
}

fn value_matches_data_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::String(_), DataType::Text)
            | (Value::Boolean(_), DataType::Boolean)
            | (Value::Integer(_), DataType::Integer)
            | (Value::Float(_), DataType::Float)
            | (Value::UnsignedInteger(_), DataType::UnsignedInteger)
    )
}

fn range_predicate_from_expression(
    table: &TableSchema,
    expression: &PlannedExpression,
) -> Option<RangePredicate> {
    match expression {
        PlannedExpression::Binary { left, op: Op::And, right } => {
            let left = range_predicate_from_expression(table, left)?;
            if let Some(left_residual) = left.residual {
                return Some(RangePredicate {
                    range: left.range,
                    residual: Some(and_expression(left_residual, (**right).clone())),
                });
            }

            match range_predicate_from_expression(table, right) {
                Some(right) => Some(RangePredicate {
                    range: combine_ranges(left.range, right.range),
                    residual: right.residual,
                }),
                None => {
                    Some(RangePredicate { range: left.range, residual: Some((**right).clone()) })
                }
            }
        }
        PlannedExpression::Binary { left, op, right } => {
            range_from_comparison(table, left, *op, right)
                .map(|range| RangePredicate { range, residual: None })
        }
        PlannedExpression::Literal(_)
        | PlannedExpression::Column(_)
        | PlannedExpression::Unary { .. } => None,
    }
}

fn and_expression(left: PlannedExpression, right: PlannedExpression) -> PlannedExpression {
    PlannedExpression::Binary { left: Box::new(left), op: Op::And, right: Box::new(right) }
}

fn range_from_comparison(
    table: &TableSchema,
    left: &PlannedExpression,
    op: Op,
    right: &PlannedExpression,
) -> Option<TableKeyRange> {
    match (left, right) {
        (PlannedExpression::Column(column), PlannedExpression::Literal(Value::Integer(value)))
            if is_table_primary_key(table, column) =>
        {
            range_from_column_comparison(op, *value)
        }
        (PlannedExpression::Literal(Value::Integer(value)), PlannedExpression::Column(column))
            if is_table_primary_key(table, column) =>
        {
            range_from_literal_comparison(op, *value)
        }
        _ => None,
    }
}

fn is_table_primary_key(table: &TableSchema, column: &BoundColumn) -> bool {
    column.table == table.name && column.ordinal == 0 && table.row.columns[0].primary_key
}

fn range_from_column_comparison(op: Op, value: TableKey) -> Option<TableKeyRange> {
    match op {
        Op::EqualsEquals => Some(TableKeyRange {
            lower: Some(TableKeyBound::Inclusive(value)),
            upper: Some(TableKeyBound::Inclusive(value)),
        }),
        Op::GreaterThan => {
            Some(TableKeyRange { lower: Some(TableKeyBound::Exclusive(value)), upper: None })
        }
        Op::GreaterThanOrEqual => {
            Some(TableKeyRange { lower: Some(TableKeyBound::Inclusive(value)), upper: None })
        }
        Op::LessThan => {
            Some(TableKeyRange { lower: None, upper: Some(TableKeyBound::Exclusive(value)) })
        }
        Op::LessThanOrEqual => {
            Some(TableKeyRange { lower: None, upper: Some(TableKeyBound::Inclusive(value)) })
        }
        Op::And | Op::Or | Op::NotEquals | Op::Not | Op::Add | Op::Sub | Op::Mul | Op::Div => None,
    }
}

fn range_from_literal_comparison(op: Op, value: TableKey) -> Option<TableKeyRange> {
    range_from_column_comparison(reverse_comparison_op(op)?, value)
}

fn combine_ranges(left: TableKeyRange, right: TableKeyRange) -> TableKeyRange {
    TableKeyRange {
        lower: tightest_lower(left.lower, right.lower),
        upper: tightest_upper(left.upper, right.upper),
    }
}

fn tightest_lower(
    left: Option<TableKeyBound>,
    right: Option<TableKeyBound>,
) -> Option<TableKeyBound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => {
            if left.value() > right.value() {
                Some(left)
            } else if right.value() > left.value() || bound_is_exclusive(right) {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn tightest_upper(
    left: Option<TableKeyBound>,
    right: Option<TableKeyBound>,
) -> Option<TableKeyBound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => {
            if left.value() < right.value() {
                Some(left)
            } else if right.value() < left.value() || bound_is_exclusive(right) {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn bound_is_exclusive(bound: TableKeyBound) -> bool {
    matches!(bound, TableKeyBound::Exclusive(_))
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
