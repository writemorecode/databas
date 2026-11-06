//! Logical-to-physical lowering and access-path selection.

use crate::relational::access::CatalogRead;
use crate::relational::{TableKeyBound, TableKeyRange, TableSchema};
use crate::sql_parser::parser::stmt::select::Ordering;

use super::{
    PhysicalPlan, PhysicalPlanNode, SecondaryIndexScanPlan,
    access_path::{
        IndexPredicate, ordered_secondary_index, primary_key_orders_column,
        primary_key_range_predicate, secondary_index_predicate,
    },
};
use crate::planner::{
    BoundExpr, BoundSortTerm, BoundUpdateAssignment, ExecColumn, ExecExpr, LogicalPlan,
    LogicalPlanNode, NodeId, PlanSchema, PlannerError, PlannerResult, RelationId, SortTerm,
    UpdateAssignment,
};

/// Selects executable operators and access paths for a logical plan.
pub(in crate::planner) struct PhysicalPlanner<'catalog> {
    catalog: &'catalog dyn CatalogRead,
}

impl<'catalog> PhysicalPlanner<'catalog> {
    pub(in crate::planner) fn new(catalog: &'catalog dyn CatalogRead) -> Self {
        Self { catalog }
    }

    pub(in crate::planner) fn plan(&self, logical: LogicalPlan) -> PlannerResult<PhysicalPlan> {
        let output_schema = logical.output_schema(logical.root_id()).cloned();
        let (logical_nodes, logical_root) = logical.into_parts();
        let mut logical_nodes = logical_nodes.into_iter().map(Some).collect::<Vec<_>>();
        let mut physical_nodes = Vec::new();
        let root =
            self.build_physical_plan(&mut logical_nodes, logical_root, true, &mut physical_nodes)?;
        Ok(PhysicalPlan::from_parts(physical_nodes, root, output_schema))
    }

    fn build_physical_plan(
        &self,
        logical_nodes: &mut [Option<LogicalPlanNode>],
        logical_node_id: NodeId,
        allow_secondary_index_scans: bool,
        physical_nodes: &mut Vec<PhysicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let logical = take_logical_node(logical_nodes, logical_node_id)?;
        self.build_physical_node(
            logical_nodes,
            logical,
            allow_secondary_index_scans,
            None,
            physical_nodes,
        )
    }

    fn build_physical_node(
        &self,
        logical_nodes: &mut [Option<LogicalPlanNode>],
        logical: LogicalPlanNode,
        allow_secondary_index_scans: bool,
        order: Option<&BoundSortTerm>,
        physical_nodes: &mut Vec<PhysicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let node = match logical {
            LogicalPlanNode::DropTable { name } => PhysicalPlanNode::DropTable { name },
            LogicalPlanNode::DropIndex { name } => PhysicalPlanNode::DropIndex { name },
            LogicalPlanNode::Explain { input } => PhysicalPlanNode::Explain {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
            },
            LogicalPlanNode::CreateTable { name, schema } => {
                PhysicalPlanNode::CreateTable { name, schema }
            }
            LogicalPlanNode::CreateIndex { name, table, columns } => {
                PhysicalPlanNode::CreateIndex { name, table, columns }
            }
            LogicalPlanNode::Values { rows, .. } => {
                PhysicalPlanNode::Values { rows: lower_rows(rows, &PlanSchema::default())? }
            }
            LogicalPlanNode::Insert { table, columns, input } => {
                match take_logical_node(logical_nodes, input)? {
                    LogicalPlanNode::Values { rows, .. } => PhysicalPlanNode::InsertValues {
                        table,
                        columns,
                        values: lower_rows(rows, &PlanSchema::default())?,
                    },
                    _ => return Err(PlannerError::InvalidInsertInput),
                }
            }
            LogicalPlanNode::Update { relation, table, assignments, input } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                PhysicalPlanNode::Update {
                    relation,
                    table,
                    assignments: lower_assignments(assignments, &input_schema)?,
                    input: self.build_physical_plan(logical_nodes, input, false, physical_nodes)?,
                }
            }
            LogicalPlanNode::Delete { relation, table, input } => PhysicalPlanNode::Delete {
                relation,
                table,
                input: self.build_physical_plan(logical_nodes, input, false, physical_nodes)?,
            },
            LogicalPlanNode::OneRow { .. } => PhysicalPlanNode::OneRow,
            LogicalPlanNode::Empty { .. } => PhysicalPlanNode::Empty,
            LogicalPlanNode::TableScan { relation, table, .. } => {
                self.table_scan(relation, table, None, allow_secondary_index_scans, order)?
            }
            LogicalPlanNode::Filter { input, predicate, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                match take_logical_node(logical_nodes, input)? {
                    LogicalPlanNode::TableScan { relation, table, .. } => {
                        let (scan, residual) = self.filtered_table_scan(
                            relation,
                            table,
                            predicate,
                            allow_secondary_index_scans,
                            order,
                        )?;
                        let input = push_physical_node(physical_nodes, scan);
                        match residual {
                            Some(predicate) => PhysicalPlanNode::Filter {
                                input,
                                predicate: lower_expression(predicate, &input_schema)?,
                            },
                            None => return Ok(input),
                        }
                    }
                    input_node => PhysicalPlanNode::Filter {
                        input: self.build_physical_node(
                            logical_nodes,
                            input_node,
                            allow_secondary_index_scans,
                            None,
                            physical_nodes,
                        )?,
                        predicate: lower_expression(predicate, &input_schema)?,
                    },
                }
            }
            LogicalPlanNode::Sort { input, terms, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                let order = match terms.as_slice() {
                    [term] => Some(term),
                    _ => None,
                };
                let input_node = take_logical_node(logical_nodes, input)?;
                let input = self.build_physical_node(
                    logical_nodes,
                    input_node,
                    allow_secondary_index_scans,
                    order,
                    physical_nodes,
                )?;
                if order.is_some_and(|term| scan_satisfies_order(physical_nodes, input, term)) {
                    return Ok(input);
                }
                PhysicalPlanNode::Sort { input, terms: lower_sort_terms(terms, &input_schema)? }
            }
            LogicalPlanNode::Project { input, expressions, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                let input = self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?;
                if matches!(physical_nodes.get(input.index()), Some(PhysicalPlanNode::Empty)) {
                    return Ok(input);
                }
                PhysicalPlanNode::Project {
                    input,
                    expressions: expressions
                        .into_iter()
                        .map(|expression| lower_expression(expression, &input_schema))
                        .collect::<PlannerResult<Vec<_>>>()?,
                }
            }
            LogicalPlanNode::Offset { input, offset, .. } => PhysicalPlanNode::Offset {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
                offset,
            },
            LogicalPlanNode::Limit { input, limit, .. } => PhysicalPlanNode::Limit {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
                limit,
            },
            LogicalPlanNode::Join { .. } => return Err(PlannerError::UnsupportedJoin),
        };
        Ok(push_physical_node(physical_nodes, node))
    }

    fn filtered_table_scan(
        &self,
        relation: RelationId,
        table: TableSchema,
        predicate: BoundExpr,
        allow_secondary_index_scans: bool,
        order: Option<&BoundSortTerm>,
    ) -> PlannerResult<(PhysicalPlanNode, Option<BoundExpr>)> {
        if let Some(range_predicate) = primary_key_range_predicate(&table, relation, &predicate) {
            if range_predicate.range.is_empty() {
                return Ok((PhysicalPlanNode::Empty, None));
            }
            let direction = primary_order(&table, relation, order)
                .map_or(Ordering::Ascending, |term| term.direction.clone());
            return Ok((
                PhysicalPlanNode::PrimaryKeyRangeScan {
                    relation,
                    table,
                    range: range_predicate.range,
                    direction,
                },
                range_predicate.residual,
            ));
        }

        let scan =
            self.table_scan(relation, table, Some(&predicate), allow_secondary_index_scans, order)?;
        Ok((scan, Some(predicate)))
    }

    fn table_scan(
        &self,
        relation: RelationId,
        table: TableSchema,
        predicate: Option<&BoundExpr>,
        allow_secondary_index_scans: bool,
        order: Option<&BoundSortTerm>,
    ) -> PlannerResult<PhysicalPlanNode> {
        let primary_order = primary_order(&table, relation, order);
        let needs_indexes = predicate.is_some() || (order.is_some() && primary_order.is_none());
        let indexes = if allow_secondary_index_scans && needs_indexes {
            self.catalog.index_schemas_for_table(&table)?
        } else {
            Vec::new()
        };
        let ordered_index = order.and_then(|term| {
            ordered_secondary_index(&table, relation, &term.column, &indexes)
                .map(|index| (index, term))
        });
        if !indexes.is_empty()
            && let Some(predicate) = predicate
        {
            if let Some((index, term)) = ordered_index
                && let Some(access) = secondary_index_predicate(
                    &table,
                    relation,
                    predicate,
                    std::slice::from_ref(index),
                )
            {
                return Ok(secondary_scan(relation, table, access, term.direction.clone()));
            }

            // Keep a bounded predicate access path ahead of an unbounded ordering scan.
            if let Some(access) = secondary_index_predicate(&table, relation, predicate, &indexes) {
                return Ok(secondary_scan(relation, table, access, Ordering::Ascending));
            }
        }

        if let Some(term) = primary_order {
            return Ok(PhysicalPlanNode::PrimaryKeyRangeScan {
                relation,
                table,
                range: TableKeyRange::unbounded(),
                direction: term.direction.clone(),
            });
        }

        if let Some((index, term)) = ordered_index {
            let access = IndexPredicate {
                index: index.clone(),
                column: term.column.clone(),
                value_range: Default::default(),
            };
            return Ok(secondary_scan(relation, table, access, term.direction.clone()));
        }

        Ok(PhysicalPlanNode::FullTableScan { relation, table })
    }
}

fn primary_order<'a>(
    table: &TableSchema,
    relation: RelationId,
    order: Option<&'a BoundSortTerm>,
) -> Option<&'a BoundSortTerm> {
    order.filter(|term| primary_key_orders_column(table, relation, &term.column))
}

fn secondary_scan(
    relation: RelationId,
    table: TableSchema,
    access: IndexPredicate,
    direction: Ordering,
) -> PhysicalPlanNode {
    PhysicalPlanNode::SecondaryIndexScan {
        scan: SecondaryIndexScanPlan {
            relation,
            table,
            index: access.index,
            column: access.column,
            value_range: access.value_range,
            direction,
        },
    }
}

fn scan_satisfies_order(nodes: &[PhysicalPlanNode], input: NodeId, term: &BoundSortTerm) -> bool {
    match &nodes[input.index()] {
        PhysicalPlanNode::Filter { input, .. } => scan_satisfies_order(nodes, *input, term),
        PhysicalPlanNode::PrimaryKeyRangeScan { relation, table, range, direction } => {
            matches!(
                (range.lower, range.upper),
                (Some(TableKeyBound::Inclusive(lower)), Some(TableKeyBound::Inclusive(upper)))
                    if lower == upper
            ) || (*direction == term.direction
                && primary_key_orders_column(table, *relation, &term.column))
        }
        PhysicalPlanNode::SecondaryIndexScan { scan } => {
            scan.direction == term.direction
                && scan.column == term.column
                && !scan.table.row.columns[scan.column.ordinal].nullable
        }
        PhysicalPlanNode::Empty => true,
        _ => false,
    }
}

fn lower_rows(rows: Vec<Vec<BoundExpr>>, schema: &PlanSchema) -> PlannerResult<Vec<Vec<ExecExpr>>> {
    rows.into_iter()
        .map(|row| row.into_iter().map(|expression| lower_expression(expression, schema)).collect())
        .collect()
}

fn lower_assignments(
    assignments: Vec<BoundUpdateAssignment>,
    schema: &PlanSchema,
) -> PlannerResult<Vec<UpdateAssignment>> {
    assignments
        .into_iter()
        .map(|assignment| {
            Ok(UpdateAssignment {
                column: assignment.column,
                expression: lower_expression(assignment.expression, schema)?,
            })
        })
        .collect()
}

fn lower_sort_terms(
    terms: Vec<BoundSortTerm>,
    schema: &PlanSchema,
) -> PlannerResult<Vec<SortTerm>> {
    terms
        .into_iter()
        .map(|term| {
            Ok(SortTerm { column: lower_column(&term.column, schema)?, direction: term.direction })
        })
        .collect()
}

fn lower_expression(expression: BoundExpr, schema: &PlanSchema) -> PlannerResult<ExecExpr> {
    match expression {
        BoundExpr::Literal(value) => Ok(ExecExpr::Literal(value)),
        BoundExpr::Column(column) => lower_column(&column, schema).map(ExecExpr::Column),
        BoundExpr::Unary { op, expr } => {
            Ok(ExecExpr::Unary { op, expr: Box::new(lower_expression(*expr, schema)?) })
        }
        BoundExpr::Binary { left, op, right } => Ok(ExecExpr::Binary {
            left: Box::new(lower_expression(*left, schema)?),
            op,
            right: Box::new(lower_expression(*right, schema)?),
        }),
    }
}

fn lower_column(
    column: &crate::planner::BoundColumn,
    schema: &PlanSchema,
) -> PlannerResult<ExecColumn> {
    let slot = schema
        .slot_for(column)
        .ok_or_else(|| PlannerError::ColumnNotInInput { column: column.to_string() })?;
    Ok(ExecColumn { slot, name: column.to_string(), data_type: column.data_type })
}

fn logical_output_schema(
    nodes: &[Option<LogicalPlanNode>],
    id: NodeId,
) -> PlannerResult<PlanSchema> {
    nodes
        .get(id.index())
        .and_then(Option::as_ref)
        .and_then(LogicalPlanNode::output_schema)
        .cloned()
        .ok_or(PlannerError::InvalidLogicalPlan)
}

fn take_logical_node(
    nodes: &mut [Option<LogicalPlanNode>],
    id: NodeId,
) -> PlannerResult<LogicalPlanNode> {
    nodes.get_mut(id.index()).and_then(Option::take).ok_or(PlannerError::InvalidLogicalPlan)
}

fn push_physical_node(nodes: &mut Vec<PhysicalPlanNode>, node: PhysicalPlanNode) -> NodeId {
    let id = NodeId::new(nodes.len());
    nodes.push(node);
    id
}
