//! Logical-to-physical lowering and access-path selection.

use crate::core::{TableSchema, access::CatalogRead};

use super::{
    PhysicalPlan, PhysicalPlanNode, SecondaryIndexScanPlan,
    access_path::{IndexPredicate, primary_key_range_predicate, secondary_index_predicate},
};
use crate::planner::{
    BoundExpr, BoundSortTerm, BoundUpdateAssignment, ExecColumn, ExecExpr, LogicalPlan,
    LogicalPlanNode, NodeId, PlanSchema, PlannerError, PlannerResult, SortTerm, UpdateAssignment,
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
        let (logical_nodes, logical_root) = logical.into_parts();
        let mut logical_nodes = logical_nodes.into_iter().map(Some).collect::<Vec<_>>();
        let mut physical_nodes = Vec::new();
        let root =
            self.build_physical_plan(&mut logical_nodes, logical_root, true, &mut physical_nodes)?;
        Ok(PhysicalPlan::from_parts(physical_nodes, root))
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
            physical_nodes,
        )
    }

    fn build_physical_node(
        &self,
        logical_nodes: &mut [Option<LogicalPlanNode>],
        logical: LogicalPlanNode,
        allow_secondary_index_scans: bool,
        physical_nodes: &mut Vec<PhysicalPlanNode>,
    ) -> PlannerResult<NodeId> {
        let node = match logical {
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
            LogicalPlanNode::TableScan { relation, table, .. } => {
                PhysicalPlanNode::FullTableScan { relation, table }
            }
            LogicalPlanNode::Filter { input, predicate, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                match take_logical_node(logical_nodes, input)? {
                    LogicalPlanNode::TableScan { relation, table, .. } => {
                        match primary_key_range_predicate(&table, &predicate) {
                            Some(range_predicate) => {
                                let scan = push_physical_node(
                                    physical_nodes,
                                    PhysicalPlanNode::PrimaryKeyRangeScan {
                                        relation,
                                        table,
                                        range: range_predicate.range,
                                    },
                                );
                                match range_predicate.residual {
                                    Some(predicate) => PhysicalPlanNode::Filter {
                                        input: scan,
                                        predicate: lower_expression(predicate, &input_schema)?,
                                    },
                                    None => return Ok(scan),
                                }
                            }
                            None if allow_secondary_index_scans => {
                                let scan = match self
                                    .secondary_index_predicate(&table, &predicate)?
                                {
                                    Some(index_predicate) => PhysicalPlanNode::SecondaryIndexScan {
                                        scan: SecondaryIndexScanPlan {
                                            relation,
                                            table,
                                            index: index_predicate.index,
                                            column: index_predicate.column,
                                            value_range: index_predicate.value_range,
                                            key_range: index_predicate.key_range,
                                        },
                                    },
                                    None => PhysicalPlanNode::FullTableScan { relation, table },
                                };
                                let input = push_physical_node(physical_nodes, scan);
                                PhysicalPlanNode::Filter {
                                    input,
                                    predicate: lower_expression(predicate, &input_schema)?,
                                }
                            }
                            None => {
                                let input = push_physical_node(
                                    physical_nodes,
                                    PhysicalPlanNode::FullTableScan { relation, table },
                                );
                                PhysicalPlanNode::Filter {
                                    input,
                                    predicate: lower_expression(predicate, &input_schema)?,
                                }
                            }
                        }
                    }
                    input_node => PhysicalPlanNode::Filter {
                        input: self.build_physical_node(
                            logical_nodes,
                            input_node,
                            allow_secondary_index_scans,
                            physical_nodes,
                        )?,
                        predicate: lower_expression(predicate, &input_schema)?,
                    },
                }
            }
            LogicalPlanNode::Sort { input, terms, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                PhysicalPlanNode::Sort {
                    input: self.build_physical_plan(
                        logical_nodes,
                        input,
                        allow_secondary_index_scans,
                        physical_nodes,
                    )?,
                    terms: lower_sort_terms(terms, &input_schema)?,
                }
            }
            LogicalPlanNode::Project { input, expressions, .. } => {
                let input_schema = logical_output_schema(logical_nodes, input)?;
                PhysicalPlanNode::Project {
                    input: self.build_physical_plan(
                        logical_nodes,
                        input,
                        allow_secondary_index_scans,
                        physical_nodes,
                    )?,
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
        };
        Ok(push_physical_node(physical_nodes, node))
    }

    fn secondary_index_predicate(
        &self,
        table: &TableSchema,
        predicate: &BoundExpr,
    ) -> PlannerResult<Option<IndexPredicate>> {
        let indexes = self.catalog.index_schemas_for_table(table)?;
        Ok(secondary_index_predicate(table, predicate, &indexes))
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
            Ok(SortTerm { column: lower_column(term.column, schema)?, direction: term.direction })
        })
        .collect()
}

fn lower_expression(expression: BoundExpr, schema: &PlanSchema) -> PlannerResult<ExecExpr> {
    match expression {
        BoundExpr::Literal(value) => Ok(ExecExpr::Literal(value)),
        BoundExpr::Column(column) => lower_column(column, schema).map(ExecExpr::Column),
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
    column: crate::planner::BoundColumn,
    schema: &PlanSchema,
) -> PlannerResult<ExecColumn> {
    let slot = schema
        .slot_for(&column)
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
