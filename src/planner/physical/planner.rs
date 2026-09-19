//! Logical-to-physical lowering and access-path selection.

use crate::core::{TableSchema, access::CatalogRead};

use super::{
    PhysicalPlan, PhysicalPlanNode, SecondaryIndexScanPlan,
    access_path::{IndexPredicate, primary_key_range_predicate, secondary_index_predicate},
};
use crate::planner::{
    LogicalPlan, LogicalPlanNode, NodeId, PlannedExpression, PlannerError, PlannerResult,
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
            LogicalPlanNode::Values { rows } => PhysicalPlanNode::Values { rows },
            LogicalPlanNode::Insert { table, columns, input } => {
                match take_logical_node(logical_nodes, input)? {
                    LogicalPlanNode::Values { rows } => {
                        PhysicalPlanNode::InsertValues { table, columns, values: rows }
                    }
                    _ => return Err(PlannerError::InvalidInsertInput),
                }
            }
            LogicalPlanNode::Update { relation, table, assignments, input } => {
                PhysicalPlanNode::Update {
                    relation,
                    table,
                    assignments,
                    input: self.build_physical_plan(logical_nodes, input, false, physical_nodes)?,
                }
            }
            LogicalPlanNode::Delete { relation, table, input } => PhysicalPlanNode::Delete {
                relation,
                table,
                input: self.build_physical_plan(logical_nodes, input, false, physical_nodes)?,
            },
            LogicalPlanNode::OneRow => PhysicalPlanNode::OneRow,
            LogicalPlanNode::TableScan { relation, table } => {
                PhysicalPlanNode::FullTableScan { relation, table }
            }
            LogicalPlanNode::Filter { input, predicate } => {
                match take_logical_node(logical_nodes, input)? {
                    LogicalPlanNode::TableScan { relation, table } => {
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
                                    Some(predicate) => {
                                        PhysicalPlanNode::Filter { input: scan, predicate }
                                    }
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
                                PhysicalPlanNode::Filter { input, predicate }
                            }
                            None => {
                                let input = push_physical_node(
                                    physical_nodes,
                                    PhysicalPlanNode::FullTableScan { relation, table },
                                );
                                PhysicalPlanNode::Filter { input, predicate }
                            }
                        }
                    }
                    input => PhysicalPlanNode::Filter {
                        input: self.build_physical_node(
                            logical_nodes,
                            input,
                            allow_secondary_index_scans,
                            physical_nodes,
                        )?,
                        predicate,
                    },
                }
            }
            LogicalPlanNode::Sort { input, terms } => PhysicalPlanNode::Sort {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
                terms,
            },
            LogicalPlanNode::Project { input, expressions } => PhysicalPlanNode::Project {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
                expressions,
            },
            LogicalPlanNode::Offset { input, offset } => PhysicalPlanNode::Offset {
                input: self.build_physical_plan(
                    logical_nodes,
                    input,
                    allow_secondary_index_scans,
                    physical_nodes,
                )?,
                offset,
            },
            LogicalPlanNode::Limit { input, limit } => PhysicalPlanNode::Limit {
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
        predicate: &PlannedExpression,
    ) -> PlannerResult<Option<IndexPredicate>> {
        let indexes = self.catalog.index_schemas_for_table(table)?;
        Ok(secondary_index_predicate(table, predicate, &indexes))
    }
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
