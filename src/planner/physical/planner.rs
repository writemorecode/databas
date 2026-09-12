//! Logical-to-physical lowering and access-path selection.

use crate::core::{TableSchema, access::CatalogRead};

use super::{
    PhysicalPlan, PhysicalPlanNode, SecondaryIndexScanPlan,
    access_path::{IndexPredicate, primary_key_range_predicate, secondary_index_predicate},
};
use crate::planner::{LogicalPlan, PlannedExpression, PlannerError, PlannerResult};

/// Selects executable operators and access paths for a logical plan.
pub(in crate::planner) struct PhysicalPlanner<'catalog> {
    catalog: &'catalog dyn CatalogRead,
}

impl<'catalog> PhysicalPlanner<'catalog> {
    pub(in crate::planner) fn new(catalog: &'catalog dyn CatalogRead) -> Self {
        Self { catalog }
    }

    pub(in crate::planner) fn plan(&self, logical: LogicalPlan) -> PlannerResult<PhysicalPlan> {
        self.physical_plan(logical)
    }

    fn physical_plan(&self, logical: LogicalPlan) -> PlannerResult<PhysicalPlan> {
        let mut nodes = Vec::new();
        let root = self.build_physical_plan(logical, true, &mut nodes)?;
        Ok(PhysicalPlan::from_parts(nodes, root))
    }

    fn build_physical_plan(
        &self,
        logical: LogicalPlan,
        allow_secondary_index_scans: bool,
        nodes: &mut Vec<PhysicalPlanNode>,
    ) -> PlannerResult<usize> {
        let node = match logical {
            LogicalPlan::Explain { input } => PhysicalPlanNode::Explain {
                input: self.build_physical_plan(*input, allow_secondary_index_scans, nodes)?,
            },
            LogicalPlan::CreateTable { name, schema } => {
                PhysicalPlanNode::CreateTable { name, schema }
            }
            LogicalPlan::CreateIndex { name, table, columns } => {
                PhysicalPlanNode::CreateIndex { name, table, columns }
            }
            LogicalPlan::Values { rows } => PhysicalPlanNode::Values { rows },
            LogicalPlan::Insert { table, columns, input } => match *input {
                LogicalPlan::Values { rows } => {
                    PhysicalPlanNode::InsertValues { table, columns, values: rows }
                }
                _ => return Err(PlannerError::InvalidInsertInput),
            },
            LogicalPlan::Update { table, assignments, input } => PhysicalPlanNode::Update {
                table,
                assignments,
                input: self.build_physical_plan(*input, false, nodes)?,
            },
            LogicalPlan::Delete { table, input } => PhysicalPlanNode::Delete {
                table,
                input: self.build_physical_plan(*input, false, nodes)?,
            },
            LogicalPlan::OneRow => PhysicalPlanNode::OneRow,
            LogicalPlan::TableScan { table } => PhysicalPlanNode::FullTableScan { table },
            LogicalPlan::Filter { input, predicate } => match *input {
                LogicalPlan::TableScan { table } => {
                    match primary_key_range_predicate(&table, &predicate) {
                        Some(range_predicate) => {
                            let scan = push_physical_node(
                                nodes,
                                PhysicalPlanNode::PrimaryKeyRangeScan {
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
                            let scan = match self.secondary_index_predicate(&table, &predicate)? {
                                Some(index_predicate) => PhysicalPlanNode::SecondaryIndexScan {
                                    scan: Box::new(SecondaryIndexScanPlan {
                                        table,
                                        index: index_predicate.index,
                                        column: index_predicate.column,
                                        value_range: index_predicate.value_range,
                                        key_range: index_predicate.key_range,
                                    }),
                                },
                                None => PhysicalPlanNode::FullTableScan { table },
                            };
                            let input = push_physical_node(nodes, scan);
                            PhysicalPlanNode::Filter { input, predicate }
                        }
                        None => {
                            let input = push_physical_node(
                                nodes,
                                PhysicalPlanNode::FullTableScan { table },
                            );
                            PhysicalPlanNode::Filter { input, predicate }
                        }
                    }
                }
                input => PhysicalPlanNode::Filter {
                    input: self.build_physical_plan(input, allow_secondary_index_scans, nodes)?,
                    predicate,
                },
            },
            LogicalPlan::Sort { input, terms } => PhysicalPlanNode::Sort {
                input: self.build_physical_plan(*input, allow_secondary_index_scans, nodes)?,
                terms,
            },
            LogicalPlan::Project { input, expressions } => PhysicalPlanNode::Project {
                input: self.build_physical_plan(*input, allow_secondary_index_scans, nodes)?,
                expressions,
            },
            LogicalPlan::Offset { input, offset } => PhysicalPlanNode::Offset {
                input: self.build_physical_plan(*input, allow_secondary_index_scans, nodes)?,
                offset,
            },
            LogicalPlan::Limit { input, limit } => PhysicalPlanNode::Limit {
                input: self.build_physical_plan(*input, allow_secondary_index_scans, nodes)?,
                limit,
            },
        };
        Ok(push_physical_node(nodes, node))
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

fn push_physical_node(nodes: &mut Vec<PhysicalPlanNode>, node: PhysicalPlanNode) -> usize {
    let index = nodes.len();
    nodes.push(node);
    index
}
