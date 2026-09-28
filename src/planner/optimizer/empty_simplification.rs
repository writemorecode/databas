//! Elimination of constant-false and otherwise empty relational subplans.

use crate::core::Value;

use super::super::{
    BoundExpr, LogicalPlan, LogicalPlanNode, NodeId, PlanSchema, PlannerError, PlannerResult,
};

/// Replaces relational subplans known to produce no rows with `Empty` nodes.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    EmptySimplification::new(logical).optimize()
}

/// Rebuilds a logical plan while eliminating branches with no possible rows.
struct EmptySimplification {
    /// Original plan nodes being traversed.
    input: Vec<LogicalPlanNode>,
    /// Root node of the original plan.
    input_root: NodeId,
    /// Rebuilt plan nodes.
    output: Vec<LogicalPlanNode>,
}

impl EmptySimplification {
    /// Creates a simplifier from an owned logical plan.
    fn new(logical: LogicalPlan) -> Self {
        let (input, input_root) = logical.into_parts();
        Self { input, input_root, output: Vec::new() }
    }

    /// Rebuilds the plan from its root and returns the simplified plan.
    fn optimize(mut self) -> PlannerResult<LogicalPlan> {
        let root = self.rebuild(self.input_root)?;
        Ok(LogicalPlan::from_parts(self.output, root))
    }

    /// Recursively rebuilds a node, replacing any empty-producing subtree.
    fn rebuild(&mut self, id: NodeId) -> PlannerResult<NodeId> {
        let node = self.input.get(id.index()).cloned().ok_or(PlannerError::InvalidLogicalPlan)?;
        let rebuilt = match node {
            LogicalPlanNode::Explain { input } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Explain { input })
            }
            LogicalPlanNode::Insert { table, columns, input } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Insert { table, columns, input })
            }
            LogicalPlanNode::Update { relation, table, assignments, input } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Update { relation, table, assignments, input })
            }
            LogicalPlanNode::Delete { relation, table, input } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Delete { relation, table, input })
            }
            LogicalPlanNode::Filter { input, predicate, output } => match predicate {
                BoundExpr::Literal(Value::Boolean(false)) => self.empty(output),
                BoundExpr::Literal(Value::Boolean(true)) => self.rebuild(input)?,
                predicate => {
                    let input = self.rebuild(input)?;
                    if self.is_empty(input)? {
                        self.empty(output)
                    } else {
                        self.push(LogicalPlanNode::Filter { input, predicate, output })
                    }
                }
            },
            LogicalPlanNode::Sort { input, terms, output } => {
                let input = self.rebuild(input)?;
                if self.is_empty(input)? {
                    self.empty(output)
                } else {
                    self.push(LogicalPlanNode::Sort { input, terms, output })
                }
            }
            LogicalPlanNode::Project { input, expressions, output } => {
                let input = self.rebuild(input)?;
                if self.is_empty(input)? {
                    self.empty(output)
                } else {
                    self.push(LogicalPlanNode::Project { input, expressions, output })
                }
            }
            LogicalPlanNode::Offset { input, offset, output } => {
                let input = self.rebuild(input)?;
                if self.is_empty(input)? {
                    self.empty(output)
                } else {
                    self.push(LogicalPlanNode::Offset { input, offset, output })
                }
            }
            LogicalPlanNode::Limit { input: _, limit: 0, output } => self.empty(output),
            LogicalPlanNode::Limit { input, limit, output } => {
                let input = self.rebuild(input)?;
                if self.is_empty(input)? {
                    self.empty(output)
                } else {
                    self.push(LogicalPlanNode::Limit { input, limit, output })
                }
            }
            LogicalPlanNode::Join {
                left: _,
                right: _,
                join_type: _,
                predicate: BoundExpr::Literal(Value::Boolean(false)),
                output,
            } => self.empty(output),
            LogicalPlanNode::Join { left, right, join_type, predicate, output } => {
                let left = self.rebuild(left)?;
                if self.is_empty(left)? {
                    self.empty(output)
                } else {
                    let right = self.rebuild(right)?;
                    if self.is_empty(right)? {
                        self.empty(output)
                    } else {
                        self.push(LogicalPlanNode::Join {
                            left,
                            right,
                            join_type,
                            predicate,
                            output,
                        })
                    }
                }
            }
            leaf @ (LogicalPlanNode::CreateTable { .. }
            | LogicalPlanNode::CreateIndex { .. }
            | LogicalPlanNode::Values { .. }
            | LogicalPlanNode::OneRow { .. }
            | LogicalPlanNode::Empty { .. }
            | LogicalPlanNode::TableScan { .. }) => self.push(leaf),
        };
        Ok(rebuilt)
    }

    /// Reports whether a rebuilt node is the empty-row source.
    fn is_empty(&self, id: NodeId) -> PlannerResult<bool> {
        self.output
            .get(id.index())
            .map(|node| matches!(node, LogicalPlanNode::Empty { .. }))
            .ok_or(PlannerError::InvalidLogicalPlan)
    }

    /// Appends an empty node with the requested output schema.
    fn empty(&mut self, output: PlanSchema) -> NodeId {
        self.push(LogicalPlanNode::Empty { output })
    }

    /// Appends a rebuilt node and returns its arena identifier.
    fn push(&mut self, node: LogicalPlanNode) -> NodeId {
        let id = NodeId::new(self.output.len());
        self.output.push(node);
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_false_filter_discards_its_input() {
        let schema = PlanSchema::default();
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        plan.push(LogicalPlanNode::Filter {
            input,
            predicate: BoundExpr::Literal(Value::Boolean(false)),
            output: schema.clone(),
        });

        let optimized = optimize(plan).unwrap();

        assert_eq!(optimized.iter().count(), 1);
        assert_eq!(optimized.root(), &LogicalPlanNode::Empty { output: schema });
    }

    #[test]
    fn limit_zero_discards_its_input() {
        let schema = PlanSchema::default();
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        plan.push(LogicalPlanNode::Limit { input, limit: 0, output: schema.clone() });

        let optimized = optimize(plan).unwrap();

        assert_eq!(optimized.iter().count(), 1);
        assert_eq!(optimized.root(), &LogicalPlanNode::Empty { output: schema });
    }

    #[test]
    fn empty_input_propagates_through_projection() {
        let input_schema = PlanSchema::default();
        let output_schema = PlanSchema::for_expressions(&[BoundExpr::Literal(Value::Integer(1))]);
        let mut plan = LogicalPlan::new(LogicalPlanNode::Empty { output: input_schema });
        let input = plan.root_id();
        plan.push(LogicalPlanNode::Project {
            input,
            expressions: vec![BoundExpr::Literal(Value::Integer(1))],
            output: output_schema.clone(),
        });

        let optimized = optimize(plan).unwrap();

        assert_eq!(optimized.root(), &LogicalPlanNode::Empty { output: output_schema });
    }
}
