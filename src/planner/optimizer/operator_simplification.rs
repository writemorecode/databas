//! Simplification of redundant and adjacent logical operators.

use crate::sql_parser::parser::op::Op;

use super::super::{
    BoundExpr, LogicalPlan, LogicalPlanNode, NodeId, PlanSchema, PlannerError, PlannerResult,
};

/// Removes redundant projections and combines adjacent compatible operators.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    OperatorSimplification::new(logical).optimize()
}

/// Rebuilds a plan while simplifying adjacent operators.
struct OperatorSimplification {
    /// Original plan nodes being traversed.
    input: Vec<LogicalPlanNode>,
    /// Root node of the original plan.
    input_root: NodeId,
    /// Simplified nodes built so far.
    output: Vec<LogicalPlanNode>,
}

impl OperatorSimplification {
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

    /// Recursively rebuilds and simplifies a node and its inputs.
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
            LogicalPlanNode::Filter { input, predicate, output } => {
                let input = self.rebuild(input)?;
                self.combine_filter(input, predicate, output)?
            }
            LogicalPlanNode::Sort { input, terms, output } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Sort { input, terms, output })
            }
            LogicalPlanNode::Project { input, expressions, output } => {
                let input = self.rebuild(input)?;
                self.simplify_project(input, expressions, output)?
            }
            LogicalPlanNode::Offset { input, offset: 0, .. } => self.rebuild(input)?,
            LogicalPlanNode::Offset { input, offset, output } => {
                let input = self.rebuild(input)?;
                self.combine_offset(input, offset, output)?
            }
            LogicalPlanNode::Limit { input, limit, output } => {
                let input = self.rebuild(input)?;
                self.combine_limit(input, limit, output)?
            }
            LogicalPlanNode::Join { left, right, join_type, predicate, output } => {
                let left = self.rebuild(left)?;
                let right = self.rebuild(right)?;
                self.push(LogicalPlanNode::Join { left, right, join_type, predicate, output })
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

    /// Merges a filter with an adjacent filter when possible.
    fn combine_filter(
        &mut self,
        input: NodeId,
        predicate: BoundExpr,
        output: PlanSchema,
    ) -> PlannerResult<NodeId> {
        let input_node = self.node(input)?.clone();
        if let LogicalPlanNode::Filter { input: filter_input, predicate: inner_predicate, .. } =
            input_node
        {
            self.output[input.index()] = LogicalPlanNode::Filter {
                input: filter_input,
                predicate: BoundExpr::Binary {
                    left: Box::new(inner_predicate),
                    op: Op::And,
                    right: Box::new(predicate),
                },
                output,
            };
            Ok(input)
        } else {
            Ok(self.push(LogicalPlanNode::Filter { input, predicate, output }))
        }
    }

    /// Removes identity projections and combines adjacent projections.
    fn simplify_project(
        &mut self,
        input: NodeId,
        expressions: Vec<BoundExpr>,
        output: PlanSchema,
    ) -> PlannerResult<NodeId> {
        let input_schema =
            self.node(input)?.output_schema().ok_or(PlannerError::InvalidLogicalPlan)?.clone();
        if is_identity_projection(&expressions, &input_schema, &output) {
            return Ok(input);
        }

        let input_node = self.node(input)?.clone();
        if let LogicalPlanNode::Project {
            input: project_input,
            expressions: input_expressions,
            output: input_output,
        } = input_node
            && input_expressions.iter().all(|expression| matches!(expression, BoundExpr::Column(_)))
            && let Some(expressions) =
                substitute_projection(&expressions, &input_output, &input_expressions)
        {
            self.output[input.index()] =
                LogicalPlanNode::Project { input: project_input, expressions, output };
            return Ok(input);
        }

        Ok(self.push(LogicalPlanNode::Project { input, expressions, output }))
    }

    /// Combines adjacent offsets when their sum is representable.
    fn combine_offset(
        &mut self,
        input: NodeId,
        offset: u32,
        output: PlanSchema,
    ) -> PlannerResult<NodeId> {
        let input_node = self.node(input)?.clone();
        if let LogicalPlanNode::Offset { input: offset_input, offset: inner_offset, .. } =
            input_node
            && let Some(offset) = inner_offset.checked_add(offset)
        {
            self.output[input.index()] =
                LogicalPlanNode::Offset { input: offset_input, offset, output };
            Ok(input)
        } else {
            Ok(self.push(LogicalPlanNode::Offset { input, offset, output }))
        }
    }

    /// Combines adjacent limits by retaining the smaller limit.
    fn combine_limit(
        &mut self,
        input: NodeId,
        limit: u32,
        output: PlanSchema,
    ) -> PlannerResult<NodeId> {
        let input_node = self.node(input)?.clone();
        if let LogicalPlanNode::Limit { input: limit_input, limit: inner_limit, .. } = input_node {
            self.output[input.index()] = LogicalPlanNode::Limit {
                input: limit_input,
                limit: inner_limit.min(limit),
                output,
            };
            Ok(input)
        } else {
            Ok(self.push(LogicalPlanNode::Limit { input, limit, output }))
        }
    }

    /// Looks up a rebuilt node, returning an error for an invalid identifier.
    fn node(&self, id: NodeId) -> PlannerResult<&LogicalPlanNode> {
        self.output.get(id.index()).ok_or(PlannerError::InvalidLogicalPlan)
    }

    /// Appends a rebuilt node and returns its arena identifier.
    fn push(&mut self, node: LogicalPlanNode) -> NodeId {
        let id = NodeId::new(self.output.len());
        self.output.push(node);
        id
    }
}

/// Reports whether a projection returns every input column unchanged and in order.
fn is_identity_projection(
    expressions: &[BoundExpr],
    input: &PlanSchema,
    output: &PlanSchema,
) -> bool {
    input == output
        && expressions.len() == input.columns.len()
        && expressions.iter().zip(&input.columns).all(|(expression, column)| {
            matches!(
                (expression, &column.source),
                (BoundExpr::Column(expression_column), Some(source)) if expression_column == source
            )
        })
}

/// Rewrites expressions to reference the inputs of a preceding projection.
fn substitute_projection(
    expressions: &[BoundExpr],
    input_schema: &PlanSchema,
    input_expressions: &[BoundExpr],
) -> Option<Vec<BoundExpr>> {
    expressions
        .iter()
        .cloned()
        .map(|expression| substitute_expression(expression, input_schema, input_expressions))
        .collect()
}

/// Rewrites one expression through the column mapping of a projection.
fn substitute_expression(
    expression: BoundExpr,
    input_schema: &PlanSchema,
    input_expressions: &[BoundExpr],
) -> Option<BoundExpr> {
    match expression {
        BoundExpr::Literal(_) => Some(expression),
        BoundExpr::Column(column) => {
            input_expressions.get(input_schema.slot_for(&column)?).cloned()
        }
        BoundExpr::Unary { op, expr } => Some(BoundExpr::Unary {
            op,
            expr: Box::new(substitute_expression(*expr, input_schema, input_expressions)?),
        }),
        BoundExpr::Binary { left, op, right } => Some(BoundExpr::Binary {
            left: Box::new(substitute_expression(*left, input_schema, input_expressions)?),
            op,
            right: Box::new(substitute_expression(*right, input_schema, input_expressions)?),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::DataType,
        planner::{BoundColumn, PlanColumn, RelationId},
    };

    use super::*;

    fn column() -> BoundColumn {
        BoundColumn {
            relation: RelationId::new(0),
            table: "items".into(),
            name: "id".into(),
            ordinal: 0,
            data_type: DataType::Integer,
        }
    }

    fn schema() -> PlanSchema {
        let column = column();
        PlanSchema {
            columns: vec![PlanColumn {
                name: column.name.clone(),
                data_type: Some(column.data_type),
                source: Some(column),
            }],
        }
    }

    #[test]
    fn removes_identity_projection() {
        let schema = schema();
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        plan.push(LogicalPlanNode::Project {
            input,
            expressions: vec![BoundExpr::Column(column())],
            output: schema.clone(),
        });

        let optimized = optimize(plan).unwrap();

        assert_eq!(optimized.iter().count(), 1);
        assert_eq!(optimized.root(), &LogicalPlanNode::OneRow { output: schema });
    }

    #[test]
    fn combines_adjacent_limits() {
        let schema = PlanSchema::default();
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        let inner = plan.push(LogicalPlanNode::Limit { input, limit: 10, output: schema.clone() });
        plan.push(LogicalPlanNode::Limit { input: inner, limit: 20, output: schema.clone() });

        let optimized = optimize(plan).unwrap();

        assert_eq!(optimized.iter().count(), 2);
        assert_eq!(
            optimized.root(),
            &LogicalPlanNode::Limit { input: NodeId::new(0), limit: 10, output: schema }
        );
    }

    #[test]
    fn combines_adjacent_filters_in_evaluation_order() {
        let schema = PlanSchema::default();
        let inner_predicate = BoundExpr::Literal(crate::core::Value::Boolean(true));
        let outer_predicate = BoundExpr::Literal(crate::core::Value::Boolean(false));
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        let inner = plan.push(LogicalPlanNode::Filter {
            input,
            predicate: inner_predicate.clone(),
            output: schema.clone(),
        });
        plan.push(LogicalPlanNode::Filter {
            input: inner,
            predicate: outer_predicate.clone(),
            output: schema.clone(),
        });

        let optimized = optimize(plan).unwrap();

        assert_eq!(
            optimized.root(),
            &LogicalPlanNode::Filter {
                input: NodeId::new(0),
                predicate: BoundExpr::Binary {
                    left: Box::new(inner_predicate),
                    op: Op::And,
                    right: Box::new(outer_predicate),
                },
                output: schema,
            }
        );
    }
}
