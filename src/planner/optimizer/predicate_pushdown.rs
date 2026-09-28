//! Predicate pushdown through relational operators.

use crate::core::Value;

use super::{
    super::{
        BoundExpr, LogicalPlan, LogicalPlanNode, NodeId, PlanSchema, PlannerError, PlannerResult,
    },
    expression::{combine_conjuncts, conjuncts, referenced_columns},
};

/// Pushes predicates through sorts and direct-column projections, and toward
/// the individual inputs of inner joins.
///
/// Conjuncts which reference only one join input become filters on that input.
/// Conjuncts which need both inputs are evaluated as part of the join predicate.
/// The latter rewrite is valid because the parser currently exposes inner joins
/// only.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    PredicatePushdown::new(logical).optimize()
}

/// Rebuilds a plan while moving predicates closer to their data sources.
struct PredicatePushdown {
    /// Original plan nodes being traversed.
    input: Vec<LogicalPlanNode>,
    /// Root node of the original plan.
    input_root: NodeId,
    /// Rebuilt plan nodes.
    output: Vec<LogicalPlanNode>,
}

impl PredicatePushdown {
    /// Creates a pushdown pass from an owned logical plan.
    fn new(logical: LogicalPlan) -> Self {
        let (input, input_root) = logical.into_parts();
        Self { input, input_root, output: Vec::new() }
    }

    /// Rebuilds the plan while pushing predicates toward scans and join inputs.
    fn optimize(mut self) -> PlannerResult<LogicalPlan> {
        let root = self.rebuild(self.input_root)?;
        Ok(LogicalPlan::from_parts(self.output, root))
    }

    /// Recursively rebuilds a node and distributes predicates where possible.
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
            LogicalPlanNode::Filter { input, predicate, .. } => {
                let input = self.rebuild(input)?;
                return self.push_predicates(input, conjuncts(predicate));
            }
            LogicalPlanNode::Sort { input, terms, output } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Sort { input, terms, output })
            }
            LogicalPlanNode::Project { input, expressions, output } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Project { input, expressions, output })
            }
            LogicalPlanNode::Offset { input, offset, output } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Offset { input, offset, output })
            }
            LogicalPlanNode::Limit { input, limit, output } => {
                let input = self.rebuild(input)?;
                self.push(LogicalPlanNode::Limit { input, limit, output })
            }
            LogicalPlanNode::Join { left, right, join_type, predicate, output } => {
                let left = self.rebuild(left)?;
                let right = self.rebuild(right)?;
                let (left_predicates, right_predicates, join_predicates) =
                    self.partition_join_predicates(left, right, conjuncts(predicate))?;
                let left = self.push_predicates(left, left_predicates)?;
                let right = self.push_predicates(right, right_predicates)?;
                let predicate = combine_conjuncts(join_predicates)
                    .unwrap_or(BoundExpr::Literal(Value::Boolean(true)));
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

    /// Pushes conjuncts through supported operators or adds a filter at this node.
    fn push_predicates(
        &mut self,
        input: NodeId,
        predicates: Vec<BoundExpr>,
    ) -> PlannerResult<NodeId> {
        if predicates.is_empty() {
            return Ok(input);
        }

        let input_node =
            self.output.get(input.index()).cloned().ok_or(PlannerError::InvalidLogicalPlan)?;
        match input_node {
            LogicalPlanNode::Sort { input: sort_input, terms, output } => {
                let sort_input = self.push_predicates(sort_input, predicates)?;
                self.output[input.index()] =
                    LogicalPlanNode::Sort { input: sort_input, terms, output };
                Ok(input)
            }
            LogicalPlanNode::Project { input: project_input, expressions, output } => {
                self.push_through_project(input, project_input, expressions, output, predicates)
            }
            LogicalPlanNode::Join { left, right, join_type, predicate, output } => {
                let mut all_join_predicates = conjuncts(predicate);
                let (left_predicates, right_predicates, join_predicates) =
                    self.partition_join_predicates(left, right, predicates)?;
                all_join_predicates.extend(join_predicates);

                let left = self.push_predicates(left, left_predicates)?;
                let right = self.push_predicates(right, right_predicates)?;
                let predicate = combine_conjuncts(all_join_predicates)
                    .unwrap_or(BoundExpr::Literal(Value::Boolean(true)));
                self.output[input.index()] =
                    LogicalPlanNode::Join { left, right, join_type, predicate, output };
                Ok(input)
            }
            LogicalPlanNode::Filter { input: filter_input, predicate, output } => {
                let mut combined = conjuncts(predicate);
                combined.extend(predicates);
                self.output[input.index()] = LogicalPlanNode::Filter {
                    input: filter_input,
                    predicate: combine_conjuncts(combined)
                        .unwrap_or(BoundExpr::Literal(Value::Boolean(true))),
                    output,
                };
                Ok(input)
            }
            _ => {
                let output = self.output_schema(input)?.clone();
                Ok(self.push(LogicalPlanNode::Filter {
                    input,
                    predicate: combine_conjuncts(predicates)
                        .unwrap_or(BoundExpr::Literal(Value::Boolean(true))),
                    output,
                }))
            }
        }
    }

    /// Rewrites pushable predicates through a projection and retains residuals above it.
    fn push_through_project(
        &mut self,
        project: NodeId,
        project_input: NodeId,
        expressions: Vec<BoundExpr>,
        output: PlanSchema,
        predicates: Vec<BoundExpr>,
    ) -> PlannerResult<NodeId> {
        let mut pushed = Vec::new();
        let mut residual = Vec::new();
        for predicate in predicates {
            match rewrite_through_project(predicate.clone(), &output, &expressions) {
                Some(predicate) => pushed.push(predicate),
                None => residual.push(predicate),
            }
        }

        let project_input = self.push_predicates(project_input, pushed)?;
        self.output[project.index()] =
            LogicalPlanNode::Project { input: project_input, expressions, output: output.clone() };
        if residual.is_empty() {
            Ok(project)
        } else {
            Ok(self.push(LogicalPlanNode::Filter {
                input: project,
                predicate: combine_conjuncts(residual)
                    .unwrap_or(BoundExpr::Literal(Value::Boolean(true))),
                output,
            }))
        }
    }

    /// Assigns predicates to the left input, right input, or join condition.
    fn partition_join_predicates(
        &self,
        left: NodeId,
        right: NodeId,
        predicates: Vec<BoundExpr>,
    ) -> PlannerResult<(Vec<BoundExpr>, Vec<BoundExpr>, Vec<BoundExpr>)> {
        let left_schema = self.output_schema(left)?;
        let right_schema = self.output_schema(right)?;
        let mut left_predicates = Vec::new();
        let mut right_predicates = Vec::new();
        let mut join_predicates = Vec::new();

        for predicate in predicates {
            match predicate_input(&predicate, left_schema, right_schema) {
                PredicateInput::Left => left_predicates.push(predicate),
                PredicateInput::Right => right_predicates.push(predicate),
                PredicateInput::Join => join_predicates.push(predicate),
            }
        }

        Ok((left_predicates, right_predicates, join_predicates))
    }

    /// Gets the output schema for a rebuilt node.
    fn output_schema(&self, id: NodeId) -> PlannerResult<&PlanSchema> {
        self.output
            .get(id.index())
            .and_then(LogicalPlanNode::output_schema)
            .ok_or(PlannerError::InvalidLogicalPlan)
    }

    /// Appends a rebuilt node and returns its arena identifier.
    fn push(&mut self, node: LogicalPlanNode) -> NodeId {
        let id = NodeId::new(self.output.len());
        self.output.push(node);
        id
    }
}

/// Rewrites references to projected columns as references to their source expressions.
fn rewrite_through_project(
    expression: BoundExpr,
    output: &PlanSchema,
    projections: &[BoundExpr],
) -> Option<BoundExpr> {
    match expression {
        BoundExpr::Literal(_) => Some(expression),
        BoundExpr::Column(column) => {
            let BoundExpr::Column(source) = projections.get(output.slot_for(&column)?)? else {
                return None;
            };
            Some(BoundExpr::Column(source.clone()))
        }
        BoundExpr::Unary { op, expr } => Some(BoundExpr::Unary {
            op,
            expr: Box::new(rewrite_through_project(*expr, output, projections)?),
        }),
        BoundExpr::Binary { left, op, right } => Some(BoundExpr::Binary {
            left: Box::new(rewrite_through_project(*left, output, projections)?),
            op,
            right: Box::new(rewrite_through_project(*right, output, projections)?),
        }),
    }
}

/// Destination selected for a predicate around a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateInput {
    /// Predicate references only columns from the left input.
    Left,
    /// Predicate references only columns from the right input.
    Right,
    /// Predicate references both inputs or is a constant.
    Join,
}

/// Determines which join inputs are required to evaluate a predicate.
fn predicate_input(predicate: &BoundExpr, left: &PlanSchema, right: &PlanSchema) -> PredicateInput {
    let columns = referenced_columns(predicate);

    // Keep constants at the join. Choosing either side would be arbitrary and
    // would make a constant-false predicate depend on that input's cardinality.
    if columns.is_empty() {
        return PredicateInput::Join;
    }
    if columns.iter().all(|column| left.slot_for(column).is_some()) {
        PredicateInput::Left
    } else if columns.iter().all(|column| right.slot_for(column).is_some()) {
        PredicateInput::Right
    } else {
        PredicateInput::Join
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::{DataType, Value},
        planner::{BoundColumn, PlanColumn, RelationId},
        sql_parser::parser::op::Op,
    };

    use super::*;

    fn column(name: &str, ordinal: usize) -> BoundColumn {
        BoundColumn {
            relation: RelationId::new(0),
            table: "items".into(),
            name: name.into(),
            ordinal,
            data_type: DataType::Integer,
        }
    }

    fn schema(columns: &[BoundColumn]) -> PlanSchema {
        PlanSchema {
            columns: columns
                .iter()
                .map(|column| PlanColumn {
                    name: column.name.clone(),
                    data_type: Some(column.data_type),
                    source: Some(column.clone()),
                })
                .collect(),
        }
    }

    fn equals(column: BoundColumn, value: i32) -> BoundExpr {
        BoundExpr::Binary {
            left: Box::new(BoundExpr::Column(column)),
            op: Op::EqualsEquals,
            right: Box::new(BoundExpr::Literal(Value::Integer(value))),
        }
    }

    #[test]
    fn pushes_filters_below_sorts() {
        let id = column("id", 0);
        let schema = schema(std::slice::from_ref(&id));
        let predicate = equals(id, 7);
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: schema.clone() });
        let input = plan.root_id();
        let sort =
            plan.push(LogicalPlanNode::Sort { input, terms: Vec::new(), output: schema.clone() });
        plan.push(LogicalPlanNode::Filter {
            input: sort,
            predicate: predicate.clone(),
            output: schema.clone(),
        });

        let optimized = optimize(plan).unwrap();
        let LogicalPlanNode::Sort { input, .. } = optimized.root() else {
            panic!("expected sort root");
        };

        assert_eq!(
            optimized.node(*input),
            &LogicalPlanNode::Filter { input: NodeId::new(0), predicate, output: schema }
        );
    }

    #[test]
    fn pushes_filters_through_direct_column_projections() {
        let id = column("id", 0);
        let unused = column("unused", 1);
        let input_schema = schema(&[id.clone(), unused]);
        let output_schema = schema(std::slice::from_ref(&id));
        let predicate = equals(id.clone(), 7);
        let mut plan = LogicalPlan::new(LogicalPlanNode::OneRow { output: input_schema.clone() });
        let input = plan.root_id();
        let project = plan.push(LogicalPlanNode::Project {
            input,
            expressions: vec![BoundExpr::Column(id)],
            output: output_schema.clone(),
        });
        plan.push(LogicalPlanNode::Filter {
            input: project,
            predicate: predicate.clone(),
            output: output_schema.clone(),
        });

        let optimized = optimize(plan).unwrap();
        let LogicalPlanNode::Project { input, .. } = optimized.root() else {
            panic!("expected project root");
        };

        assert_eq!(
            optimized.node(*input),
            &LogicalPlanNode::Filter { input: NodeId::new(0), predicate, output: input_schema }
        );
    }
}
