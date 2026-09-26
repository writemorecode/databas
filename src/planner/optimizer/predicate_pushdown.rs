//! Predicate pushdown for inner joins.

use crate::{core::Value, sql_parser::parser::op::Op};

use super::super::{
    BoundColumn, BoundExpr, LogicalPlan, LogicalPlanNode, NodeId, PlanSchema, PlannerError,
    PlannerResult,
};

/// Pushes predicates as close as possible to the inputs of inner joins.
///
/// Conjuncts which reference only one join input become filters on that input.
/// Conjuncts which need both inputs are evaluated as part of the join predicate.
/// The latter rewrite is valid because the parser currently exposes inner joins
/// only.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    PredicatePushdown::new(logical).optimize()
}

struct PredicatePushdown {
    input: Vec<LogicalPlanNode>,
    input_root: NodeId,
    output: Vec<LogicalPlanNode>,
}

impl PredicatePushdown {
    fn new(logical: LogicalPlan) -> Self {
        let (input, input_root) = logical.into_parts();
        Self { input, input_root, output: Vec::new() }
    }

    fn optimize(mut self) -> PlannerResult<LogicalPlan> {
        let root = self.rebuild(self.input_root)?;
        Ok(LogicalPlan::from_parts(self.output, root))
    }

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

    fn output_schema(&self, id: NodeId) -> PlannerResult<&PlanSchema> {
        self.output
            .get(id.index())
            .and_then(LogicalPlanNode::output_schema)
            .ok_or(PlannerError::InvalidLogicalPlan)
    }

    fn push(&mut self, node: LogicalPlanNode) -> NodeId {
        let id = NodeId::new(self.output.len());
        self.output.push(node);
        id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateInput {
    Left,
    Right,
    Join,
}

fn predicate_input(predicate: &BoundExpr, left: &PlanSchema, right: &PlanSchema) -> PredicateInput {
    let mut columns = Vec::new();
    referenced_columns(predicate, &mut columns);

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

fn referenced_columns<'expression>(
    expression: &'expression BoundExpr,
    columns: &mut Vec<&'expression BoundColumn>,
) {
    match expression {
        BoundExpr::Literal(_) => {}
        BoundExpr::Column(column) => columns.push(column),
        BoundExpr::Unary { expr, .. } => referenced_columns(expr, columns),
        BoundExpr::Binary { left, right, .. } => {
            referenced_columns(left, columns);
            referenced_columns(right, columns);
        }
    }
}

fn conjuncts(expression: BoundExpr) -> Vec<BoundExpr> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expression, &mut conjuncts);
    conjuncts
}

fn flatten_conjuncts(expression: BoundExpr, conjuncts: &mut Vec<BoundExpr>) {
    match expression {
        BoundExpr::Binary { left, op: Op::And, right } => {
            flatten_conjuncts(*left, conjuncts);
            flatten_conjuncts(*right, conjuncts);
        }
        expression => conjuncts.push(expression),
    }
}

fn combine_conjuncts(conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    conjuncts.into_iter().reduce(|left, right| BoundExpr::Binary {
        left: Box::new(left),
        op: Op::And,
        right: Box::new(right),
    })
}
