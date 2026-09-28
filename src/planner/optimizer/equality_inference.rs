//! Inference of column-to-literal predicates from inner-join equalities.

use crate::{
    core::{DataType, Value},
    sql_parser::parser::op::Op,
};

use super::{
    super::{
        BoundColumn, BoundExpr, LogicalPlan, LogicalPlanNode, NodeId, PlannerError, PlannerResult,
    },
    expression::{combine_conjuncts, conjuncts},
};

/// Propagates literal equality constraints through equality-connected columns.
///
/// For example, `left.id = right.id AND left.id = 7` also constrains
/// `right.id = 7`. The inferred conjunct is retained alongside the original
/// equalities so a following predicate-pushdown pass can place it on the right
/// input.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    let (mut nodes, root) = logical.into_parts();
    for index in 0..nodes.len() {
        let LogicalPlanNode::Join { predicate, .. } = &nodes[index] else {
            continue;
        };
        let mut available = Vec::new();
        collect_predicates(&nodes, NodeId::new(index), &mut available)?;
        let inferred = infer_equalities(&available);
        if inferred.is_empty() {
            continue;
        }

        let mut predicates = conjuncts(predicate.clone());
        predicates.extend(inferred);
        let predicate =
            combine_conjuncts(predicates).unwrap_or(BoundExpr::Literal(Value::Boolean(true)));
        match &mut nodes[index] {
            LogicalPlanNode::Join { predicate: join_predicate, .. } => {
                *join_predicate = predicate;
            }
            _ => return Err(PlannerError::InvalidLogicalPlan),
        }
    }
    Ok(LogicalPlan::from_parts(nodes, root))
}

/// Collects filter and join conjuncts reachable below a plan node.
fn collect_predicates(
    nodes: &[LogicalPlanNode],
    id: NodeId,
    predicates: &mut Vec<BoundExpr>,
) -> PlannerResult<()> {
    let node = nodes.get(id.index()).ok_or(PlannerError::InvalidLogicalPlan)?;
    match node {
        LogicalPlanNode::Filter { input, predicate, .. } => {
            predicates.extend(conjuncts(predicate.clone()));
            collect_predicates(nodes, *input, predicates)?;
        }
        LogicalPlanNode::Join { left, right, predicate, .. } => {
            predicates.extend(conjuncts(predicate.clone()));
            collect_predicates(nodes, *left, predicates)?;
            collect_predicates(nodes, *right, predicates)?;
        }
        _ => {}
    }
    Ok(())
}

/// Derives missing column-to-literal equalities from equality-connected columns.
fn infer_equalities(predicates: &[BoundExpr]) -> Vec<BoundExpr> {
    let mut classes = EquivalenceClasses::default();
    for predicate in predicates {
        let Some((left, right)) = column_equality(predicate) else {
            continue;
        };
        classes.union(left, right);
    }

    let constraints = predicates
        .iter()
        .filter_map(column_literal_equality)
        .filter(|(column, value)| value_matches_column(value, column))
        .collect::<Vec<_>>();
    for (column, _) in &constraints {
        classes.add(column);
    }

    let mut inferred = Vec::new();
    for (source, value) in constraints {
        for column in classes.equivalent_columns(source) {
            if column == source || has_constraint(predicates, column, value) {
                continue;
            }
            let predicate = BoundExpr::Binary {
                left: Box::new(BoundExpr::Column(column.clone())),
                op: Op::EqualsEquals,
                right: Box::new(BoundExpr::Literal(value.clone())),
            };
            if !inferred.contains(&predicate) {
                inferred.push(predicate);
            }
        }
    }
    inferred
}

/// Extracts same-type column equality operands from a predicate.
fn column_equality(predicate: &BoundExpr) -> Option<(&BoundColumn, &BoundColumn)> {
    let BoundExpr::Binary { left, op: Op::EqualsEquals, right } = predicate else {
        return None;
    };
    let (BoundExpr::Column(left), BoundExpr::Column(right)) = (&**left, &**right) else {
        return None;
    };
    (left.data_type == right.data_type).then_some((left, right))
}

/// Extracts a column and literal compared for equality, in either operand order.
fn column_literal_equality(predicate: &BoundExpr) -> Option<(&BoundColumn, &Value)> {
    let BoundExpr::Binary { left, op: Op::EqualsEquals, right } = predicate else {
        return None;
    };
    match (&**left, &**right) {
        (BoundExpr::Column(column), BoundExpr::Literal(value))
        | (BoundExpr::Literal(value), BoundExpr::Column(column)) => Some((column, value)),
        _ => None,
    }
}

/// Checks whether a predicate already constrains a column to the given value.
fn has_constraint(predicates: &[BoundExpr], column: &BoundColumn, value: &Value) -> bool {
    predicates.iter().any(|predicate| {
        column_literal_equality(predicate).is_some_and(|(candidate, candidate_value)| {
            candidate == column && candidate_value == value
        })
    })
}

/// Reports whether a literal's type is compatible with a bound column.
fn value_matches_column(value: &Value, column: &BoundColumn) -> bool {
    match value {
        Value::Integer(_) => column.data_type == DataType::Integer,
        Value::Float(value) => column.data_type == DataType::Float && !value.is_nan(),
        Value::String(_) => column.data_type == DataType::Text,
        Value::Boolean(_) => column.data_type == DataType::Boolean,
        Value::UnsignedInteger(_) => column.data_type == DataType::UnsignedInteger,
        // NULL equality has non-standard runtime behavior in the current
        // executor and is therefore not used for inferred predicates.
        Value::Null => false,
    }
}

/// Disjoint sets of columns connected by equality predicates.
#[derive(Default)]
struct EquivalenceClasses {
    /// Columns represented by this disjoint-set forest.
    columns: Vec<BoundColumn>,
    /// Parent index for each column; roots point to themselves.
    parents: Vec<usize>,
}

impl EquivalenceClasses {
    /// Adds a column if absent and returns its set index.
    fn add(&mut self, column: &BoundColumn) -> usize {
        if let Some(index) = self.columns.iter().position(|candidate| candidate == column) {
            return index;
        }
        let index = self.columns.len();
        self.columns.push(column.clone());
        self.parents.push(index);
        index
    }

    /// Merges the sets containing two columns.
    fn union(&mut self, left: &BoundColumn, right: &BoundColumn) {
        let left = self.add(left);
        let right = self.add(right);
        let left_root = self.root(left);
        let right_root = self.root(right);
        if left_root != right_root {
            self.parents[right_root] = left_root;
        }
    }

    /// Iterates over columns in the same equality class as `column`.
    fn equivalent_columns(&self, column: &BoundColumn) -> impl Iterator<Item = &BoundColumn> {
        let index = self.columns.iter().position(|candidate| candidate == column);
        let root = index.map(|index| self.root(index));
        self.columns.iter().enumerate().filter_map(move |(index, column)| {
            (root.is_some() && Some(self.root(index)) == root).then_some(column)
        })
    }

    /// Finds the representative index for a set member.
    fn root(&self, mut index: usize) -> usize {
        while self.parents[index] != index {
            index = self.parents[index];
        }
        index
    }
}

#[cfg(test)]
mod tests {
    use crate::planner::RelationId;

    use super::*;

    fn column(relation: u32, name: &str) -> BoundColumn {
        BoundColumn {
            relation: RelationId::new(relation),
            table: format!("table_{relation}"),
            name: name.into(),
            ordinal: 0,
            data_type: DataType::Integer,
        }
    }

    fn equals(left: BoundExpr, right: BoundExpr) -> BoundExpr {
        BoundExpr::Binary { left: Box::new(left), op: Op::EqualsEquals, right: Box::new(right) }
    }

    #[test]
    fn propagates_literals_across_column_equalities() {
        let left = column(0, "id");
        let middle = column(1, "left_id");
        let right = column(2, "middle_id");
        let predicates = vec![
            equals(BoundExpr::Column(left.clone()), BoundExpr::Column(middle.clone())),
            equals(BoundExpr::Column(middle.clone()), BoundExpr::Column(right.clone())),
            equals(BoundExpr::Column(left), BoundExpr::Literal(Value::Integer(7))),
        ];

        let inferred = infer_equalities(&predicates);

        assert_eq!(inferred.len(), 2);
        assert!(
            inferred.contains(&equals(
                BoundExpr::Column(middle),
                BoundExpr::Literal(Value::Integer(7)),
            ))
        );
        assert!(
            inferred
                .contains(
                    &equals(BoundExpr::Column(right), BoundExpr::Literal(Value::Integer(7)),)
                )
        );
    }

    #[test]
    fn does_not_duplicate_existing_constraints() {
        let left = column(0, "id");
        let right = column(1, "left_id");
        let predicates = vec![
            equals(BoundExpr::Column(left.clone()), BoundExpr::Column(right.clone())),
            equals(BoundExpr::Column(left), BoundExpr::Literal(Value::Integer(7))),
            equals(BoundExpr::Literal(Value::Integer(7)), BoundExpr::Column(right)),
        ];

        assert!(infer_equalities(&predicates).is_empty());
    }

    #[test]
    fn ignores_mismatched_and_null_literals() {
        let left = column(0, "id");
        let right = column(1, "left_id");
        let equality = equals(BoundExpr::Column(left.clone()), BoundExpr::Column(right.clone()));

        for value in [Value::String("7".into()), Value::Null] {
            let predicates = vec![
                equality.clone(),
                equals(BoundExpr::Column(left.clone()), BoundExpr::Literal(value)),
            ];
            assert!(infer_equalities(&predicates).is_empty());
        }
    }
}
