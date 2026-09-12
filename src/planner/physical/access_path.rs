//! Predicate analysis and scan-range derivation for physical table access.

use crate::{
    core::{
        DataType, IndexKeyBound, IndexKeyRange, IndexSchema, TableKey, TableKeyBound,
        TableKeyRange, TableSchema, Tuple, Value,
    },
    relational::cursor::encode_index_entry_key,
    sql_parser::parser::op::Op,
};

use crate::planner::{BoundColumn, IndexValueBound, IndexValueRange, PlannedExpression};

#[derive(Debug, Clone, PartialEq)]
pub(super) struct RangePredicate {
    pub(super) range: TableKeyRange,
    pub(super) residual: Option<PlannedExpression>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct IndexPredicate {
    pub(super) index: IndexSchema,
    pub(super) column: BoundColumn,
    pub(super) value_range: IndexValueRange,
    pub(super) key_range: IndexKeyRange,
}

pub(super) fn primary_key_range_predicate(
    table: &TableSchema,
    predicate: &PlannedExpression,
) -> Option<RangePredicate> {
    let primary_key = table.row.columns.first()?;
    if !primary_key.primary_key || primary_key.data_type != DataType::Integer {
        return None;
    }

    range_predicate_from_expression(table, predicate)
}

pub(super) fn secondary_index_predicate(
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
