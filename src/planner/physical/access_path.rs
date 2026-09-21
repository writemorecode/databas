//! Predicate analysis and scan-range derivation for physical table access.

use crate::{
    core::{
        DataType, IndexKeyBound, IndexKeyRange, IndexSchema, TableKey, TableKeyBound,
        TableKeyRange, TableSchema, TupleRef, Value, ValueRef,
    },
    relational::cursor::encode_index_entry_key,
    sql_parser::parser::op::Op,
};

use crate::planner::{BoundColumn, BoundExpr, IndexValueBound, IndexValueRange, RelationId};

#[derive(Debug, Clone, PartialEq)]
/// A primary-key scan range and an optional predicate that must be evaluated afterward.
pub(super) struct RangePredicate {
    /// The table-key range that can be applied during the scan.
    pub(super) range: TableKeyRange,
    /// The part of the predicate that could not be represented by `range`.
    pub(super) residual: Option<BoundExpr>,
}

#[derive(Debug, Clone, PartialEq)]
/// A secondary-index scan range and the index column it constrains.
pub(super) struct IndexPredicate {
    /// The secondary index selected for the scan.
    pub(super) index: IndexSchema,
    /// The bound table column represented by the index.
    pub(super) column: BoundColumn,
    /// The range of logical values accepted by the predicate.
    pub(super) value_range: IndexValueRange,
    /// The encoded index-key range used to seek and scan the index.
    pub(super) key_range: IndexKeyRange,
}

/// Derives a table-key range from a predicate on an integer primary key.
///
/// Returns `None` when the table does not have an integer primary key in its
/// first column or when the predicate does not provide a usable range.
pub(super) fn primary_key_range_predicate(
    table: &TableSchema,
    relation: RelationId,
    predicate: &BoundExpr,
) -> Option<RangePredicate> {
    let primary_key = table.row.columns.first()?;
    if !primary_key.primary_key || primary_key.data_type != DataType::Integer {
        return None;
    }

    range_predicate_from_expression(table, relation, predicate)
}

/// Derives an index-key range from predicates that constrain an exact,
/// single-column secondary index.
///
/// All compatible comparisons for the selected indexed column are intersected
/// into the returned value and encoded-key ranges.
pub(super) fn secondary_index_predicate(
    table: &TableSchema,
    relation: RelationId,
    predicate: &BoundExpr,
    indexes: &[IndexSchema],
) -> Option<IndexPredicate> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(predicate, &mut conjuncts);

    for conjunct in &conjuncts {
        let Some(candidate) = secondary_index_comparison(table, relation, conjunct, indexes) else {
            continue;
        };
        let mut value_range = IndexValueRange::default();
        let mut key_range = IndexKeyRange::default();

        for conjunct in &conjuncts {
            if let Some(comparison) =
                index_comparison_for_column(relation, conjunct, &candidate.column)
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

/// Flattens a tree of `AND` expressions into individual conjuncts.
fn flatten_conjuncts<'a>(expression: &'a BoundExpr, conjuncts: &mut Vec<&'a BoundExpr>) {
    match expression {
        BoundExpr::Binary { left, op: Op::And, right } => {
            flatten_conjuncts(left, conjuncts);
            flatten_conjuncts(right, conjuncts);
        }
        _ => conjuncts.push(expression),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The comparison operators supported when deriving an index range.
enum IndexComparisonKind {
    /// Matches values equal to the comparison value.
    Equals,
    /// Matches values greater than the comparison value.
    GreaterThan,
    /// Matches values greater than or equal to the comparison value.
    GreaterThanOrEqual,
    /// Matches values less than the comparison value.
    LessThan,
    /// Matches values less than or equal to the comparison value.
    LessThanOrEqual,
}

/// A normalized comparison between an indexed column and a literal value.
struct IndexComparison<'a> {
    /// The indexed column being compared.
    column: &'a BoundColumn,
    /// The literal value used by the comparison.
    value: &'a Value,
    /// The normalized comparison operator.
    kind: IndexComparisonKind,
}

/// An index and column that can satisfy one predicate conjunct.
struct SecondaryIndexCandidate {
    /// The matching single-column secondary index.
    index: IndexSchema,
    /// The indexed column constrained by the conjunct.
    column: BoundColumn,
}

/// Finds an indexed column comparison suitable for secondary-index access.
fn secondary_index_comparison(
    table: &TableSchema,
    relation: RelationId,
    comparison: &BoundExpr,
    indexes: &[IndexSchema],
) -> Option<SecondaryIndexCandidate> {
    let comparison = index_comparison(relation, comparison)?;
    let index =
        indexes.iter().find(|index| exact_single_column_index(index, table, comparison.column))?;
    Some(SecondaryIndexCandidate { index: index.clone(), column: comparison.column.clone() })
}

/// Extracts a comparison when it targets the requested bound column.
fn index_comparison_for_column<'a>(
    relation: RelationId,
    comparison: &'a BoundExpr,
    column: &BoundColumn,
) -> Option<IndexComparison<'a>> {
    let comparison = index_comparison(relation, comparison)?;
    (*comparison.column == *column).then_some(comparison)
}

/// Parses a planned expression as a column-to-literal comparison.
///
/// Comparisons written with the literal on the left are normalized so the
/// returned comparison always describes the column relative to the literal.
fn index_comparison<'a>(
    relation: RelationId,
    comparison: &'a BoundExpr,
) -> Option<IndexComparison<'a>> {
    let BoundExpr::Binary { left, op, right } = comparison else {
        return None;
    };
    index_comparison_from_operands(relation, left, *op, right)
        .or_else(|| index_comparison_from_reversed_operands(relation, left, *op, right))
}

/// Builds an index comparison from a column followed by a literal operand.
fn index_comparison_from_operands<'a>(
    relation: RelationId,
    column: &'a BoundExpr,
    op: Op,
    value: &'a BoundExpr,
) -> Option<IndexComparison<'a>> {
    let BoundExpr::Column(column) = column else {
        return None;
    };
    let BoundExpr::Literal(value) = value else {
        return None;
    };
    if column.relation != relation || !value_matches_data_type(value, column.data_type) {
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

    Some(IndexComparison { column, value, kind })
}

/// Builds an index comparison from a literal followed by a column operand.
fn index_comparison_from_reversed_operands<'a>(
    relation: RelationId,
    value: &'a BoundExpr,
    op: Op,
    column: &'a BoundExpr,
) -> Option<IndexComparison<'a>> {
    let reversed = reverse_comparison_op(op)?;
    index_comparison_from_operands(relation, column, reversed, value)
}

/// Reverses a comparison operator when its operands are swapped.
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

/// Reports whether a comparison can use the ordering of a secondary index.
///
/// Text indexes support equality lookups but not range comparisons because the
/// index ordering is not suitable for those comparisons.
fn secondary_index_comparison_is_order_compatible(
    data_type: DataType,
    kind: IndexComparisonKind,
) -> bool {
    kind == IndexComparisonKind::Equals || data_type != DataType::Text
}

/// Intersects one index comparison into the value and encoded-key ranges.
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

/// Applies a lower bound if it is tighter than the current lower bound.
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

/// Applies an upper bound if it is tighter than the current upper bound.
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

/// Encodes the first index entry key allowed by a lower value bound.
fn index_lower_key(value: &Value, inclusive: bool) -> Option<Vec<u8>> {
    let prefix = TupleRef::new(&[ValueRef::from(value)]).to_bytes().ok()?;
    let table_key = if inclusive { TableKey::MIN } else { TableKey::MAX };
    Some(encode_index_entry_key(&prefix, table_key))
}

/// Encodes the last index entry key allowed by an upper value bound.
fn index_upper_key(value: &Value, inclusive: bool) -> Option<Vec<u8>> {
    let prefix = TupleRef::new(&[ValueRef::from(value)]).to_bytes().ok()?;
    let table_key = if inclusive { TableKey::MAX } else { TableKey::MIN };
    Some(encode_index_entry_key(&prefix, table_key))
}

/// Reports whether a candidate lower key bound narrows the current bound.
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

/// Reports whether a candidate upper key bound narrows the current bound.
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

/// Reports whether an index belongs to the table and exactly covers a column.
fn exact_single_column_index(
    index: &IndexSchema,
    table: &TableSchema,
    column: &BoundColumn,
) -> bool {
    index.table_id == table.table_id
        && index.columns.len() == 1
        && index.columns[0].source_column_ordinal == column.ordinal
}

/// Reports whether a literal value has the expected column data type.
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

/// Derives a primary-key range and retains predicates that cannot be ranged.
///
/// Conjunctive expressions are intersected when both sides yield ranges;
/// otherwise, the unusable side is preserved as a residual predicate.
fn range_predicate_from_expression(
    table: &TableSchema,
    relation: RelationId,
    expression: &BoundExpr,
) -> Option<RangePredicate> {
    match expression {
        BoundExpr::Binary { left, op: Op::And, right } => {
            let left = range_predicate_from_expression(table, relation, left)?;
            if let Some(left_residual) = left.residual {
                return Some(RangePredicate {
                    range: left.range,
                    residual: Some(and_expression(left_residual, (**right).clone())),
                });
            }

            match range_predicate_from_expression(table, relation, right) {
                Some(right) => Some(RangePredicate {
                    range: combine_ranges(left.range, right.range),
                    residual: right.residual,
                }),
                None => {
                    Some(RangePredicate { range: left.range, residual: Some((**right).clone()) })
                }
            }
        }
        BoundExpr::Binary { left, op, right } => {
            range_from_comparison(table, relation, left, *op, right)
                .map(|range| RangePredicate { range, residual: None })
        }
        BoundExpr::Literal(_) | BoundExpr::Column(_) | BoundExpr::Unary { .. } => None,
    }
}

/// Combines two planned expressions into a logical `AND` expression.
fn and_expression(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    BoundExpr::Binary { left: Box::new(left), op: Op::And, right: Box::new(right) }
}

/// Converts a primary-key comparison into a table-key range.
fn range_from_comparison(
    table: &TableSchema,
    relation: RelationId,
    left: &BoundExpr,
    op: Op,
    right: &BoundExpr,
) -> Option<TableKeyRange> {
    match (left, right) {
        (BoundExpr::Column(column), BoundExpr::Literal(Value::Integer(value)))
            if is_table_primary_key(table, relation, column) =>
        {
            range_from_column_comparison(op, *value)
        }
        (BoundExpr::Literal(Value::Integer(value)), BoundExpr::Column(column))
            if is_table_primary_key(table, relation, column) =>
        {
            range_from_literal_comparison(op, *value)
        }
        _ => None,
    }
}

/// Reports whether a bound column is the table's first-column primary key.
fn is_table_primary_key(table: &TableSchema, relation: RelationId, column: &BoundColumn) -> bool {
    column.relation == relation && column.ordinal == 0 && table.row.columns[0].primary_key
}

/// Converts a comparison with the column on the left into a key range.
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

/// Converts a comparison with the literal on the left into a key range.
fn range_from_literal_comparison(op: Op, value: TableKey) -> Option<TableKeyRange> {
    range_from_column_comparison(reverse_comparison_op(op)?, value)
}

/// Intersects two table-key ranges by retaining their tightest bounds.
fn combine_ranges(left: TableKeyRange, right: TableKeyRange) -> TableKeyRange {
    TableKeyRange {
        lower: tightest_lower(left.lower, right.lower),
        upper: tightest_upper(left.upper, right.upper),
    }
}

/// Selects the tighter of two optional lower bounds.
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

/// Selects the tighter of two optional upper bounds.
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

/// Reports whether a table-key bound excludes its endpoint.
fn bound_is_exclusive(bound: TableKeyBound) -> bool {
    matches!(bound, TableKeyBound::Exclusive(_))
}
