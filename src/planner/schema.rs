//! Output schemas carried by logical relational operators.

use crate::core::{DataType, TableSchema};

use super::{BoundColumn, BoundExpr, RelationId};

/// Ordered columns produced by one relational operator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanSchema {
    /// Columns in output-slot order.
    pub columns: Vec<PlanColumn>,
}

impl PlanSchema {
    pub(crate) fn for_table(relation: RelationId, table: &TableSchema) -> Self {
        Self::for_qualified_table(relation, table, &table.name)
    }

    pub(crate) fn for_qualified_table(
        relation: RelationId,
        table: &TableSchema,
        qualifier: &str,
    ) -> Self {
        let columns = table
            .row
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, column)| {
                let source = BoundColumn {
                    relation,
                    table: qualifier.to_owned(),
                    name: column.name.clone(),
                    ordinal,
                    data_type: column.data_type,
                };
                PlanColumn {
                    name: column.name.clone(),
                    data_type: Some(column.data_type),
                    source: Some(source),
                }
            })
            .collect();
        Self { columns }
    }

    pub(crate) fn for_expressions(expressions: &[BoundExpr]) -> Self {
        let columns = expressions
            .iter()
            .map(|expression| PlanColumn {
                name: expression.to_string(),
                data_type: expression.data_type(),
                source: match expression {
                    BoundExpr::Column(column) => Some(column.clone()),
                    _ => None,
                },
            })
            .collect();
        Self { columns }
    }

    pub(crate) fn slot_for(&self, column: &BoundColumn) -> Option<usize> {
        self.columns.iter().position(|candidate| candidate.source.as_ref() == Some(column))
    }

    pub(crate) fn join(left: Self, right: Self) -> Self {
        let mut columns = vec![];
        columns.extend(left.columns);
        columns.extend(right.columns);
        Self { columns }
    }
}

/// Metadata for one output slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanColumn {
    /// Display name of the output expression or source column.
    pub name: String,
    /// Statically known type, if binding could determine one.
    pub data_type: Option<DataType>,
    /// Source column identity for a direct column output.
    pub source: Option<BoundColumn>,
}
