//! SQL query planning.
//!
//! The planner is the boundary between parsed SQL syntax and executable
//! database work. It lowers one parsed [`Statement`] into a [`Plan`] containing:
//!
//! - a catalog-bound [`LogicalPlan`] that describes the statement's meaning; and
//! - a [`PhysicalPlan`] that selects the executor operators used to run it.
//!
//! During logical planning, table and column names are resolved against the
//! catalog, wildcard projections are expanded, duplicate target columns are
//! rejected, and scalar expressions are converted into [`PlannedExpression`]
//! trees. The resulting plan carries [`TableSchema`] and [`BoundColumn`] values
//! so later stages can work with ordinals and storage types instead of repeating
//! name lookup.
//!
//! Physical planning is intentionally small and predictable. Most relational
//! operators are translated directly, while table access can be narrowed from a
//! full scan to [`PhysicalPlan::PrimaryKeyRangeScan`] for integer primary-key
//! predicates, or to [`PhysicalPlan::SecondaryIndexScan`] for compatible
//! single-column secondary-index predicates. Predicates that cannot be fully
//! represented by an access path are retained as residual
//! [`PhysicalPlan::Filter`] operators. Mutation inputs use primary-key range or
//! full table scans so writes cannot invalidate the index that drives them.
//!
//! The planner validates statement shape, but it does not execute side effects
//! or enforce every runtime constraint. Storage, type, arithmetic, and mutation
//! errors that depend on actual row values are still reported by the executor
//! and storage layers.

mod ir;
mod planning;

pub use ir::*;
pub use planning::Planner;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use crate::{
    core::{
        ColumnSchema, DataType, Database, IndexKeyBound, IndexKeyRange, IndexSchema, TableKey,
        TableKeyBound, TableKeyRange, TableSchema, Tuple, TupleSchema, Value,
        access::SchemaAccess,
        error::{InvalidArgumentError, StorageError},
    },
    relational::cursor::encode_index_entry_key,
    sql_parser::{
        NumberKind,
        parser::{
            expr::{Expression, Literal},
            op::Op,
            stmt::{
                Statement,
                create_index::CreateIndexQuery,
                create_table::CreateTableQuery,
                delete::DeleteQuery,
                insert::InsertQuery,
                select::{Ordering, SelectQuery},
                update::UpdateQuery,
            },
        },
    },
};
