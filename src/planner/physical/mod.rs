//! Executable plan representation and physical operator selection.

use std::fmt;

use crate::core::{IndexKeyRange, IndexSchema, TableKeyRange, TableSchema, TupleSchema, Value};

use super::{BoundColumn, NodeId, PlannedExpression, SortTerm, UpdateAssignment};

mod access_path;
mod planner;

pub(super) use planner::PhysicalPlanner;

/// Executable physical plan stored in a contiguous arena.
///
/// Nodes refer to their inputs by [`NodeId`], avoiding one allocation and
/// deallocation per operator. The root is always the last node added.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalPlan {
    nodes: Vec<PhysicalPlanNode>,
    root: NodeId,
}

impl PhysicalPlan {
    /// Creates a plan containing a single root node.
    pub fn new(root: PhysicalPlanNode) -> Self {
        Self { nodes: vec![root], root: NodeId::new(0) }
    }

    /// Adds a node and makes it the plan root, returning its ID.
    pub fn push(&mut self, node: PhysicalPlanNode) -> NodeId {
        let id = NodeId::new(self.nodes.len());
        self.nodes.push(node);
        self.root = id;
        id
    }

    /// Returns the root node.
    pub fn root(&self) -> &PhysicalPlanNode {
        self.node(self.root)
    }

    /// Returns the root node ID.
    pub fn root_id(&self) -> NodeId {
        self.root
    }

    /// Returns a node by arena ID.
    pub fn node(&self, id: NodeId) -> &PhysicalPlanNode {
        &self.nodes[id.index()]
    }

    /// Iterates over all nodes in arena order.
    pub fn iter(&self) -> impl Iterator<Item = &PhysicalPlanNode> {
        self.nodes.iter()
    }

    pub(crate) fn from_parts(nodes: Vec<PhysicalPlanNode>, root: NodeId) -> Self {
        debug_assert!(root.index() < nodes.len());
        Self { nodes, root }
    }

    pub(crate) fn into_parts(self) -> (Vec<PhysicalPlanNode>, NodeId) {
        (self.nodes, self.root)
    }

    pub(crate) fn display_node(&self, index: NodeId) -> impl fmt::Display + '_ {
        PhysicalPlanDisplay { plan: self, root: index }
    }
}

struct PhysicalPlanDisplay<'a> {
    plan: &'a PhysicalPlan,
    root: NodeId,
}

impl fmt::Display for PhysicalPlanDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_physical_plan(self.plan, self.root, f, "", true, true)
    }
}

/// One operator in a [`PhysicalPlan`] arena.
#[allow(
    clippy::large_enum_variant,
    reason = "physical operators are stored inline in the plan arena by design"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlanNode {
    /// Return the formatted input plan without executing it.
    Explain {
        /// Plan to describe.
        input: NodeId,
    },
    /// Execute a catalog table creation.
    CreateTable {
        /// Table name to create.
        name: String,
        /// Row schema for the new table.
        schema: TupleSchema,
    },
    /// Execute a catalog secondary-index creation.
    CreateIndex {
        /// Index name to create.
        name: String,
        /// Table whose rows the index covers.
        table: TableSchema,
        /// Bound table columns that form the index key.
        columns: Vec<BoundColumn>,
    },
    /// Produce literal rows.
    Values {
        /// Planned expressions for each literal row.
        rows: Vec<Vec<PlannedExpression>>,
    },
    /// Insert literal values into bound table columns.
    InsertValues {
        /// Target table.
        table: TableSchema,
        /// Target columns in value order.
        columns: Vec<BoundColumn>,
        /// Literal value rows to insert.
        values: Vec<Vec<PlannedExpression>>,
    },
    /// Update rows from a table selected by an input operator.
    Update {
        /// Target table.
        table: TableSchema,
        /// Bound column assignments.
        assignments: Vec<UpdateAssignment>,
        /// Row-producing operator that yields target table records.
        input: NodeId,
    },
    /// Delete rows from a table selected by an input operator.
    Delete {
        /// Target table.
        table: TableSchema,
        /// Row-producing operator that yields target table records.
        input: NodeId,
    },
    /// Produce exactly one empty row.
    ///
    /// This is the row source for `SELECT` statements without a `FROM` clause.
    OneRow,
    /// Scan all rows from a table.
    FullTableScan {
        /// Table to scan.
        table: TableSchema,
    },
    /// Scan rows from a table whose primary key falls in a bounded range.
    ///
    /// The planner emits this for compatible comparisons against the first
    /// integer primary-key column. If only part of the `WHERE` predicate can be
    /// expressed as a key range, the range scan is wrapped in a
    /// [`PhysicalPlanNode::Filter`] for the residual expression.
    PrimaryKeyRangeScan {
        /// Table to scan.
        table: TableSchema,
        /// Primary-key range to scan.
        range: TableKeyRange,
    },
    /// Scan rows from a table through a secondary-index key range.
    ///
    /// This is selected for predicates on a compatible single-column secondary
    /// index. The scan produces candidate table rows; the original predicate is
    /// still applied by a surrounding [`PhysicalPlanNode::Filter`] so the executor
    /// preserves SQL semantics when the access range is only an approximation.
    SecondaryIndexScan {
        /// Scan metadata and key bounds.
        scan: SecondaryIndexScanPlan,
    },
    /// Filter rows from an input physical operator.
    Filter {
        /// Input operator.
        input: NodeId,
        /// Predicate evaluated for each input row.
        predicate: PlannedExpression,
    },
    /// Sort rows from an input physical operator.
    Sort {
        /// Input operator.
        input: NodeId,
        /// Sort keys in priority order.
        terms: Vec<SortTerm>,
    },
    /// Evaluate expressions for each input row.
    Project {
        /// Input operator.
        input: NodeId,
        /// Output expressions in result-column order.
        expressions: Vec<PlannedExpression>,
    },
    /// Skip input rows before producing output.
    Offset {
        /// Input operator.
        input: NodeId,
        /// Number of rows to skip.
        offset: u32,
    },
    /// Stop after producing a bounded number of rows.
    Limit {
        /// Input operator.
        input: NodeId,
        /// Maximum number of rows to emit.
        limit: u32,
    },
}

/// Metadata needed to scan a table through a secondary index.
///
/// Secondary indexes store encoded index keys, while diagnostics and `EXPLAIN`
/// output should stay close to SQL values. This struct therefore keeps both the
/// human-readable [`IndexValueRange`] and the encoded [`IndexKeyRange`] that the
/// storage layer scans.
#[derive(Debug, Clone, PartialEq)]
pub struct SecondaryIndexScanPlan {
    /// Table to fetch rows from.
    pub table: TableSchema,
    /// Secondary index to scan.
    pub index: IndexSchema,
    /// Bound table column matched by the index lookup.
    pub column: BoundColumn,
    /// Human-readable indexed value range.
    pub value_range: IndexValueRange,
    /// Encoded index-key range to scan.
    pub key_range: IndexKeyRange,
}

impl PartialEq<PhysicalPlanNode> for PhysicalPlan {
    fn eq(&self, other: &PhysicalPlanNode) -> bool {
        self.nodes.len() == 1 && self.root() == other
    }
}

impl PartialEq<PhysicalPlan> for PhysicalPlanNode {
    fn eq(&self, other: &PhysicalPlan) -> bool {
        other == self
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_physical_plan(self, self.root, f, "", true, true)
    }
}

fn format_physical_plan(
    plan: &PhysicalPlan,
    node_index: NodeId,
    f: &mut fmt::Formatter<'_>,
    prefix: &str,
    is_last: bool,
    is_root: bool,
) -> fmt::Result {
    let node = plan.node(node_index);
    if !is_root {
        write!(f, "\n{}{} ", prefix, if is_last { "`-" } else { "|-" })?;
    }
    write!(f, "{}", physical_plan_label(node))?;

    if let Some(input) = physical_plan_input(node) {
        let child_prefix = if is_root {
            String::new()
        } else {
            format!("{}{}", prefix, if is_last { "   " } else { "|  " })
        };
        format_physical_plan(plan, input, f, &child_prefix, true, false)?;
    }
    Ok(())
}

fn physical_plan_input(plan: &PhysicalPlanNode) -> Option<NodeId> {
    match plan {
        PhysicalPlanNode::Explain { input }
        | PhysicalPlanNode::Update { input, .. }
        | PhysicalPlanNode::Delete { input, .. }
        | PhysicalPlanNode::Filter { input, .. }
        | PhysicalPlanNode::Sort { input, .. }
        | PhysicalPlanNode::Project { input, .. }
        | PhysicalPlanNode::Offset { input, .. }
        | PhysicalPlanNode::Limit { input, .. } => Some(*input),
        _ => None,
    }
}

fn physical_plan_label(plan: &PhysicalPlanNode) -> String {
    match plan {
        PhysicalPlanNode::Explain { .. } => "Explain".to_owned(),
        PhysicalPlanNode::CreateTable { name, .. } => format!("CreateTable table={name}"),
        PhysicalPlanNode::CreateIndex { name, table, columns } => format!(
            "CreateIndex index={name} table={} columns=[{}]",
            table.name,
            display_list(columns)
        ),
        PhysicalPlanNode::Values { rows } => format!("Values rows={}", rows.len()),
        PhysicalPlanNode::InsertValues { table, columns, values } => format!(
            "InsertValues table={} columns=[{}] rows={}",
            table.name,
            display_list(columns),
            values.len()
        ),
        PhysicalPlanNode::Update { table, assignments, .. } => {
            format!("Update table={} assignments=[{}]", table.name, display_list(assignments))
        }
        PhysicalPlanNode::Delete { table, .. } => format!("Delete table={}", table.name),
        PhysicalPlanNode::OneRow => "OneRow".to_owned(),
        PhysicalPlanNode::FullTableScan { table } => format!("FullTableScan table={}", table.name),
        PhysicalPlanNode::PrimaryKeyRangeScan { table, range } => {
            format!("PrimaryKeyRangeScan table={} range=[{}]", table.name, range)
        }
        PhysicalPlanNode::SecondaryIndexScan { scan } => format!(
            "SecondaryIndexScan table={} index={} column={} range=[{}]",
            scan.table.name, scan.index.name, scan.column, scan.value_range
        ),
        PhysicalPlanNode::Filter { predicate, .. } => format!("Filter predicate={predicate}"),
        PhysicalPlanNode::Sort { terms, .. } => format!("Sort terms=[{}]", display_list(terms)),
        PhysicalPlanNode::Project { expressions, .. } => {
            format!("Project expressions=[{}]", display_list(expressions))
        }
        PhysicalPlanNode::Offset { offset, .. } => format!("Offset offset={offset}"),
        PhysicalPlanNode::Limit { limit, .. } => format!("Limit limit={limit}"),
    }
}

fn display_list<T: fmt::Display>(values: &[T]) -> String {
    values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

/// Inclusive or exclusive display bound for a secondary-index scan value.
///
/// These bounds are used for formatted plans and tests. The storage-facing byte
/// bounds live in [`crate::core::IndexKeyBound`] values inside
/// [`SecondaryIndexScanPlan`].
#[derive(Debug, Clone, PartialEq)]
pub enum IndexValueBound {
    /// The scan includes this value.
    Inclusive(Value),
    /// The scan excludes this value.
    Exclusive(Value),
}

impl fmt::Display for IndexValueBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inclusive(value) => write!(f, "{value} inclusive"),
            Self::Exclusive(value) => write!(f, "{value} exclusive"),
        }
    }
}

/// Human-readable range over values in a single-column secondary index.
///
/// This mirrors the encoded [`IndexKeyRange`] selected for storage, but keeps
/// the original SQL value type visible for debugging and `EXPLAIN` output.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexValueRange {
    /// Optional lower value bound.
    pub lower: Option<IndexValueBound>,
    /// Optional upper value bound.
    pub upper: Option<IndexValueBound>,
}

impl fmt::Display for IndexValueRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.lower, &self.upper) {
            (None, None) => write!(f, "unbounded"),
            (Some(lower), None) => write!(f, "lower={lower}"),
            (None, Some(upper)) => write!(f, "upper={upper}"),
            (Some(lower), Some(upper)) => write!(f, "lower={lower} upper={upper}"),
        }
    }
}
