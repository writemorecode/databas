//! Catalog-bound logical plan representation.

use crate::core::{TableSchema, TupleSchema};

use super::{BoundColumn, NodeId, PlannedExpression, RelationId, SortTerm, UpdateAssignment};

/// Catalog-bound logical plan stored in a contiguous arena.
///
/// Nodes refer to their inputs by [`NodeId`]. A logical plan describes
/// relational meaning without selecting concrete scan or execution strategies.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    nodes: Vec<LogicalPlanNode>,
    root: NodeId,
}

impl LogicalPlan {
    /// Creates a plan containing a single root node.
    pub fn new(root: LogicalPlanNode) -> Self {
        Self { nodes: vec![root], root: NodeId::new(0) }
    }

    /// Adds a node and makes it the plan root, returning its ID.
    pub fn push(&mut self, node: LogicalPlanNode) -> NodeId {
        let id = NodeId::new(self.nodes.len());
        self.nodes.push(node);
        self.root = id;
        id
    }

    /// Returns the root node.
    pub fn root(&self) -> &LogicalPlanNode {
        self.node(self.root)
    }

    /// Returns the root node ID.
    pub fn root_id(&self) -> NodeId {
        self.root
    }

    /// Returns a node by arena ID.
    pub fn node(&self, id: NodeId) -> &LogicalPlanNode {
        &self.nodes[id.index()]
    }

    /// Iterates over all nodes in arena order.
    pub fn iter(&self) -> impl Iterator<Item = &LogicalPlanNode> {
        self.nodes.iter()
    }

    pub(crate) fn from_parts(nodes: Vec<LogicalPlanNode>, root: NodeId) -> Self {
        debug_assert!(root.index() < nodes.len());
        Self { nodes, root }
    }

    pub(crate) fn into_parts(self) -> (Vec<LogicalPlanNode>, NodeId) {
        (self.nodes, self.root)
    }
}

/// One operator in a [`LogicalPlan`] arena.
///
/// Every table or column reference is already bound to catalog metadata. Input
/// fields contain IDs into the owning plan's node arena.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlanNode {
    /// Return the physical plan for an input statement without executing it.
    Explain { input: NodeId },
    /// Create a table with the provided tuple schema.
    CreateTable { name: String, schema: TupleSchema },
    /// Create a secondary index over bound columns from an existing table.
    CreateIndex { name: String, table: TableSchema, columns: Vec<BoundColumn> },
    /// Literal rows, usually produced by an `INSERT ... VALUES` statement.
    ///
    /// The current planner accepts only literal expressions in insert values, so
    /// this node is side-effect free and independent of table input.
    Values { rows: Vec<Vec<PlannedExpression>> },
    /// Insert rows from an input plan into bound table columns.
    ///
    /// The input is currently expected to be [`LogicalPlanNode::Values`] during
    /// physical planning.
    Insert { table: TableSchema, columns: Vec<BoundColumn>, input: NodeId },
    /// Update rows in a table selected by an input plan.
    ///
    /// Assignment targets are bound and checked for duplicate names before this
    /// node is built. Primary-key columns are rejected here because changing
    /// them would require moving table records.
    Update {
        relation: RelationId,
        table: TableSchema,
        assignments: Vec<UpdateAssignment>,
        input: NodeId,
    },
    /// Delete rows from a table selected by an input plan.
    Delete { relation: RelationId, table: TableSchema, input: NodeId },
    /// Synthetic single-row input used for projection-only selects without a
    /// `FROM` clause.
    OneRow,
    /// Read every row from a catalog table.
    TableScan { relation: RelationId, table: TableSchema },
    /// Keep only rows for which the predicate evaluates truthfully.
    ///
    /// Physical planning may use part of this predicate to choose a narrower
    /// table access path. Any remaining predicate is preserved as a filter.
    Filter { input: NodeId, predicate: PlannedExpression },
    /// Order input rows by one or more columns.
    Sort { input: NodeId, terms: Vec<SortTerm> },
    /// Produce output expressions from each input row.
    Project { input: NodeId, expressions: Vec<PlannedExpression> },
    /// Skip the first `offset` input rows.
    Offset { input: NodeId, offset: u32 },
    /// Emit at most `limit` input rows.
    Limit { input: NodeId, limit: u32 },
}
