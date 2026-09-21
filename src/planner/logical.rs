//! Catalog-bound logical plan representation.

use crate::{
    core::{TableSchema, TupleSchema},
    sql_parser::parser::stmt::select::JoinType,
};

use super::{
    BoundColumn, BoundExpr, BoundSortTerm, BoundUpdateAssignment, NodeId, PlanSchema, RelationId,
};

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

    /// Returns the output schema of a relational node.
    pub fn output_schema(&self, id: NodeId) -> Option<&PlanSchema> {
        self.node(id).output_schema()
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
    Values { rows: Vec<Vec<BoundExpr>>, output: PlanSchema },
    /// Insert rows from an input plan into bound table columns.
    Insert { table: TableSchema, columns: Vec<BoundColumn>, input: NodeId },
    /// Update rows in a table selected by an input plan.
    Update {
        relation: RelationId,
        table: TableSchema,
        assignments: Vec<BoundUpdateAssignment>,
        input: NodeId,
    },
    /// Delete rows from a table selected by an input plan.
    Delete { relation: RelationId, table: TableSchema, input: NodeId },
    /// Synthetic single-row input used for projection-only selects.
    OneRow { output: PlanSchema },
    /// Read every row from a bound table occurrence.
    TableScan { relation: RelationId, table: TableSchema, output: PlanSchema },
    /// Keep only rows for which the predicate evaluates truthfully.
    Filter { input: NodeId, predicate: BoundExpr, output: PlanSchema },
    /// Order input rows by one or more columns.
    Sort { input: NodeId, terms: Vec<BoundSortTerm>, output: PlanSchema },
    /// Produce output expressions from each input row.
    Project { input: NodeId, expressions: Vec<BoundExpr>, output: PlanSchema },
    /// Skip the first `offset` input rows.
    Offset { input: NodeId, offset: u32, output: PlanSchema },
    /// Emit at most `limit` input rows.
    Limit { input: NodeId, limit: u32, output: PlanSchema },
    /// Join rows from another table for which a predicate evaluates truthfully.
    Join {
        left: NodeId,
        right: NodeId,
        join_type: JoinType,
        predicate: BoundExpr,
        output: PlanSchema,
    },
}

impl LogicalPlanNode {
    /// Returns the ordered output schema for row-producing operators.
    pub fn output_schema(&self) -> Option<&PlanSchema> {
        match self {
            Self::Values { output, .. }
            | Self::OneRow { output }
            | Self::TableScan { output, .. }
            | Self::Filter { output, .. }
            | Self::Sort { output, .. }
            | Self::Project { output, .. }
            | Self::Offset { output, .. }
            | Self::Limit { output, .. }
            | Self::Join { output, .. } => Some(output),
            Self::Explain { .. }
            | Self::CreateTable { .. }
            | Self::CreateIndex { .. }
            | Self::Insert { .. }
            | Self::Update { .. }
            | Self::Delete { .. } => None,
        }
    }
}
