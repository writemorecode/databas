//! Query-local identities used by logical and physical plans.

/// Index of a node in its owning plan arena.
///
/// Node IDs are meaningful only together with the [`LogicalPlan`](super::LogicalPlan)
/// or [`PhysicalPlan`](super::PhysicalPlan) that produced them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(usize);

impl NodeId {
    /// Creates an arena-local node ID.
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Returns the underlying arena index.
    pub const fn index(self) -> usize {
        self.0
    }
}

/// Identity of one table occurrence within a bound query.
///
/// This is deliberately distinct from a catalog table ID: self-joins create
/// multiple relation IDs for the same catalog table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationId(u32);

impl RelationId {
    /// Creates a query-local relation ID.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the query-local numeric identity.
    pub const fn value(self) -> u32 {
        self.0
    }
}
