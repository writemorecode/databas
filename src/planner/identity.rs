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
