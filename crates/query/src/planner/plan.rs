//! Combined planning artifacts used for inspection and focused tests.

use super::{LogicalPlan, PhysicalPlan};

/// Complete planning result for one SQL statement.
///
/// A plan keeps both representations because they answer different questions:
/// the logical plan is the catalog-bound statement shape, while the physical
/// plan is the exact operator tree consumed by the executor. Tests often assert
/// both to verify that binding and access-path selection agree.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// Catalog-bound statement representation before physical operator
    /// selection.
    pub logical: LogicalPlan,
    /// Executable operator tree selected from the logical plan.
    pub physical: PhysicalPlan,
}
