//! Public orchestration of the binding and physical-planning phases.

use crate::{
    core::{Database, access::CatalogRead},
    sql_parser::parser::stmt::Statement,
};

use super::{PhysicalPlan, Plan, PlannerResult, binder::Binder, physical::PhysicalPlanner};

/// Planner bound to a database catalog.
///
/// `Planner` orchestrates SQL binding and physical planning. The individual
/// phases live in dedicated modules and can evolve independently.
pub struct Planner<'catalog> {
    catalog: &'catalog dyn CatalogRead,
}

impl<'catalog> Planner<'catalog> {
    /// Creates a planner that resolves names through `database`.
    pub fn new(database: &'catalog Database) -> Self {
        Self::with_schema(database)
    }

    /// Creates a planner over an arbitrary catalog implementation.
    pub(crate) fn with_schema(catalog: &'catalog dyn CatalogRead) -> Self {
        Self { catalog }
    }

    /// Builds both the bound logical plan and executable physical plan.
    pub fn plan_statement(&self, statement: &Statement<'_>) -> PlannerResult<Plan> {
        let logical = Binder::new(self.catalog).bind(statement)?;
        let physical = PhysicalPlanner::new(self.catalog).plan(logical.clone())?;
        Ok(Plan { logical, physical })
    }

    /// Plans one parsed SQL statement directly into an executable physical plan.
    pub fn plan_physical_statement(
        &self,
        statement: &Statement<'_>,
    ) -> PlannerResult<PhysicalPlan> {
        let logical = Binder::new(self.catalog).bind(statement)?;
        PhysicalPlanner::new(self.catalog).plan(logical)
    }
}
