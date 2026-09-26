//! Logical-plan optimization performed after binding.

mod empty_simplification;
mod equality_inference;
mod expression;
mod expression_simplification;
mod operator_simplification;
mod predicate_pushdown;

use super::{LogicalPlan, PlannerResult};

/// Applies the logical rewrite pipeline to a bound plan.
pub(super) fn optimize(logical: LogicalPlan) -> PlannerResult<LogicalPlan> {
    let logical = expression_simplification::optimize(logical);
    let logical = empty_simplification::optimize(logical)?;
    let logical = operator_simplification::optimize(logical)?;
    let logical = predicate_pushdown::optimize(logical)?;
    let logical = equality_inference::optimize(logical)?;
    predicate_pushdown::optimize(logical)
}
