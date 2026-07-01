use std::fmt::Display;

use crate::sql_parser::lexer::token_kind::NumberKind;
use crate::sql_parser::parser::Op;

#[derive(Debug, PartialEq)]
pub enum Literal<'a> {
    String(&'a str),
    Number(NumberKind),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AggregateFunctionKind {
    Sum,
    Count,
    Avg,
    StdDev,
    Min,
    Max,
}

#[derive(Debug, PartialEq)]
pub struct AggregateFunction<'a> {
    pub kind: AggregateFunctionKind,
    pub expr: Box<Expression<'a>>,
}

#[derive(Debug, PartialEq)]
pub enum Expression<'a> {
    Literal(Literal<'a>),
    Identifier(&'a str),
    UnaryOp((Op, Box<Expression<'a>>)),
    BinaryOp((Box<Expression<'a>>, Op, Box<Expression<'a>>)),
    Wildcard,
    AggregateFunction(AggregateFunction<'a>),
}

impl From<i32> for Expression<'_> {
    fn from(value: i32) -> Self {
        Expression::Literal(Literal::Number(NumberKind::Integer(value)))
    }
}

impl From<f32> for Expression<'_> {
    fn from(value: f32) -> Self {
        Expression::Literal(Literal::Number(NumberKind::Float(value)))
    }
}

impl From<bool> for Expression<'_> {
    fn from(value: bool) -> Self {
        Expression::Literal(Literal::Boolean(value))
    }
}

impl Display for Expression<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_with_parent_op(f, None, ChildSide::Left)
    }
}

#[derive(Copy, Clone)]
enum ChildSide {
    Left,
    Right,
}

impl Expression<'_> {
    fn fmt_with_parent_op(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        parent_op: Option<Op>,
        side: ChildSide,
    ) -> std::fmt::Result {
        let needs_parens = match (self, parent_op) {
            (Expression::BinaryOp((_, child_op, _)), Some(parent_op)) => {
                let child_bp = child_op.infix_binding_power().map(|(l_bp, _)| l_bp).unwrap_or(0);
                let parent_bp = parent_op.infix_binding_power().map(|(l_bp, _)| l_bp).unwrap_or(0);

                child_bp < parent_bp || matches!(side, ChildSide::Right) && child_bp == parent_bp
            }
            _ => false,
        };

        if needs_parens {
            write!(f, "(")?;
        }

        match self {
            Expression::Literal(literal) => write!(f, "{}", literal),
            Expression::Identifier(ident) => write!(f, "{}", ident),
            Expression::UnaryOp((op, expr)) => {
                write!(f, "{}", op)?;
                if matches!(**expr, Expression::BinaryOp(_)) {
                    write!(f, "({})", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Expression::BinaryOp((left, op, right)) => {
                left.fmt_with_parent_op(f, Some(*op), ChildSide::Left)?;
                write!(f, " {} ", op)?;
                right.fmt_with_parent_op(f, Some(*op), ChildSide::Right)
            }
            Expression::Wildcard => write!(f, "*"),
            Expression::AggregateFunction(agg) => write!(f, "{}", agg),
        }?;

        if needs_parens {
            write!(f, ")")?;
        }

        Ok(())
    }
}

impl Display for AggregateFunctionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunctionKind::Sum => write!(f, "SUM"),
            AggregateFunctionKind::Count => write!(f, "COUNT"),
            AggregateFunctionKind::Avg => write!(f, "AVG"),
            AggregateFunctionKind::StdDev => write!(f, "STDDEV"),
            AggregateFunctionKind::Min => write!(f, "MIN"),
            AggregateFunctionKind::Max => write!(f, "MAX"),
        }
    }
}

impl Display for AggregateFunction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.kind, self.expr)
    }
}

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(s) => write!(f, "\"{}\"", s),
            Literal::Number(n) => write!(f, "{}", n),
            Literal::Boolean(b) => write!(f, "{}", b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::parser::Parser;
    use crate::sql_parser::parser::stmt::{
        Statement::{self},
        lists::ExpressionList,
        select::SelectQuery,
    };

    #[test]
    fn test_all_aggregate_functions() {
        let sql = "SELECT COUNT(*), SUM(price), AVG(price), STDDEV(price), MAX(price), MIN(price) FROM products;";
        let mut parser = Parser::new(sql);
        let query = parser.stmt();

        let expected_query = Statement::Select(SelectQuery {
            table: Some("products"),
            columns: ExpressionList(vec![
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::Count,
                    expr: Box::new(Expression::Wildcard),
                }),
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::Sum,
                    expr: Box::new(Expression::Identifier("price")),
                }),
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::Avg,
                    expr: Box::new(Expression::Identifier("price")),
                }),
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::StdDev,
                    expr: Box::new(Expression::Identifier("price")),
                }),
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::Max,
                    expr: Box::new(Expression::Identifier("price")),
                }),
                Expression::AggregateFunction(AggregateFunction {
                    kind: AggregateFunctionKind::Min,
                    expr: Box::new(Expression::Identifier("price")),
                }),
            ]),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        });
        assert_eq!(query, Ok(expected_query));

        // Test that the struct format works correctly
        let test_agg = AggregateFunction {
            kind: AggregateFunctionKind::Sum,
            expr: Box::new(Expression::Identifier("price")),
        };
        assert_eq!(format!("{}", test_agg), "SUM(price)");
    }

    #[test]
    fn aggregate_functions_display_with_their_argument() {
        let cases = [
            (AggregateFunctionKind::Count, Expression::Wildcard, "COUNT(*)"),
            (AggregateFunctionKind::Avg, Expression::Identifier("salary"), "AVG(salary)"),
            (AggregateFunctionKind::Max, Expression::Identifier("salary"), "MAX(salary)"),
            (AggregateFunctionKind::Min, Expression::Identifier("salary"), "MIN(salary)"),
            (AggregateFunctionKind::StdDev, Expression::Identifier("salary"), "STDDEV(salary)"),
        ];

        for (kind, expr, expected) in cases {
            let aggregate = AggregateFunction { kind, expr: Box::new(expr) };

            assert_eq!(aggregate.to_string(), expected);
        }
    }
}
