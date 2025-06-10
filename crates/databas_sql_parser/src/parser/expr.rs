use std::fmt::Display;

use crate::lexer::token_kind::NumberKind;
use crate::parser::Op;

#[derive(Debug, PartialEq)]
pub enum Literal<'a> {
    String(&'a str),
    Number(NumberKind),
    Boolean(bool),
}

#[derive(Debug, PartialEq)]
pub enum AggregateFunction<'a> {
    Sum(Box<Expression<'a>>),
    Count(Box<Expression<'a>>),
    Avg(Box<Expression<'a>>),
    StdDev(Box<Expression<'a>>),
    Min(Box<Expression<'a>>),
    Max(Box<Expression<'a>>),
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
        match self {
            Expression::Literal(literal) => write!(f, "{}", literal),
            Expression::Identifier(ident) => write!(f, "{}", ident),
            Expression::UnaryOp((op, expr)) => write!(f, "{}{}", op, expr),
            Expression::BinaryOp((left, op, right)) => write!(f, "{} {} {}", left, op, right),
            Expression::Wildcard => write!(f, "*"),
            Expression::AggregateFunction(agg) => write!(f, "{}", agg),
        }
    }
}

impl Display for AggregateFunction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunction::Sum(expr) => write!(f, "SUM({})", expr),
            AggregateFunction::Count(expr) => write!(f, "COUNT({})", expr),
            AggregateFunction::Avg(expr) => write!(f, "AVG({})", expr),
            AggregateFunction::StdDev(expr) => write!(f, "STDDEV({})", expr),
            AggregateFunction::Min(expr) => write!(f, "MIN({})", expr),
            AggregateFunction::Max(expr) => write!(f, "MAX({})", expr),
        }
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
    use crate::parser::stmt::{
        Statement::{self},
        lists::ExpressionList,
        select::SelectQuery,
    };
    use crate::parser::Parser;

    #[test]
    fn test_all_aggregate_functions() {
        let sql = "SELECT COUNT(*), SUM(price), AVG(price), STDDEV(price), MAX(price), MIN(price) FROM products;";
        let mut parser = Parser::new(sql);
        let query = parser.stmt();

        let expected_query = Statement::Select(SelectQuery {
            table: Some("products"),
            columns: ExpressionList(vec![
                Expression::AggregateFunction(AggregateFunction::Count(Box::new(Expression::Wildcard))),
                Expression::AggregateFunction(AggregateFunction::Sum(Box::new(
                    Expression::Identifier("price"),
                ))),
                Expression::AggregateFunction(AggregateFunction::Avg(Box::new(
                    Expression::Identifier("price"),
                ))),
                Expression::AggregateFunction(AggregateFunction::StdDev(Box::new(
                    Expression::Identifier("price"),
                ))),
                Expression::AggregateFunction(AggregateFunction::Max(Box::new(
                    Expression::Identifier("price"),
                ))),
                Expression::AggregateFunction(AggregateFunction::Min(Box::new(
                    Expression::Identifier("price"),
                ))),
            ]),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        });
        assert_eq!(query, Ok(expected_query));
    }
}
