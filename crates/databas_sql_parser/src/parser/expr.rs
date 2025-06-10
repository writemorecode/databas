use std::fmt::Display;

use crate::lexer::token_kind::NumberKind;
use crate::parser::Op;

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
    fn test_aggregate_function_struct_usage() {
        // Test direct field access
        let sum_func = AggregateFunction {
            kind: AggregateFunctionKind::Sum,
            expr: Box::new(Expression::Identifier("salary")),
        };
        
        assert_eq!(sum_func.kind, AggregateFunctionKind::Sum);
        assert_eq!(*sum_func.expr, Expression::Identifier("salary"));
        assert_eq!(format!("{}", sum_func), "SUM(salary)");
        
        // Test different aggregate kinds
        let kinds = vec![
            AggregateFunctionKind::Count,
            AggregateFunctionKind::Avg,
            AggregateFunctionKind::Max,
            AggregateFunctionKind::Min,
            AggregateFunctionKind::StdDev,
        ];
        
        for kind in kinds {
            let agg = AggregateFunction {
                kind,
                expr: Box::new(Expression::Wildcard),
            };
            
            // Test that kind field is accessible
            assert_eq!(agg.kind, kind);
            
            // Test that expression field is accessible
            assert_eq!(*agg.expr, Expression::Wildcard);
        }
    }
}
