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
pub enum Expression<'a> {
    Literal(Literal<'a>),
    Identifier(&'a str),
    UnaryOp((Op, Box<Expression<'a>>)),
    BinaryOp((Box<Expression<'a>>, Op, Box<Expression<'a>>)),
    Wildcard,
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
