use std::fmt::Display;

use crate::error::Error;
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Keyword, TokenKind};

impl<'a> TryFrom<Token<'a>> for Op {
    type Error = Error<'a>;

    fn try_from(token: Token<'a>) -> Result<Self, Self::Error> {
        let op = match token.kind {
            TokenKind::Keyword(Keyword::And) => Op::And,
            TokenKind::Keyword(Keyword::Or) => Op::Or,
            TokenKind::Keyword(Keyword::Not) => Op::Not,
            TokenKind::Plus => Op::Add,
            TokenKind::Minus => Op::Sub,
            TokenKind::Asterisk => Op::Mul,
            TokenKind::Slash => Op::Div,
            TokenKind::EqualsEquals => Op::EqualsEquals,
            TokenKind::NotEquals => Op::NotEquals,
            TokenKind::LessThan => Op::LessThan,
            TokenKind::GreaterThan => Op::GreaterThan,
            TokenKind::LessThanOrEqual => Op::LessThanOrEqual,
            TokenKind::GreaterThanOrEqual => Op::GreaterThanOrEqual,
            _ => {
                return Err(Error::InvalidOperator { op: token.kind, pos: token.offset });
            }
        };
        Ok(op)
    }
}

#[derive(Debug, PartialEq)]
pub enum Op {
    And,
    Or,
    NotEquals,
    EqualsEquals,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Not,
    Add,
    Sub,
    Mul,
    Div,
}

impl Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::And => write!(f, "AND"),
            Op::Or => write!(f, "OR"),
            Op::Not => write!(f, "NOT "),
            Op::Add => write!(f, "+"),
            Op::Sub => write!(f, "-"),
            Op::Mul => write!(f, "*"),
            Op::Div => write!(f, "/"),
            Op::NotEquals => write!(f, "!="),
            Op::EqualsEquals => write!(f, "=="),
            Op::LessThan => write!(f, "<"),
            Op::GreaterThan => write!(f, ">"),
            Op::LessThanOrEqual => write!(f, "<="),
            Op::GreaterThanOrEqual => write!(f, ">="),
        }
    }
}

pub fn prefix_binding_power(op: &Op) -> Option<((), u8)> {
    let res = match op {
        Op::Not | Op::Sub => ((), 7),
        _ => return None,
    };
    Some(res)
}

pub fn infix_binding_power(op: &Op) -> Option<(u8, u8)> {
    let res = match op {
        Op::And | Op::Or => (1, 2),
        Op::NotEquals
        | Op::EqualsEquals
        | Op::LessThan
        | Op::GreaterThan
        | Op::LessThanOrEqual
        | Op::GreaterThanOrEqual => (3, 4),
        Op::Add | Op::Sub => (5, 6),
        Op::Mul | Op::Div => (6, 7),
        _ => return None,
    };
    Some(res)
}
