use std::fmt::Display;

use crate::error::{SQLError, SQLErrorKind};
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Keyword, TokenKind};

impl<'a> TryFrom<Token<'a>> for Op {
    type Error = SQLError<'a>;

    fn try_from(token: Token<'a>) -> Result<Self, Self::Error> {
        let op = match token.kind {
            TokenKind::Keyword(Keyword::And) => Self::And,
            TokenKind::Keyword(Keyword::Or) => Self::Or,
            TokenKind::Keyword(Keyword::Not) => Self::Not,
            TokenKind::Plus => Self::Add,
            TokenKind::Minus => Self::Sub,
            TokenKind::Asterisk => Self::Mul,
            TokenKind::Slash => Self::Div,
            TokenKind::EqualsEquals => Self::EqualsEquals,
            TokenKind::NotEquals => Self::NotEquals,
            TokenKind::LessThan => Self::LessThan,
            TokenKind::GreaterThan => Self::GreaterThan,
            TokenKind::LessThanOrEqual => Self::LessThanOrEqual,
            TokenKind::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            _ => {
                return Err(SQLError::new(
                    SQLErrorKind::InvalidOperator { op: token.kind },
                    token.offset,
                ));
            }
        };
        Ok(op)
    }
}

#[derive(Debug, PartialEq, Eq)]
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
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Not => write!(f, "NOT "),
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Mul => write!(f, "*"),
            Self::Div => write!(f, "/"),
            Self::NotEquals => write!(f, "!="),
            Self::EqualsEquals => write!(f, "=="),
            Self::LessThan => write!(f, "<"),
            Self::GreaterThan => write!(f, ">"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThanOrEqual => write!(f, ">="),
        }
    }
}

impl Op {
    pub const fn prefix_binding_power(&self) -> Option<((), u8)> {
        let res = match self {
            Self::Not | Self::Sub => ((), 7),
            _ => return None,
        };
        Some(res)
    }

    pub const fn infix_binding_power(&self) -> Option<(u8, u8)> {
        let res = match self {
            Self::And | Self::Or => (1, 2),
            Self::NotEquals
            | Self::EqualsEquals
            | Self::LessThan
            | Self::GreaterThan
            | Self::LessThanOrEqual
            | Self::GreaterThanOrEqual => (3, 4),
            Self::Add | Self::Sub => (5, 6),
            Self::Mul | Self::Div => (6, 7),
            _ => return None,
        };
        Some(res)
    }
}
