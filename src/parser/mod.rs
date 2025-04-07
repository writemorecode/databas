use std::fmt::Display;

use crate::error::Error;
use crate::lexer::Lexer;
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Keyword, NumberKind, TokenKind};

#[derive(Debug)]
pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            lexer: Lexer::new(source),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Literal<'a> {
    String(&'a str),
    Number(NumberKind),
    Boolean(bool),
}

impl<'a> TryFrom<TokenKind<'a>> for Op {
    type Error = Error<'a>;

    fn try_from(kind: TokenKind<'a>) -> Result<Self, Self::Error> {
        let op = match kind {
            TokenKind::Keyword(Keyword::And) => Op::And,
            TokenKind::Keyword(Keyword::Or) => Op::Or,
            TokenKind::Plus => Op::Add,
            TokenKind::Minus => Op::Sub,
            TokenKind::Asterisk => Op::Mul,
            TokenKind::Slash => Op::Div,
            TokenKind::Bang => Op::Not,
            TokenKind::EqualsEquals => Op::EqualsEquals,
            TokenKind::NotEquals => Op::NotEquals,
            TokenKind::LessThan => Op::LessThan,
            TokenKind::GreaterThan => Op::GreaterThan,
            TokenKind::LessThanOrEqual => Op::LessThanOrEqual,
            TokenKind::GreaterThanOrEqual => Op::GreaterThanOrEqual,
            _ => return Err(Error::Other(kind)),
        };
        Ok(op)
    }
}

#[derive(Debug, PartialEq)]
pub enum Expression<'a> {
    Literal(Literal<'a>),
    Identifier(&'a str),
    UnaryOp((Op, Box<Expression<'a>>)),
    BinaryOp((Box<Expression<'a>>, Op, Box<Expression<'a>>)),
}

impl From<i32> for Expression<'_> {
    fn from(value: i32) -> Self {
        Expression::Literal(Literal::Number(NumberKind::Integer(value)))
    }
}

impl From<bool> for Expression<'_> {
    fn from(value: bool) -> Self {
        Expression::Literal(Literal::Boolean(value))
    }
}

impl<'a> Parser<'a> {
    pub fn expr(mut self) -> Result<Expression<'a>, Error<'a>> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expression<'a>, Error<'a>> {
        let Some(token_result) = self.lexer.next() else {
            return Err(Error::UnexpectedEnd {
                pos: self.lexer.position,
            });
        };
        let token = token_result?;
        let op = token.kind;

        let mut lhs = match op {
            TokenKind::String(lit) => Expression::Literal(Literal::String(lit)),
            TokenKind::Number(num) => Expression::Literal(Literal::Number(num)),
            TokenKind::Keyword(Keyword::True) => Expression::Literal(Literal::Boolean(true)),
            TokenKind::Keyword(Keyword::False) => Expression::Literal(Literal::Boolean(false)),
            TokenKind::Identifier(id) => Expression::Identifier(id),
            TokenKind::LeftParen => {
                let lhs = self
                    .expr_bp(0)
                    .map_err(|_| Error::UnclosedParenthesis { pos: token.offset })?;
                self.lexer.expect_token(TokenKind::RightParen)?;
                lhs
            }

            TokenKind::Minus => {
                let op = Op::try_from(token.kind)?;
                if let Some(((), r_bp)) = prefix_binding_power(&op) {
                    let rhs = self.expr_bp(r_bp)?;
                    Expression::UnaryOp((Op::Neg, Box::new(rhs)))
                } else {
                    return Err(Error::InvalidPrefixOperator {
                        op: token.kind,
                        pos: self.lexer.position,
                    });
                }
            }

            TokenKind::Bang => {
                let op = Op::try_from(token.kind)?;
                if let Some(((), r_bp)) = prefix_binding_power(&op) {
                    let rhs = self.expr_bp(r_bp)?;
                    Expression::UnaryOp((Op::Not, Box::new(rhs)))
                } else {
                    return Err(Error::InvalidPrefixOperator {
                        op: token.kind,
                        pos: self.lexer.position,
                    });
                }
            }

            other => {
                return Err(Error::Other(other));
            }
        };

        loop {
            let op = self.lexer.peek();
            if op.is_some_and(|op| op.is_err()) {
                return Err(self
                    .lexer
                    .next()
                    .expect("checked Some above")
                    .expect_err("checked Err above"));
            }
            let op = match op.map(|res| res.as_ref().expect("checked Some above")) {
                None => break,

                Some(Token {
                    kind: TokenKind::Comma | TokenKind::RightParen | TokenKind::Semicolon,
                    ..
                }) => break,

                Some(Token {
                    kind: TokenKind::Plus,
                    ..
                }) => Op::Add,

                Some(Token {
                    kind: TokenKind::Minus,
                    ..
                }) => Op::Sub,

                Some(Token {
                    kind: TokenKind::Asterisk,
                    ..
                }) => Op::Mul,

                Some(Token {
                    kind: TokenKind::Slash,
                    ..
                }) => Op::Div,

                Some(Token {
                    kind: TokenKind::Keyword(Keyword::And),
                    ..
                }) => Op::And,

                Some(Token {
                    kind: TokenKind::Keyword(Keyword::Or),
                    ..
                }) => Op::Or,

                Some(Token {
                    kind: TokenKind::NotEquals,
                    ..
                }) => Op::NotEquals,

                Some(Token {
                    kind: TokenKind::EqualsEquals,
                    ..
                }) => Op::EqualsEquals,

                Some(Token {
                    kind: TokenKind::LessThan,
                    ..
                }) => Op::LessThan,

                Some(Token {
                    kind: TokenKind::GreaterThan,
                    ..
                }) => Op::GreaterThan,

                Some(Token {
                    kind: TokenKind::LessThanOrEqual,
                    ..
                }) => Op::LessThanOrEqual,

                Some(Token {
                    kind: TokenKind::GreaterThanOrEqual,
                    ..
                }) => Op::GreaterThanOrEqual,

                Some(Token { kind, offset }) => {
                    return Err(Error::InvalidOperator {
                        op: *kind,
                        pos: *offset,
                    });
                }
            };

            if let Some((l_bp, r_bp)) = infix_binding_power(&op) {
                if l_bp < min_bp {
                    break;
                }
                self.lexer.next();
                let rhs = self.expr_bp(r_bp)?;
                lhs = Expression::BinaryOp((Box::new(lhs), op, Box::new(rhs)));
                continue;
            }
            break;
        }
        Ok(lhs)
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
    Neg,
    Not,
    Add,
    Sub,
    Mul,
    Div,
}

impl Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::And => write!(f, "&&"),
            Op::Or => write!(f, "||"),
            Op::Neg => write!(f, "-"),
            Op::Not => write!(f, "!"),
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

fn prefix_binding_power(op: &Op) -> Option<((), u8)> {
    let res = match op {
        Op::Not | Op::Sub => ((), 7),
        _ => return None,
    };
    Some(res)
}

fn infix_binding_power(op: &Op) -> Option<(u8, u8)> {
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
