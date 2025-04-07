pub mod op;

use op::{Op, infix_binding_power, prefix_binding_power};

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
                let op = Op::try_from(token)?;
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
                let op = Op::try_from(token)?;
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
            let peeked = self.lexer.peek();

            let token = match peeked {
                None => break,
                Some(Err(_)) => return Err(self.lexer.next().unwrap().unwrap_err()),
                Some(Ok(Token {
                    kind: TokenKind::Comma | TokenKind::RightParen | TokenKind::Semicolon,
                    ..
                })) => break,
                Some(Ok(tok)) => tok,
            };

            let op = Op::try_from(token.to_owned())?;

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
