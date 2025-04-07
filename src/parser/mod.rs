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
            TokenKind::Plus => Op::Add,
            TokenKind::Minus => Op::Sub,
            TokenKind::Asterisk => Op::Mul,
            TokenKind::Slash => Op::Div,
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

            TokenKind::Minus | TokenKind::Bang => {
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
            let op = match op.map(|res| {
                res.as_ref().expect("checked Some above")
                //.expect_err("checked Err above")
            }) {
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

                other => panic!("Op {other:?} not yet implemented!"),
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
    Neg,
    Not,
    Add,
    Sub,
    Mul,
    Div,
}

fn prefix_binding_power(op: &Op) -> Option<((), u8)> {
    let res = match op {
        Op::Sub | Op::Neg => ((), 5),
        _ => return None,
    };
    Some(res)
}

fn infix_binding_power(op: &Op) -> Option<(u8, u8)> {
    let res = match op {
        Op::Add | Op::Sub => (1, 2),
        Op::Mul | Op::Div => (3, 4),
        _ => return None,
    };
    Some(res)
}
