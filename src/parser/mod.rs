pub mod expr;
pub mod op;
pub mod stmt;

use expr::{Expression, Literal};
use op::{Op, infix_binding_power, prefix_binding_power};
use stmt::Statement;
use stmt::select::SelectQuery;

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
        Self { lexer: Lexer::new(source) }
    }

    fn parse_non_negative_integer(&mut self) -> Result<Option<u32>, Error<'a>> {
        self.lexer.next().ok_or(Error::UnexpectedEnd { pos: self.lexer.position }).and_then(
            |tok| {
                tok.map(|tok| match tok.kind {
                    TokenKind::Number(NumberKind::Integer(num)) => Ok(num.try_into().ok()),
                    TokenKind::Minus => {
                        if let Some(Ok(Token {
                            kind: TokenKind::Number(NumberKind::Integer(num)),
                            ..
                        })) = self.lexer.next()
                        {
                            Err(Error::ExpectedNonNegativeInteger { pos: tok.offset, got: -num })
                        } else {
                            Err(Error::Other(TokenKind::Minus))
                        }
                    }
                    other => Err(Error::ExpectedInteger { pos: tok.offset, got: other }),
                })
            },
        )?
    }
}

impl<'a> Parser<'a> {
    fn parse_expression_list(&mut self) -> Result<Vec<Expression<'a>>, Error<'a>> {
        let mut expr_list = vec![self.expr_bp(0)?];
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            expr_list.push(self.expr_bp(0)?);
        }
        Ok(expr_list)
    }

    fn parse_identifier(&mut self) -> Result<&'a str, Error<'a>> {
        self.lexer.next().ok_or(Error::UnexpectedEnd { pos: self.lexer.position }).and_then(
            |tok| {
                tok.map(|tok| match tok.kind {
                    TokenKind::Identifier(id) => Ok(id),
                    other => {
                        Err(Error::ExpectedIdentifier { pos: self.lexer.position, got: other })
                    }
                })
            },
        )?
    }

    pub fn stmt(mut self) -> Result<Statement<'a>, Error<'a>> {
        let token =
            self.lexer.next().ok_or(Error::UnexpectedEnd { pos: self.lexer.position })??;
        match token.kind {
            TokenKind::Keyword(Keyword::Select) => SelectQuery::parse(self),
            other => Err(Error::Other(other)),
        }
    }

    pub fn parse_unary_op(&mut self, tok: Token<'a>) -> Result<Expression<'a>, Error<'a>> {
        let op = tok.try_into()?;
        let ((), r_bp) = prefix_binding_power(&op)
            .ok_or(Error::InvalidPrefixOperator { op: tok.kind, pos: tok.offset })?;
        let rhs = self.expr_bp(r_bp)?;
        Ok(Expression::UnaryOp((op, Box::new(rhs))))
    }

    pub fn expr(mut self) -> Result<Expression<'a>, Error<'a>> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expression<'a>, Error<'a>> {
        let token =
            self.lexer.next().ok_or(Error::UnexpectedEnd { pos: self.lexer.position })??;
        let mut lhs = match token.kind {
            TokenKind::String(lit) => Expression::Literal(Literal::String(lit)),
            TokenKind::Number(num) => Expression::Literal(Literal::Number(num)),
            TokenKind::Keyword(Keyword::True) => Expression::Literal(Literal::Boolean(true)),
            TokenKind::Keyword(Keyword::False) => Expression::Literal(Literal::Boolean(false)),
            TokenKind::Identifier(id) => Expression::Identifier(id),
            TokenKind::Asterisk => Expression::Wildcard,
            TokenKind::LeftParen => {
                let lhs = self
                    .expr_bp(0)
                    .map_err(|_| Error::UnclosedParenthesis { pos: token.offset })?;
                self.lexer.expect_token(TokenKind::RightParen)?;
                lhs
            }
            TokenKind::Minus | TokenKind::Keyword(Keyword::Not) => self.parse_unary_op(token)?,
            other => return Err(Error::Other(other)),
        };

        while let Some(Ok(token)) = self.lexer.peek() {
            if {
                matches!(
                    token.kind,
                    TokenKind::Comma
                        | TokenKind::RightParen
                        | TokenKind::Semicolon
                        | TokenKind::Keyword(
                            Keyword::From
                                | Keyword::Where
                                | Keyword::Order
                                | Keyword::Desc
                                | Keyword::Asc
                                | Keyword::Limit
                                | Keyword::Offset,
                        ),
                )
            } {
                break;
            }
            let op = Op::try_from(*token)?;
            let (l_bp, r_bp) = infix_binding_power(&op)
                .ok_or(Error::InvalidOperator { op: token.kind, pos: token.offset })?;
            if l_bp < min_bp {
                break;
            }
            self.lexer.next();
            let rhs = self.expr_bp(r_bp)?;
            lhs = Expression::BinaryOp((Box::new(lhs), op, Box::new(rhs)));
        }
        Ok(lhs)
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;
    #[test]
    fn test_parse_non_negative_integer() {
        let mut parser = Parser::new("123");
        assert_eq!(parser.parse_non_negative_integer(), Ok(Some(123)));

        let mut parser = Parser::new("-123");
        assert_eq!(
            parser.parse_non_negative_integer(),
            Err(Error::ExpectedNonNegativeInteger { pos: 0, got: -123 })
        );

        let mut parser = Parser::new("abc");
        assert_eq!(
            parser.parse_non_negative_integer(),
            Err(Error::ExpectedInteger { pos: 0, got: TokenKind::Identifier("abc") })
        );
    }
}
