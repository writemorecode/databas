pub mod expr;
pub mod op;
pub mod stmt;

use expr::{Expression, Literal};
use op::Op;
use stmt::Statement;
use stmt::lists::{ExpressionList, IdentifierList};

use crate::error::{SQLError, SQLErrorKind};
use crate::lexer::Lexer;
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Keyword, NumberKind, TokenKind};

#[derive(Debug)]
pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Statement<'a>, SQLError<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt() {
            Err(SQLError { kind: SQLErrorKind::UnexpectedEnd, .. }) => None,
            other => Some(other),
        }
    }
}

impl<'a> Parser<'a> {
    pub fn new(source: &'a str) -> Self {
        Self { lexer: Lexer::new(source) }
    }

    fn parse_non_negative_integer(&mut self) -> Result<Option<u32>, SQLError<'a>> {
        let tok = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
        match tok.kind {
            TokenKind::Number(NumberKind::Integer(num)) => Ok(num.try_into().ok()),
            TokenKind::Minus => {
                if let Some(Ok(Token {
                    kind: TokenKind::Number(NumberKind::Integer(num)), ..
                })) = self.lexer.next()
                {
                    Err(SQLError::new(
                        SQLErrorKind::ExpectedNonNegativeInteger { got: -num },
                        tok.offset,
                    ))
                } else {
                    Err(SQLError::new(SQLErrorKind::Other(TokenKind::Minus), tok.offset))
                }
            }
            other => Err(SQLError::new(SQLErrorKind::ExpectedInteger { got: other }, tok.offset)),
        }
    }

    fn parse_expression_list(&mut self) -> Result<ExpressionList<'a>, SQLError<'a>> {
        let mut expr_list = vec![self.expr_bp(0)?];
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            expr_list.push(self.expr_bp(0)?);
        }
        Ok(ExpressionList(expr_list))
    }

    fn parse_identifier_list(&mut self) -> Result<IdentifierList<'a>, SQLError<'a>> {
        let mut expr_list = vec![self.parse_identifier()?];
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            expr_list.push(self.parse_identifier()?);
        }
        Ok(IdentifierList(expr_list))
    }

    fn parse_identifier(&mut self) -> Result<&'a str, SQLError<'a>> {
        self.lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })
            .and_then(|tok| {
                tok.map(|tok| match tok.kind {
                    TokenKind::Identifier(id) => Ok(id),
                    other => Err(SQLError::new(
                        SQLErrorKind::ExpectedIdentifier { got: other },
                        self.lexer.position,
                    )),
                })
            })?
    }

    pub fn stmt(&mut self) -> Result<Statement<'a>, SQLError<'a>> {
        let token = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
        match token.kind {
            TokenKind::Keyword(Keyword::Select) => {
                Ok(Statement::Select(self.parse_select_query()?))
            }
            TokenKind::Keyword(Keyword::Insert) => {
                Ok(Statement::Insert(self.parse_insert_query()?))
            }
            TokenKind::Keyword(Keyword::Create) => {
                Ok(Statement::CreateTable(self.parse_create_table_query()?))
            }
            other => Err(SQLError::new(SQLErrorKind::Other(other), token.offset)),
        }
    }

    pub fn parse_unary_op(&mut self, tok: Token<'a>) -> Result<Expression<'a>, SQLError<'a>> {
        let op: Op = tok.try_into()?;
        let ((), r_bp) = op.prefix_binding_power().ok_or(SQLError::new(
            SQLErrorKind::InvalidPrefixOperator { op: tok.kind },
            tok.offset,
        ))?;
        let rhs = self.expr_bp(r_bp)?;
        Ok(Expression::UnaryOp((op, Box::new(rhs))))
    }

    pub fn expr(mut self) -> Result<Expression<'a>, SQLError<'a>> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expression<'a>, SQLError<'a>> {
        let token = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
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
                    .map_err(|_| SQLError::new(SQLErrorKind::UnclosedParenthesis, token.offset))?;
                self.lexer.expect_token(TokenKind::RightParen)?;
                lhs
            }
            TokenKind::Minus | TokenKind::Keyword(Keyword::Not) => self.parse_unary_op(token)?,
            other => {
                return Err(SQLError::new(SQLErrorKind::Other(other), token.offset));
            }
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
            let (l_bp, r_bp) = op.infix_binding_power().ok_or(SQLError::new(
                SQLErrorKind::InvalidOperator { op: token.kind },
                token.offset,
            ))?;
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
            Err(SQLError { kind: SQLErrorKind::ExpectedNonNegativeInteger { got: -123 }, pos: 0 })
        );

        let mut parser = Parser::new("abc");
        assert_eq!(
            parser.parse_non_negative_integer(),
            Err(SQLError {
                kind: SQLErrorKind::ExpectedInteger { got: TokenKind::Identifier("abc") },
                pos: 0
            })
        );
    }
}
