pub mod expr;
pub mod op;
pub mod stmt;

use expr::{Expression, Literal};
use op::{Op, infix_binding_power, prefix_binding_power};
use stmt::{OrderBy, SelectQuery, Statement};

use crate::error::Error;
use crate::lexer::Lexer;
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Keyword, TokenKind};

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

impl<'a> Parser<'a> {
    fn parse_expression_list(&mut self) -> Result<Vec<Expression<'a>>, Error<'a>> {
        let mut expr_list = vec![self.expr_bp(0)?];
        while let Some(Ok(Token {
            kind: TokenKind::Comma,
            ..
        })) = self.lexer.peek()
        {
            self.lexer.next();
            expr_list.push(self.expr_bp(0)?);
        }
        Ok(expr_list)
    }

    fn parse_identifier(&mut self) -> Result<&'a str, Error<'a>> {
        self.lexer
            .next()
            .ok_or(Error::UnexpectedEnd {
                pos: self.lexer.position,
            })
            .and_then(|tok| {
                tok.map(|tok| match tok.kind {
                    TokenKind::Identifier(id) => Ok(id),
                    other => Err(Error::ExpectedIdentifier {
                        pos: self.lexer.position,
                        got: other,
                    }),
                })
            })?
    }

    fn parse_select_query(&mut self) -> Result<Statement<'a>, Error<'a>> {
        let columns = match self.parse_expression_list() {
            Err(Error::UnexpectedEnd { pos }) => return Err(Error::ExpectedExpression { pos }),
            Ok(cols) => cols,
            Err(err) => return Err(err),
        };

        let table = if let Some(Ok(Token {
            kind: TokenKind::Keyword(Keyword::From),
            ..
        })) = self.lexer.peek()
        {
            self.lexer.next();
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let where_clause = if let Some(Ok(Token {
            kind: TokenKind::Keyword(Keyword::Where),
            ..
        })) = self.lexer.peek()
        {
            self.lexer.next();
            Some(self.expr_bp(0)?)
        } else {
            None
        };

        let order_by = OrderBy::parse(self)?;

        self.lexer
            .expect_token(TokenKind::Semicolon)
            .map_err(|err| match err {
                Error::UnexpectedEnd { pos } => Error::ExpectedCommaOrSemicolon { pos },
                err => err,
            })?;

        Ok(Statement::Select(SelectQuery {
            columns,
            table,
            where_clause,
            order_by,
            limit: None,
        }))
    }

    pub fn stmt(&mut self) -> Result<Statement<'a>, Error<'a>> {
        let token = self.lexer.next().ok_or(Error::UnexpectedEnd {
            pos: self.lexer.position,
        })??;
        match token.kind {
            TokenKind::Keyword(Keyword::Select) => self.parse_select_query(),
            other => Err(Error::Other(other)),
        }
    }

    pub fn parse_unary_op(&mut self, tok: Token<'a>) -> Result<Expression<'a>, Error<'a>> {
        let op = tok.try_into()?;
        let ((), r_bp) = prefix_binding_power(&op).ok_or(Error::InvalidPrefixOperator {
            op: tok.kind,
            pos: tok.offset,
        })?;
        let rhs = self.expr_bp(r_bp)?;
        Ok(Expression::UnaryOp((op, Box::new(rhs))))
    }

    pub fn expr(mut self) -> Result<Expression<'a>, Error<'a>> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expression<'a>, Error<'a>> {
        let token = self.lexer.next().ok_or(Error::UnexpectedEnd {
            pos: self.lexer.position,
        })??;
        let mut lhs = match token.kind {
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
                                | Keyword::Asc,
                        ),
                )
            } {
                break;
            }
            let op = Op::try_from(*token)?;
            let (l_bp, r_bp) = infix_binding_power(&op).ok_or(Error::InvalidOperator {
                op: token.kind,
                pos: token.offset,
            })?;
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
