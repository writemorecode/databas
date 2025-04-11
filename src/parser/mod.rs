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
        match self.lexer.next() {
            Some(Ok(Token {
                kind: TokenKind::Identifier(id),
                ..
            })) => Ok(id),
            None => Err(Error::UnexpectedEnd {
                pos: self.lexer.position,
            }),
            Some(Ok(other)) => Err(Error::ExpectedIdentifier {
                pos: other.offset,
                got: other.kind,
            }),
            Some(Err(err)) => Err(err),
        }
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
        let token = match self.lexer.next() {
            None => {
                return Err(Error::UnexpectedEnd {
                    pos: self.lexer.position,
                });
            }
            Some(Err(err)) => return Err(err),
            Some(Ok(token)) => token,
        };

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
            TokenKind::Minus | TokenKind::Keyword(Keyword::Not) => self.parse_unary_op(token)?,
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
                    kind:
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

#[cfg(test)]
mod parser_tests {
    use super::*;
    #[test]
    fn test_parse_expression_list() {
        let s = "abc, def, ghi FROM";
        let mut parser = Parser::new(s);
        let expr_list = parser.parse_expression_list();
        let expected_expr_list: Vec<Expression> = vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ];
        assert_eq!(Ok(expected_expr_list), expr_list);
    }

    #[test]
    fn test_parse_expression_list_single() {
        let s = "abc FROM";
        let mut parser = Parser::new(s);
        let expr_list = parser.parse_expression_list();
        let expected_expr_list: Vec<Expression> = vec![Expression::Identifier("abc")];
        assert_eq!(Ok(expected_expr_list), expr_list);
    }

    #[test]
    fn test_parse_expression_list_single_where() {
        let s = "abc WHERE";
        let mut parser = Parser::new(s);
        let expr_list = parser.parse_expression_list();
        let expected_expr_list: Vec<Expression> = vec![Expression::Identifier("abc")];
        assert_eq!(Ok(expected_expr_list), expr_list);
    }

    #[test]
    fn test_parse_expression_list_single_asc_desc() {
        let s = "abc ASC";
        let mut parser = Parser::new(s);
        let expr_list = parser.parse_expression_list();
        let expected_expr_list: Vec<Expression> = vec![Expression::Identifier("abc")];
        assert_eq!(Ok(expected_expr_list), expr_list);

        let s = "abc DESC";
        let mut parser = Parser::new(s);
        let expr_list = parser.parse_expression_list();
        let expected_expr_list: Vec<Expression> = vec![Expression::Identifier("abc")];
        assert_eq!(Ok(expected_expr_list), expr_list);
    }
}
