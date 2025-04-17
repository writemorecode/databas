use std::fmt::Display;

use crate::{
    error::Error,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{Parser, expr::Expression, stmt::lists::ExpressionList},
};
#[derive(Debug, PartialEq)]
pub enum Ordering {
    Ascending,
    Descending,
}

impl Display for Ordering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ordering::Ascending => write!(f, "ASC"),
            Ordering::Descending => write!(f, "DESC"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct OrderBy<'a> {
    pub terms: ExpressionList<'a>,
    pub order: Option<Ordering>,
}

impl<'a> Parser<'a> {
    pub fn parse_order_by(&mut self) -> Result<Option<OrderBy<'a>>, Error<'a>> {
        let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Order), .. })) = self.lexer.peek()
        else {
            return Ok(None);
        };
        self.lexer.next();
        self.lexer.expect_token(TokenKind::Keyword(Keyword::By))?;
        let terms = self.parse_expression_list()?;
        let order = match self.lexer.peek() {
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Asc), .. })) => {
                self.lexer.next();
                Some(Ordering::Ascending)
            }
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Desc), .. })) => {
                self.lexer.next();
                Some(Ordering::Descending)
            }
            _ => None,
        };

        Ok(Some(OrderBy { terms, order }))
    }
}

impl Display for OrderBy<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.terms)?;

        if let Some(ref order) = self.order {
            write!(f, " {}", order)?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub columns: ExpressionList<'a>,
    pub table: Option<&'a str>,
    pub where_clause: Option<Expression<'a>>,
    pub order_by: Option<OrderBy<'a>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Display for SelectQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT {}", self.columns)?;

        if let Some(table) = self.table {
            write!(f, " FROM {}", table)?;
        }
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }

        if let Some(ref order_by_clause) = self.order_by {
            write!(f, " ORDER BY {}", order_by_clause)?;
        }

        write!(f, ";")
    }
}

impl<'a> Parser<'a> {
    pub fn parse_select_query(&mut self) -> Result<SelectQuery<'a>, Error<'a>> {
        let columns = match self.parse_expression_list() {
            Err(Error::UnexpectedEnd { pos }) => return Err(Error::ExpectedExpression { pos }),
            Ok(cols) => cols,
            Err(err) => return Err(err),
        };

        let table = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::From), .. })) =
            self.lexer.peek()
        {
            self.lexer.next();
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let where_clause =
            if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Where), .. })) =
                self.lexer.peek()
            {
                self.lexer.next();
                Some(self.expr_bp(0)?)
            } else {
                None
            };

        let order_by = self.parse_order_by()?;

        let limit = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Limit), .. })) =
            self.lexer.peek()
        {
            self.lexer.next();
            self.parse_non_negative_integer()?
        } else {
            None
        };

        let offset = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Offset), .. })) =
            self.lexer.peek()
        {
            self.lexer.next();
            self.parse_non_negative_integer()?
        } else {
            None
        };

        self.lexer.expect_token(TokenKind::Semicolon).map_err(|err| match err {
            Error::UnexpectedEnd { pos } => Error::ExpectedCommaOrSemicolon { pos },
            err => err,
        })?;

        Ok(SelectQuery { columns, table, where_clause, order_by, limit, offset })
    }
}
