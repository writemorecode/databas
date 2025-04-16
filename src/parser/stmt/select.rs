use std::fmt::Display;

use crate::{
    error::Error,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{Parser, expr::Expression},
};

use super::Statement;

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
    pub terms: Vec<Expression<'a>>,
    pub order: Option<Ordering>,
}

impl<'a> OrderBy<'a> {
    pub fn parse(parser: &mut Parser<'a>) -> Result<Option<OrderBy<'a>>, Error<'a>> {
        let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Order), .. })) = parser.lexer.peek()
        else {
            return Ok(None);
        };
        parser.lexer.next();
        parser.lexer.expect_token(TokenKind::Keyword(Keyword::By))?;
        let terms = parser.parse_expression_list()?;
        let order = match parser.lexer.peek() {
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Asc), .. })) => {
                parser.lexer.next();
                Some(Ordering::Ascending)
            }
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Desc), .. })) => {
                parser.lexer.next();
                Some(Ordering::Descending)
            }
            _ => None,
        };

        Ok(Some(OrderBy { terms, order }))
    }
}

impl Display for OrderBy<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, col) in self.terms.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }

        if let Some(ref order) = self.order {
            write!(f, " {}", order)?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub columns: Vec<Expression<'a>>,
    pub table: Option<&'a str>,
    pub where_clause: Option<Expression<'a>>,
    pub order_by: Option<OrderBy<'a>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Display for SelectQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT ")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }

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

impl<'a> SelectQuery<'a> {
    pub fn parse(parser: &mut Parser<'a>) -> Result<SelectQuery<'a>, Error<'a>> {
        let columns = match parser.parse_expression_list() {
            Err(Error::UnexpectedEnd { pos }) => return Err(Error::ExpectedExpression { pos }),
            Ok(cols) => cols,
            Err(err) => return Err(err),
        };

        let table = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::From), .. })) =
            parser.lexer.peek()
        {
            parser.lexer.next();
            Some(parser.parse_identifier()?)
        } else {
            None
        };

        let where_clause =
            if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Where), .. })) =
                parser.lexer.peek()
            {
                parser.lexer.next();
                Some(parser.expr_bp(0)?)
            } else {
                None
            };

        let order_by = OrderBy::parse(parser)?;

        let limit = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Limit), .. })) =
            parser.lexer.peek()
        {
            parser.lexer.next();
            parser.parse_non_negative_integer()?
        } else {
            None
        };

        let offset = if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Offset), .. })) =
            parser.lexer.peek()
        {
            parser.lexer.next();
            parser.parse_non_negative_integer()?
        } else {
            None
        };

        parser.lexer.expect_token(TokenKind::Semicolon).map_err(|err| match err {
            Error::UnexpectedEnd { pos } => Error::ExpectedCommaOrSemicolon { pos },
            err => err,
        })?;

        Ok(SelectQuery { columns, table, where_clause, order_by, limit, offset })
    }
}

#[derive(Debug, Default)]
pub struct SelectQueryBuilder {
    pub columns: Vec<Expression<'static>>,
    pub table: Option<&'static str>,
    pub where_clause: Option<Expression<'static>>,
    pub order_by: Option<OrderBy<'static>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl SelectQueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn order_by(mut self, order_by: OrderBy<'static>) -> Self {
        self.order_by = Some(order_by);
        self
    }
    pub fn where_clause(mut self, where_clause: Expression<'static>) -> Self {
        self.where_clause = Some(where_clause);
        self
    }
    pub fn table(mut self, table: &'static str) -> Self {
        self.table = Some(table);
        self
    }
    pub fn columns(mut self, columns: Vec<Expression<'static>>) -> Self {
        self.columns = columns;
        self
    }
    pub fn build(self) -> SelectQuery<'static> {
        SelectQuery {
            columns: self.columns,
            table: self.table,
            where_clause: self.where_clause,
            order_by: self.order_by,
            limit: self.limit,
            offset: self.offset,
        }
    }
}
