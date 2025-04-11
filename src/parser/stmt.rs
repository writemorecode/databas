use std::fmt::Display;

use crate::{
    error::Error,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
};

use super::{Parser, expr::Expression};

#[derive(Debug, PartialEq)]
pub enum Statement<'a> {
    Select(SelectQuery<'a>),
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub columns: Vec<Expression<'a>>,
    pub table: Option<&'a str>,
    pub where_clause: Option<Expression<'a>>,
    pub order_by: Option<OrderBy<'a>>,
    pub limit: Option<u32>,
}

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
        let Some(res) = parser.lexer.peek() else {
            return Ok(None);
        };

        let Ok(Token {
            kind: TokenKind::Keyword(Keyword::Order),
            ..
        }) = res
        else {
            return Ok(None);
        };

        parser.lexer.next();
        parser.lexer.expect_token(TokenKind::Keyword(Keyword::By))?;
        let terms = parser.parse_expression_list()?;

        let Some(peeked) = parser.lexer.peek() else {
            return Err(Error::UnexpectedEnd {
                pos: parser.lexer.position,
            });
        };
        let Ok(token) = peeked else {
            return Err(parser.lexer.next().unwrap().unwrap_err());
        };

        let order = match token.kind {
            TokenKind::Keyword(Keyword::Asc) => {
                parser.lexer.next();
                Some(Ordering::Ascending)
            }
            TokenKind::Keyword(Keyword::Desc) => {
                parser.lexer.next();
                Some(Ordering::Descending)
            }
            TokenKind::Semicolon => None,
            other => return Err(Error::Other(other)),
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

        writeln!(f, ";")
    }
}

impl Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Select(query) => query.fmt(f),
        }
    }
}
