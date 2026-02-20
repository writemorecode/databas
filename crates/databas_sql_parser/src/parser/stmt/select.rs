use std::fmt::Display;

use crate::{
    error::{SQLError, SQLErrorKind},
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
    pub fn parse_order_by(&mut self) -> Result<Option<OrderBy<'a>>, SQLError<'a>> {
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

        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }

        if let Some(ref offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }

        write!(f, ";")
    }
}

impl<'a> Parser<'a> {
    pub fn parse_select_query(&mut self) -> Result<SelectQuery<'a>, SQLError<'a>> {
        let columns = match self.parse_expression_list() {
            Err(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos }) => {
                return Err(SQLError { kind: SQLErrorKind::ExpectedExpression, pos });
            }
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
            SQLError { kind: SQLErrorKind::UnexpectedEnd, pos } => {
                SQLError { kind: SQLErrorKind::ExpectedCommaOrSemicolon, pos }
            }
            err => err,
        })?;

        Ok(SelectQuery { columns, table, where_clause, order_by, limit, offset })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{SQLError, SQLErrorKind},
        lexer::token_kind::{Keyword, TokenKind},
        parser::{Parser, op::Op, stmt::Statement::Select},
    };

    #[test]
    fn test_parse_select_query() {
        let s = "SELECT abc, def, ghi;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![
                Expression::Identifier("abc"),
                Expression::Identifier("def"),
                Expression::Identifier("ghi"),
            ]),
            table: None,
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_select_query_with_from_table() {
        let s = "SELECT abc, def, ghi FROM big_table;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![
                Expression::Identifier("abc"),
                Expression::Identifier("def"),
                Expression::Identifier("ghi"),
            ]),
            table: Some("big_table"),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_select_query_with_from_table_and_where_clause() {
        let s = "SELECT abc, def, ghi FROM some_table WHERE abc < def;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![
                Expression::Identifier("abc"),
                Expression::Identifier("def"),
                Expression::Identifier("ghi"),
            ]),
            table: Some("some_table"),
            where_clause: Some(Expression::BinaryOp((
                Box::new(Expression::Identifier("abc")),
                Op::LessThan,
                Box::new(Expression::Identifier("def")),
            ))),
            order_by: None,
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_select_query_without_from() {
        let s = "SELECT 3 WHERE 1;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::from(3)]),
            table: None,
            where_clause: Some(Expression::from(1)),
            order_by: None,
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_invalid_select_query() {
        let s = "SELECT";
        let mut parser = Parser::new(s);
        let expected = Err(SQLError::new(SQLErrorKind::ExpectedExpression, 6));
        assert_eq!(expected, parser.stmt());

        let s = "SELECT 1";
        let mut parser = Parser::new(s);
        let expected = Err(SQLError::new(SQLErrorKind::ExpectedCommaOrSemicolon, 8));
        assert_eq!(expected, parser.stmt());

        let s = "SELECT 1,";
        let mut parser = Parser::new(s);
        let expected = Err(SQLError::new(SQLErrorKind::ExpectedExpression, 9));
        assert_eq!(expected, parser.stmt());
    }

    #[test]
    fn test_parse_select_query_with_order_by() {
        let s = "SELECT foo FROM bar WHERE baz ORDER BY qax, quux DESC;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: Some(Expression::Identifier("baz")),
            order_by: Some(OrderBy {
                terms: ExpressionList(vec![
                    Expression::Identifier("qax"),
                    Expression::Identifier("quux"),
                ]),
                order: Some(Ordering::Descending),
            }),
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());

        let s = "SELECT foo FROM bar WHERE baz ORDER BY qax ASC;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: Some(Expression::Identifier("baz")),
            order_by: Some(OrderBy {
                terms: ExpressionList(vec![Expression::Identifier("qax")]),
                order: Some(Ordering::Ascending),
            }),
            limit: None,
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]

    fn test_parse_select_query_with_limit() {
        let s = "SELECT foo FROM bar LIMIT 5;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: None,
            order_by: None,
            limit: Some(5),
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());

        let s = "SELECT foo FROM bar WHERE baz ORDER BY qux LIMIT 10;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: Some(Expression::Identifier("baz")),
            order_by: Some(OrderBy {
                terms: ExpressionList(vec![Expression::Identifier("qux")]),
                order: None,
            }),
            limit: Some(10),
            offset: None,
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());

        let s = "SELECT foo LIMIT -1;";
        let mut parser = Parser::new(s);
        let expected = SQLError::new(SQLErrorKind::ExpectedNonNegativeInteger { got: -1 }, 17);
        assert_eq!(Err(expected), parser.stmt());
    }

    #[test]
    fn test_parse_select_query_with_offset() {
        let s = "SELECT foo FROM bar OFFSET 5;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: Some(5),
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());

        let s = "SELECT foo FROM bar LIMIT 10 OFFSET 5;";
        let mut parser = Parser::new(s);
        let expected_query = SelectQuery {
            columns: ExpressionList(vec![Expression::Identifier("foo")]),
            table: Some("bar"),
            where_clause: None,
            order_by: None,
            limit: Some(10),
            offset: Some(5),
        };
        let expected = Select(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_select_with_invalid_table_name() {
        let s = "SELECT col FROM table;";
        let mut parser = Parser::new(s);
        let got = parser.stmt();
        let expected = SQLError {
            kind: SQLErrorKind::ExpectedIdentifier { got: TokenKind::Keyword(Keyword::Table) },
            pos: 21,
        };
        assert_eq!(Err(expected), got);
    }
}
