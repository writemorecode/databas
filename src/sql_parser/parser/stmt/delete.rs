use std::fmt::Display;

use crate::sql_parser::{
    error::SQLError,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{Parser, expr::Expression},
};

#[derive(Debug, PartialEq)]
pub struct DeleteQuery<'a> {
    pub table: &'a str,
    pub where_clause: Option<Expression<'a>>,
}

impl Display for DeleteQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {}", self.table)?;

        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }

        write!(f, ";")
    }
}

impl<'a> Parser<'a> {
    pub fn parse_delete_query(&mut self) -> Result<DeleteQuery<'a>, SQLError<'a>> {
        self.lexer.expect_token(TokenKind::Keyword(Keyword::From))?;
        let table = self.parse_identifier()?;
        let where_clause =
            if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Where), .. })) =
                self.lexer.peek()
            {
                self.lexer.next();
                Some(self.expr_bp(0)?)
            } else {
                None
            };

        self.lexer.expect_token(TokenKind::Semicolon)?;
        Ok(DeleteQuery { table, where_clause })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::parser::{Parser, SqlItem, expr::Expression, op::Op, stmt::Statement};

    #[test]
    fn test_parse_delete_query() {
        let s = "DELETE FROM users;";
        let mut parser = Parser::new(s);
        let expected = DeleteQuery { table: "users", where_clause: None };

        assert_eq!(Some(Ok(SqlItem::Statement(Statement::Delete(expected)))), parser.next());
    }

    #[test]
    fn test_parse_delete_query_with_where_clause() {
        let s = "DELETE FROM users WHERE id == 1;";
        let mut parser = Parser::new(s);
        let expected = DeleteQuery {
            table: "users",
            where_clause: Some(Expression::BinaryOp((
                Box::new(Expression::Identifier("id")),
                Op::EqualsEquals,
                Box::new(Expression::from(1)),
            ))),
        };

        assert_eq!(Some(Ok(SqlItem::Statement(Statement::Delete(expected)))), parser.next());
    }
}
