use std::fmt::Display;

use crate::sql_parser::{
    error::SQLError,
    lexer::token_kind::{Keyword, TokenKind},
    parser::{Parser, stmt::lists::IdentifierList},
};

#[derive(Debug, PartialEq)]
pub struct CreateIndexQuery<'a> {
    pub index_name: &'a str,
    pub table_name: &'a str,
    pub columns: IdentifierList<'a>,
}

impl Display for CreateIndexQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE INDEX {} ON {} ({});", self.index_name, self.table_name, self.columns)
    }
}

impl<'a> Parser<'a> {
    pub fn parse_create_index_query(&mut self) -> Result<CreateIndexQuery<'a>, SQLError<'a>> {
        let index_name = self.parse_identifier()?;
        self.lexer.expect_token(TokenKind::Keyword(Keyword::On))?;
        let table_name = self.parse_identifier()?;

        self.lexer.expect_token(TokenKind::LeftParen)?;
        let columns = self.parse_identifier_list()?;
        self.lexer.expect_token(TokenKind::RightParen)?;
        self.lexer.expect_token(TokenKind::Semicolon)?;

        Ok(CreateIndexQuery { index_name, table_name, columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::parser::{
        Parser,
        stmt::{Statement, lists::IdentifierList},
    };

    #[test]
    fn test_parse_simple_create_index() {
        let s = "CREATE INDEX idx_users_name ON users (name);";
        let mut parser = Parser::new(s);

        let expected = CreateIndexQuery {
            index_name: "idx_users_name",
            table_name: "users",
            columns: IdentifierList(vec!["name"]),
        };

        assert_eq!(Some(Ok(Statement::CreateIndex(expected))), parser.next());
    }

    #[test]
    fn test_parse_multi_column_create_index() {
        let s = "CREATE INDEX idx_orders_customer_date ON orders (customer_id, created_at);";
        let mut parser = Parser::new(s);

        let expected = CreateIndexQuery {
            index_name: "idx_orders_customer_date",
            table_name: "orders",
            columns: IdentifierList(vec!["customer_id", "created_at"]),
        };

        assert_eq!(Some(Ok(Statement::CreateIndex(expected))), parser.next());
    }

    #[test]
    fn test_parse_create_index_rejects_empty_columns() {
        let s = "CREATE INDEX idx ON users ();";
        let mut parser = Parser::new(s);

        assert!(parser.next().unwrap().is_err());
    }
}
