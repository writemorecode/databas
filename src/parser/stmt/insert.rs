use std::fmt::Display;

use crate::{
    error::Error,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{Parser, expr::Expression},
};

#[derive(Debug, PartialEq)]
pub struct InsertQuery<'a> {
    pub table: &'a str,
    pub columns: Vec<&'a str>,
    pub values: Vec<Vec<Expression<'a>>>,
}

impl Display for InsertQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns_string: String =
            self.columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
        write!(f, "INSERT INTO {} ({}) VALUES (", self.table, columns_string)?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            let values_string: String =
                v.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
            write!(f, "({values_string})")?;
        }
        write!(f, ";")
    }
}

impl<'a> InsertQuery<'a> {
    pub fn parse(parser: &mut Parser<'a>) -> Result<InsertQuery<'a>, Error<'a>> {
        parser.lexer.expect_token(TokenKind::Keyword(Keyword::Into))?;
        let table = parser.parse_identifier()?;

        parser.lexer.expect_token(TokenKind::LeftParen)?;
        let columns = parser.parse_identifier_list()?;
        parser.lexer.expect_token(TokenKind::RightParen)?;

        parser.lexer.expect_token(TokenKind::Keyword(Keyword::Values))?;

        parser.lexer.expect_token(TokenKind::LeftParen)?;
        let mut values = vec![parser.parse_expression_list()?];
        parser.lexer.expect_token(TokenKind::RightParen)?;
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = parser.lexer.peek() {
            parser.lexer.next();

            parser.lexer.expect_token(TokenKind::LeftParen)?;
            values.push(parser.parse_expression_list()?);
            parser.lexer.expect_token(TokenKind::RightParen)?;
        }

        parser.lexer.expect_token(TokenKind::Semicolon)?;
        Ok(InsertQuery { table, columns, values })
    }
}
