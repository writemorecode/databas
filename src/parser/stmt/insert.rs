use std::fmt::Display;

use crate::{
    error::Error,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{
        Parser,
        stmt::lists::{ExpressionList, IdentifierList},
    },
};

#[derive(Debug, PartialEq)]
pub struct Values<'a>(pub Vec<ExpressionList<'a>>);

impl Display for Values<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.0.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ");
        write!(f, "{s}")
    }
}

impl<'a> Parser<'a> {
    fn parse_values(&mut self) -> Result<Values<'a>, Error<'a>> {
        self.lexer.expect_token(TokenKind::LeftParen)?;
        let mut values = vec![self.parse_expression_list()?];
        self.lexer.expect_token(TokenKind::RightParen)?;
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            self.lexer.expect_token(TokenKind::LeftParen)?;
            values.push(self.parse_expression_list()?);
            self.lexer.expect_token(TokenKind::RightParen)?;
        }
        Ok(Values(values))
    }
}

#[derive(Debug, PartialEq)]
pub struct InsertQuery<'a> {
    pub table: &'a str,
    pub columns: IdentifierList<'a>,
    pub values: Values<'a>,
}

impl Display for InsertQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSERT INTO {} ({}) VALUES ({});", self.table, self.columns, self.values)
    }
}

impl<'a> Parser<'a> {
    pub fn parse_insert_query(&mut self) -> Result<InsertQuery<'a>, Error<'a>> {
        self.lexer.expect_token(TokenKind::Keyword(Keyword::Into))?;
        let table = self.parse_identifier()?;

        self.lexer.expect_token(TokenKind::LeftParen)?;
        let columns = self.parse_identifier_list()?;
        self.lexer.expect_token(TokenKind::RightParen)?;

        self.lexer.expect_token(TokenKind::Keyword(Keyword::Values))?;

        let values = self.parse_values()?;
        self.lexer.expect_token(TokenKind::Semicolon)?;
        Ok(InsertQuery { table, columns, values })
    }
}
