use std::fmt::Display;

use crate::{
    error::{SQLError, SQLErrorKind},
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::Parser,
};

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    Text,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Int => write!(f, "INT"),
            ColumnType::Float => write!(f, "FLOAT"),
            ColumnType::Text => write!(f, "TEXT"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Column<'a> {
    pub name: &'a str,
    pub column_type: ColumnType,
}

impl Display for Column<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateTableQuery<'a> {
    pub table_name: &'a str,
    pub columns: Vec<Column<'a>>,
}

impl Display for CreateTableQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table_name)?;

        let mut column_iter = self.columns.iter();
        if let Some(first_col) = column_iter.next() {
            write!(f, "{}", first_col)?;
            for col in column_iter {
                write!(f, ", {}", col)?;
            }
        }

        write!(f, ");")
    }
}

impl<'a> Parser<'a> {
    pub fn parse_create_table_query(&mut self) -> Result<CreateTableQuery<'a>, SQLError<'a>> {
        self.lexer.expect_token(TokenKind::Keyword(Keyword::Table))?;
        let table_name = self.parse_identifier()?;

        self.lexer.expect_token(TokenKind::LeftParen)?;

        let mut columns = vec![self.parse_column_definition()?];

        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            columns.push(self.parse_column_definition()?);
        }

        self.lexer.expect_token(TokenKind::RightParen)?;
        self.lexer.expect_token(TokenKind::Semicolon)?;

        Ok(CreateTableQuery { table_name, columns })
    }

    fn parse_column_definition(&mut self) -> Result<Column<'a>, SQLError<'a>> {
        let name = self.parse_identifier()?;

        let column_type = match self.lexer.next() {
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Int), .. })) => ColumnType::Int,
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Float), .. })) => ColumnType::Float,
            Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Text), .. })) => ColumnType::Text,
            Some(Ok(Token { kind, offset })) => {
                return Err(SQLError::new(SQLErrorKind::InvalidDataType { got: kind }, offset));
            }
            Some(Err(e)) => return Err(e),
            None => {
                return Err(SQLError::new(SQLErrorKind::UnexpectedEnd, self.lexer.position));
            }
        };

        Ok(Column { name, column_type })
    }
}
