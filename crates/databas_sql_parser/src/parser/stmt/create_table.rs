use std::{collections::HashSet, fmt::Display};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnConstraint {
    PrimaryKey,
    Nullable,
}

impl Display for ColumnConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnConstraint::PrimaryKey => write!(f, "PRIMARY KEY"),
            ColumnConstraint::Nullable => write!(f, "NULLABLE"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Column<'a> {
    pub name: &'a str,
    pub column_type: ColumnType,
    pub constraints: HashSet<ColumnConstraint>,
}

impl Display for Column<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)?;
        if let Some(constraint) = self.constraints.iter().next() {
            write!(f, " {}", constraint)?;
            for constraint in self.constraints.iter().skip(1) {
                write!(f, " {}", constraint)?;
            }
        }
        Ok(())
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

        let columns = self.parse_comma_separated_list(|p| p.parse_column_definition())?;

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


        let mut constraints = HashSet::new();
        while let Some(Ok(token)) = self.lexer.peek() {
            let pos = token.offset;
            match &token.kind {
                TokenKind::Keyword(Keyword::Primary) => {
                    self.lexer.next();
                    self.lexer.expect_token(TokenKind::Keyword(Keyword::Key))?;
                    if !constraints.insert(ColumnConstraint::PrimaryKey) {
                        return Err(SQLError::new(
                            SQLErrorKind::DuplicateConstraint {
                                column: name,
                                constraint: ColumnConstraint::PrimaryKey,
                            },
                            pos,
                        ));
                    }
                }
                TokenKind::Keyword(Keyword::Nullable) => {
                    self.lexer.next();
                    if !constraints.insert(ColumnConstraint::Nullable) {
                        return Err(SQLError::new(
                            SQLErrorKind::DuplicateConstraint {
                                column: name,
                                constraint: ColumnConstraint::Nullable,
                            },
                            pos,
                        ));
                    }
                }
                _ => break,
            }
        }

        Ok(Column { name, column_type, constraints })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{SQLError, SQLErrorKind},
        lexer::token_kind::TokenKind,
        parser::{Parser, stmt::Statement::CreateTable},
    };

    #[test]
    fn test_parse_simple_create_table() {
        let s = "CREATE TABLE users (id INT, name TEXT, age INT);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "users",
            columns: vec![
                Column { name: "id", column_type: ColumnType::Int, constraints: HashSet::new() },
                Column { name: "name", column_type: ColumnType::Text, constraints: HashSet::new() },
                Column { name: "age", column_type: ColumnType::Int, constraints: HashSet::new() },
            ],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_create_table_with_all_types() {
        let s = "CREATE TABLE products (id INT, name TEXT, price FLOAT);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "products",
            columns: vec![
                Column { name: "id", column_type: ColumnType::Int, constraints: HashSet::new() },
                Column { name: "name", column_type: ColumnType::Text, constraints: HashSet::new() },
                Column {
                    name: "price",
                    column_type: ColumnType::Float,
                    constraints: HashSet::new(),
                },
            ],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_create_table_with_single_column() {
        let s = "CREATE TABLE single_column (id INT);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "single_column",
            columns: vec![Column {
                name: "id",
                column_type: ColumnType::Int,
                constraints: HashSet::new(),
            }],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_parse_create_table_invalid_column_type() {
        let s = "CREATE TABLE invalid (id INVALID_TYPE);";
        let mut parser = Parser::new(s);

        let err = SQLError {
            kind: SQLErrorKind::InvalidDataType { got: TokenKind::Identifier("INVALID_TYPE") },
            pos: 25,
        };

        assert_eq!(Err(err), parser.stmt());
    }

    #[test]
    fn test_create_table_with_missing_table_name() {
        let s = "CREATE TABLE (id INT);";
        let mut parser = Parser::new(s);

        let err = SQLError {
            kind: SQLErrorKind::ExpectedIdentifier { got: TokenKind::LeftParen },
            pos: 14,
        };

        assert_eq!(Err(err), parser.stmt());
    }

    #[test]
    fn test_create_table_with_primary_key_constraint() {
        let s = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "users",
            columns: vec![
                Column {
                    name: "id",
                    column_type: ColumnType::Int,
                    constraints: HashSet::from([ColumnConstraint::PrimaryKey]),
                },
                Column { name: "name", column_type: ColumnType::Text, constraints: HashSet::new() },
            ],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_create_table_with_nullable_constraint() {
        let s = "CREATE TABLE users (id INT, name TEXT NULLABLE);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "users",
            columns: vec![
                Column { name: "id", column_type: ColumnType::Int, constraints: HashSet::new() },
                Column {
                    name: "name",
                    column_type: ColumnType::Text,
                    constraints: HashSet::from_iter(vec![ColumnConstraint::Nullable]),
                },
            ],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_columns_not_nullable_by_default() {
        let s = "CREATE TABLE test (a INT);";
        let mut parser = Parser::new(s);

        let expected_query = CreateTableQuery {
            table_name: "test",
            columns: vec![Column {
                name: "a",
                column_type: ColumnType::Int,
                constraints: HashSet::new(),
            }],
        };

        let expected = CreateTable(expected_query);
        assert_eq!(Ok(expected), parser.stmt());
    }

    #[test]
    fn test_create_table_with_duplicate_nullable_constraint() {
        let s = "CREATE TABLE users (id INT NULLABLE NULLABLE, name TEXT);";
        let mut parser = Parser::new(s);

        let err = SQLError {
            kind: SQLErrorKind::DuplicateConstraint {
                column: "id",
                constraint: ColumnConstraint::Nullable,
            },
            pos: 36,
        };

        assert_eq!(Err(err), parser.stmt());
    }

    #[test]
    fn test_create_table_with_duplicate_primary_key_constraint() {
        let s = "CREATE TABLE users (id INT PRIMARY KEY PRIMARY KEY, name TEXT);";
        let mut parser = Parser::new(s);

        let err = SQLError {
            kind: SQLErrorKind::DuplicateConstraint {
                column: "id",
                constraint: ColumnConstraint::PrimaryKey,
            },
            pos: 39,
        };

        assert_eq!(Err(err), parser.stmt());
    }
}
