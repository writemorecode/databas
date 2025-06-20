use crate::lexer::token_kind::TokenKind;
use crate::parser::stmt::create_table::ColumnConstraint;

use std::fmt::Display;

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct SQLError<'a> {
    pub kind: SQLErrorKind<'a>,
    pub pos: usize,
}

impl<'a> SQLError<'a> {
    pub fn new(kind: SQLErrorKind<'a>, pos: usize) -> Self {
        Self { kind, pos }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SQLErrorKind<'a> {
    ExpectedCommaOrSemicolon,
    ExpectedExpression,
    ExpectedIdentifier { got: TokenKind<'a> },
    ExpectedInteger { got: TokenKind<'a> },
    ExpectedNonNegativeInteger { got: i32 },
    ExpectedOther { expected: TokenKind<'a> },
    InvalidCharacter { c: char },
    InvalidNumber,
    InvalidOperator { op: TokenKind<'a> },
    InvalidPrefixOperator { op: TokenKind<'a> },
    InvalidDataType { got: TokenKind<'a> },
    Other(TokenKind<'a>),
    UnclosedParenthesis,
    UnexpectedEnd,
    UnexpectedTokenKind { expected: TokenKind<'a>, got: TokenKind<'a> },
    UnterminatedStatement,
    UnterminatedString,
    DuplicateConstraint { column: &'a str, constraint: ColumnConstraint },
}

impl Display for SQLErrorKind<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SQLErrorKind::UnterminatedString => {
                write!(f, "Unterminated string")
            }
            SQLErrorKind::InvalidCharacter { c } => {
                write!(f, "Invalid character '{c}'")
            }
            SQLErrorKind::InvalidNumber => {
                write!(f, "Invalid numeric literal")
            }
            SQLErrorKind::UnexpectedEnd => {
                write!(f, "Unexpected end of input")
            }
            SQLErrorKind::UnexpectedTokenKind { expected, got } => {
                write!(f, "Unexpected token, got {got}, expected {expected}")
            }
            SQLErrorKind::InvalidPrefixOperator { op } => {
                write!(f, "Invalid prefix operator '{op}'")
            }
            SQLErrorKind::InvalidOperator { op } => {
                write!(f, "Invalid operator '{op}'")
            }
            SQLErrorKind::UnclosedParenthesis => {
                write!(f, "Parenthesis not closed")
            }
            SQLErrorKind::Other(token) => {
                write!(f, "Bad token: {token}")
            }
            SQLErrorKind::ExpectedExpression => {
                write!(f, "Unexpected end of input, expected expression")
            }
            SQLErrorKind::UnterminatedStatement => {
                write!(f, "Unterminated statement, missing semicolon")
            }
            SQLErrorKind::ExpectedOther { expected } => {
                write!(f, "Expected token {expected}")
            }
            SQLErrorKind::ExpectedIdentifier { got } => {
                write!(f, "Expected identifier got token kind {got}")
            }
            SQLErrorKind::ExpectedCommaOrSemicolon => {
                write!(f, "Expected colon or semicolon")
            }
            SQLErrorKind::ExpectedInteger { got } => {
                write!(f, "Expected integer, got token kind {got}")
            }
            SQLErrorKind::ExpectedNonNegativeInteger { got } => {
                write!(f, "Expected non-negative integer, got {got}")
            }
            SQLErrorKind::InvalidDataType { got: data_type } => {
                write!(f, "Invalid data type '{data_type}'")
            }
            SQLErrorKind::DuplicateConstraint { column, constraint } => {
                write!(f, "Duplicate constraint for column '{column}': {constraint}")
            }
        }
    }
}

impl Display for SQLError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error at position {}: {}.", self.pos, self.kind)
    }
}
