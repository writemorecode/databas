use crate::lexer::token_kind::TokenKind;

use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum Error<'a> {
    UnterminatedString {
        pos: usize,
    },
    InvalidCharacter {
        pos: usize,
        c: char,
    },
    InvalidNumber {
        pos: usize,
    },
    UnexpectedEnd {
        pos: usize,
    },
    UnexpectedTokenKind {
        expected: TokenKind<'a>,
        got: TokenKind<'a>,
    },
    InvalidPrefixOperator {
        op: TokenKind<'a>,
        pos: usize,
    },
    Other(TokenKind<'a>),
    UnclosedParenthesis {
        pos: usize,
    },
}

impl Display for Error<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnterminatedString { pos } => {
                write!(f, "Unterminated string starting at position {pos}")
            }
            Error::InvalidCharacter { c, pos } => {
                write!(f, "Invalid character '{c}' at position {pos}")
            }
            Error::InvalidNumber { pos } => {
                write!(f, "Invalid numeric literal at position {pos}")
            }
            Error::UnexpectedEnd { pos } => {
                write!(f, "Unexpected end of input at position {pos}.")
            }
            Error::UnexpectedTokenKind { expected, got } => {
                write!(f, "Unexpected token, got {got}, expected {expected}.")
            }
            Error::InvalidPrefixOperator { op, pos } => {
                write!(f, "Invalid prefix operator '{op}' at position {pos}.")
            }
            Error::UnclosedParenthesis { pos } => {
                write!(f, "Parenthesis at position {pos} not closed.")
            }
            Error::Other(token) => {
                write!(f, "Bad token: {token}")
            }
        }
    }
}
