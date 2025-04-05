use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum LexerError {
    UnterminatedString { pos: usize },
    InvalidCharacter { pos: usize, c: char },
    InvalidNumber { pos: usize },
}

impl Display for LexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lexer error: ")?;
        match self {
            LexerError::UnterminatedString { pos } => {
                write!(f, "Unterminated string starting at position {pos}")
            }
            LexerError::InvalidCharacter { c, pos } => {
                write!(f, "Invalid character '{c}' at position {pos}")
            }
            LexerError::InvalidNumber { pos } => {
                write!(f, "Invalid numeric literal at position {pos}")
            }
        }
    }
}
