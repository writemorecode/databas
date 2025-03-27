use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum LexerError {
    UnterminatedString { pos: usize },
    InvalidCharacter { pos: usize, c: char },
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
        }
    }
}

#[derive(Debug)]
pub struct UnterminatedStringError {
    pub pos: usize,
}
