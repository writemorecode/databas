use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum LexerError {
    UnterminatedString { pos: usize },
    InvalidCharacter { c: char },
}

impl Display for LexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lexer error: ")?;
        match self {
            LexerError::UnterminatedString { pos } => {
                write!(f, "Unterminated string at position {}", pos)
            }
            LexerError::InvalidCharacter { c } => {
                write!(f, "Invalid character at position {}", c)
            }
        }
    }
}

#[derive(Debug)]
pub struct UnterminatedStringError {
    pub pos: usize,
}
