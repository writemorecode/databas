use std::fmt::Display;

use crate::lexer::token_kind::TokenKind;

#[derive(Debug, Eq, PartialEq)]
pub struct Token<'a> {
    pub kind: TokenKind<'a>,
    pub offset: usize,
}

impl Display for Token<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Position: {}\t", self.offset)?;
        write!(f, "{}\t", self.kind)?;
        Ok(())
    }
}
