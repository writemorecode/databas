pub mod error;
pub mod token;
pub mod token_kind;

use error::LexerError;
use token::Token;
use token_kind::TokenKind;

#[derive(Debug)]
pub struct Lexer<'a> {
    pub source: &'a str,
    pub rest: &'a str,
    pub position: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            rest: source,
            position: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        let trimmed = self.rest.trim_start();
        let whitespace_skipped = self.rest.len() - trimmed.len();
        self.position += whitespace_skipped;
        self.rest = trimmed;
    }

    fn skip_comment(&mut self) -> bool {
        let mut skipped = false;
        if self.rest.starts_with("--") {
            if let Some((comment_text, rest)) = self.rest.split_once("\n") {
                self.position += comment_text.len() + 1;
                self.rest = rest;
                skipped = true;
            } else {
                self.position = self.source.len();
                self.rest = "";
                skipped = false;
            }
        } else if self.rest.starts_with("/*") {
            if let Some((comment_text, rest)) = self.rest.split_once("*/") {
                self.position += comment_text.len() + 2;
                self.rest = rest;
                skipped = true;
            } else {
                self.position = self.source.len();
                self.rest = "";
                skipped = false;
            }
        }
        self.skip_whitespace();
        skipped
    }
}

enum Started {
    Number,
    String,
    Keyword,
    MaybeEqualsOp(MaybeEquals),
}

enum MaybeEquals {
    LessThan,
    GreaterThan,
    Equals,
    Bang,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token<'a>, LexerError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.skip_whitespace();
        loop {
            if !self.skip_comment() {
                break;
            };
        }

        let mut chars = self.rest.chars();
        let c = chars.next()?;
        let c_at = self.position;
        let c_rest = self.rest;
        self.rest = chars.as_str();
        self.position += c.len_utf8();

        let tok = |kind: TokenKind<'a>| -> Option<Result<Token<'a>, LexerError>> {
            Some(Ok(Token { kind, offset: c_at }))
        };

        let started = match c {
            '0'..='9' => Started::Number,
            '"' => Started::String,
            'a'..='z' | 'A'..='Z' => Started::Keyword,
            '<' => Started::MaybeEqualsOp(MaybeEquals::LessThan),
            '>' => Started::MaybeEqualsOp(MaybeEquals::GreaterThan),
            '!' => Started::MaybeEqualsOp(MaybeEquals::Bang),
            '=' => Started::MaybeEqualsOp(MaybeEquals::Equals),
            '(' => return tok(TokenKind::LeftParen),
            ')' => return tok(TokenKind::RightParen),
            '+' => return tok(TokenKind::Plus),
            '-' => return tok(TokenKind::Minus),
            '*' => return tok(TokenKind::Asterisk),
            '/' => return tok(TokenKind::Slash),
            ',' => return tok(TokenKind::Comma),
            ';' => return tok(TokenKind::Semicolon),

            c => return Some(Err(LexerError::InvalidCharacter { c, pos: c_at })),
        };

        match started {
            Started::Number => {
                let literal = c_rest.split(|c: char| !c.is_ascii_digit()).next()?;
                let Ok(parsed) = literal.parse::<i32>() else {
                    return Some(Err(LexerError::InvalidNumber { pos: c_at }));
                };
                let token = Token {
                    kind: TokenKind::Number(parsed),
                    offset: c_at,
                };
                let extra = literal.len() - 1;
                self.position += extra;
                self.rest = &self.rest[extra..];
                Some(Ok(token))
            }
            Started::String => {
                let Some((literal, rest)) = self.rest.split_once('"') else {
                    return Some(Err(LexerError::UnterminatedString { pos: c_at }));
                };
                let token = Token {
                    kind: TokenKind::String(literal),
                    offset: c_at,
                };
                self.position += literal.len() + 1;
                self.rest = rest;
                Some(Ok(token))
            }
            Started::Keyword => {
                let is_not_part_of_keyword = |c| !matches!(c, 'a'..='z' | 'A'..='Z' | '_' );
                let literal = c_rest.split(is_not_part_of_keyword).next()?;

                let kind = TokenKind::from(literal);
                let token = Token { kind, offset: c_at };

                self.position += literal.len() - 1;
                self.rest = &self.rest[literal.len() - 1..];
                Some(Ok(token))
            }
            Started::MaybeEqualsOp(maybe_equals) => {
                let kind = if self.rest.starts_with('=') {
                    self.position += 1;
                    self.rest = &self.rest[1..];
                    match maybe_equals {
                        MaybeEquals::LessThan => TokenKind::LessThanOrEqual,
                        MaybeEquals::GreaterThan => TokenKind::GreaterThanOrEqual,
                        MaybeEquals::Equals => TokenKind::EqualsEquals,
                        MaybeEquals::Bang => TokenKind::NotEquals,
                    }
                } else {
                    match maybe_equals {
                        MaybeEquals::LessThan => TokenKind::LessThan,
                        MaybeEquals::GreaterThan => TokenKind::GreaterThan,
                        MaybeEquals::Equals => TokenKind::Equals,
                        MaybeEquals::Bang => TokenKind::Bang,
                    }
                };
                let token = Token { kind, offset: c_at };
                Some(Ok(token))
            }
        }
    }
}
