pub mod token;
pub mod token_kind;

use crate::error::Error;
use token::Token;
use token_kind::{NumberKind, TokenKind};

#[derive(Debug)]
pub struct Lexer<'a> {
    pub source: &'a str,
    pub rest: &'a str,
    pub position: usize,

    pub peeked: Option<Result<Token<'a>, Error<'a>>>,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self { source, rest: source, position: 0, peeked: None }
    }

    pub fn expect_where(&mut self, check: impl Fn(TokenKind<'a>) -> bool) -> Result<(), Error<'a>> {
        match self.next() {
            Some(Ok(token)) if check(token.kind) => Ok(()),
            Some(Ok(token)) => Err(Error::Other(token.kind)),
            Some(Err(err)) => Err(err),
            None => Err(Error::UnexpectedEnd { pos: self.position }),
        }
    }

    pub fn expect_token(&mut self, expected_kind: TokenKind<'a>) -> Result<(), Error<'a>> {
        self.expect_where(|kind| kind == expected_kind)
    }

    fn skip_whitespace(&mut self) {
        let trimmed = self.rest.trim_start();
        let whitespace_skipped = self.rest.len() - trimmed.len();
        self.position += whitespace_skipped;
        self.rest = trimmed;
    }

    fn skip_to_next(&mut self, end: &str) {
        if let Some((comment_text, rest)) = self.rest.split_once(end) {
            self.position += comment_text.len() + end.len();
            self.rest = rest;
        } else {
            self.position += self.rest.len();
            self.rest = "";
        }
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            self.skip_whitespace();
            if self.rest.starts_with("--") {
                self.skip_to_next("\n");
            } else if self.rest.starts_with("/*") {
                self.skip_to_next("*/");
            } else {
                break;
            }
        }
    }

    pub fn peek(&mut self) -> Option<&Result<Token<'a>, Error>> {
        if self.peeked.is_some() {
            return self.peeked.as_ref();
        }
        self.peeked = self.next();
        self.peeked.as_ref()
    }
}

enum Started {
    Number,
    DoubleQuotedString,
    SingleQuotedString,
    Keyword,
    MaybeEqualsOp(MaybeEquals),
}

enum MaybeEquals {
    LessThan,
    GreaterThan,
    Equals,
    NotEquals,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token<'a>, Error<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.peeked.take() {
            return Some(next);
        }

        self.skip_whitespace_and_comments();

        let mut chars = self.rest.chars();
        let c = chars.next()?;
        let c_at = self.position;
        let c_rest = self.rest;
        self.rest = chars.as_str();
        self.position += c.len_utf8();

        let tok = |kind: TokenKind<'a>| -> Option<Result<Token<'a>, Error>> {
            Some(Ok(Token { kind, offset: c_at }))
        };

        let started = match c {
            '0'..='9' => Started::Number,
            '"' => Started::DoubleQuotedString,
            '\'' => Started::SingleQuotedString,
            'a'..='z' | 'A'..='Z' => Started::Keyword,
            '<' => Started::MaybeEqualsOp(MaybeEquals::LessThan),
            '>' => Started::MaybeEqualsOp(MaybeEquals::GreaterThan),
            '!' => Started::MaybeEqualsOp(MaybeEquals::NotEquals),
            '=' => Started::MaybeEqualsOp(MaybeEquals::Equals),
            '(' => return tok(TokenKind::LeftParen),
            ')' => return tok(TokenKind::RightParen),
            '+' => return tok(TokenKind::Plus),
            '-' => return tok(TokenKind::Minus),
            '*' => return tok(TokenKind::Asterisk),
            '/' => return tok(TokenKind::Slash),
            ',' => return tok(TokenKind::Comma),
            ';' => return tok(TokenKind::Semicolon),

            c => return Some(Err(Error::InvalidCharacter { c, pos: c_at })),
        };

        match started {
            Started::Number => {
                let literal = c_rest.split(|c: char| !matches!(c, '.' | '0'..='9')).next()?;

                let kind = if let Ok(parsed) = literal.parse::<i32>() {
                    NumberKind::Integer(parsed)
                } else if let Ok(parsed) = literal.parse::<f32>() {
                    NumberKind::Float(parsed)
                } else {
                    return Some(Err(Error::InvalidNumber { pos: c_at }));
                };

                let token = Token { kind: TokenKind::Number(kind), offset: c_at };
                let extra = literal.len() - 1;
                self.position += extra;
                self.rest = &self.rest[extra..];
                Some(Ok(token))
            }
            quote @ (Started::SingleQuotedString | Started::DoubleQuotedString) => {
                let terminator = if let Started::SingleQuotedString = quote { '\'' } else { '"' };
                let Some((literal, rest)) = self.rest.split_once(terminator) else {
                    return Some(Err(Error::UnterminatedString { pos: c_at }));
                };
                let token = Token { kind: TokenKind::String(literal), offset: c_at };
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
                        MaybeEquals::NotEquals => TokenKind::NotEquals,
                    }
                } else {
                    match maybe_equals {
                        MaybeEquals::LessThan => TokenKind::LessThan,
                        MaybeEquals::GreaterThan => TokenKind::GreaterThan,
                        MaybeEquals::Equals => TokenKind::Equals,
                        MaybeEquals::NotEquals => {
                            return Some(Err(Error::InvalidCharacter {
                                pos: self.position,
                                c: '!',
                            }));
                        }
                    }
                };
                let token = Token { kind, offset: c_at };
                Some(Ok(token))
            }
        }
    }
}
