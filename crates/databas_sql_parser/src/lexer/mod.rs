pub mod token;
pub mod token_kind;

use crate::error::{SQLError, SQLErrorKind};
use token::Token;
use token_kind::{NumberKind, TokenKind};

#[derive(Debug)]
pub struct Lexer<'a> {
    pub source: &'a str,
    pub rest: &'a str,
    pub position: usize,

    pub peeked: Option<Result<Token<'a>, SQLError<'a>>>,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self { source, rest: source, position: 0, peeked: None }
    }

    pub fn expect_where(
        &mut self,
        check: impl Fn(TokenKind<'a>) -> bool,
    ) -> Result<(), SQLError<'a>> {
        match self.next() {
            Some(Ok(token)) if check(token.kind) => Ok(()),
            Some(Ok(token)) => Err(SQLError::new(SQLErrorKind::Other(token.kind), token.offset)),
            Some(Err(err)) => Err(err),
            None => Err(SQLError::new(SQLErrorKind::UnexpectedEnd, self.position)),
        }
    }

    pub fn expect_token(&mut self, expected: TokenKind<'a>) -> Result<(), SQLError<'a>> {
        match self.expect_where(|kind| kind == expected) {
            Err(SQLError { kind: SQLErrorKind::Other(got), pos }) => {
                Err(SQLError { kind: SQLErrorKind::UnexpectedTokenKind { expected, got }, pos })
            }
            other => other,
        }
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

    pub fn peek(&mut self) -> Option<&Result<Token<'a>, SQLError>> {
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
    type Item = Result<Token<'a>, SQLError<'a>>;

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

        let tok = |kind: TokenKind<'a>| -> Option<Result<Token<'a>, SQLError>> {
            Some(Ok(Token { kind, offset: c_at }))
        };

        let started = match c {
            '0'..='9' => Started::Number,
            '"' => Started::DoubleQuotedString,
            '\'' => Started::SingleQuotedString,
            c if c.is_alphabetic() => Started::Keyword,
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

            c => {
                return Some(Err(SQLError::new(
                    SQLErrorKind::InvalidCharacter { c },
                    self.position,
                )));
            }
        };

        match started {
            Started::Number => {
                let literal = c_rest.split(|c: char| !matches!(c, '.' | '0'..='9')).next()?;

                let kind = if let Ok(parsed) = literal.parse::<i32>() {
                    NumberKind::Integer(parsed)
                } else if let Ok(parsed) = literal.parse::<f32>() {
                    NumberKind::Float(parsed)
                } else {
                    return Some(Err(SQLError::new(SQLErrorKind::InvalidNumber, c_at)));
                };

                let token = Token { kind: TokenKind::Number(kind), offset: c_at };
                let extra = literal.len() - c.len_utf8();
                self.position += extra;
                self.rest = &self.rest[extra..];
                Some(Ok(token))
            }
            quote @ (Started::SingleQuotedString | Started::DoubleQuotedString) => {
                let terminator = if let Started::SingleQuotedString = quote { '\'' } else { '"' };
                let Some((literal, rest)) = self.rest.split_once(terminator) else {
                    return Some(Err(SQLError::new(SQLErrorKind::UnterminatedString, c_at)));
                };
                let token = Token { kind: TokenKind::String(literal), offset: c_at };
                self.position += literal.len() + 1;
                self.rest = rest;
                Some(Ok(token))
            }
            Started::Keyword => {
                let is_not_part_of_keyword = |c: char| !(c.is_alphabetic() || c == '_');
                let literal = c_rest.split(is_not_part_of_keyword).next()?;

                let kind = TokenKind::from(literal);
                let token = Token { kind, offset: c_at };

                let extra = literal.len() - c.len_utf8();
                self.position += extra;
                self.rest = &self.rest[extra..];
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
                            return Some(Err(SQLError::new(
                                SQLErrorKind::InvalidCharacter { c: '!' },
                                self.position,
                            )));
                        }
                    }
                };
                let token = Token { kind, offset: c_at };
                Some(Ok(token))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SQLErrorKind;
    use token_kind::Keyword;
    use token_kind::NumberKind::{Float, Integer};

    trait LexerExt {
        fn expect(&mut self, kind: TokenKind, offset: usize);
    }

    impl LexerExt for Lexer<'_> {
        #[track_caller]
        fn expect(&mut self, kind: TokenKind, offset: usize) {
            let expected = Token { kind, offset };
            let got = self.next();
            assert_eq!(Some(Ok(expected)), got);
        }
    }

    #[test]
    fn test_comparison_symbols() {
        let s = " <  <=   >=  >";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::LessThan, 1);
        lexer.expect(TokenKind::LessThanOrEqual, 4);
        lexer.expect(TokenKind::GreaterThanOrEqual, 9);
        lexer.expect(TokenKind::GreaterThan, 13);
    }

    #[test]
    fn test_equality_symbols() {
        let s = "== != =";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::EqualsEquals, 0);
        lexer.expect(TokenKind::NotEquals, 3);
        lexer.expect(TokenKind::Equals, 6);
    }

    #[test]
    fn test_skip_whitespace() {
        let s = "   (";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::LeftParen, 3);
        assert!(lexer.rest.is_empty());
        assert_eq!(lexer.position, s.len());
    }

    #[test]
    fn test_lex_number() {
        let s = "1234";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(1234)), 0);
        assert!(lexer.rest.is_empty());
        assert_eq!(lexer.position, s.len());
    }

    #[test]
    fn test_lex_floating_point_number() {
        let s = "12.345";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Float(12.345f32)), 0);
        assert!(lexer.rest.is_empty());
        assert_eq!(lexer.position, s.len());
    }

    #[test]
    fn test_lex_number_between_whitespace() {
        let s = " 1234 ";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(1234)), 1);
        assert_eq!(lexer.rest, " ");
        assert_eq!(lexer.position, s.len() - 1);
    }

    #[test]
    fn test_double_quoted_string() {
        let s = r#""hello world""#;
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::String("hello world"), 0);
    }

    #[test]
    fn test_single_quoted_string() {
        let s = r#"'hello world'"#;
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::String("hello world"), 0);
    }

    #[test]
    fn test_keywords() {
        let s = "sEleCT * FrOm users whERe user_id < 100 aND NoT is_admin;";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Keyword(Keyword::Select), 0);
        lexer.expect(TokenKind::Asterisk, 7);
        lexer.expect(TokenKind::Keyword(Keyword::From), 9);
        lexer.expect(TokenKind::Identifier("users"), 14);
        lexer.expect(TokenKind::Keyword(Keyword::Where), 20);
        lexer.expect(TokenKind::Identifier("user_id"), 26);
        lexer.expect(TokenKind::LessThan, 34);
        lexer.expect(TokenKind::Number(Integer(100)), 36);
        lexer.expect(TokenKind::Keyword(Keyword::And), 40);
        lexer.expect(TokenKind::Keyword(Keyword::Not), 44);
        lexer.expect(TokenKind::Identifier("is_admin"), 48);
        lexer.expect(TokenKind::Semicolon, 56);

        let s = "INSERT INTO some_table VALUES (a, b, c);";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Keyword(Keyword::Insert), 0);
        lexer.expect(TokenKind::Keyword(Keyword::Into), 7);
        lexer.expect(TokenKind::Identifier("some_table"), 12);
        lexer.expect(TokenKind::Keyword(Keyword::Values), 23);
    }

    #[test]
    fn test_expression() {
        let s = "12 + 23 * (36 / 8)";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(12)), 0);
        lexer.expect(TokenKind::Plus, 3);
        lexer.expect(TokenKind::Number(Integer(23)), 5);
        lexer.expect(TokenKind::Asterisk, 8);
        lexer.expect(TokenKind::LeftParen, 10);
        lexer.expect(TokenKind::Number(Integer(36)), 11);
        lexer.expect(TokenKind::Slash, 14);
        lexer.expect(TokenKind::Number(Integer(8)), 16);
    }

    #[test]
    fn test_unterminated_string() {
        let s = r#""hello world"#;
        let mut lexer = Lexer::new(s);
        assert_eq!(
            lexer.next(),
            Some(Err(SQLError { kind: SQLErrorKind::UnterminatedString, pos: 0 }))
        );
    }

    #[test]
    fn test_line_comment() {
        let s = "3 -- 4 5";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(3)), 0);
        assert_eq!(lexer.next(), None);

        let s = "3 -- 4 5\n6";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(3)), 0);
        lexer.expect(TokenKind::Number(Integer(6)), 9);
    }

    #[test]
    fn test_block_comment() {
        let s = "3 /* 4 5 */ 6";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(3)), 0);
        lexer.expect(TokenKind::Number(Integer(6)), 12);
    }

    #[test]
    fn test_multiline_line_comment() {
        let s = "-- hello world\n-- another comment\n123 * 456";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Number(Integer(123)), 34);
        lexer.expect(TokenKind::Asterisk, 38);
        lexer.expect(TokenKind::Number(Integer(456)), 40);
    }

    #[test]
    fn test_logical_not() {
        let s = "NOT false";
        let mut lexer = Lexer::new(s);
        lexer.expect(TokenKind::Keyword(Keyword::Not), 0);
        lexer.expect(TokenKind::Keyword(Keyword::False), 4);
    }

    #[test]
    fn test_non_ascii_identifier() {
        let s = "åäö";
        let mut lexer = Lexer::new(s);
        let got = lexer.next();
        let expected = Token { kind: TokenKind::Identifier("åäö"), offset: 0 };
        assert_eq!(Some(Ok(expected)), got);
    }
}
