#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenKind<'a> {
    String(&'a str),
    Number(i32),
    LeftParen,
    RightParen,
    Plus,
    Minus,
    Asterisk,
    Slash,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Token<'a> {
    pub kind: TokenKind<'a>,
    pub lexeme: Option<&'a str>,
    pub offset: usize,
}

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

    fn lex_number(&mut self, rest: &'a str, start: usize) -> Option<Token<'a>> {
        let first_non_digit = rest
            .chars()
            .position(|c| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let literal = &rest[..first_non_digit];
        let parsed = literal.parse::<i32>().unwrap();
        let token = Token {
            kind: TokenKind::Number(parsed),
            lexeme: None,
            offset: start,
        };
        let extra = literal.len() - 1;
        self.position += extra;
        self.rest = &self.rest[extra..];
        Some(token)
    }

    fn lex_string(&mut self, rest: &'a str, start: usize) -> Option<Token<'a>> {
        let first_after_string = self.rest.find('"')?;
        let literal = &rest[..first_after_string + 1 + 1];
        let literal = literal.trim_matches('"');
        let token = Token {
            kind: TokenKind::String(literal),
            lexeme: None,
            offset: start,
        };
        self.position += first_after_string + 1;
        self.rest = &self.rest[first_after_string + 1..];
        Some(token)
    }

    fn skip_whitespace(&mut self) {
        let trimmed = self.rest.trim_start();
        let whitespace_skipped = self.rest.len() - trimmed.len();
        self.position += whitespace_skipped;
        self.rest = trimmed;
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.skip_whitespace();

        let mut chars = self.rest.chars();
        let c = chars.next()?;
        let c_at = self.position;
        let c_rest = self.rest;
        self.rest = chars.as_str();
        self.position += c.len_utf8();

        let tok = |kind: TokenKind<'a>| -> Option<Token<'a>> {
            Some(Token {
                kind,
                lexeme: None,
                offset: c_at,
            })
        };

        let tok = match c {
            '0'..='9' => self.lex_number(c_rest, c_at),
            '"' => self.lex_string(c_rest, c_at),
            '(' => tok(TokenKind::LeftParen),
            ')' => tok(TokenKind::RightParen),

            '+' => tok(TokenKind::Plus),
            '-' => tok(TokenKind::Minus),
            '*' => tok(TokenKind::Asterisk),
            '/' => tok(TokenKind::Slash),

            other => {
                eprintln!("Invalid character '{other}'");
                None
            }
        };
        tok
    }
}
