use std::fmt::Display;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenKind<'a> {
    String(&'a str),
    Number(i32),
    LeftParen,
    RightParen,
    Plus,
    Minus,
    LessThan,
    GreaterThan,
    Asterisk,
    Comma,
    Semicolon,
    Slash,
    Identifier,
    Select,
    From,
    Where,
    And,
    Or,
}

impl Display for TokenKind<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::String(s) => write!(f, "STRING ('{s}')"),
            TokenKind::Number(n) => write!(f, "NUMBER ({n})"),
            TokenKind::LeftParen => write!(f, "LP"),
            TokenKind::RightParen => write!(f, "RP"),
            TokenKind::Plus => write!(f, "PLUS"),
            TokenKind::Minus => write!(f, "MINUS"),
            TokenKind::LessThan => write!(f, "LT"),
            TokenKind::GreaterThan => write!(f, "GT"),
            TokenKind::Asterisk => write!(f, "ASTERISK"),
            TokenKind::Comma => write!(f, "COMMA"),
            TokenKind::Semicolon => write!(f, "SEMICOLON"),
            TokenKind::Slash => write!(f, "SLASH"),
            TokenKind::Identifier => write!(f, "IDENT"),
            TokenKind::Select => write!(f, "SELECT"),
            TokenKind::From => write!(f, "FROM"),
            TokenKind::Where => write!(f, "WHERE"),
            TokenKind::And => write!(f, "AND"),
            TokenKind::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Token<'a> {
    pub kind: TokenKind<'a>,
    pub lexeme: Option<&'a str>,
    pub offset: usize,
}

impl Display for Token<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Position: {}\t", self.offset)?;
        write!(f, "{}\t", self.kind)?;
        if let Some(lexeme) = self.lexeme {
            write!(f, "\"{lexeme}\"\t")?;
        }
        Ok(())
    }
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

    fn lex_keyword(&mut self, rest: &'a str, start: usize) -> Option<Token<'a>> {
        let first_after = rest
            .chars()
            .position(|c| !matches!(c, 'a'..='z' | 'A'..='Z' | '_' ))
            .unwrap_or(rest.len());
        let literal = &rest[..first_after];

        let kind = match literal {
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            _ => TokenKind::Identifier,
        };
        let lexeme = if kind == TokenKind::Identifier {
            Some(literal)
        } else {
            None
        };

        let token = Token {
            kind,
            lexeme,
            offset: start,
        };

        self.position += first_after - 1;
        self.rest = &self.rest[first_after - 1..];
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
            'a'..='z' | 'A'..='Z' => self.lex_keyword(c_rest, c_at),

            '(' => tok(TokenKind::LeftParen),
            ')' => tok(TokenKind::RightParen),
            '<' => tok(TokenKind::LessThan),
            '>' => tok(TokenKind::GreaterThan),
            '+' => tok(TokenKind::Plus),
            '-' => tok(TokenKind::Minus),
            '*' => tok(TokenKind::Asterisk),
            '/' => tok(TokenKind::Slash),
            ',' => tok(TokenKind::Comma),
            ';' => tok(TokenKind::Semicolon),

            other => {
                eprintln!("Invalid character '{other}'");
                None
            }
        };
        tok
    }
}
