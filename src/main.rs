#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenKind<'a> {
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
struct Token<'a> {
    kind: TokenKind<'a>,
    lexeme: Option<&'a str>,
    offset: usize,
}

#[derive(Debug)]
struct Lexer<'a> {
    source: &'a str,
    rest: &'a str,

    position: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
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

fn main() {
    let source = " 3 + 7 * (9 - 5) / 2 ";
    let lexer = Lexer::new(source);

    let tokens: Vec<_> = lexer.into_iter().collect();
    println!("Source: '{}'", source);
    dbg!(tokens);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_whitespace() {
        let s = "   (";
        let token = Token {
            kind: TokenKind::LeftParen,
            lexeme: None,
            offset: s.find('(').unwrap(),
        };
        let mut lexer = Lexer::new(s);
        let next = lexer.next().unwrap();
        assert_eq!(token, next);
        assert!(lexer.rest.is_empty());
        assert_eq!(lexer.position, s.len());
    }

    #[test]
    fn test_lex_number() {
        let s = "1234";
        let token = Token {
            kind: TokenKind::Number(1234),
            lexeme: None,
            offset: 0,
        };

        let mut lexer = Lexer::new(s);
        let next = lexer.next().unwrap();
        assert_eq!(token, next);
        assert!(lexer.rest.is_empty());
        assert_eq!(lexer.position, s.len());
    }

    #[test]
    fn test_lex_number_between_whitespace() {
        let s = " 1234 ";
        let token = Token {
            kind: TokenKind::Number(1234),
            lexeme: None,
            offset: 1,
        };

        let mut lexer = Lexer::new(s);
        let next = lexer.next().unwrap();
        assert_eq!(token, next);
        assert_eq!(lexer.rest, " ");
        assert_eq!(lexer.position, s.len() - 1);
    }

    #[test]
    fn test_big() {
        let s = "12 + 23 * (36 / 8)";
        let lexer = Lexer::new(s);
        let tokens: Vec<Token> = lexer.into_iter().collect();
        dbg!(tokens);
    }

    #[test]
    fn test_string() {
        let s = r#""hello world""#;
        let mut lexer = Lexer::new(s);
        let token = lexer.next().unwrap();
        let expected = Token {
            kind: TokenKind::String("hello world"),
            lexeme: None,
            offset: 0,
        };
        assert_eq!(expected, token);
    }
}
