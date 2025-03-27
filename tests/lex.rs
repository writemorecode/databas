use databas::lex::{Lexer, Token, TokenKind};

trait LexerExt {
    fn expect(&mut self, kind: TokenKind, offset: usize);
}

impl<'a> LexerExt for Lexer<'a> {
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
    let s = "== != ! =";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::EqualsEquals, 0);
    lexer.expect(TokenKind::NotEquals, 3);
    lexer.expect(TokenKind::Bang, 6);
    lexer.expect(TokenKind::Equals, 8);
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
    lexer.expect(TokenKind::Number(1234), 0);
    assert!(lexer.rest.is_empty());
    assert_eq!(lexer.position, s.len());
}

#[test]
fn test_lex_number_between_whitespace() {
    let s = " 1234 ";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Number(1234), 1);
    assert_eq!(lexer.rest, " ");
    assert_eq!(lexer.position, s.len() - 1);
}

#[test]
fn test_string() {
    let s = r#""hello world""#;
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::String("hello world"), 0);
}

#[test]
fn test_keywords() {
    let s = "SELECT * FROM users;";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Select, 0);
    lexer.expect(TokenKind::Asterisk, 7);
    lexer.expect(TokenKind::From, 9);
    lexer.expect(TokenKind::Identifier("users"), 14);
    lexer.expect(TokenKind::Semicolon, 19);
}

#[test]
fn test_expression() {
    let s = "12 + 23 * (36 / 8)";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Number(12), 0);
    lexer.expect(TokenKind::Plus, 3);
    lexer.expect(TokenKind::Number(23), 5);
    lexer.expect(TokenKind::Asterisk, 8);
    lexer.expect(TokenKind::LeftParen, 10);
    lexer.expect(TokenKind::Number(36), 11);
    lexer.expect(TokenKind::Slash, 14);
    lexer.expect(TokenKind::Number(8), 16);
}
