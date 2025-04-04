use databas::lexer::Lexer;
use databas::lexer::error::LexerError;
use databas::lexer::token::Token;
use databas::lexer::token_kind::TokenKind;

trait LexerExt {
    fn expect(&mut self, kind: TokenKind, offset: usize);
}

impl LexerExt for Lexer<'_> {
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
    let s = "sEleCT * FrOm users;";
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

#[test]
fn test_unterminated_string() {
    let s = r#""hello world"#;
    let mut lexer = Lexer::new(s);
    assert_eq!(
        lexer.next(),
        Some(Err(LexerError::UnterminatedString { pos: 0 }))
    );
}

#[test]
fn test_line_comment() {
    let s = "3 -- 4 5";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Number(3), 0);
    assert_eq!(lexer.next(), None);

    let s = "3 -- 4 5\n6";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Number(3), 0);
    lexer.expect(TokenKind::Number(6), 9);
}

#[test]
fn test_block_comment() {
    let s = "3 /* 4 5 */ 6";
    let mut lexer = Lexer::new(s);
    lexer.expect(TokenKind::Number(3), 0);
    lexer.expect(TokenKind::Number(6), 12);
}

#[test]
fn test_multiline_line_comment() {
    let s = "-- hello world\n-- another comment\n123 * 456";
    let mut lexer = Lexer::new(&s);
    lexer.expect(TokenKind::Number(123), 34);
    lexer.expect(TokenKind::Asterisk, 38);
    lexer.expect(TokenKind::Number(456), 40);
}
