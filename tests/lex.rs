use databas::lex::{Lexer, Token, TokenKind};

#[test]
fn test_skip_whitespace() {
    let s = "   (";
    let mut lexer = Lexer::new(s);
    let got = lexer.next();
    let expected = Token {
        kind: TokenKind::LeftParen,
        lexeme: None,
        offset: 3,
    };
    assert_eq!(Some(Ok(expected)), got);
    assert!(lexer.rest.is_empty());
    assert_eq!(lexer.position, s.len());
}

#[test]
fn test_lex_number() {
    let s = "1234";
    let mut lexer = Lexer::new(s);
    let next = lexer.next();
    let expected = Token {
        kind: TokenKind::Number(1234),
        lexeme: None,
        offset: 0,
    };
    assert_eq!(Some(Ok(expected)), next);
    assert!(lexer.rest.is_empty());
    assert_eq!(lexer.position, s.len());
}

#[test]
fn test_lex_number_between_whitespace() {
    let s = " 1234 ";
    let mut lexer = Lexer::new(s);
    let got = lexer.next();
    let expected = Token {
        kind: TokenKind::Number(1234),
        lexeme: None,
        offset: 1,
    };
    assert_eq!(Some(Ok(expected)), got);
    assert_eq!(lexer.rest, " ");
    assert_eq!(lexer.position, s.len() - 1);
}

#[test]
fn test_string() {
    let s = r#""hello world""#;
    let mut lexer = Lexer::new(s);
    let got = lexer.next();
    let expected = Token {
        kind: TokenKind::String("hello world"),
        lexeme: None,
        offset: 0,
    };
    assert_eq!(Some(Ok(expected)), got);
}

#[test]
fn test_keywords() {
    let s = "SELECT * FROM users;";
    let mut lexer = Lexer::new(s);

    let mut expect = |kind: TokenKind, lexeme: Option<&str>, offset: usize| {
        let expected = Token {
            kind,
            lexeme,
            offset,
        };
        let got = lexer.next();
        assert_eq!(Some(Ok(expected)), got);
    };

    expect(TokenKind::Select, None, 0);
    expect(TokenKind::Asterisk, None, 7);
    expect(TokenKind::From, None, 9);
    expect(TokenKind::Identifier, Some("users"), 14);
    expect(TokenKind::Semicolon, None, 19);
}

#[test]
fn test_expression() {
    let s = "12 + 23 * (36 / 8)";
    let mut lexer = Lexer::new(s);

    let mut expect = |kind: TokenKind, offset: usize| {
        let expected = Token {
            kind,
            lexeme: None,
            offset,
        };
        let got = lexer.next();
        assert_eq!(Some(Ok(expected)), got);
    };

    expect(TokenKind::Number(12), 0);
    expect(TokenKind::Plus, 3);
    expect(TokenKind::Number(23), 5);
    expect(TokenKind::Asterisk, 8);
    expect(TokenKind::LeftParen, 10);
    expect(TokenKind::Number(36), 11);
    expect(TokenKind::Slash, 14);
    expect(TokenKind::Number(8), 16);
}
