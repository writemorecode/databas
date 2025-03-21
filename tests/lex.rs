use databas::lex::{Lexer, Token, TokenKind};

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

#[test]
fn test_keywords() {
    let s = "SELECT * FROM users;";
    let mut lexer = Lexer::new(s);
    let expected = [
        Token {
            kind: TokenKind::Select,
            lexeme: None,
            offset: 0,
        },
        Token {
            kind: TokenKind::Asterisk,
            lexeme: None,
            offset: 7,
        },
        Token {
            kind: TokenKind::From,
            lexeme: None,
            offset: 9,
        },
        Token {
            kind: TokenKind::Identifier,
            lexeme: Some("users"),
            offset: 14,
        },
        Token {
            kind: TokenKind::Semicolon,
            lexeme: None,
            offset: 19,
        },
    ];

    for t in expected {
        let got = lexer.next();
        assert_eq!(got, Some(t));
    }
}
