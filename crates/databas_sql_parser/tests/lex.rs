use databas_sql_parser::error::SQLError;
use databas_sql_parser::error::SQLErrorKind;
use databas_sql_parser::lexer::Lexer;
use databas_sql_parser::lexer::token::Token;
use databas_sql_parser::lexer::token_kind::Keyword;
use databas_sql_parser::lexer::token_kind::NumberKind::Float;
use databas_sql_parser::lexer::token_kind::NumberKind::Integer;
use databas_sql_parser::lexer::token_kind::TokenKind;

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
