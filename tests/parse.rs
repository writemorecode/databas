use databas::{
    error::Error,
    lexer::token_kind::TokenKind,
    parser::{
        Parser,
        expr::Expression,
        op::Op,
        stmt::{OrderBy, Ordering, SelectQuery, Statement::Select},
    },
};

#[test]
fn test_parse_plus_exp() {
    let s = "12 + 34";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        Expression::BinaryOp((a, Op::Add, b))
    };
    assert_eq!(Ok(expected), parser.expr())
}

#[test]
fn test_parse_mul_and_plus_exp() {
    let s = "12 + 34 * 56";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        let c = Box::new(Expression::from(56));
        Expression::BinaryOp((a, Op::Add, Box::new(Expression::BinaryOp((b, Op::Mul, c)))))
    };

    assert_eq!(Ok(expected), parser.expr())
}

#[test]
fn test_parse_mul_and_plus_exp_with_parens() {
    let s = "12 + (34 * 56)";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        let c = Box::new(Expression::from(56));
        Expression::BinaryOp((a, Op::Add, Box::new(Expression::BinaryOp((b, Op::Mul, c)))))
    };
    assert_eq!(Ok(expected), parser.expr())
}

#[test]
fn test_parse_not_exp() {
    let s = "not true";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(true));
        Expression::UnaryOp((Op::Not, a))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "not false";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(false));
        Expression::UnaryOp((Op::Not, a))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "not (a AND (b != c))";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::Identifier("a"));
        let b = Box::new(Expression::Identifier("b"));
        let c = Box::new(Expression::Identifier("c"));
        let d = Box::new(Expression::BinaryOp((b, Op::NotEquals, c)));
        let e = Box::new(Expression::BinaryOp((a, Op::And, d)));
        Expression::UnaryOp((Op::Not, e))
    };
    assert_eq!(Ok(expected), parser.expr());
}

#[test]
fn test_negative_exp() {
    let s = "-12";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        Expression::UnaryOp((Op::Sub, a))
    };
    assert_eq!(Ok(expected), parser.expr());
}

#[test]
fn test_invalid_operator() {
    let s = "operand invalid_operator";
    let parser = Parser::new(s);
    let expected_err =
        Error::InvalidOperator { op: TokenKind::Identifier("invalid_operator"), pos: 8 };
    assert_eq!(Err(expected_err), parser.expr());
}

#[test]
fn test_parse_inequality_operators() {
    let s = "12 < 34";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        Expression::BinaryOp((a, Op::LessThan, b))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "12 <= 34";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        Expression::BinaryOp((a, Op::LessThanOrEqual, b))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "12 > 34";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        Expression::BinaryOp((a, Op::GreaterThan, b))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "12 >= 34";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        let b = Box::new(Expression::from(34));
        Expression::BinaryOp((a, Op::GreaterThanOrEqual, b))
    };
    assert_eq!(Ok(expected), parser.expr());
}

#[test]
fn test_parse_select_query() {
    let s = "SELECT abc, def, ghi;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ],
        table: None,
        where_clause: None,
        order_by: None,
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_with_from_table() {
    let s = "SELECT abc, def, ghi FROM table;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ],
        table: Some("table"),
        where_clause: None,
        order_by: None,
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_with_from_table_and_where_clause() {
    let s = "SELECT abc, def, ghi FROM table WHERE abc < def;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ],
        table: Some("table"),
        where_clause: Some(Expression::BinaryOp((
            Box::new(Expression::Identifier("abc")),
            Op::LessThan,
            Box::new(Expression::Identifier("def")),
        ))),
        order_by: None,
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_without_from() {
    let s = "SELECT 3 WHERE 1;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![Expression::from(3)],
        table: None,
        where_clause: Some(Expression::from(1)),
        order_by: None,
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_invalid_select_query() {
    let s = "SELECT";
    let mut parser = Parser::new(s);
    let expected = Err(Error::ExpectedExpression { pos: 6 });
    assert_eq!(expected, parser.stmt());

    let s = "SELECT 1";
    let mut parser = Parser::new(s);
    let expected = Err(Error::ExpectedCommaOrSemicolon { pos: 8 });
    assert_eq!(expected, parser.stmt());

    let s = "SELECT 1,";
    let mut parser = Parser::new(s);
    let expected = Err(Error::ExpectedExpression { pos: 9 });
    assert_eq!(expected, parser.stmt());
}

#[test]
fn test_parse_select_query_with_order_by() {
    let s = "SELECT foo FROM bar WHERE baz ORDER BY qax, quux DESC;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![Expression::Identifier("foo")],
        table: Some("bar"),
        where_clause: Some(Expression::Identifier("baz")),
        order_by: Some(OrderBy {
            terms: vec![Expression::Identifier("qax"), Expression::Identifier("quux")],
            order: Some(Ordering::Descending),
        }),
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());

    let s = "SELECT foo FROM bar WHERE baz ORDER BY qax ASC;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: vec![Expression::Identifier("foo")],
        table: Some("bar"),
        where_clause: Some(Expression::Identifier("baz")),
        order_by: Some(OrderBy {
            terms: vec![Expression::Identifier("qax")],
            order: Some(Ordering::Ascending),
        }),
        limit: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}
