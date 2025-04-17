use databas::{
    error::{SQLError, SQLErrorKind},
    parser::{
        Parser,
        expr::Expression,
        op::Op,
        stmt::{
            Statement::Select,
            lists::ExpressionList,
            select::{OrderBy, Ordering, SelectQuery},
        },
    },
};

#[test]
fn test_parse_select_query() {
    let s = "SELECT abc, def, ghi;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ]),
        table: None,
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_with_from_table() {
    let s = "SELECT abc, def, ghi FROM table;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ]),
        table: Some("table"),
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_with_from_table_and_where_clause() {
    let s = "SELECT abc, def, ghi FROM table WHERE abc < def;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![
            Expression::Identifier("abc"),
            Expression::Identifier("def"),
            Expression::Identifier("ghi"),
        ]),
        table: Some("table"),
        where_clause: Some(Expression::BinaryOp((
            Box::new(Expression::Identifier("abc")),
            Op::LessThan,
            Box::new(Expression::Identifier("def")),
        ))),
        order_by: None,
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_without_from() {
    let s = "SELECT 3 WHERE 1;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::from(3)]),
        table: None,
        where_clause: Some(Expression::from(1)),
        order_by: None,
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_invalid_select_query() {
    let s = "SELECT";
    let mut parser = Parser::new(s);
    let expected = Err(SQLError::new(SQLErrorKind::ExpectedExpression, 6));
    assert_eq!(expected, parser.stmt());

    let s = "SELECT 1";
    let mut parser = Parser::new(s);
    let expected = Err(SQLError::new(SQLErrorKind::ExpectedCommaOrSemicolon, 8));
    assert_eq!(expected, parser.stmt());

    let s = "SELECT 1,";
    let mut parser = Parser::new(s);
    let expected = Err(SQLError::new(SQLErrorKind::ExpectedExpression, 9));
    assert_eq!(expected, parser.stmt());
}

#[test]
fn test_parse_select_query_with_order_by() {
    let s = "SELECT foo FROM bar WHERE baz ORDER BY qax, quux DESC;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: Some(Expression::Identifier("baz")),
        order_by: Some(OrderBy {
            terms: ExpressionList(vec![
                Expression::Identifier("qax"),
                Expression::Identifier("quux"),
            ]),
            order: Some(Ordering::Descending),
        }),
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());

    let s = "SELECT foo FROM bar WHERE baz ORDER BY qax ASC;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: Some(Expression::Identifier("baz")),
        order_by: Some(OrderBy {
            terms: ExpressionList(vec![Expression::Identifier("qax")]),
            order: Some(Ordering::Ascending),
        }),
        limit: None,
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]

fn test_parse_select_query_with_limit() {
    let s = "SELECT foo FROM bar LIMIT 5;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: None,
        order_by: None,
        limit: Some(5),
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());

    let s = "SELECT foo FROM bar WHERE baz ORDER BY qux LIMIT 10;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: Some(Expression::Identifier("baz")),
        order_by: Some(OrderBy {
            terms: ExpressionList(vec![Expression::Identifier("qux")]),
            order: None,
        }),
        limit: Some(10),
        offset: None,
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());

    let s = "SELECT foo LIMIT -1;";
    let mut parser = Parser::new(s);
    let expected = SQLError::new(SQLErrorKind::ExpectedNonNegativeInteger { got: -1 }, 17);
    assert_eq!(Err(expected), parser.stmt());
}

#[test]
fn test_parse_select_query_with_offset() {
    let s = "SELECT foo FROM bar OFFSET 5;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: None,
        order_by: None,
        limit: None,
        offset: Some(5),
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());

    let s = "SELECT foo FROM bar LIMIT 10 OFFSET 5;";
    let mut parser = Parser::new(s);
    let expected_query = SelectQuery {
        columns: ExpressionList(vec![Expression::Identifier("foo")]),
        table: Some("bar"),
        where_clause: None,
        order_by: None,
        limit: Some(10),
        offset: Some(5),
    };
    let expected = Select(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}
