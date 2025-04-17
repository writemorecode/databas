use databas::parser::{
    Parser,
    expr::{Expression, Literal},
    stmt::{
        Statement,
        insert::{InsertQuery, Values},
        lists::{ExpressionList, IdentifierList},
    },
};

#[test]
fn test_parse_insert_query() {
    let s = "INSERT INTO products (id, name, price) VALUES (123, 'Cake', 45.67), (789, 'Waffles', 10.00);";
    let mut parser = Parser::new(s);
    let got = parser.next();
    let expected = InsertQuery {
        table: "products",
        columns: IdentifierList(vec!["id", "name", "price"]),
        values: Values(vec![
            ExpressionList(vec![
                Expression::from(123),
                Expression::Literal(Literal::String("Cake")),
                Expression::from(45.67f32),
            ]),
            ExpressionList(vec![
                Expression::from(789),
                Expression::Literal(Literal::String("Waffles")),
                Expression::from(10.00f32),
            ]),
        ]),
    };
    assert_eq!(Some(Ok(Statement::Insert(expected))), got);
}
