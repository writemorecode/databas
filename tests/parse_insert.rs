use databas::{
    lexer::token_kind::NumberKind,
    parser::{
        Parser,
        expr::{Expression, Literal},
        stmt::{Statement, insert::InsertQuery},
    },
};

#[test]
fn test_insert() {
    let s = "INSERT INTO products (id, name, price) VALUES (123, 'Chocolate Cake', 45.67);";
    let mut parser = Parser::new(s);
    let got = parser.next();
    let expected = InsertQuery {
        table: "products",
        columns: vec!["id", "name", "price"],
        values: vec![vec![
            Expression::Literal(Literal::Number(NumberKind::Integer(123))),
            Expression::Literal(Literal::String("Chocolate Cake")),
            Expression::Literal(Literal::Number(NumberKind::Float(45.67f32))),
        ]],
    };
    assert_eq!(Some(Ok(Statement::Insert(expected))), got);
}
