use databas_sql_parser::parser::{
    Parser,
    expr::{AggregateFunction, Expression},
    stmt::{
        Statement::{self},
        lists::ExpressionList,
        select::SelectQuery,
    },
};

#[test]
fn test_all_aggregate_functions() {
    let sql = "SELECT COUNT(*), SUM(price), AVG(price), STDDEV(price), MAX(price), MIN(price) FROM products;";
    let mut parser = Parser::new(sql);
    let query = parser.stmt();

    let expected_query = Statement::Select(SelectQuery {
        table: Some("products"),
        columns: ExpressionList(vec![
            Expression::AggregateFunction(AggregateFunction::Count(Box::new(Expression::Wildcard))),
            Expression::AggregateFunction(AggregateFunction::Sum(Box::new(
                Expression::Identifier("price"),
            ))),
            Expression::AggregateFunction(AggregateFunction::Avg(Box::new(
                Expression::Identifier("price"),
            ))),
            Expression::AggregateFunction(AggregateFunction::StdDev(Box::new(
                Expression::Identifier("price"),
            ))),
            Expression::AggregateFunction(AggregateFunction::Max(Box::new(
                Expression::Identifier("price"),
            ))),
            Expression::AggregateFunction(AggregateFunction::Min(Box::new(
                Expression::Identifier("price"),
            ))),
        ]),
        where_clause: None,
        order_by: None,
        limit: None,
        offset: None,
    });
    assert_eq!(query, Ok(expected_query));
}
