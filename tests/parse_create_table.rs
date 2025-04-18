use databas::{
    error::{SQLError, SQLErrorKind},
    lexer::token_kind::TokenKind,
    parser::{
        Parser,
        stmt::{
            Statement::CreateTable,
            create_table::{Column, ColumnType, CreateTableQuery},
        },
    },
};

#[test]
fn test_parse_simple_create_table() {
    let s = "CREATE TABLE users (id INT, name TEXT, age INT);";
    let mut parser = Parser::new(s);

    let expected_query = CreateTableQuery {
        table_name: "users",
        columns: vec![
            Column { name: "id", column_type: ColumnType::Int },
            Column { name: "name", column_type: ColumnType::Text },
            Column { name: "age", column_type: ColumnType::Int },
        ],
    };

    let expected = CreateTable(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_create_table_with_all_types() {
    let s = "CREATE TABLE products (id INT, name TEXT, price FLOAT);";
    let mut parser = Parser::new(s);

    let expected_query = CreateTableQuery {
        table_name: "products",
        columns: vec![
            Column { name: "id", column_type: ColumnType::Int },
            Column { name: "name", column_type: ColumnType::Text },
            Column { name: "price", column_type: ColumnType::Float },
        ],
    };

    let expected = CreateTable(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_create_table_with_single_column() {
    let s = "CREATE TABLE single_column (id INT);";
    let mut parser = Parser::new(s);

    let expected_query = CreateTableQuery {
        table_name: "single_column",
        columns: vec![Column { name: "id", column_type: ColumnType::Int }],
    };

    let expected = CreateTable(expected_query);
    assert_eq!(Ok(expected), parser.stmt());
}

#[test]
fn test_parse_create_table_invalid_column_type() {
    let s = "CREATE TABLE invalid (id INVALID_TYPE);";
    let mut parser = Parser::new(s);

    let err = SQLError {
        kind: SQLErrorKind::InvalidDataType { got: TokenKind::Identifier("INVALID_TYPE") },
        pos: 25,
    };

    assert_eq!(Err(err), parser.stmt());
}

#[test]
fn test_create_table_with_missing_table_name() {
    let s = "CREATE TABLE (id INT);";
    let mut parser = Parser::new(s);

    let err =
        SQLError { kind: SQLErrorKind::ExpectedIdentifier { got: TokenKind::LeftParen }, pos: 14 };

    assert_eq!(Err(err), parser.stmt());
}
