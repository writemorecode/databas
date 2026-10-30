use databas_sql_parser::parser::{Parser, SqlItem, stmt::Statement};

#[test]
fn drop_statements_parse_and_round_trip() {
    for (sql, expected) in [
        ("DROP TABLE items;", Statement::DropTable { name: "items" }),
        ("dRoP InDeX items_name;", Statement::DropIndex { name: "items_name" }),
    ] {
        let parsed = Parser::new(sql).item().unwrap();
        assert_eq!(parsed, SqlItem::Statement(expected));
        let formatted = parsed.to_string();
        assert_eq!(Parser::new(&formatted).item().unwrap(), parsed);
    }
}

#[test]
fn drop_rejects_missing_or_extra_arguments() {
    for sql in [
        "DROP;",
        "DROP VIEW items;",
        "DROP TABLE;",
        "DROP INDEX;",
        "DROP TABLE items",
        "DROP TABLE items, other;",
        "DROP INDEX idx ON items;",
    ] {
        assert!(Parser::new(sql).item().is_err(), "{sql}");
    }
}
