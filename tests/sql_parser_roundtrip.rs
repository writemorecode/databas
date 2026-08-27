#![allow(clippy::panic, reason = "panics in tests provide clear assertion failures")]

use databas::sql_parser::parser::{Parser, SqlItem, stmt::Statement};
use proptest::prelude::*;

const IDENTIFIERS: &[&str] =
    &["alpha", "beta", "gamma", "delta", "epsilon", "customer_id", "order_total", "created_at"];
const STRING_LITERALS: &[&str] = &["cake", "waffles", "coffee", "tea", "pending", "done"];

fn identifier() -> BoxedStrategy<&'static str> {
    prop::sample::select(IDENTIFIERS).boxed()
}

fn atom(allow_wildcard: bool) -> BoxedStrategy<String> {
    let atoms = prop_oneof![
        identifier().prop_map(str::to_owned),
        (-1000i32..=1000i32).prop_map(|n| n.to_string()),
        (0u32..=1000, 0u32..=99).prop_map(|(whole, fraction)| format!("{whole}.{fraction}")),
        prop::sample::select(STRING_LITERALS).prop_map(|literal| format!("'{literal}'")),
    ];

    if allow_wildcard { prop_oneof![atoms, Just("*".to_owned())].boxed() } else { atoms.boxed() }
}

fn expression(allow_wildcard: bool) -> BoxedStrategy<String> {
    prop_oneof![
        atom(allow_wildcard),
        (
            atom(false),
            atom(false),
            prop::sample::select(&["+", "-", "*", "/", "==", "!=", "<", "<=", "AND"]),
        )
            .prop_map(|(left, right, operator)| format!("{left} {operator} {right}")),
    ]
    .boxed()
}

fn insert_query() -> BoxedStrategy<String> {
    (identifier(), prop::collection::vec(identifier(), 1..=4), 1usize..=4)
        .prop_flat_map(|(table, columns, row_count)| {
            prop::collection::vec(
                prop::collection::vec(expression(false), columns.len()),
                row_count,
            )
            .prop_map(move |rows| {
                let rows = rows
                    .into_iter()
                    .map(|values| format!("({})", values.join(", ")))
                    .collect::<Vec<_>>();
                format!("INSERT INTO {table} ({}) VALUES {};", columns.join(", "), rows.join(", "))
            })
        })
        .boxed()
}

fn select_query() -> BoxedStrategy<String> {
    (
        prop::collection::vec(expression(true), 1..=4),
        prop::option::of(identifier()),
        prop::option::of(expression(false)),
        prop::option::of(prop::collection::vec(
            (identifier(), prop::sample::select(&["", " ASC", " DESC"]))
                .prop_map(|(column, direction)| format!("{column}{direction}")),
            1..=3,
        )),
        prop::option::of(0u32..=1000),
        prop::option::of(0u32..=1000),
    )
        .prop_map(|(columns, table, predicate, order_by, limit, offset)| {
            let mut sql = format!("SELECT {}", columns.join(", "));
            if let Some(table) = table {
                sql.push_str(&format!(" FROM {table}"));
            }
            if let Some(predicate) = predicate {
                sql.push_str(&format!(" WHERE {predicate}"));
            }
            if let Some(order_by) = order_by {
                sql.push_str(&format!(" ORDER BY {}", order_by.join(", ")));
            }
            if let Some(limit) = limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {offset}"));
            }
            sql.push(';');
            sql
        })
        .boxed()
}

fn delete_query() -> BoxedStrategy<String> {
    (identifier(), prop::option::of(expression(false)))
        .prop_map(|(table, predicate)| match predicate {
            Some(predicate) => format!("DELETE FROM {table} WHERE {predicate};"),
            None => format!("DELETE FROM {table};"),
        })
        .boxed()
}

fn update_query() -> BoxedStrategy<String> {
    (
        identifier(),
        prop::collection::vec((identifier(), expression(false)), 1..=4),
        prop::option::of(expression(false)),
    )
        .prop_map(|(table, assignments, predicate)| {
            let assignments = assignments
                .into_iter()
                .map(|(column, value)| format!("{column} = {value}"))
                .collect::<Vec<_>>();
            let mut sql = format!("UPDATE {table} SET {}", assignments.join(", "));
            if let Some(predicate) = predicate {
                sql.push_str(&format!(" WHERE {predicate}"));
            }
            sql.push(';');
            sql
        })
        .boxed()
}

fn create_table_query() -> BoxedStrategy<String> {
    (
        identifier(),
        identifier(),
        prop::collection::vec(
            (identifier(), prop::sample::select(&["INT", "FLOAT", "TEXT"]), any::<bool>()),
            0..=4,
        ),
    )
        .prop_map(|(table, primary_key, extra_columns)| {
            let mut columns = vec![format!("{primary_key} INT PRIMARY KEY")];
            columns.extend(extra_columns.into_iter().map(|(name, column_type, nullable)| {
                let nullable = if nullable { " NULLABLE" } else { "" };
                format!("{name} {column_type}{nullable}")
            }));
            format!("CREATE TABLE {table} ({});", columns.join(", "))
        })
        .boxed()
}

fn create_index_query() -> BoxedStrategy<String> {
    (identifier(), identifier(), prop::collection::vec(identifier(), 1..=5))
        .prop_map(|(index, table, columns)| {
            format!("CREATE INDEX {index} ON {table} ({});", columns.join(", "))
        })
        .boxed()
}

fn transaction_control() -> BoxedStrategy<String> {
    prop::sample::select(&["BEGIN;", "COMMIT;", "ROLLBACK;"]).prop_map(str::to_owned).boxed()
}

fn statement() -> BoxedStrategy<String> {
    prop_oneof![
        insert_query(),
        select_query(),
        create_table_query(),
        create_index_query(),
        select_query().prop_map(|sql| format!("EXPLAIN {sql}")),
        transaction_control(),
        delete_query(),
        update_query(),
    ]
    .boxed()
}

fn parse_statement(sql: &str) -> Statement<'_> {
    Parser::new(sql).stmt().unwrap_or_else(|err| panic!("failed to parse `{sql}`: {err:?}"))
}

fn assert_round_trips(sql: &str) {
    let parsed = parse_statement(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_statement(&displayed);

    assert_eq!(parsed, reparsed, "SQL did not round-trip: {sql}\ndisplayed as: {displayed}");
}

fn parse_item(sql: &str) -> SqlItem<'_> {
    Parser::new(sql).item().unwrap_or_else(|err| panic!("failed to parse `{sql}`: {err:?}"))
}

fn assert_item_round_trips(sql: &str) {
    let parsed = parse_item(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_item(&displayed);

    assert_eq!(parsed, reparsed, "SQL item did not round-trip: {sql}\ndisplayed as: {displayed}");
}

fn parse_items(sql: &str) -> Vec<SqlItem<'_>> {
    Parser::new(sql)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|err| panic!("failed to parse `{sql}`: {err:?}"))
}

fn assert_items_round_trip(sql: &str) {
    let parsed = parse_items(sql);
    let displayed = parsed.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n");
    let reparsed = parse_items(&displayed);

    assert_eq!(parsed, reparsed, "SQL did not round-trip: {sql}\ndisplayed as: {displayed}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(250))]

    #[test]
    fn insert_queries_round_trip_through_display(sql in insert_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn select_queries_round_trip_through_display(sql in select_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn explain_select_queries_round_trip_through_display(sql in select_query().prop_map(|sql| format!("EXPLAIN {sql}"))) {
        assert_round_trips(&sql);
    }

    #[test]
    fn explain_delete_queries_round_trip_through_display(sql in delete_query().prop_map(|sql| format!("EXPLAIN {sql}"))) {
        assert_round_trips(&sql);
    }

    #[test]
    fn explain_update_queries_round_trip_through_display(sql in update_query().prop_map(|sql| format!("EXPLAIN {sql}"))) {
        assert_round_trips(&sql);
    }

    #[test]
    fn delete_queries_round_trip_through_display(sql in delete_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn update_queries_round_trip_through_display(sql in update_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn create_table_queries_round_trip_through_display(sql in create_table_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn create_index_queries_round_trip_through_display(sql in create_index_query()) {
        assert_round_trips(&sql);
    }

    #[test]
    fn multiple_queries_round_trip_through_display(sql in prop::collection::vec(statement(), 1..=8).prop_map(|statements| statements.join("\n"))) {
        assert_items_round_trip(&sql);
    }
}

#[test]
fn explain_select_parses_as_explain_statement() {
    let parsed = parse_statement("EXPLAIN SELECT alpha FROM beta;");

    let Statement::Explain(statement) = parsed else {
        panic!("expected EXPLAIN statement");
    };
    assert!(matches!(statement.as_ref(), Statement::Select(_)));
}

#[test]
fn explain_delete_parses_as_explain_statement() {
    let parsed = parse_statement("EXPLAIN DELETE FROM beta WHERE alpha == 1;");

    let Statement::Explain(statement) = parsed else {
        panic!("expected EXPLAIN statement");
    };
    assert!(matches!(statement.as_ref(), Statement::Delete(_)));
}

#[test]
fn explain_update_parses_as_explain_statement() {
    let parsed = parse_statement("EXPLAIN UPDATE beta SET alpha = 1 WHERE gamma == 2;");

    let Statement::Explain(statement) = parsed else {
        panic!("expected EXPLAIN statement");
    };
    assert!(matches!(statement.as_ref(), Statement::Update(_)));
}

#[test]
fn transaction_control_queries_round_trip_through_display() {
    assert_item_round_trips("BEGIN;");
    assert_item_round_trips("COMMIT;");
    assert_item_round_trips("ROLLBACK;");
}

#[test]
fn transaction_control_queries_round_trip_in_mixed_script() {
    assert_items_round_trip(
        "\
BEGIN;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'Ada');
COMMIT;
BEGIN;
ROLLBACK;
",
    );
}

#[test]
fn parenthesized_expression_display_loses_grouping() {
    let sql = "SELECT (alpha + beta) * gamma;";
    let parsed = parse_statement(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_statement(&displayed);

    assert_eq!(displayed, "SELECT (alpha + beta) * gamma;");
    assert_eq!(parsed, reparsed, "displayed SQL: {displayed}");
}

#[test]
fn float_literals_with_zero_fraction_keep_fractional_display() {
    let sql = "SELECT 0.0;";
    let parsed = parse_statement(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_statement(&displayed);

    assert_eq!(displayed, "SELECT 0.0;");
    assert_eq!(parsed, reparsed, "displayed SQL: {displayed}");
}
