use databas::sql_parser::parser::{Parser, stmt::Statement};
use hegel::TestCase;
use hegel::generators as gs;

const IDENTIFIERS: &[&str] =
    &["alpha", "beta", "gamma", "delta", "epsilon", "customer_id", "order_total", "created_at"];

const STRING_LITERALS: &[&str] = &["cake", "waffles", "coffee", "tea", "pending", "done"];

fn draw_bool(tc: &TestCase) -> bool {
    tc.draw(gs::booleans())
}

fn draw_index(tc: &TestCase, len: usize) -> usize {
    tc.draw(gs::integers::<usize>().min_value(0).max_value(len - 1))
}

fn draw_identifier(tc: &TestCase) -> &'static str {
    IDENTIFIERS[draw_index(tc, IDENTIFIERS.len())]
}

fn draw_non_empty_len(tc: &TestCase, max: usize) -> usize {
    tc.draw(gs::integers::<usize>().min_value(1).max_value(max))
}

fn draw_u32(tc: &TestCase) -> u32 {
    tc.draw(gs::integers::<u32>().min_value(0).max_value(1000))
}

fn draw_i32(tc: &TestCase) -> i32 {
    tc.draw(gs::integers::<i32>().min_value(-1000).max_value(1000))
}

fn draw_atom(tc: &TestCase, allow_wildcard: bool) -> String {
    let variant_count = if allow_wildcard { 4 } else { 3 };
    match draw_index(tc, variant_count) {
        0 => draw_identifier(tc).to_string(),
        1 => draw_i32(tc).to_string(),
        2 => {
            let lit = STRING_LITERALS[draw_index(tc, STRING_LITERALS.len())];
            format!("'{lit}'")
        }
        3 => "*".to_string(),
        _ => unreachable!(),
    }
}

fn draw_expression(tc: &TestCase, allow_wildcard: bool) -> String {
    if draw_bool(tc) {
        return draw_atom(tc, allow_wildcard);
    }

    let left = draw_atom(tc, false);
    let right = draw_atom(tc, false);
    let op = match draw_index(tc, 9) {
        0 => "+",
        1 => "-",
        2 => "*",
        3 => "/",
        4 => "==",
        5 => "!=",
        6 => "<",
        7 => "<=",
        8 => "AND",
        _ => unreachable!(),
    };
    format!("{left} {op} {right}")
}

fn draw_expression_list(tc: &TestCase, max_len: usize, allow_wildcard: bool) -> Vec<String> {
    (0..draw_non_empty_len(tc, max_len)).map(|_| draw_expression(tc, allow_wildcard)).collect()
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

fn draw_insert(tc: &TestCase) -> String {
    let table = draw_identifier(tc);
    let column_count = draw_non_empty_len(tc, 4);
    let columns = (0..column_count).map(|_| draw_identifier(tc)).collect::<Vec<_>>();
    let row_count = draw_non_empty_len(tc, 4);
    let rows = (0..row_count)
        .map(|_| {
            let values = (0..column_count).map(|_| draw_expression(tc, false)).collect::<Vec<_>>();
            format!("({})", values.join(", "))
        })
        .collect::<Vec<_>>();

    format!("INSERT INTO {table} ({}) VALUES {};", columns.join(", "), rows.join(", "))
}

fn draw_select(tc: &TestCase) -> String {
    let columns = draw_expression_list(tc, 4, true).join(", ");
    let mut sql = format!("SELECT {columns}");

    if draw_bool(tc) {
        sql.push_str(" FROM ");
        sql.push_str(draw_identifier(tc));
    }
    if draw_bool(tc) {
        sql.push_str(" WHERE ");
        sql.push_str(&draw_expression(tc, false));
    }
    if draw_bool(tc) {
        sql.push_str(" ORDER BY ");
        sql.push_str(&draw_expression_list(tc, 3, false).join(", "));
        match draw_index(tc, 3) {
            0 => {}
            1 => sql.push_str(" ASC"),
            2 => sql.push_str(" DESC"),
            _ => unreachable!(),
        }
    }
    if draw_bool(tc) {
        sql.push_str(" LIMIT ");
        sql.push_str(&draw_u32(tc).to_string());
    }
    if draw_bool(tc) {
        sql.push_str(" OFFSET ");
        sql.push_str(&draw_u32(tc).to_string());
    }

    sql.push(';');
    sql
}

fn draw_create_table(tc: &TestCase) -> String {
    let table = draw_identifier(tc);
    let columns = (0..draw_non_empty_len(tc, 5))
        .map(|_| {
            let column_type = match draw_index(tc, 3) {
                0 => "INT",
                1 => "FLOAT",
                2 => "TEXT",
                _ => unreachable!(),
            };
            let mut column = format!("{} {column_type}", draw_identifier(tc));
            if draw_bool(tc) {
                column.push_str(" PRIMARY KEY");
            }
            if draw_bool(tc) {
                column.push_str(" NULLABLE");
            }
            column
        })
        .collect::<Vec<_>>();

    format!("CREATE TABLE {table} ({});", columns.join(", "))
}

#[hegel::test(test_cases = 250)]
fn insert_queries_round_trip_through_display(tc: TestCase) {
    let sql = draw_insert(&tc);
    tc.note(&sql);
    assert_round_trips(&sql);
}

#[hegel::test(test_cases = 250)]
fn select_queries_round_trip_through_display(tc: TestCase) {
    let sql = draw_select(&tc);
    tc.note(&sql);
    assert_round_trips(&sql);
}

#[hegel::test(test_cases = 250)]
fn create_table_queries_round_trip_through_display(tc: TestCase) {
    let sql = draw_create_table(&tc);
    tc.note(&sql);
    assert_round_trips(&sql);
}

#[test]
fn parenthesized_expression_display_loses_grouping() {
    let sql = "SELECT (alpha + beta) * gamma;";
    let parsed = parse_statement(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_statement(&displayed);

    assert_eq!(parsed, reparsed, "displayed SQL: {displayed}");
}

#[test]
#[ignore = "documents a parser/display round-trip bug; do not fix yet"]
fn float_literals_with_zero_fraction_display_as_integers() {
    let sql = "SELECT 0.0;";
    let parsed = parse_statement(sql);
    let displayed = parsed.to_string();
    let reparsed = parse_statement(&displayed);

    assert_eq!(parsed, reparsed, "displayed SQL: {displayed}");
}
