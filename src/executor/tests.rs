use std::fmt::Write as _;

use tempfile::tempdir;

use super::*;
use crate::{
    core::{
        ColumnSchema, DataType, PAGE_SIZE, TableKey, Tuple, TupleSchema,
        error::{ConstraintError, InternalError, InvariantViolation, StorageError},
    },
    error::DatabaseError,
    planner::{BoundColumn, PlannedExpression, Planner},
    session::{Session, SessionError},
    sql_parser::parser::Parser,
};

fn record(table_key: TableKey, values: Vec<Value>) -> TableRecord {
    record_from_values(table_key, values).unwrap()
}

fn values(record: &TableRecord) -> Vec<Value> {
    Tuple::from_bytes(&record.record).unwrap().into_values()
}

fn collect_rows(output: ExecutionOutput) -> ExecutorResult<Vec<TableRecord>> {
    output.into_rows("TEST")?.collect()
}

fn execute_sql<'a>(
    database: &Database,
    sql: &'a str,
) -> Result<ExecutionOutput, DatabaseError<'a>> {
    Session::new(database).execute_sql(sql)
}

fn execute_sql_with_session<'a>(
    session: &mut Session<'_>,
    sql: &'a str,
) -> Result<ExecutionOutput, DatabaseError<'a>> {
    session.execute_sql(sql)
}

fn execute_script(database: &Database, sql: &str) {
    let items = Parser::new(sql).collect::<Result<Vec<_>, _>>().unwrap();
    let mut session = Session::new(database);

    for item in items {
        session.execute_item(item).unwrap();
    }
}

fn is_null_value_error(error: ExecutorError, expected_column: &str) -> bool {
    matches!(
        error,
        ExecutorError::Storage(StorageError::Constraint(ConstraintError::NullValue {
            column,
        })) if column == expected_column
    )
}

fn is_type_mismatch_error(
    error: ExecutorError,
    expected_column: &str,
    expected_type: DataType,
    actual_type: &'static str,
) -> bool {
    matches!(
        error,
        ExecutorError::Storage(StorageError::Constraint(
            ConstraintError::ColumnTypeMismatch {
                column,
                expected,
                actual,
            },
        )) if column == expected_column && expected == expected_type && actual == actual_type
    )
}

fn is_database_type_mismatch_error(
    error: DatabaseError<'_>,
    expected_column: &str,
    expected_type: DataType,
    actual_type: &'static str,
) -> bool {
    match error {
        DatabaseError::Executor(executor_error) => {
            is_type_mismatch_error(executor_error, expected_column, expected_type, actual_type)
        }
        _ => false,
    }
}

fn bound(name: &str, ordinal: usize, data_type: DataType) -> BoundColumn {
    BoundColumn { table: "users".to_owned(), name: name.to_owned(), ordinal, data_type }
}

fn insert_values_plan(
    database: &Database,
    columns: Vec<BoundColumn>,
    rows: Vec<Vec<Value>>,
) -> PhysicalPlan {
    PhysicalPlan::InsertValues {
        table: database.table_schema_by_name("users").unwrap(),
        columns,
        values: rows
            .into_iter()
            .map(|row| row.into_iter().map(PlannedExpression::Literal).collect())
            .collect(),
    }
}

fn users_schema() -> TupleSchema {
    TupleSchema {
        columns: vec![
            ColumnSchema {
                name: "id".to_owned(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
            },
            ColumnSchema {
                name: "active".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                primary_key: false,
            },
        ],
    }
}

fn insert_many_users_sql(count: u64) -> String {
    let mut sql = String::from("INSERT INTO users (id, name, active) VALUES ");
    for id in 1..=count {
        if id > 1 {
            sql.push_str(", ");
        }
        write!(&mut sql, "({id}, 'user{id}', TRUE)").unwrap();
    }
    sql.push(';');
    sql
}

fn assert_user_row(database: &Database, table_key: TableKey, expected_name: &str) {
    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(table_key).unwrap().expect("user row should exist");
    assert_eq!(
        values(&row),
        vec![
            Value::Integer(table_key),
            Value::String(expected_name.to_owned()),
            Value::Boolean(true),
        ]
    );
}

fn assert_name_index_entry(database: &Database, name: &str, table_key: TableKey) {
    let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
    let key = Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap();
    let entry = index.get(&key).unwrap().expect("index entry should exist");
    assert_eq!(entry.table_key, table_key);
}

fn assert_user_row_absent(database: &Database, table_key: TableKey) {
    let mut users = database.table_cursor_by_name("users").unwrap();
    assert!(users.get(table_key).unwrap().is_none());
}

fn assert_name_index_absent(database: &Database, name: &str) {
    let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
    let key = Tuple::new(vec![Value::String(name.to_owned())]).to_bytes().unwrap();
    assert!(index.get(&key).unwrap().is_none());
}

#[test]
fn single_literal_expression_produces_one_column_record() {
    let input = record(7, vec![Value::Integer(1)]);
    let output =
        evaluate_expression(&PlannedExpression::Literal(Value::String("Ada".to_owned())), &input)
            .unwrap();

    assert_eq!(output.table_key, 7);
    assert_eq!(values(&output), vec![Value::String("Ada".to_owned())]);
}

#[test]
fn single_column_expression_reads_bound_ordinal() {
    let input =
        record(8, vec![Value::Integer(1), Value::String("Grace".to_owned()), Value::Boolean(true)]);
    let output =
        evaluate_expression(&PlannedExpression::Column(bound("name", 1, DataType::Text)), &input)
            .unwrap();

    assert_eq!(output.table_key, 8);
    assert_eq!(values(&output), vec![Value::String("Grace".to_owned())]);
}

#[test]
fn project_evaluates_multiple_expressions_in_order() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Project {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![vec![
                PlannedExpression::Literal(Value::Integer(4)),
                PlannedExpression::Literal(Value::Integer(5)),
            ]],
        }),
        expressions: vec![
            PlannedExpression::Column(bound("right", 1, DataType::Integer)),
            PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Column(bound("left", 0, DataType::Integer))),
                op: Op::Add,
                right: Box::new(PlannedExpression::Column(bound("right", 1, DataType::Integer))),
            },
        ],
    };

    let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].table_key, 0);
    assert_eq!(values(&rows[0]), vec![Value::Integer(5), Value::Integer(9)]);
}

#[test]
fn filter_keeps_only_rows_with_true_predicate() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![
                vec![
                    PlannedExpression::Literal(Value::Integer(1)),
                    PlannedExpression::Literal(Value::Boolean(true)),
                ],
                vec![
                    PlannedExpression::Literal(Value::Integer(2)),
                    PlannedExpression::Literal(Value::Boolean(false)),
                ],
            ],
        }),
        predicate: PlannedExpression::Column(bound("active", 1, DataType::Boolean)),
    };

    let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(values(&rows[0]), vec![Value::Integer(1), Value::Boolean(true)]);
}

#[test]
fn filter_rejects_non_boolean_predicate() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
        }),
        predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
    };

    assert!(matches!(
        collect_rows(executor.execute(plan).unwrap()),
        Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
    ));
}

#[test]
fn limit_does_not_evaluate_rows_beyond_limit() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Limit {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![
                vec![PlannedExpression::Literal(Value::Integer(1))],
                vec![PlannedExpression::Binary {
                    left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
                    op: Op::Div,
                    right: Box::new(PlannedExpression::Literal(Value::Integer(0))),
                }],
            ],
        }),
        limit: 1,
    };

    let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(values(&rows[0]), vec![Value::Integer(1)]);
}

#[test]
fn limit_larger_than_child_rows_returns_all_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Limit {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![
                vec![PlannedExpression::Literal(Value::Integer(1))],
                vec![PlannedExpression::Literal(Value::Integer(2))],
            ],
        }),
        limit: u32::MAX,
    };

    let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(values(&rows[0]), vec![Value::Integer(1)]);
    assert_eq!(values(&rows[1]), vec![Value::Integer(2)]);
}

#[test]
fn limit_rejects_non_row_child() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Limit {
        input: Box::new(PhysicalPlan::CreateTable {
            name: "users".to_owned(),
            schema: users_schema(),
        }),
        limit: 1,
    };

    assert!(matches!(
        executor.execute(plan),
        Err(ExecutorError::ExpectedRows { operator: "LIMIT" })
    ));
}

#[test]
fn offset_reports_errors_from_skipped_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Offset {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![
                    vec![PlannedExpression::Literal(Value::Integer(1))],
                    vec![PlannedExpression::Literal(Value::Integer(2))],
                ],
            }),
            predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
        }),
        offset: 1,
    };

    assert!(matches!(
        collect_rows(executor.execute(plan).unwrap()),
        Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
    ));
}

#[test]
fn offset_larger_than_child_rows_returns_no_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Offset {
        input: Box::new(PhysicalPlan::Values {
            rows: vec![
                vec![PlannedExpression::Literal(Value::Integer(1))],
                vec![PlannedExpression::Literal(Value::Integer(2))],
            ],
        }),
        offset: u32::MAX,
    };

    let rows = collect_rows(executor.execute(plan).unwrap()).unwrap();

    assert!(rows.is_empty());
}

#[test]
fn offset_rejects_non_row_child() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Offset {
        input: Box::new(PhysicalPlan::CreateTable {
            name: "users".to_owned(),
            schema: users_schema(),
        }),
        offset: 1,
    };

    assert!(matches!(
        executor.execute(plan),
        Err(ExecutorError::ExpectedRows { operator: "OFFSET" })
    ));
}

#[test]
fn filter_propagates_child_row_errors() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
            }),
            predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
        }),
        predicate: PlannedExpression::Literal(Value::Boolean(true)),
    };

    assert!(matches!(
        collect_rows(executor.execute(plan).unwrap()),
        Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
    ));
}

#[test]
fn project_propagates_child_row_errors() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Project {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Values {
                rows: vec![vec![PlannedExpression::Literal(Value::Integer(1))]],
            }),
            predicate: PlannedExpression::Column(bound("id", 0, DataType::Integer)),
        }),
        expressions: vec![PlannedExpression::Literal(Value::Integer(2))],
    };

    assert!(matches!(
        collect_rows(executor.execute(plan).unwrap()),
        Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
    ));
}

#[test]
fn row_operator_rejects_non_row_child() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Project {
        input: Box::new(PhysicalPlan::CreateTable {
            name: "users".to_owned(),
            schema: users_schema(),
        }),
        expressions: vec![PlannedExpression::Literal(Value::Integer(1))],
    };

    assert!(matches!(
        executor.execute(plan),
        Err(ExecutorError::ExpectedRows { operator: "PROJECT" })
    ));
}

#[test]
fn sort_returns_unsupported_error_instead_of_panicking() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let mut executor = Executor::new(&database);
    let plan = PhysicalPlan::Sort {
        input: Box::new(PhysicalPlan::Values { rows: Vec::new() }),
        terms: Vec::new(),
    };

    assert!(matches!(
        executor.execute(plan),
        Err(ExecutorError::UnsupportedOperator { operator: "SORT" })
    ));
}

#[test]
fn evaluates_arithmetic_comparison_boolean_and_unary_expressions() {
    let input = record(9, Vec::new());
    let expression = PlannedExpression::Binary {
        left: Box::new(PlannedExpression::Unary {
            op: Op::Not,
            expr: Box::new(PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Literal(Value::Integer(2))),
                op: Op::GreaterThan,
                right: Box::new(PlannedExpression::Literal(Value::Integer(3))),
            }),
        }),
        op: Op::And,
        right: Box::new(PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Literal(Value::Integer(2))),
                op: Op::Mul,
                right: Box::new(PlannedExpression::Literal(Value::Integer(4))),
            }),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(8))),
        }),
    };

    let output = evaluate_expression(&expression, &input).unwrap();

    assert_eq!(values(&output), vec![Value::Boolean(true)]);
}

#[test]
fn boolean_expressions_short_circuit() {
    let input = record(9, Vec::new());
    let divide_by_zero = PlannedExpression::Binary {
        left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
        op: Op::Div,
        right: Box::new(PlannedExpression::Literal(Value::Integer(0))),
    };
    let false_and_error = PlannedExpression::Binary {
        left: Box::new(PlannedExpression::Literal(Value::Boolean(false))),
        op: Op::And,
        right: Box::new(divide_by_zero.clone()),
    };
    let true_or_error = PlannedExpression::Binary {
        left: Box::new(PlannedExpression::Literal(Value::Boolean(true))),
        op: Op::Or,
        right: Box::new(divide_by_zero),
    };

    let false_and_output = evaluate_expression(&false_and_error, &input).unwrap();
    let true_or_output = evaluate_expression(&true_or_error, &input).unwrap();

    assert_eq!(values(&false_and_output), vec![Value::Boolean(false)]);
    assert_eq!(values(&true_or_output), vec![Value::Boolean(true)]);
}

#[test]
fn invalid_type_combinations_return_executor_errors() {
    let input = record(10, Vec::new());
    let expression = PlannedExpression::Binary {
        left: Box::new(PlannedExpression::Literal(Value::Integer(1))),
        op: Op::Add,
        right: Box::new(PlannedExpression::Literal(Value::Float(1.0))),
    };

    assert!(matches!(
        evaluate_expression(&expression, &input),
        Err(ExecutorError::UnsupportedBinary {
            left: Value::Integer(1),
            op: Op::Add,
            right: Value::Float(1.0),
        })
    ));
}

#[test]
fn select_with_projection_and_filter_executes_end_to_end() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let mut users = database.table_cursor_by_name("users").unwrap();
    users
        .insert(
            1,
            &Tuple::new(vec![
                Value::Integer(1),
                Value::String("Ada".to_owned()),
                Value::Boolean(true),
            ])
            .to_bytes()
            .unwrap(),
        )
        .unwrap();
    users
        .insert(
            2,
            &Tuple::new(vec![
                Value::Integer(2),
                Value::String("Grace".to_owned()),
                Value::Boolean(false),
            ])
            .to_bytes()
            .unwrap(),
        )
        .unwrap();

    let statement = Parser::new("SELECT name FROM users WHERE id == 1;").stmt().unwrap();
    let plan = Planner::new(&database).plan_statement(&statement).unwrap();
    let mut executor = Executor::new(&database);

    let rows = collect_rows(executor.execute(plan.physical).unwrap()).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].table_key, 1);
    assert_eq!(values(&rows[0]), vec![Value::String("Ada".to_owned())]);
}

#[test]
fn insert_values_uses_primary_keys_and_persists_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    let statement = Parser::new(
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .stmt()
    .unwrap();
    let plan = Planner::new(&database).plan_statement(&statement).unwrap();
    let mut executor = Executor::new(&database);

    let output = executor.execute(plan.physical).unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(2)));

    let mut users = database.table_cursor_by_name("users").unwrap();
    let first = users.get(1).unwrap().expect("first inserted row should exist");
    let second = users.get(2).unwrap().expect("second inserted row should exist");
    assert_eq!(
        values(&first),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true),]
    );
    assert_eq!(
        values(&second),
        vec![Value::Integer(2), Value::String("Grace".to_owned()), Value::Boolean(false),]
    );
}

#[test]
fn insert_values_rejects_omitted_non_nullable_columns() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    let plan = insert_values_plan(
        &database,
        vec![bound("id", 0, DataType::Integer), bound("name", 1, DataType::Text)],
        vec![vec![Value::Integer(1), Value::String("Ada".to_owned())]],
    );
    let mut executor = Executor::new(&database);

    assert!(executor.execute(plan).is_err_and(|error| is_null_value_error(error, "active")));
}

#[test]
fn insert_values_rejects_values_with_wrong_type() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    let statement =
        Parser::new("INSERT INTO users (id, name, active) VALUES ('one', 'Ada', TRUE);")
            .stmt()
            .unwrap();
    let plan = Planner::new(&database).plan_statement(&statement).unwrap();
    let mut executor = Executor::new(&database);

    assert!(executor.execute(plan.physical).is_err_and(|error| is_type_mismatch_error(
        error,
        "id",
        DataType::Integer,
        "text"
    )));
}

#[test]
fn insert_values_rejects_null_for_non_nullable_columns() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    let plan = insert_values_plan(
        &database,
        vec![
            bound("id", 0, DataType::Integer),
            bound("name", 1, DataType::Text),
            bound("active", 2, DataType::Boolean),
        ],
        vec![vec![Value::Integer(1), Value::Null, Value::Boolean(true)]],
    );
    let mut executor = Executor::new(&database);

    assert!(executor.execute(plan).is_err_and(|error| is_null_value_error(error, "name")));
}

#[test]
fn failed_insert_does_not_write_partial_row() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let mut executor = Executor::new(&database);

    let valid = insert_values_plan(
        &database,
        vec![
            bound("id", 0, DataType::Integer),
            bound("name", 1, DataType::Text),
            bound("active", 2, DataType::Boolean),
        ],
        vec![vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true)]],
    );
    executor.execute(valid).unwrap();

    let invalid = insert_values_plan(
        &database,
        vec![
            bound("id", 0, DataType::Integer),
            bound("name", 1, DataType::Text),
            bound("active", 2, DataType::Boolean),
        ],
        vec![vec![
            Value::Integer(2),
            Value::String("Grace".to_owned()),
            Value::String("yes".to_owned()),
        ]],
    );

    assert!(executor.execute(invalid).is_err_and(|error| {
        is_type_mismatch_error(error, "active", DataType::Boolean, "text")
    }));

    let mut users = database.table_cursor_by_name("users").unwrap();
    assert!(users.get(2).unwrap().is_none());
}

#[test]
fn failed_multi_row_insert_rolls_back_rows_already_inserted_in_statement() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    let result = execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', 'yes');",
    );

    assert!(result.is_err_and(|error| {
        is_database_type_mismatch_error(error, "active", DataType::Boolean, "text")
    }));

    let mut users = database.table_cursor_by_name("users").unwrap();
    assert!(users.get(1).unwrap().is_none());
    assert!(users.get(2).unwrap().is_none());
}

#[test]
fn failed_multi_row_insert_in_explicit_transaction_rolls_back_statement_before_commit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();

    let mut session = Session::new(&database);
    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    let result = execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', 'yes');",
    );

    assert!(result.is_err_and(|error| {
        is_database_type_mismatch_error(error, "active", DataType::Boolean, "text")
    }));
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (2, 'Linus', TRUE);",
    )
    .unwrap();
    execute_sql_with_session(&mut session, "COMMIT;").unwrap();
    drop(session);
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let mut index = reopened.index_cursor_by_name("idx_users_name").unwrap();
    let ada = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();
    let linus = Tuple::new(vec![Value::String("Linus".to_owned())]).to_bytes().unwrap();
    let row = users.get(2).unwrap().expect("successful statement should commit");

    assert_eq!(
        values(&row),
        vec![Value::Integer(2), Value::String("Linus".to_owned()), Value::Boolean(true)]
    );
    assert!(users.get(1).unwrap().is_none());
    assert!(index.get(&ada).unwrap().is_none());
    assert_eq!(index.get(&linus).unwrap().unwrap().table_key, 2);
}

#[test]
fn savepoint_rollback_error_takes_precedence_over_executor_error() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    session.fail_next_savepoint_rollback_for_test();
    let result = execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', 'yes');",
    );

    assert!(matches!(
        result,
        Err(DatabaseError::Storage(crate::core::error::StorageError::Internal(
            InternalError::InvariantViolation(InvariantViolation::WalLog { message })
        ))) if message == "WAL LSN exhausted"
    ));
}

#[test]
fn wal_logging_failure_during_explicit_statement_is_reported_immediately() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    database.force_next_lsn_exhausted_for_test();
    let result = execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);",
    );

    assert!(matches!(
        result,
        Err(DatabaseError::Storage(StorageError::Internal(InternalError::InvariantViolation(
            InvariantViolation::TransactionPoisoned { txn_id: 1 }
        ))))
    ));
}

#[test]
fn create_index_backfills_existing_table_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let insert = Parser::new(
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .stmt()
    .unwrap();
    let insert_plan = Planner::new(&database).plan_statement(&insert).unwrap();
    let mut executor = Executor::new(&database);
    executor.execute(insert_plan.physical).unwrap();

    let create_index = Parser::new("CREATE INDEX idx_users_name ON users (name);").stmt().unwrap();
    let create_index_plan = Planner::new(&database).plan_statement(&create_index).unwrap();
    executor.execute(create_index_plan.physical).unwrap();

    let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
    let key = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();
    let entry = index.get(&key).unwrap().expect("index entry should be backfilled");

    assert_eq!(entry.table_key, 1);
}

#[test]
fn insert_values_updates_existing_secondary_indexes() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    let create_index = Parser::new("CREATE INDEX idx_users_name ON users (name);").stmt().unwrap();
    let create_index_plan = Planner::new(&database).plan_statement(&create_index).unwrap();
    let mut executor = Executor::new(&database);
    executor.execute(create_index_plan.physical).unwrap();

    let insert = Parser::new("INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .stmt()
        .unwrap();
    let insert_plan = Planner::new(&database).plan_statement(&insert).unwrap();
    executor.execute(insert_plan.physical).unwrap();

    let mut index = database.index_cursor_by_name("idx_users_name").unwrap();
    let key = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();
    let entry = index.get(&key).unwrap().expect("index entry should track inserted row");

    assert_eq!(entry.table_key, 1);
}

#[test]
fn delete_all_rows_returns_count_and_empties_table() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .unwrap();

    let output = execute_sql(&database, "DELETE FROM users;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(2)));
    assert_user_row_absent(&database, 1);
    assert_user_row_absent(&database, 2);
}

#[test]
fn delete_where_removes_only_matching_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .unwrap();

    let output = execute_sql(&database, "DELETE FROM users WHERE active == FALSE;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_user_row(&database, 1, "Ada");
    assert_user_row_absent(&database, 2);
}

#[test]
fn delete_without_matches_returns_zero_rows_affected() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    let output = execute_sql(&database, "DELETE FROM users WHERE id == 99;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(0)));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn delete_removes_secondary_index_entries() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', TRUE);",
    )
    .unwrap();

    let output = execute_sql(&database, "DELETE FROM users WHERE name == 'Ada';").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_user_row_absent(&database, 1);
    assert_name_index_absent(&database, "Ada");
    assert_user_row(&database, 2, "Grace");
    assert_name_index_entry(&database, "Grace", 2);
}

#[test]
fn delete_with_non_boolean_where_does_not_delete_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    let result = execute_sql(&database, "DELETE FROM users WHERE id;");

    assert!(matches!(
        result,
        Err(DatabaseError::Executor(ExecutorError::NonBooleanPredicate {
            value: Value::Integer(1)
        }))
    ));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn update_all_rows_returns_count_and_replaces_values() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .unwrap();

    let output =
        execute_sql(&database, "UPDATE users SET name = 'Updated', active = TRUE;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(2)));
    assert_user_row(&database, 1, "Updated");
    assert_user_row(&database, 2, "Updated");
}

#[test]
fn update_where_replaces_only_matching_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .unwrap();

    let output =
        execute_sql(&database, "UPDATE users SET name = 'Linus' WHERE active == FALSE;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_user_row(&database, 1, "Ada");
    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(2).unwrap().expect("updated row should exist");
    assert_eq!(
        values(&row),
        vec![Value::Integer(2), Value::String("Linus".to_owned()), Value::Boolean(false)]
    );
}

#[test]
fn update_without_matches_returns_zero_rows_affected() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    let output = execute_sql(&database, "UPDATE users SET name = 'Linus' WHERE id == 99;").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(0)));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn update_assignment_expression_reads_original_row_values() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    execute_sql(&database, "UPDATE users SET active = NOT active WHERE id == 1;").unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("table key should be preserved");
    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(false)]
    );
}

#[test]
fn update_rejects_primary_key_assignment() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    assert!(matches!(
        execute_sql(&database, "UPDATE users SET id = id + 1 WHERE id == 1;"),
        Err(DatabaseError::Planner(crate::planner::PlannerError::PrimaryKeyUpdate { column }))
            if column == "id"
    ));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn update_rejects_wrong_type_without_changing_row() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    let result = execute_sql(&database, "UPDATE users SET active = 'yes' WHERE id == 1;");

    assert!(result.is_err_and(|error| {
        is_database_type_mismatch_error(error, "active", DataType::Boolean, "text")
    }));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn update_with_non_boolean_where_does_not_change_rows() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();

    let result = execute_sql(&database, "UPDATE users SET name = 'Linus' WHERE id;");

    assert!(matches!(
        result,
        Err(DatabaseError::Executor(ExecutorError::NonBooleanPredicate {
            value: Value::Integer(1)
        }))
    ));
    assert_user_row(&database, 1, "Ada");
}

#[test]
fn update_refreshes_secondary_index_entries() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', TRUE);",
    )
    .unwrap();

    let output =
        execute_sql(&database, "UPDATE users SET name = 'Linus' WHERE name == 'Ada';").unwrap();

    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_name_index_absent(&database, "Ada");
    assert_name_index_entry(&database, "Linus", 1);
    assert_name_index_entry(&database, "Grace", 2);
}

#[test]
fn failed_multi_row_update_rolls_back_rows_already_updated_in_statement() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', TRUE);",
    )
    .unwrap();

    let result = execute_sql(
        &database,
        "UPDATE users SET name = 'Updated', active = id == 1 OR 1 / (2 - id) == 0;",
    );

    assert!(matches!(result, Err(DatabaseError::Executor(ExecutorError::DivisionByZero))));
    assert_user_row(&database, 1, "Ada");
    assert_user_row(&database, 2, "Grace");
}

#[test]
fn explicit_transaction_rollback_restores_updated_rows_and_indexes() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    let output =
        execute_sql_with_session(&mut session, "UPDATE users SET name = 'Linus' WHERE id == 1;")
            .unwrap();
    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_user_row(&database, 1, "Linus");
    assert_name_index_absent(&database, "Ada");
    assert_name_index_entry(&database, "Linus", 1);

    execute_sql_with_session(&mut session, "ROLLBACK;").unwrap();

    assert_user_row(&database, 1, "Ada");
    assert_name_index_entry(&database, "Ada", 1);
    assert_name_index_absent(&database, "Linus");
}

#[test]
fn explicit_transaction_commit_persists_schema_rows_and_indexes_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.flush().unwrap();

    execute_script(
        &database,
        "\
BEGIN;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);
INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1), (2, 'Grace', 0);
CREATE INDEX idx_users_name ON users (name);
COMMIT;
",
    );
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let schema = reopened.table_schema_by_name("users").unwrap();

    assert_eq!(schema.name, "users");
    assert_eq!(schema.row.columns.len(), 3);
    assert_name_index_entry(&reopened, "Ada", 1);
    assert_name_index_entry(&reopened, "Grace", 2);
}

#[test]
fn explicit_transaction_rollback_discards_dml_and_ddl_but_preserves_prior_commits() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();

    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);").unwrap();

    execute_script(
        &database,
        "\
BEGIN;
INSERT INTO users (id, name, active) VALUES (2, 'Grace', 0);
CREATE TABLE rolled_back (id INT PRIMARY KEY);
CREATE INDEX idx_users_name ON users (name);
ROLLBACK;
",
    );

    let mut users = database.table_cursor_by_name("users").unwrap();
    assert!(users.get(1).unwrap().is_some());
    assert!(users.get(2).unwrap().is_none());
    assert!(database.table_schema_by_name("rolled_back").is_err());
    assert!(database.index_cursor_by_name("idx_users_name").is_err());
}

#[test]
fn explicit_transaction_rollback_restores_deleted_rows_and_indexes() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();
    execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', TRUE);",
    )
    .unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    let output =
        execute_sql_with_session(&mut session, "DELETE FROM users WHERE id == 1;").unwrap();
    assert!(matches!(output, ExecutionOutput::RowsAffected(1)));
    assert_user_row_absent(&database, 1);
    assert_name_index_absent(&database, "Ada");

    execute_sql_with_session(&mut session, "ROLLBACK;").unwrap();

    assert_user_row(&database, 1, "Ada");
    assert_name_index_entry(&database, "Ada", 1);
    assert_user_row(&database, 2, "Grace");
    assert_name_index_entry(&database, "Grace", 2);
}

#[test]
fn implicit_transactions_still_commit_before_and_after_explicit_rollback() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();

    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);").unwrap();

    execute_script(
        &database,
        "\
BEGIN;
INSERT INTO users (id, name, active) VALUES (2, 'Grace', 0);
ROLLBACK;
",
    );
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (2, 'Linus', 1);").unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let first = users.get(1).unwrap().expect("pre-transaction row should remain committed");
    let second = users.get(2).unwrap().expect("post-rollback implicit insert should commit");

    assert_eq!(
        values(&first),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Integer(1)]
    );
    assert_eq!(
        values(&second),
        vec![Value::Integer(2), Value::String("Linus".to_owned()), Value::Integer(1)]
    );
}

#[test]
fn commit_without_active_transaction_errors_and_later_implicit_statement_commits() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    let mut session = Session::new(&database);

    assert!(matches!(
        execute_sql_with_session(&mut session, "COMMIT;"),
        Err(DatabaseError::Session(SessionError::NoActiveTransaction))
    ));
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);",
    )
    .unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("later implicit insert should commit");
    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Integer(1)]
    );
}

#[test]
fn failed_commit_clears_session_when_storage_transaction_was_cleared() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);",
    )
    .unwrap();
    database.fail_next_wal_flush_for_test();

    assert!(matches!(
        execute_sql_with_session(&mut session, "COMMIT;"),
        Err(DatabaseError::Storage(StorageError::Io(error)))
            if error.kind() == std::io::ErrorKind::Other
                && error.to_string() == "injected WAL flush failure"
    ));
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (2, 'Linus', 1);",
    )
    .unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(2).unwrap().expect("later implicit insert should commit");
    assert_eq!(
        values(&row),
        vec![Value::Integer(2), Value::String("Linus".to_owned()), Value::Integer(1)]
    );
}

#[test]
fn rollback_without_active_transaction_errors_and_later_implicit_statement_commits() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    let mut session = Session::new(&database);

    assert!(matches!(
        execute_sql_with_session(&mut session, "ROLLBACK;"),
        Err(DatabaseError::Session(SessionError::NoActiveTransaction))
    ));
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);",
    )
    .unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("later implicit insert should commit");
    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Integer(1)]
    );
}

#[test]
fn nested_begin_errors_without_ending_outer_transaction() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    execute_sql(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active INT);")
        .unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    execute_sql_with_session(
        &mut session,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);",
    )
    .unwrap();
    assert!(matches!(
        execute_sql_with_session(&mut session, "BEGIN;"),
        Err(DatabaseError::Session(SessionError::TransactionAlreadyActive { txn_id })) if txn_id > 0
    ));
    execute_sql_with_session(&mut session, "ROLLBACK;").unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    assert!(users.get(1).unwrap().is_none());
}

#[test]
fn dropping_session_with_active_transaction_rolls_back_and_releases_database_handle() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    database.create_table("users", users_schema()).unwrap();

    {
        let mut session = Session::new(&database);
        execute_sql_with_session(&mut session, "BEGIN;").unwrap();
        execute_sql_with_session(
            &mut session,
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);",
        )
        .unwrap();
    }

    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (2, 'Linus', TRUE);")
        .unwrap();

    let mut users = database.table_cursor_by_name("users").unwrap();
    let row = users.get(2).unwrap().expect("new statement should commit after session drop");

    assert_eq!(
        values(&row),
        vec![Value::Integer(2), Value::String("Linus".to_owned()), Value::Boolean(true)]
    );
    assert!(users.get(1).unwrap().is_none());
}

#[test]
fn committed_create_table_recovers_from_wal_after_crash_without_database_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.flush().unwrap();

    execute_sql(&database, "CREATE TABLE recovered (id INT PRIMARY KEY, name TEXT);").unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let schema = reopened.table_schema_by_name("recovered").unwrap();

    assert_eq!(schema.name, "recovered");
    assert_eq!(schema.row.columns.len(), 2);
    assert_eq!(schema.row.columns[0].name, "id");
    assert_eq!(schema.row.columns[1].name, "name");
    reopened.table_cursor_by_name("recovered").unwrap();
}

#[test]
fn committed_create_index_backfill_recovers_from_wal_after_crash_without_database_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.flush().unwrap();

    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', FALSE);",
    )
    .unwrap();
    database.flush().unwrap();
    execute_sql(&database, "CREATE INDEX idx_users_name ON users (name);").unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();

    assert_name_index_entry(&reopened, "Ada", 1);
    assert_name_index_entry(&reopened, "Grace", 2);
}

#[test]
fn committed_large_insert_with_btree_splits_recovers_from_wal_after_crash() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.flush().unwrap();

    execute_sql(&database, &insert_many_users_sql(500)).unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();

    assert_user_row(&reopened, 1, "user1");
    assert_user_row(&reopened, 250, "user250");
    assert_user_row(&reopened, 500, "user500");
}

#[test]
fn committed_overflow_insert_recovers_from_wal_after_crash() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.flush().unwrap();
    let large_name = "x".repeat(PAGE_SIZE * 3);
    let insert = format!("INSERT INTO users (id, name, active) VALUES (1, '{large_name}', TRUE);");

    execute_sql(&database, &insert).unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("overflow row should recover from WAL");

    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String(large_name), Value::Boolean(true)]
    );
}

#[test]
fn failed_indexed_multi_row_insert_rolls_back_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();

    let result = execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', 'yes');",
    );

    assert!(result.is_err_and(|error| {
        is_database_type_mismatch_error(error, "active", DataType::Boolean, "text")
    }));
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let mut index = reopened.index_cursor_by_name("idx_users_name").unwrap();
    let ada = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();

    assert!(users.get(1).unwrap().is_none());
    assert!(index.get(&ada).unwrap().is_none());
}

#[test]
fn uncommitted_flushed_insert_is_undone_during_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();
    database.begin_transaction().unwrap();
    let table = database.table_schema_by_name("users").unwrap();

    execute_insert_values(
        &database,
        table,
        vec![
            bound("id", 0, DataType::Integer),
            bound("name", 1, DataType::Text),
            bound("active", 2, DataType::Boolean),
        ],
        vec![vec![
            PlannedExpression::Literal(Value::Integer(1)),
            PlannedExpression::Literal(Value::String("Ada".to_owned())),
            PlannedExpression::Literal(Value::Boolean(true)),
        ]],
    )
    .unwrap();
    database.flush().unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let mut index = reopened.index_cursor_by_name("idx_users_name").unwrap();
    let ada = Tuple::new(vec![Value::String("Ada".to_owned())]).to_bytes().unwrap();

    assert!(users.get(1).unwrap().is_none());
    assert!(index.get(&ada).unwrap().is_none());
}

#[test]
fn create_without_explicit_flush_does_not_promise_catalog_durability() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();

    std::mem::forget(database);

    assert!(Database::open(&path).is_err());
}

#[test]
fn committed_insert_recovers_from_wal_after_crash_without_database_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.flush().unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("committed row should recover from WAL");

    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String("Ada".to_owned()), Value::Boolean(true),]
    );
}

#[test]
fn committed_delete_recovers_from_wal_after_crash() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();
    execute_sql(
        &database,
        "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE), (2, 'Grace', TRUE);",
    )
    .unwrap();
    execute_sql(&database, "DELETE FROM users WHERE name == 'Ada';").unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();

    assert_user_row_absent(&reopened, 1);
    assert_name_index_absent(&reopened, "Ada");
    assert_user_row(&reopened, 2, "Grace");
    assert_name_index_entry(&reopened, "Grace", 2);
}

#[test]
fn committed_update_recovers_from_wal_after_crash() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();
    execute_sql(&database, "UPDATE users SET name = 'Linus', active = FALSE WHERE id == 1;")
        .unwrap();
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();
    let mut users = reopened.table_cursor_by_name("users").unwrap();
    let row = users.get(1).unwrap().expect("updated row should recover from WAL");

    assert_eq!(
        values(&row),
        vec![Value::Integer(1), Value::String("Linus".to_owned()), Value::Boolean(false)]
    );
    assert_name_index_absent(&reopened, "Ada");
    assert_name_index_entry(&reopened, "Linus", 1);
}

#[test]
fn uncommitted_flushed_delete_is_undone_during_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();
    database.flush().unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    execute_sql_with_session(&mut session, "DELETE FROM users WHERE id == 1;").unwrap();
    database.flush().unwrap();
    std::mem::forget(session);
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();

    assert_user_row(&reopened, 1, "Ada");
    assert_name_index_entry(&reopened, "Ada", 1);
}

#[test]
fn uncommitted_flushed_update_is_undone_during_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.flush().unwrap();
    execute_sql(&database, "INSERT INTO users (id, name, active) VALUES (1, 'Ada', TRUE);")
        .unwrap();
    database.flush().unwrap();
    let mut session = Session::new(&database);

    execute_sql_with_session(&mut session, "BEGIN;").unwrap();
    execute_sql_with_session(&mut session, "UPDATE users SET name = 'Linus' WHERE id == 1;")
        .unwrap();
    database.flush().unwrap();
    std::mem::forget(session);
    std::mem::forget(database);

    let reopened = Database::open(&path).unwrap();

    assert_user_row(&reopened, 1, "Ada");
    assert_name_index_entry(&reopened, "Ada", 1);
    assert_name_index_absent(&reopened, "Linus");
}

#[test]
fn select_without_from_executes_through_one_row_and_project() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let statement = Parser::new("SELECT 1 + 2;").stmt().unwrap();
    let plan = Planner::new(&database).plan_statement(&statement).unwrap();
    let mut executor = Executor::new(&database);

    let rows = collect_rows(executor.execute(plan.physical).unwrap()).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].table_key, 0);
    assert_eq!(values(&rows[0]), vec![Value::Integer(3)]);
}
