use tempfile::tempdir;

use super::*;
use crate::{
    core::{ColumnSchema, DataType},
    sql_parser::parser::Parser,
};

fn parse(sql: &str) -> Statement<'_> {
    Parser::new(sql).stmt().unwrap()
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
                name: "age".to_owned(),
                data_type: DataType::Integer,
                nullable: true,
                primary_key: false,
            },
        ],
    }
}

fn database_with_users() -> (tempfile::TempDir, Database) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let database = Database::create(&path).unwrap();
    database.create_table("users", users_schema()).unwrap();
    (dir, database)
}

struct EmptyCatalog;

impl CatalogRead for EmptyCatalog {
    fn table_schema_by_name(&self, name: &str) -> Result<TableSchema, StorageError> {
        Err(StorageError::InvalidArgument(InvalidArgumentError::TableNotFound {
            name: name.to_owned(),
        }))
    }

    fn index_schemas_for_table(
        &self,
        _table: &TableSchema,
    ) -> Result<Vec<IndexSchema>, StorageError> {
        Ok(Vec::new())
    }
}

#[test]
fn planner_accepts_lightweight_schema_access() {
    let planner = Planner::with_schema(&EmptyCatalog);

    assert!(matches!(
        planner.plan_statement(&parse("SELECT * FROM missing;")),
        Err(PlannerError::TableNotFound { name }) if name == "missing"
    ));
}

#[test]
fn create_table_produces_logical_and_physical_create_table_plans() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path().join("test.db")).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT NULLABLE);");

    let plan = planner.plan_statement(&statement).unwrap();

    let expected_schema = users_schema();
    assert_eq!(
        plan.logical,
        LogicalPlan::CreateTable { name: "users".to_owned(), schema: expected_schema.clone() }
    );
    assert_eq!(
        plan.physical,
        PhysicalPlan::CreateTable { name: "users".to_owned(), schema: expected_schema }
    );
}

#[test]
fn create_index_binds_table_and_index_columns() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("CREATE INDEX idx_users_name_age ON users (name, age);");

    let plan = planner.plan_statement(&statement).unwrap();

    let expected_columns = vec![
        bound("users", "name", 1, DataType::Text),
        bound("users", "age", 2, DataType::Integer),
    ];
    assert_eq!(
        plan.logical,
        LogicalPlan::CreateIndex {
            name: "idx_users_name_age".to_owned(),
            table: database.table_schema_by_name("users").unwrap(),
            columns: expected_columns.clone(),
        }
    );
    assert_eq!(
        plan.physical,
        PhysicalPlan::CreateIndex {
            name: "idx_users_name_age".to_owned(),
            table: database.table_schema_by_name("users").unwrap(),
            columns: expected_columns,
        }
    );
}

#[test]
fn insert_binds_table_columns_and_values() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace');");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Insert { table, columns, input } = &plan.logical else {
        panic!("expected logical insert plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(
        columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["id", "name"]
    );
    assert_eq!(
        input.as_ref(),
        &LogicalPlan::Values {
            rows: vec![
                vec![
                    PlannedExpression::Literal(Value::Integer(1)),
                    PlannedExpression::Literal(Value::String("Ada".to_owned())),
                ],
                vec![
                    PlannedExpression::Literal(Value::Integer(2)),
                    PlannedExpression::Literal(Value::String("Grace".to_owned())),
                ],
            ],
        }
    );

    let PhysicalPlan::InsertValues { table, columns, values } = &plan.physical else {
        panic!("expected physical insert values plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(
        columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["id", "name"]
    );
    assert_eq!(values.len(), 2);
}

#[test]
fn update_all_plans_full_table_scan_under_update() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("UPDATE users SET name = 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Update { table, assignments, input } = &plan.logical else {
        panic!("expected logical update plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].column, bound("users", "name", 1, DataType::Text));
    assert_eq!(
        assignments[0].expression,
        PlannedExpression::Literal(Value::String("Ada".to_owned()))
    );
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users"));

    let PhysicalPlan::Update { table, assignments, input } = &plan.physical else {
        panic!("expected physical update plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(assignments.len(), 1);
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn update_where_binds_filter_and_assignment_column_refs() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("UPDATE users SET age = age + 1 WHERE id == 1;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Update { assignments, input, .. } = &plan.logical else {
        panic!("expected logical update plan: {plan:?}");
    };
    assert_eq!(
        assignments,
        &[UpdateAssignment {
            column: bound("users", "age", 2, DataType::Integer),
            expression: PlannedExpression::Binary {
                left: Box::new(PlannedExpression::Column(bound(
                    "users",
                    "age",
                    2,
                    DataType::Integer
                ))),
                op: Op::Add,
                right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
            },
        }]
    );

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected filter below update: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users"));
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "id", 0, DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
        }
    );

    let PhysicalPlan::Update { input, .. } = &plan.physical else {
        panic!("expected physical update plan: {plan:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::PrimaryKeyRangeScan {
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(1)),
                upper: Some(TableKeyBound::Inclusive(1)),
            },
            ..
        }
    ));
}

#[test]
fn update_where_secondary_index_predicate_uses_full_table_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_age", "users", &["age"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("UPDATE users SET age = age + 1 WHERE age < 3;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Update { input, .. } = &plan.physical else {
        panic!("expected physical update plan: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter below update: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn update_rejects_duplicate_assignment_columns() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("UPDATE users SET name = 'Ada', name = 'Grace';");

    assert!(matches!(
        planner.plan_statement(&statement),
        Err(PlannerError::DuplicateUpdateColumn { column }) if column == "name"
    ));
}

#[test]
fn delete_all_plans_full_table_scan_under_delete() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("DELETE FROM users;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Delete { table, input } = &plan.logical else {
        panic!("expected logical delete plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users"));

    let PhysicalPlan::Delete { table, input } = &plan.physical else {
        panic!("expected physical delete plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn delete_where_binds_column_refs_in_filter() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("DELETE FROM users WHERE id == 1;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Delete { input, .. } = &plan.logical else {
        panic!("expected logical delete plan: {plan:?}");
    };
    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected filter below delete: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users"));
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "id", 0, DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
        }
    );

    let PhysicalPlan::Delete { input, .. } = &plan.physical else {
        panic!("expected physical delete plan: {plan:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::PrimaryKeyRangeScan {
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(1)),
                upper: Some(TableKeyBound::Inclusive(1)),
            },
            ..
        }
    ));
}

#[test]
fn delete_where_secondary_index_predicate_uses_full_table_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("DELETE FROM users WHERE name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Delete { input, .. } = &plan.physical else {
        panic!("expected physical delete plan: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter below delete: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn select_star_projects_all_columns_over_full_table_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT * FROM users;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, expressions } = &plan.physical else {
        panic!("expected physical project plan: {plan:?}");
    };
    assert_eq!(
        expressions
            .iter()
            .map(|expr| match expr {
                PlannedExpression::Column(column) => column.name.as_str(),
                other => panic!("expected column expression, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        ["id", "name", "age"]
    );
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn select_where_binds_column_refs_in_filter_and_projection() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE id == 1;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Project { input, expressions } = &plan.logical else {
        panic!("expected logical project plan: {plan:?}");
    };
    assert_eq!(
        expressions,
        &[PlannedExpression::Column(bound("users", "name", 1, DataType::Text))]
    );

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected filter below project: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { table } if table.name == "users"));
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "id", 0, DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(1))),
        }
    );
}

#[test]
fn select_primary_key_range_uses_range_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE 10 <= id AND id < 20;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::PrimaryKeyRangeScan { table, range } = input.as_ref() else {
        panic!("expected primary key range scan under project: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(
        *range,
        TableKeyRange {
            lower: Some(TableKeyBound::Inclusive(10)),
            upper: Some(TableKeyBound::Exclusive(20)),
        }
    );
}

#[test]
fn select_primary_key_equality_uses_single_key_range_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE id == 10;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::PrimaryKeyRangeScan { range, .. } = input.as_ref() else {
        panic!("expected primary key range scan under project: {plan:?}");
    };
    assert_eq!(
        *range,
        TableKeyRange {
            lower: Some(TableKeyBound::Inclusive(10)),
            upper: Some(TableKeyBound::Inclusive(10)),
        }
    );
}

#[test]
fn select_primary_key_range_with_residual_filter_uses_range_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE id < 10 AND age == 7;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "age", 2, DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(7))),
        }
    );
    let PhysicalPlan::PrimaryKeyRangeScan { range, .. } = input.as_ref() else {
        panic!("expected primary key range scan under residual filter: {plan:?}");
    };
    assert_eq!(*range, TableKeyRange { lower: None, upper: Some(TableKeyBound::Exclusive(10)) });
}

#[test]
fn select_multiple_primary_key_bounds_with_residual_filter_uses_combined_range_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE 1 <= id AND id <= 10 AND age == 7;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "age", 2, DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::Integer(7))),
        }
    );
    let PhysicalPlan::PrimaryKeyRangeScan { range, .. } = input.as_ref() else {
        panic!("expected primary key range scan under residual filter: {plan:?}");
    };
    assert_eq!(
        *range,
        TableKeyRange {
            lower: Some(TableKeyBound::Inclusive(1)),
            upper: Some(TableKeyBound::Inclusive(10)),
        }
    );
}

#[test]
fn select_non_leading_primary_key_range_stays_full_table_scan() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE age == 7 AND id < 10;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter under project: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn select_secondary_index_integer_column_range_uses_index_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_age", "users", &["age"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE 18 <= age AND age < 65;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter under project: {plan:?}");
    };
    let PhysicalPlan::SecondaryIndexScan { scan } = input.as_ref() else {
        panic!("expected secondary index scan under residual filter: {plan:?}");
    };
    assert_eq!(scan.index.name, "idx_users_age");
    assert_eq!(
        &scan.value_range,
        &IndexValueRange {
            lower: Some(IndexValueBound::Inclusive(Value::Integer(18))),
            upper: Some(IndexValueBound::Exclusive(Value::Integer(65))),
        }
    );
}

#[test]
fn select_secondary_index_equality_uses_index_scan_with_residual_filter() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert_eq!(
        predicate,
        &PlannedExpression::Binary {
            left: Box::new(PlannedExpression::Column(bound("users", "name", 1, DataType::Text))),
            op: Op::EqualsEquals,
            right: Box::new(PlannedExpression::Literal(Value::String("Ada".to_owned()))),
        }
    );
    let PhysicalPlan::SecondaryIndexScan { scan } = input.as_ref() else {
        panic!("expected secondary index scan under residual filter: {plan:?}");
    };
    assert_eq!(scan.table.name, "users");
    assert_eq!(scan.index.name, "idx_users_name");
    assert_eq!(scan.column.name, "name");
    assert_eq!(
        &scan.value_range,
        &IndexValueRange {
            lower: Some(IndexValueBound::Inclusive(Value::String("Ada".to_owned()))),
            upper: Some(IndexValueBound::Inclusive(Value::String("Ada".to_owned()))),
        }
    );
}

#[test]
fn primary_key_scan_wins_over_secondary_index_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE id == 1 AND name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::PrimaryKeyRangeScan {
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(1)),
                upper: Some(TableKeyBound::Inclusive(1)),
            },
            ..
        }
    ));
}

#[test]
fn select_text_secondary_index_range_stays_full_table_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE name >= 'Amy' AND name <= 'Charlotte';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn select_reversed_text_secondary_index_range_stays_full_table_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE 'Amy' < name AND 'Charlotte' >= name;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn secondary_index_selection_skips_text_range_conjuncts() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE age == 7 AND name >= 'Amy';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn leftmost_usable_secondary_index_predicate_wins() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    database.create_index("idx_users_age", "users", &["age"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE age == 7 AND name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    let PhysicalPlan::SecondaryIndexScan { scan } = input.as_ref() else {
        panic!("expected secondary index scan under residual filter: {plan:?}");
    };
    assert_eq!(scan.index.name, "idx_users_age");
    assert_eq!(scan.column.name, "age");
    assert_eq!(
        &scan.value_range,
        &IndexValueRange {
            lower: Some(IndexValueBound::Inclusive(Value::Integer(7))),
            upper: Some(IndexValueBound::Inclusive(Value::Integer(7))),
        }
    );
}

#[test]
fn earliest_created_exact_secondary_index_wins_for_same_column() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name_first", "users", &["name"]).unwrap();
    database.create_index("idx_users_name_second", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter under project: {plan:?}");
    };
    let PhysicalPlan::SecondaryIndexScan { scan } = input.as_ref() else {
        panic!("expected secondary index scan under residual filter: {plan:?}");
    };
    assert_eq!(scan.index.name, "idx_users_name_first");
}

#[test]
fn unindexed_equality_stays_full_table_scan() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users WHERE age == 7;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Project { input, .. } = &plan.physical else {
        panic!("expected project root: {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter under project: {plan:?}");
    };
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn explain_select_wraps_planned_select() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("EXPLAIN SELECT name FROM users WHERE id == 1;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Explain { input } = &plan.logical else {
        panic!("expected logical explain plan: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));

    let PhysicalPlan::Explain { input } = &plan.physical else {
        panic!("expected physical explain plan: {plan:?}");
    };
    let PhysicalPlan::Project { input, expressions } = input.as_ref() else {
        panic!("expected explained select project plan: {plan:?}");
    };
    assert_eq!(
        expressions,
        &[PlannedExpression::Column(bound("users", "name", 1, DataType::Text))]
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::PrimaryKeyRangeScan {
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(1)),
                upper: Some(TableKeyBound::Inclusive(1)),
            },
            ..
        }
    ));
}

#[test]
fn explain_update_wraps_planned_update() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("EXPLAIN UPDATE users SET name = 'Ada' WHERE id == 1;");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Explain { input } = &plan.logical else {
        panic!("expected logical explain plan: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Update { table, .. } if table.name == "users"));

    let PhysicalPlan::Explain { input } = &plan.physical else {
        panic!("expected physical explain plan: {plan:?}");
    };
    let PhysicalPlan::Update { table, assignments, input } = input.as_ref() else {
        panic!("expected explained update plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    assert_eq!(
        assignments,
        &[UpdateAssignment {
            column: bound("users", "name", 1, DataType::Text),
            expression: PlannedExpression::Literal(Value::String("Ada".to_owned())),
        }]
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::PrimaryKeyRangeScan {
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(1)),
                upper: Some(TableKeyBound::Inclusive(1)),
            },
            ..
        }
    ));
}

#[test]
fn explain_delete_wraps_planned_delete() {
    let (_dir, database) = database_with_users();
    database.create_index("idx_users_name", "users", &["name"]).unwrap();
    let planner = Planner::new(&database);
    let statement = parse("EXPLAIN DELETE FROM users WHERE name == 'Ada';");

    let plan = planner.plan_statement(&statement).unwrap();

    let LogicalPlan::Explain { input } = &plan.logical else {
        panic!("expected logical explain plan: {plan:?}");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Delete { table, .. } if table.name == "users"));

    let PhysicalPlan::Explain { input } = &plan.physical else {
        panic!("expected physical explain plan: {plan:?}");
    };
    let PhysicalPlan::Delete { table, input } = input.as_ref() else {
        panic!("expected explained delete plan: {plan:?}");
    };
    assert_eq!(table.name, "users");
    let PhysicalPlan::Filter { input, .. } = input.as_ref() else {
        panic!("expected residual filter over full table scan: {plan:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalPlan::FullTableScan { table } if table.name == "users"
    ));
}

#[test]
fn select_order_limit_offset_preserves_operator_order() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);
    let statement = parse("SELECT name FROM users ORDER BY id LIMIT 10 OFFSET 5;");

    let plan = planner.plan_statement(&statement).unwrap();

    let PhysicalPlan::Limit { input, limit } = &plan.physical else {
        panic!("expected limit root: {plan:?}");
    };
    assert_eq!(*limit, 10);
    let PhysicalPlan::Offset { input, offset } = input.as_ref() else {
        panic!("expected offset under limit: {plan:?}");
    };
    assert_eq!(*offset, 5);
    let PhysicalPlan::Project { input, .. } = input.as_ref() else {
        panic!("expected project under offset: {plan:?}");
    };
    let PhysicalPlan::Sort { input, terms } = input.as_ref() else {
        panic!("expected sort under project: {plan:?}");
    };
    assert_eq!(
        terms,
        &[SortTerm { column: bound("users", "id", 0, DataType::Integer), direction: None }]
    );
    assert!(
        matches!(input.as_ref(), PhysicalPlan::FullTableScan { table } if table.name == "users")
    );
}

#[test]
fn reports_semantic_planning_errors() {
    let (_dir, database) = database_with_users();
    let planner = Planner::new(&database);

    assert!(matches!(
        planner.plan_statement(&parse("SELECT * FROM missing;")),
        Err(PlannerError::TableNotFound { name }) if name == "missing"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("SELECT missing FROM users;")),
        Err(PlannerError::ColumnNotFound { column }) if column == "missing"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("DELETE FROM missing;")),
        Err(PlannerError::TableNotFound { name }) if name == "missing"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("DELETE FROM users WHERE missing == 1;")),
        Err(PlannerError::ColumnNotFound { column }) if column == "missing"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("SELECT id FROM users WHERE * == id;")),
        Err(PlannerError::UnsupportedWildcardPosition)
    ));
    assert!(matches!(
        planner.plan_statement(&parse("SELECT *;")),
        Err(PlannerError::WildcardRequiresTable)
    ));
    assert!(matches!(
        planner.plan_statement(&parse("SELECT COUNT(*) FROM users;")),
        Err(PlannerError::UnsupportedAggregate { function }) if function == "COUNT"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("INSERT INTO users (id, id) VALUES (1, 2);")),
        Err(PlannerError::DuplicateInsertColumn { column }) if column == "id"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("INSERT INTO users (id, name) VALUES (1);")),
        Err(PlannerError::InsertColumnValueCount { columns: 2, values: 1 })
    ));
    assert!(matches!(
        planner.plan_statement(&parse("CREATE INDEX idx_missing ON users (missing);")),
        Err(PlannerError::ColumnNotFound { column }) if column == "missing"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("CREATE INDEX idx_duplicate ON users (name, name);")),
        Err(PlannerError::DuplicateIndexColumn { column }) if column == "name"
    ));
    assert!(matches!(
        planner.plan_statement(&parse("EXPLAIN INSERT INTO users (id) VALUES (1);")),
        Err(PlannerError::UnsupportedStatement { statement }) if statement.starts_with("INSERT")
    ));
}

fn bound(table: &str, name: &str, ordinal: usize, data_type: DataType) -> BoundColumn {
    BoundColumn { table: table.to_owned(), name: name.to_owned(), ordinal, data_type }
}
