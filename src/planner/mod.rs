//! SQL query planning.
//!
//! The planner is the boundary between parsed SQL syntax and executable
//! database work. It lowers one parsed
//! [`Statement`](crate::sql_parser::parser::stmt::Statement) into a [`Plan`] containing:
//!
//! - a catalog-bound [`LogicalPlan`] that describes the statement's meaning; and
//! - a [`PhysicalPlan`] that selects the executor operators used to run it.
//!
//! During logical planning, table and column names are resolved against the
//! catalog, wildcard projections are expanded, duplicate target columns are
//! rejected, and scalar expressions are converted into [`BoundExpr`] trees.
//! The resulting plan carries [`TableSchema`](crate::core::TableSchema) and
//! [`BoundColumn`] values
//! so later stages can work with ordinals and storage types instead of repeating
//! name lookup. Logical optimization folds constant scalar expressions,
//! eliminates empty and redundant operators, pushes predicates toward source
//! scans, and propagates literal constraints through inner-join equalities.
//!
//! Physical planning is intentionally small and predictable. Most relational
//! operators are translated directly, while table access can be narrowed from a
//! full scan to [`PhysicalPlanNode::PrimaryKeyRangeScan`] for integer primary-key
//! predicates, or to [`PhysicalPlanNode::SecondaryIndexScan`] for compatible
//! single-column secondary-index predicates. Predicates that cannot be fully
//! represented by an access path are retained as residual
//! [`PhysicalPlanNode::Filter`] operators. Mutation inputs use primary-key range
//! or full table scans so writes cannot invalidate the index that drives them.
//!
//! The planner validates statement shape, but it does not execute side effects
//! or enforce every runtime constraint. Storage, type, arithmetic, and mutation
//! errors that depend on actual row values are still reported by the executor
//! and storage layers.

mod binder;
mod error;
mod expression;
mod identity;
mod logical;
mod optimizer;
mod physical;
mod plan;
mod planning;
mod schema;

pub use error::{PlannerError, PlannerResult};
pub use expression::{
    BoundColumn, BoundExpr, BoundSortTerm, BoundUpdateAssignment, ExecColumn, ExecExpr, SortTerm,
    UpdateAssignment,
};

/// Backwards-compatible name for bound scalar expressions.
pub type PlannedExpression = BoundExpr;
pub use identity::{NodeId, RelationId};
pub use logical::{LogicalPlan, LogicalPlanNode};
pub use physical::{
    IndexValueBound, IndexValueRange, PhysicalPlan, PhysicalPlanNode, SecondaryIndexScanPlan,
};
pub use plan::Plan;
pub use planning::Planner;
pub use schema::{PlanColumn, PlanSchema};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{
            CatalogId, ColumnSchema, DataType, IndexColumnSchema, IndexKeyBound, IndexKeyRange,
            IndexSchema, InvalidArgumentError, StorageError, TableKeyBound, TableKeyRange,
            TableSchema, TupleSchema, Value, access::CatalogRead,
        },
        relational::{cursor::encode_index_entry_key, tuple::Tuple},
        sql_parser::parser::{
            Parser,
            op::Op,
            stmt::{Statement, select::Ordering},
        },
    };

    fn parse(sql: &str) -> Statement<'_> {
        Parser::new(sql).stmt().unwrap()
    }

    fn users_table() -> TableSchema {
        TableSchema {
            table_id: 4,
            name: "users".into(),
            root_page_id: 4,
            row: TupleSchema {
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        data_type: DataType::Integer,
                        nullable: false,
                        primary_key: true,
                    },
                    ColumnSchema {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: false,
                        primary_key: false,
                    },
                    ColumnSchema {
                        name: "age".into(),
                        data_type: DataType::Integer,
                        nullable: true,
                        primary_key: false,
                    },
                ],
            },
        }
    }

    #[derive(Default)]
    struct MemoryCatalog {
        indexes: Vec<IndexSchema>,
    }

    impl MemoryCatalog {
        fn with_indexes(indexes: &[(&str, usize)]) -> Self {
            let table = users_table();
            let indexes = indexes
                .iter()
                .enumerate()
                .map(|(position, &(name, ordinal))| {
                    let index_id = CatalogId::try_from(position + 5).unwrap();
                    IndexSchema {
                        index_id,
                        name: name.into(),
                        table_id: table.table_id,
                        root_page_id: index_id.try_into().unwrap(),
                        unique: false,
                        columns: vec![IndexColumnSchema {
                            source_column_ordinal: ordinal,
                            column: table.row.columns[ordinal].clone(),
                        }],
                    }
                })
                .collect();
            Self { indexes }
        }
    }

    impl CatalogRead for MemoryCatalog {
        fn table_schema_by_name(&self, name: &str) -> Result<TableSchema, StorageError> {
            (name == "users").then(users_table).ok_or_else(|| {
                StorageError::InvalidArgument(InvalidArgumentError::TableNotFound {
                    name: name.into(),
                })
            })
        }

        fn index_schemas_for_table(
            &self,
            _table: &TableSchema,
        ) -> Result<Vec<IndexSchema>, StorageError> {
            Ok(self.indexes.clone())
        }
    }

    fn plan(catalog: &MemoryCatalog, sql: &str) -> Plan {
        Planner::with_schema(catalog).plan_statement(&parse(sql)).unwrap()
    }

    fn physical_plan(
        nodes: Vec<PhysicalPlanNode>,
        output_schema: Option<PlanSchema>,
    ) -> PhysicalPlan {
        let root = NodeId::new(nodes.len() - 1);
        PhysicalPlan::from_parts(nodes, root, output_schema)
    }

    fn assert_physical_plan(catalog: &MemoryCatalog, sql: &str, expected: PhysicalPlan) {
        assert_eq!(plan(catalog, sql).physical, expected, "{sql}");
    }

    fn bound_column(relation: RelationId, table: &str, ordinal: usize) -> BoundColumn {
        let column = &users_table().row.columns[ordinal];
        BoundColumn {
            relation,
            table: table.into(),
            name: column.name.clone(),
            ordinal,
            data_type: column.data_type,
        }
    }

    fn exec_column(slot: usize, name: &str, data_type: DataType) -> ExecColumn {
        ExecColumn { slot, name: name.into(), data_type }
    }

    fn users_output(relation: RelationId, table: &str, ordinals: &[usize]) -> PlanSchema {
        let columns = PlanSchema::for_qualified_table(relation, &users_table(), table).columns;
        PlanSchema { columns: ordinals.iter().map(|&ordinal| columns[ordinal].clone()).collect() }
    }

    fn secondary_index_scan(
        catalog: &MemoryCatalog,
        index: usize,
        relation: RelationId,
        column: usize,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    ) -> PhysicalPlanNode {
        let key_bound = |value: &Value, inclusive, table_key| {
            let value = Tuple::new(vec![value.clone()]).to_bytes().unwrap();
            let bound = encode_index_entry_key(&value, table_key);
            if inclusive {
                IndexKeyBound::Inclusive(bound)
            } else {
                IndexKeyBound::Exclusive(bound)
            }
        };
        let lower_key = lower.as_ref().map(|(value, inclusive)| {
            key_bound(value, *inclusive, if *inclusive { i32::MIN } else { i32::MAX })
        });
        let upper_key = upper.as_ref().map(|(value, inclusive)| {
            key_bound(value, *inclusive, if *inclusive { i32::MAX } else { i32::MIN })
        });
        let value_bound = |(value, inclusive): (Value, bool)| {
            if inclusive {
                IndexValueBound::Inclusive(value)
            } else {
                IndexValueBound::Exclusive(value)
            }
        };
        PhysicalPlanNode::SecondaryIndexScan {
            scan: SecondaryIndexScanPlan {
                relation,
                table: users_table(),
                index: catalog.indexes[index].clone(),
                column: bound_column(relation, "users", column),
                value_range: IndexValueRange {
                    lower: lower.map(value_bound),
                    upper: upper.map(value_bound),
                },
                key_range: IndexKeyRange { lower: lower_key, upper: upper_key },
            },
        }
    }

    #[test]
    fn create_table_preserves_the_declared_schema() {
        assert_eq!(
            plan(
                &MemoryCatalog::default(),
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT NULLABLE);",
            ),
            Plan {
                logical: LogicalPlan::from_parts(
                    vec![LogicalPlanNode::CreateTable {
                        name: "users".into(),
                        schema: users_table().row.clone(),
                    }],
                    NodeId::new(0),
                ),
                physical: physical_plan(
                    vec![PhysicalPlanNode::CreateTable {
                        name: "users".into(),
                        schema: users_table().row,
                    }],
                    None,
                ),
            }
        );
    }

    #[test]
    fn create_index_binds_source_columns() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "CREATE INDEX users_name_age ON users (name, age);",
            physical_plan(
                vec![PhysicalPlanNode::CreateIndex {
                    name: "users_name_age".into(),
                    table: users_table(),
                    columns: vec![
                        bound_column(relation, "users", 1),
                        bound_column(relation, "users", 2),
                    ],
                }],
                None,
            ),
        );
    }

    #[test]
    fn insert_binds_columns_and_literal_rows() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "INSERT INTO users (name, id) VALUES ('Ada', 1), ('Grace', 2);",
            physical_plan(
                vec![PhysicalPlanNode::InsertValues {
                    table: users_table(),
                    columns: vec![
                        bound_column(relation, "users", 1),
                        bound_column(relation, "users", 0),
                    ],
                    values: vec![
                        vec![
                            ExecExpr::Literal(Value::String("Ada".into())),
                            ExecExpr::Literal(Value::Integer(1)),
                        ],
                        vec![
                            ExecExpr::Literal(Value::String("Grace".into())),
                            ExecExpr::Literal(Value::Integer(2)),
                        ],
                    ],
                }],
                None,
            ),
        );
    }

    #[test]
    fn select_star_expands_bound_table_columns() {
        let relation = RelationId::new(0);
        let expected = physical_plan(
            vec![PhysicalPlanNode::FullTableScan { relation, table: users_table() }],
            Some(PlanSchema::for_table(relation, &users_table())),
        );

        assert_physical_plan(&MemoryCatalog::default(), "SELECT * FROM users;", expected);
    }

    #[test]
    fn select_binds_qualified_columns_and_expressions() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "SELECT users.name, age + 1 FROM users WHERE users.id == 7;",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: Some(TableKeyBound::Inclusive(7)),
                            upper: Some(TableKeyBound::Inclusive(7)),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(0),
                        expressions: vec![
                            ExecExpr::Column(exec_column(1, "users.name", DataType::Text)),
                            ExecExpr::Binary {
                                left: Box::new(ExecExpr::Column(exec_column(
                                    2,
                                    "users.age",
                                    DataType::Integer,
                                ))),
                                op: Op::Add,
                                right: Box::new(ExecExpr::Literal(Value::Integer(1))),
                            },
                        ],
                    },
                ],
                Some(PlanSchema::for_expressions(&[
                    BoundExpr::Column(bound_column(relation, "users", 1)),
                    BoundExpr::Binary {
                        left: Box::new(BoundExpr::Column(bound_column(relation, "users", 2))),
                        op: Op::Add,
                        right: Box::new(BoundExpr::Literal(Value::Integer(1))),
                    },
                ])),
            ),
        );
    }

    #[test]
    fn select_table_aliases_bind_qualified_columns_in_every_clause() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "SELECT u.name FROM users AS u WHERE u.id == 7 ORDER BY u.age DESC;",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: Some(TableKeyBound::Inclusive(7)),
                            upper: Some(TableKeyBound::Inclusive(7)),
                        },
                    },
                    PhysicalPlanNode::Sort {
                        input: NodeId::new(0),
                        terms: vec![SortTerm {
                            column: exec_column(2, "u.age", DataType::Integer),
                            direction: Some(Ordering::Descending),
                        }],
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "u.name",
                            DataType::Text,
                        ))],
                    },
                ],
                Some(PlanSchema {
                    columns: vec![
                        PlanSchema::for_qualified_table(relation, &users_table(), "u").columns[1]
                            .clone(),
                    ],
                }),
            ),
        );

        assert_physical_plan(
            &MemoryCatalog::default(),
            "SELECT * FROM users AS u;",
            physical_plan(
                vec![PhysicalPlanNode::FullTableScan { relation, table: users_table() }],
                Some(PlanSchema::for_qualified_table(relation, &users_table(), "u")),
            ),
        );
    }

    #[test]
    fn table_alias_hides_the_catalog_table_name() {
        let catalog = MemoryCatalog::default();
        let planner = Planner::with_schema(&catalog);

        assert!(matches!(
            planner.plan_statement(&parse("SELECT users.name FROM users AS u;")),
            Err(PlannerError::TableNotInScope { table }) if table == "users"
        ));
    }

    #[test]
    fn aliased_predicates_can_use_secondary_indexes() {
        let catalog = MemoryCatalog::with_indexes(&[("users_name", 1)]);
        let relation = RelationId::new(0);
        let indexed_value = Value::String("Ada".into());
        let encoded_value = Tuple::new(vec![indexed_value.clone()]).to_bytes().unwrap();
        let indexed_column = BoundColumn {
            relation,
            table: "u".into(),
            name: "name".into(),
            ordinal: 1,
            data_type: DataType::Text,
        };
        assert_physical_plan(
            &catalog,
            "SELECT u.id FROM users AS u WHERE u.name == 'Ada';",
            physical_plan(
                vec![
                    PhysicalPlanNode::SecondaryIndexScan {
                        scan: SecondaryIndexScanPlan {
                            relation,
                            table: users_table(),
                            index: catalog.indexes[0].clone(),
                            column: indexed_column,
                            value_range: IndexValueRange {
                                lower: Some(IndexValueBound::Inclusive(indexed_value.clone())),
                                upper: Some(IndexValueBound::Inclusive(indexed_value.clone())),
                            },
                            key_range: IndexKeyRange {
                                lower: Some(IndexKeyBound::Inclusive(encode_index_entry_key(
                                    &encoded_value,
                                    i32::MIN,
                                ))),
                                upper: Some(IndexKeyBound::Inclusive(encode_index_entry_key(
                                    &encoded_value,
                                    i32::MAX,
                                ))),
                            },
                        },
                    },
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Column(exec_column(
                                1,
                                "u.name",
                                DataType::Text,
                            ))),
                            op: Op::EqualsEquals,
                            right: Box::new(ExecExpr::Literal(indexed_value)),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            0,
                            "u.id",
                            DataType::Integer,
                        ))],
                    },
                ],
                Some(PlanSchema {
                    columns: vec![
                        PlanSchema::for_qualified_table(relation, &users_table(), "u").columns[0]
                            .clone(),
                    ],
                }),
            ),
        );
    }

    #[test]
    fn select_without_from_uses_one_synthetic_row() {
        let expression = ExecExpr::Literal(Value::Integer(3));
        let output = PlanSchema::for_expressions(&[BoundExpr::Binary {
            left: Box::new(BoundExpr::Literal(Value::Integer(1))),
            op: Op::Add,
            right: Box::new(BoundExpr::Literal(Value::Integer(2))),
        }]);
        let expected = physical_plan(
            vec![
                PhysicalPlanNode::OneRow,
                PhysicalPlanNode::Project { input: NodeId::new(0), expressions: vec![expression] },
            ],
            Some(output),
        );

        assert_physical_plan(&MemoryCatalog::default(), "SELECT 1 + 2;", expected);
    }

    #[test]
    fn update_and_delete_choose_primary_key_scans() {
        let catalog = MemoryCatalog::default();
        let relation = RelationId::new(0);

        assert_physical_plan(
            &catalog,
            "UPDATE users SET age = age + 1 WHERE id == 7;",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: Some(TableKeyBound::Inclusive(7)),
                            upper: Some(TableKeyBound::Inclusive(7)),
                        },
                    },
                    PhysicalPlanNode::Update {
                        relation,
                        table: users_table(),
                        assignments: vec![UpdateAssignment {
                            column: bound_column(relation, "users", 2),
                            expression: ExecExpr::Binary {
                                left: Box::new(ExecExpr::Column(exec_column(
                                    2,
                                    "users.age",
                                    DataType::Integer,
                                ))),
                                op: Op::Add,
                                right: Box::new(ExecExpr::Literal(Value::Integer(1))),
                            },
                        }],
                        input: NodeId::new(0),
                    },
                ],
                None,
            ),
        );
        assert_physical_plan(
            &catalog,
            "DELETE FROM users WHERE 2 <= id AND id < 5;",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: Some(TableKeyBound::Inclusive(2)),
                            upper: Some(TableKeyBound::Exclusive(5)),
                        },
                    },
                    PhysicalPlanNode::Delete {
                        relation,
                        table: users_table(),
                        input: NodeId::new(0),
                    },
                ],
                None,
            ),
        );
    }

    #[test]
    fn contradictory_primary_key_bounds_produce_empty_scans() {
        let catalog = MemoryCatalog::default();
        let relation = RelationId::new(0);
        for predicate in [
            "id == 2 AND id == 3",
            "id > 3 AND id < 3",
            "id >= 3 AND id < 3",
            "id > 3 AND id <= 3",
            "id > 9 AND id < 2",
        ] {
            assert_physical_plan(
                &catalog,
                &format!("SELECT name FROM users WHERE {predicate};"),
                physical_plan(
                    vec![PhysicalPlanNode::Empty],
                    Some(users_output(relation, "users", &[1])),
                ),
            );
            assert_physical_plan(
                &catalog,
                &format!("UPDATE users SET age = 1 WHERE {predicate};"),
                physical_plan(
                    vec![
                        PhysicalPlanNode::Empty,
                        PhysicalPlanNode::Update {
                            relation,
                            table: users_table(),
                            assignments: vec![UpdateAssignment {
                                column: bound_column(relation, "users", 2),
                                expression: ExecExpr::Literal(Value::Integer(1)),
                            }],
                            input: NodeId::new(0),
                        },
                    ],
                    None,
                ),
            );
            assert_physical_plan(
                &catalog,
                &format!("DELETE FROM users WHERE {predicate};"),
                physical_plan(
                    vec![
                        PhysicalPlanNode::Empty,
                        PhysicalPlanNode::Delete {
                            relation,
                            table: users_table(),
                            input: NodeId::new(0),
                        },
                    ],
                    None,
                ),
            );
        }
    }

    #[test]
    fn mutations_never_scan_a_secondary_index() {
        let catalog = MemoryCatalog::with_indexes(&[("users_age", 2)]);
        let relation = RelationId::new(0);
        let age_is_seven = ExecExpr::Binary {
            left: Box::new(ExecExpr::Column(exec_column(2, "users.age", DataType::Integer))),
            op: Op::EqualsEquals,
            right: Box::new(ExecExpr::Literal(Value::Integer(7))),
        };

        assert_physical_plan(
            &catalog,
            "UPDATE users SET name = 'Ada' WHERE age == 7;",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: age_is_seven.clone(),
                    },
                    PhysicalPlanNode::Update {
                        relation,
                        table: users_table(),
                        assignments: vec![UpdateAssignment {
                            column: bound_column(relation, "users", 1),
                            expression: ExecExpr::Literal(Value::String("Ada".into())),
                        }],
                        input: NodeId::new(1),
                    },
                ],
                None,
            ),
        );
        assert_physical_plan(
            &catalog,
            "DELETE FROM users WHERE age == 7;",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Filter { input: NodeId::new(0), predicate: age_is_seven },
                    PhysicalPlanNode::Delete {
                        relation,
                        table: users_table(),
                        input: NodeId::new(1),
                    },
                ],
                None,
            ),
        );
    }

    #[test]
    fn primary_key_predicates_produce_tight_ranges() {
        let catalog = MemoryCatalog::default();
        let relation = RelationId::new(0);

        for (predicate, range) in [
            (
                "id == 10",
                TableKeyRange {
                    lower: Some(TableKeyBound::Inclusive(10)),
                    upper: Some(TableKeyBound::Inclusive(10)),
                },
            ),
            (
                "10 <= id AND id < 20",
                TableKeyRange {
                    lower: Some(TableKeyBound::Inclusive(10)),
                    upper: Some(TableKeyBound::Exclusive(20)),
                },
            ),
            (
                "id > 3 AND id <= 8",
                TableKeyRange {
                    lower: Some(TableKeyBound::Exclusive(3)),
                    upper: Some(TableKeyBound::Inclusive(8)),
                },
            ),
        ] {
            assert_physical_plan(
                &catalog,
                &format!("SELECT name FROM users WHERE {predicate};"),
                physical_plan(
                    vec![
                        PhysicalPlanNode::PrimaryKeyRangeScan {
                            relation,
                            table: users_table(),
                            range,
                        },
                        PhysicalPlanNode::Project {
                            input: NodeId::new(0),
                            expressions: vec![ExecExpr::Column(exec_column(
                                1,
                                "users.name",
                                DataType::Text,
                            ))],
                        },
                    ],
                    Some(users_output(relation, "users", &[1])),
                ),
            );
        }
    }

    #[test]
    fn unused_predicates_remain_as_residual_filters() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "SELECT name FROM users WHERE id < 10 AND age == 7;",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: None,
                            upper: Some(TableKeyBound::Exclusive(10)),
                        },
                    },
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Column(exec_column(
                                2,
                                "users.age",
                                DataType::Integer,
                            ))),
                            op: Op::EqualsEquals,
                            right: Box::new(ExecExpr::Literal(Value::Integer(7))),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[1])),
            ),
        );
    }

    #[test]
    fn compatible_secondary_index_predicates_use_index_scans() {
        let catalog = MemoryCatalog::with_indexes(&[("users_name", 1), ("users_age", 2)]);
        let relation = RelationId::new(0);

        assert_physical_plan(
            &catalog,
            "SELECT id FROM users WHERE name == 'Ada';",
            physical_plan(
                vec![
                    secondary_index_scan(
                        &catalog,
                        0,
                        relation,
                        1,
                        Some((Value::String("Ada".into()), true)),
                        Some((Value::String("Ada".into()), true)),
                    ),
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Column(exec_column(
                                1,
                                "users.name",
                                DataType::Text,
                            ))),
                            op: Op::EqualsEquals,
                            right: Box::new(ExecExpr::Literal(Value::String("Ada".into()))),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            0,
                            "users.id",
                            DataType::Integer,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[0])),
            ),
        );
        assert_physical_plan(
            &catalog,
            "SELECT id FROM users WHERE 18 <= age AND age < 65;",
            physical_plan(
                vec![
                    secondary_index_scan(
                        &catalog,
                        1,
                        relation,
                        2,
                        Some((Value::Integer(18), true)),
                        Some((Value::Integer(65), false)),
                    ),
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Binary {
                                left: Box::new(ExecExpr::Literal(Value::Integer(18))),
                                op: Op::LessThanOrEqual,
                                right: Box::new(ExecExpr::Column(exec_column(
                                    2,
                                    "users.age",
                                    DataType::Integer,
                                ))),
                            }),
                            op: Op::And,
                            right: Box::new(ExecExpr::Binary {
                                left: Box::new(ExecExpr::Column(exec_column(
                                    2,
                                    "users.age",
                                    DataType::Integer,
                                ))),
                                op: Op::LessThan,
                                right: Box::new(ExecExpr::Literal(Value::Integer(65))),
                            }),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            0,
                            "users.id",
                            DataType::Integer,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[0])),
            ),
        );
    }

    #[test]
    fn planner_chooses_the_highest_priority_usable_index() {
        let catalog = MemoryCatalog::with_indexes(&[
            ("users_name_first", 1),
            ("users_name_second", 1),
            ("users_age", 2),
        ]);

        let relation = RelationId::new(0);
        assert_physical_plan(
            &catalog,
            "SELECT name FROM users WHERE id == 1 AND name == 'Ada';",
            physical_plan(
                vec![
                    PhysicalPlanNode::PrimaryKeyRangeScan {
                        relation,
                        table: users_table(),
                        range: TableKeyRange {
                            lower: Some(TableKeyBound::Inclusive(1)),
                            upper: Some(TableKeyBound::Inclusive(1)),
                        },
                    },
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Column(exec_column(
                                1,
                                "users.name",
                                DataType::Text,
                            ))),
                            op: Op::EqualsEquals,
                            right: Box::new(ExecExpr::Literal(Value::String("Ada".into()))),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[1])),
            ),
        );
        assert_physical_plan(
            &catalog,
            "SELECT name FROM users WHERE age == 7 AND name == 'Ada';",
            physical_plan(
                vec![
                    secondary_index_scan(
                        &catalog,
                        2,
                        relation,
                        2,
                        Some((Value::Integer(7), true)),
                        Some((Value::Integer(7), true)),
                    ),
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Binary {
                                left: Box::new(ExecExpr::Column(exec_column(
                                    2,
                                    "users.age",
                                    DataType::Integer,
                                ))),
                                op: Op::EqualsEquals,
                                right: Box::new(ExecExpr::Literal(Value::Integer(7))),
                            }),
                            op: Op::And,
                            right: Box::new(ExecExpr::Binary {
                                left: Box::new(ExecExpr::Column(exec_column(
                                    1,
                                    "users.name",
                                    DataType::Text,
                                ))),
                                op: Op::EqualsEquals,
                                right: Box::new(ExecExpr::Literal(Value::String("Ada".into()))),
                            }),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[1])),
            ),
        );
        assert_physical_plan(
            &catalog,
            "SELECT name FROM users WHERE name == 'Ada';",
            physical_plan(
                vec![
                    secondary_index_scan(
                        &catalog,
                        0,
                        relation,
                        1,
                        Some((Value::String("Ada".into()), true)),
                        Some((Value::String("Ada".into()), true)),
                    ),
                    PhysicalPlanNode::Filter {
                        input: NodeId::new(0),
                        predicate: ExecExpr::Binary {
                            left: Box::new(ExecExpr::Column(exec_column(
                                1,
                                "users.name",
                                DataType::Text,
                            ))),
                            op: Op::EqualsEquals,
                            right: Box::new(ExecExpr::Literal(Value::String("Ada".into()))),
                        },
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                ],
                Some(users_output(relation, "users", &[1])),
            ),
        );
    }

    #[test]
    fn unsupported_access_predicates_use_a_full_scan() {
        let catalog = MemoryCatalog::with_indexes(&[("users_name", 1)]);
        let relation = RelationId::new(0);

        for (predicate, filter) in [
            (
                "age == 7",
                ExecExpr::Binary {
                    left: Box::new(ExecExpr::Column(exec_column(
                        2,
                        "users.age",
                        DataType::Integer,
                    ))),
                    op: Op::EqualsEquals,
                    right: Box::new(ExecExpr::Literal(Value::Integer(7))),
                },
            ),
            (
                "name >= 'Amy'",
                ExecExpr::Binary {
                    left: Box::new(ExecExpr::Column(exec_column(1, "users.name", DataType::Text))),
                    op: Op::GreaterThanOrEqual,
                    right: Box::new(ExecExpr::Literal(Value::String("Amy".into()))),
                },
            ),
            (
                "age == 7 AND id < 10",
                ExecExpr::Binary {
                    left: Box::new(ExecExpr::Binary {
                        left: Box::new(ExecExpr::Column(exec_column(
                            2,
                            "users.age",
                            DataType::Integer,
                        ))),
                        op: Op::EqualsEquals,
                        right: Box::new(ExecExpr::Literal(Value::Integer(7))),
                    }),
                    op: Op::And,
                    right: Box::new(ExecExpr::Binary {
                        left: Box::new(ExecExpr::Column(exec_column(
                            0,
                            "users.id",
                            DataType::Integer,
                        ))),
                        op: Op::LessThan,
                        right: Box::new(ExecExpr::Literal(Value::Integer(10))),
                    }),
                },
            ),
        ] {
            assert_physical_plan(
                &catalog,
                &format!("SELECT name FROM users WHERE {predicate};"),
                physical_plan(
                    vec![
                        PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                        PhysicalPlanNode::Filter { input: NodeId::new(0), predicate: filter },
                        PhysicalPlanNode::Project {
                            input: NodeId::new(1),
                            expressions: vec![ExecExpr::Column(exec_column(
                                1,
                                "users.name",
                                DataType::Text,
                            ))],
                        },
                    ],
                    Some(users_output(relation, "users", &[1])),
                ),
            );
        }
    }

    #[test]
    fn select_operators_are_planned_in_sql_evaluation_order() {
        let relation = RelationId::new(0);
        assert_physical_plan(
            &MemoryCatalog::default(),
            "SELECT name FROM users ORDER BY id DESC LIMIT 10 OFFSET 5;",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Sort {
                        input: NodeId::new(0),
                        terms: vec![SortTerm {
                            column: exec_column(0, "users.id", DataType::Integer),
                            direction: Some(Ordering::Descending),
                        }],
                    },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(1),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                    PhysicalPlanNode::Offset { input: NodeId::new(2), offset: 5 },
                    PhysicalPlanNode::Limit { input: NodeId::new(3), limit: 10 },
                ],
                Some(users_output(relation, "users", &[1])),
            ),
        );
    }

    #[test]
    fn explain_wraps_supported_statement_plans() {
        let catalog = MemoryCatalog::default();
        let relation = RelationId::new(0);

        assert_physical_plan(
            &catalog,
            "EXPLAIN SELECT name FROM users;",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Project {
                        input: NodeId::new(0),
                        expressions: vec![ExecExpr::Column(exec_column(
                            1,
                            "users.name",
                            DataType::Text,
                        ))],
                    },
                    PhysicalPlanNode::Explain { input: NodeId::new(1) },
                ],
                None,
            ),
        );
        assert_physical_plan(
            &catalog,
            "EXPLAIN UPDATE users SET name = 'Ada';",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Update {
                        relation,
                        table: users_table(),
                        assignments: vec![UpdateAssignment {
                            column: bound_column(relation, "users", 1),
                            expression: ExecExpr::Literal(Value::String("Ada".into())),
                        }],
                        input: NodeId::new(0),
                    },
                    PhysicalPlanNode::Explain { input: NodeId::new(1) },
                ],
                None,
            ),
        );
        assert_physical_plan(
            &catalog,
            "EXPLAIN DELETE FROM users;",
            physical_plan(
                vec![
                    PhysicalPlanNode::FullTableScan { relation, table: users_table() },
                    PhysicalPlanNode::Delete {
                        relation,
                        table: users_table(),
                        input: NodeId::new(0),
                    },
                    PhysicalPlanNode::Explain { input: NodeId::new(1) },
                ],
                None,
            ),
        );
    }

    #[test]
    fn missing_or_out_of_scope_names_are_rejected() {
        let catalog = MemoryCatalog::default();
        let planner = Planner::with_schema(&catalog);

        assert!(matches!(
            planner.plan_statement(&parse("SELECT * FROM missing;")),
            Err(PlannerError::TableNotFound { name }) if name == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT missing FROM users;")),
            Err(PlannerError::ColumnNotFound { column }) if column == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT other.name FROM users;")),
            Err(PlannerError::TableNotInScope { table }) if table == "other"
        ));
    }

    #[test]
    fn invalid_projection_expressions_are_rejected() {
        let catalog = MemoryCatalog::default();
        let planner = Planner::with_schema(&catalog);

        assert!(matches!(
            planner.plan_statement(&parse("SELECT *;")),
            Err(PlannerError::WildcardRequiresTable)
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT id FROM users WHERE * == id;")),
            Err(PlannerError::UnsupportedWildcardPosition)
        ));
        assert!(matches!(
            planner.plan_statement(&parse("SELECT COUNT(*) FROM users;")),
            Err(PlannerError::UnsupportedAggregate { function }) if function == "COUNT"
        ));
    }

    #[test]
    fn invalid_mutation_and_index_shapes_are_rejected() {
        let catalog = MemoryCatalog::default();
        let planner = Planner::with_schema(&catalog);

        assert!(matches!(
            planner.plan_statement(&parse("INSERT INTO users (id, id) VALUES (1, 2);")),
            Err(PlannerError::DuplicateInsertColumn { column }) if column == "id"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("INSERT INTO users (id, name) VALUES (1);")),
            Err(PlannerError::InsertColumnValueCount { columns: 2, values: 1 })
        ));
        assert!(matches!(
            planner.plan_statement(&parse("UPDATE users SET name = 'A', name = 'B';")),
            Err(PlannerError::DuplicateUpdateColumn { column }) if column == "name"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("UPDATE users SET id = 2;")),
            Err(PlannerError::PrimaryKeyUpdate { column }) if column == "id"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("CREATE INDEX bad ON users (missing);")),
            Err(PlannerError::ColumnNotFound { column }) if column == "missing"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("CREATE INDEX bad ON users (name, name);")),
            Err(PlannerError::DuplicateIndexColumn { column }) if column == "name"
        ));
        assert!(matches!(
            planner.plan_statement(&parse("EXPLAIN INSERT INTO users (id) VALUES (1);")),
            Err(PlannerError::UnsupportedStatement { .. })
        ));
    }
}
