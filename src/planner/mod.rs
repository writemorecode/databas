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
//! name lookup.
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

    fn physical(catalog: &MemoryCatalog, sql: &str) -> String {
        plan(catalog, sql).physical.to_string()
    }

    #[test]
    fn create_table_preserves_the_declared_schema() {
        let plan = plan(
            &MemoryCatalog::default(),
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT NULLABLE);",
        );

        assert_eq!(
            plan.logical.root(),
            &LogicalPlanNode::CreateTable {
                name: "users".into(),
                schema: users_table().row.clone()
            }
        );
        assert_eq!(
            plan.physical.root(),
            &PhysicalPlanNode::CreateTable { name: "users".into(), schema: users_table().row }
        );
    }

    #[test]
    fn create_index_binds_source_columns() {
        let plan =
            plan(&MemoryCatalog::default(), "CREATE INDEX users_name_age ON users (name, age);");
        let PhysicalPlanNode::CreateIndex { name, table, columns } = plan.physical.root() else {
            panic!("expected CreateIndex, got {:?}", plan.physical.root());
        };

        assert_eq!(name, "users_name_age");
        assert_eq!(table.name, "users");
        assert_eq!(
            columns.iter().map(|column| (&*column.name, column.ordinal)).collect::<Vec<_>>(),
            vec![("name", 1), ("age", 2)]
        );
    }

    #[test]
    fn insert_binds_columns_and_literal_rows() {
        let plan = plan(
            &MemoryCatalog::default(),
            "INSERT INTO users (name, id) VALUES ('Ada', 1), ('Grace', 2);",
        );
        let PhysicalPlanNode::InsertValues { table, columns, values } = plan.physical.root() else {
            panic!("expected InsertValues, got {:?}", plan.physical.root());
        };

        assert_eq!(table.name, "users");
        assert_eq!(columns.iter().map(|column| &*column.name).collect::<Vec<_>>(), ["name", "id"]);
        assert_eq!(
            values,
            &[
                vec![
                    ExecExpr::Literal(Value::String("Ada".into())),
                    ExecExpr::Literal(Value::Integer(1)),
                ],
                vec![
                    ExecExpr::Literal(Value::String("Grace".into())),
                    ExecExpr::Literal(Value::Integer(2)),
                ],
            ]
        );
    }

    #[test]
    fn select_star_expands_bound_table_columns() {
        assert_eq!(
            physical(&MemoryCatalog::default(), "SELECT * FROM users;"),
            "Project expressions=[users.id, users.name, users.age]\n`- FullTableScan table=users"
        );
    }

    #[test]
    fn select_binds_qualified_columns_and_expressions() {
        assert_eq!(
            physical(
                &MemoryCatalog::default(),
                "SELECT users.name, age + 1 FROM users WHERE users.id == 7;",
            ),
            "Project expressions=[users.name, (users.age + 1)]\n\
             `- PrimaryKeyRangeScan table=users range=[lower=7 inclusive upper=7 inclusive]"
        );
    }

    #[test]
    fn select_table_aliases_bind_qualified_columns_in_every_clause() {
        let relation = RelationId::new(0);
        let mut expected = PhysicalPlan::new(PhysicalPlanNode::PrimaryKeyRangeScan {
            relation,
            table: users_table(),
            range: TableKeyRange {
                lower: Some(TableKeyBound::Inclusive(7)),
                upper: Some(TableKeyBound::Inclusive(7)),
            },
        });
        let scan = expected.root_id();
        let sort = expected.push(PhysicalPlanNode::Sort {
            input: scan,
            terms: vec![SortTerm {
                column: ExecColumn { slot: 2, name: "u.age".into(), data_type: DataType::Integer },
                direction: Some(Ordering::Descending),
            }],
        });
        expected.push(PhysicalPlanNode::Project {
            input: sort,
            expressions: vec![ExecExpr::Column(ExecColumn {
                slot: 1,
                name: "u.name".into(),
                data_type: DataType::Text,
            })],
        });

        assert_eq!(
            plan(
                &MemoryCatalog::default(),
                "SELECT u.name FROM users AS u WHERE u.id == 7 ORDER BY u.age DESC;",
            )
            .physical,
            expected
        );

        let mut expected =
            PhysicalPlan::new(PhysicalPlanNode::FullTableScan { relation, table: users_table() });
        let scan = expected.root_id();
        expected.push(PhysicalPlanNode::Project {
            input: scan,
            expressions: vec![
                ExecExpr::Column(ExecColumn {
                    slot: 0,
                    name: "u.id".into(),
                    data_type: DataType::Integer,
                }),
                ExecExpr::Column(ExecColumn {
                    slot: 1,
                    name: "u.name".into(),
                    data_type: DataType::Text,
                }),
                ExecExpr::Column(ExecColumn {
                    slot: 2,
                    name: "u.age".into(),
                    data_type: DataType::Integer,
                }),
            ],
        });

        assert_eq!(plan(&MemoryCatalog::default(), "SELECT * FROM users AS u;").physical, expected);
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
        let mut expected = PhysicalPlan::new(PhysicalPlanNode::SecondaryIndexScan {
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
        });
        let scan = expected.root_id();
        let filter = expected.push(PhysicalPlanNode::Filter {
            input: scan,
            predicate: ExecExpr::Binary {
                left: Box::new(ExecExpr::Column(ExecColumn {
                    slot: 1,
                    name: "u.name".into(),
                    data_type: DataType::Text,
                })),
                op: crate::sql_parser::parser::op::Op::EqualsEquals,
                right: Box::new(ExecExpr::Literal(indexed_value)),
            },
        });
        expected.push(PhysicalPlanNode::Project {
            input: filter,
            expressions: vec![ExecExpr::Column(ExecColumn {
                slot: 0,
                name: "u.id".into(),
                data_type: DataType::Integer,
            })],
        });

        assert_eq!(
            plan(&catalog, "SELECT u.id FROM users AS u WHERE u.name == 'Ada';").physical,
            expected
        );
    }

    #[test]
    fn select_without_from_uses_one_synthetic_row() {
        assert_eq!(
            physical(&MemoryCatalog::default(), "SELECT 1 + 2;"),
            "Project expressions=[(1 + 2)]\n`- OneRow"
        );
    }

    #[test]
    fn update_and_delete_choose_primary_key_scans() {
        let catalog = MemoryCatalog::default();

        assert_eq!(
            physical(&catalog, "UPDATE users SET age = age + 1 WHERE id == 7;"),
            "Update table=users assignments=[users.age = (users.age + 1)]\n\
             `- PrimaryKeyRangeScan table=users range=[lower=7 inclusive upper=7 inclusive]"
        );
        assert_eq!(
            physical(&catalog, "DELETE FROM users WHERE 2 <= id AND id < 5;"),
            "Delete table=users\n\
             `- PrimaryKeyRangeScan table=users range=[lower=2 inclusive upper=5 exclusive]"
        );
    }

    #[test]
    fn mutations_never_scan_a_secondary_index() {
        let catalog = MemoryCatalog::with_indexes(&[("users_age", 2)]);

        for sql in
            ["UPDATE users SET name = 'Ada' WHERE age == 7;", "DELETE FROM users WHERE age == 7;"]
        {
            let plan = physical(&catalog, sql);
            assert!(plan.contains("FullTableScan table=users"), "{plan}");
            assert!(!plan.contains("SecondaryIndexScan"), "{plan}");
        }
    }

    #[test]
    fn primary_key_predicates_produce_tight_ranges() {
        let catalog = MemoryCatalog::default();

        for (predicate, range) in [
            ("id == 10", "lower=10 inclusive upper=10 inclusive"),
            ("10 <= id AND id < 20", "lower=10 inclusive upper=20 exclusive"),
            ("id > 3 AND id <= 8", "lower=3 exclusive upper=8 inclusive"),
        ] {
            let plan = physical(&catalog, &format!("SELECT name FROM users WHERE {predicate};"));
            assert!(
                plan.contains(&format!("PrimaryKeyRangeScan table=users range=[{range}]")),
                "{plan}"
            );
            assert!(!plan.contains("Filter"), "{plan}");
        }
    }

    #[test]
    fn unused_predicates_remain_as_residual_filters() {
        assert_eq!(
            physical(
                &MemoryCatalog::default(),
                "SELECT name FROM users WHERE id < 10 AND age == 7;",
            ),
            concat!(
                "Project expressions=[users.name]\n",
                "`- Filter predicate=(users.age == 7)\n",
                "   `- PrimaryKeyRangeScan table=users range=[upper=10 exclusive]",
            )
        );
    }

    #[test]
    fn compatible_secondary_index_predicates_use_index_scans() {
        let catalog = MemoryCatalog::with_indexes(&[("users_name", 1), ("users_age", 2)]);

        let equality = physical(&catalog, "SELECT id FROM users WHERE name == 'Ada';");
        assert!(
            equality.contains("SecondaryIndexScan table=users index=users_name column=users.name")
        );
        assert!(equality.contains("range=[lower=Ada inclusive upper=Ada inclusive]"));

        let range = physical(&catalog, "SELECT id FROM users WHERE 18 <= age AND age < 65;");
        assert!(range.contains("SecondaryIndexScan table=users index=users_age column=users.age"));
        assert!(range.contains("range=[lower=18 inclusive upper=65 exclusive]"));
    }

    #[test]
    fn planner_chooses_the_highest_priority_usable_index() {
        let catalog = MemoryCatalog::with_indexes(&[
            ("users_name_first", 1),
            ("users_name_second", 1),
            ("users_age", 2),
        ]);

        let primary = physical(&catalog, "SELECT name FROM users WHERE id == 1 AND name == 'Ada';");
        assert!(primary.contains("PrimaryKeyRangeScan"), "{primary}");

        let leftmost =
            physical(&catalog, "SELECT name FROM users WHERE age == 7 AND name == 'Ada';");
        assert!(leftmost.contains("index=users_age"), "{leftmost}");

        let creation_order = physical(&catalog, "SELECT name FROM users WHERE name == 'Ada';");
        assert!(creation_order.contains("index=users_name_first"), "{creation_order}");
    }

    #[test]
    fn unsupported_access_predicates_use_a_full_scan() {
        let catalog = MemoryCatalog::with_indexes(&[("users_name", 1)]);

        for predicate in ["age == 7", "name >= 'Amy'", "age == 7 AND id < 10"] {
            let plan = physical(&catalog, &format!("SELECT name FROM users WHERE {predicate};"));
            assert!(plan.contains("FullTableScan table=users"), "{plan}");
        }
    }

    #[test]
    fn select_operators_are_planned_in_sql_evaluation_order() {
        assert_eq!(
            physical(
                &MemoryCatalog::default(),
                "SELECT name FROM users ORDER BY id DESC LIMIT 10 OFFSET 5;",
            ),
            concat!(
                "Limit limit=10\n",
                "`- Offset offset=5\n",
                "   `- Project expressions=[users.name]\n",
                "      `- Sort terms=[users.id DESC]\n",
                "         `- FullTableScan table=users",
            )
        );
    }

    #[test]
    fn explain_wraps_supported_statement_plans() {
        let catalog = MemoryCatalog::default();

        for (sql, operator) in [
            ("EXPLAIN SELECT name FROM users;", "Project"),
            ("EXPLAIN UPDATE users SET name = 'Ada';", "Update"),
            ("EXPLAIN DELETE FROM users;", "Delete"),
        ] {
            let plan = physical(&catalog, sql);
            assert!(plan.starts_with(&format!("Explain\n`- {operator}")), "{plan}");
            assert!(plan.contains("FullTableScan table=users"), "{plan}");
        }
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
