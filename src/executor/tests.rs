use std::{cell::RefCell, collections::BTreeMap};

use super::*;
use crate::{
    core::{
        DataType, IndexColumnSchema, IndexKeyRange, IndexSchema, InvalidArgumentError,
        StorageResult, TableKey, TableKeyRange, TableSchema, TupleSchema, Value,
        access::{CatalogRead, RelationalAccess},
        error::{ConstraintError, StorageError},
    },
    error::DatabaseError,
    planner::{ExecExpr, PhysicalPlan, PhysicalPlanNode, Planner},
    relational::{
        catalog_manager::validate_user_table_schema,
        cursor::encode_index_entry_key,
        index_manager::index_key_from_record,
        record_manager::{table_key_from_values, validate_table_row},
    },
    sql_parser::parser::{Parser, op::Op},
};

#[derive(Clone)]
struct MemoryTable {
    schema: TableSchema,
    rows: BTreeMap<TableKey, OwnedTableRecord>,
}

#[derive(Clone)]
struct MemoryIndex {
    schema: IndexSchema,
    entries: BTreeMap<Vec<u8>, TableKey>,
}

#[derive(Clone)]
struct MemoryState {
    next_id: CatalogId,
    tables: BTreeMap<CatalogId, MemoryTable>,
    indexes: BTreeMap<CatalogId, MemoryIndex>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self { next_id: 4, tables: BTreeMap::new(), indexes: BTreeMap::new() }
    }
}

#[derive(Default)]
struct MemoryStore {
    state: RefCell<MemoryState>,
}

fn missing_table(name: &str) -> StorageError {
    StorageError::InvalidArgument(InvalidArgumentError::TableNotFound { name: name.to_owned() })
}

fn scan(records: Vec<OwnedTableRecord>) -> std::vec::IntoIter<StorageResult<OwnedTableRecord>> {
    records.into_iter().map(Ok).collect::<Vec<_>>().into_iter()
}

fn entry_key(
    table: &TableSchema,
    index: &IndexSchema,
    record: &OwnedTableRecord,
) -> StorageResult<Vec<u8>> {
    let key = index_key_from_record(table, index, record)?;
    Ok(encode_index_entry_key(&key, record.table_key))
}

impl CatalogRead for MemoryStore {
    fn table_schema_by_name(&self, name: &str) -> StorageResult<TableSchema> {
        self.state
            .borrow()
            .tables
            .values()
            .find(|table| table.schema.name == name)
            .map(|table| table.schema.clone())
            .ok_or_else(|| missing_table(name))
    }

    fn index_schemas_for_table(&self, table: &TableSchema) -> StorageResult<Vec<IndexSchema>> {
        Ok(self
            .state
            .borrow()
            .indexes
            .values()
            .filter(|index| index.schema.table_id == table.table_id)
            .map(|index| index.schema.clone())
            .collect())
    }
}

impl RelationalAccess for MemoryStore {
    type TableScan = std::vec::IntoIter<StorageResult<OwnedTableRecord>>;
    type IndexScan = std::vec::IntoIter<StorageResult<OwnedTableRecord>>;

    fn create_table(&self, name: &str, row: TupleSchema) -> StorageResult<TableSchema> {
        validate_user_table_schema(name, &row)?;
        let mut state = self.state.borrow_mut();
        if state.tables.values().any(|table| table.schema.name == name) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateTableName {
                name: name.into(),
            }));
        }
        let id = state.next_id;
        state.next_id += 1;
        let schema = TableSchema {
            table_id: id,
            name: name.into(),
            root_page_id: u64::from(id.unsigned_abs()),
            row,
        };
        state.tables.insert(id, MemoryTable { schema: schema.clone(), rows: BTreeMap::new() });
        Ok(schema)
    }

    fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: &[&str],
    ) -> StorageResult<IndexSchema> {
        if columns.is_empty() {
            return Err(StorageError::InvalidArgument(InvalidArgumentError::EmptyIndexColumns));
        }
        let table = self.table_schema_by_name(table_name)?;
        let mut state = self.state.borrow_mut();
        if state.indexes.values().any(|index| index.schema.name == name) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateIndexName {
                name: name.into(),
            }));
        }
        let index_columns = columns
            .iter()
            .map(|name| {
                table
                    .row
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == *name)
                    .map(|(ordinal, column)| IndexColumnSchema {
                        source_column_ordinal: ordinal,
                        column: column.clone(),
                    })
                    .ok_or_else(|| {
                        StorageError::InvalidArgument(InvalidArgumentError::ColumnNotFound {
                            table: table.name.clone(),
                            column: (*name).into(),
                        })
                    })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let id = state.next_id;
        state.next_id += 1;
        let schema = IndexSchema {
            index_id: id,
            name: name.into(),
            table_id: table.table_id,
            root_page_id: u64::from(id.unsigned_abs()),
            unique: false,
            columns: index_columns,
        };
        let mut entries = BTreeMap::new();
        for record in state
            .tables
            .get(&table.table_id)
            .ok_or_else(|| missing_table(table_name))?
            .rows
            .values()
        {
            entries.insert(entry_key(&table, &schema, record)?, record.table_key);
        }
        state.indexes.insert(id, MemoryIndex { schema: schema.clone(), entries });
        Ok(schema)
    }

    fn scan_table(&self, table: &TableSchema) -> StorageResult<Self::TableScan> {
        self.scan_table_range(table, TableKeyRange::unbounded())
    }

    fn scan_table_range(
        &self,
        table: &TableSchema,
        range: TableKeyRange,
    ) -> StorageResult<Self::TableScan> {
        let state = self.state.borrow();
        let records = state
            .tables
            .get(&table.table_id)
            .ok_or_else(|| missing_table(&table.name))?
            .rows
            .iter()
            .filter(|(key, _)| range.contains(**key))
            .map(|(_, record)| record.clone())
            .collect();
        Ok(scan(records))
    }

    fn scan_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        range: IndexKeyRange,
    ) -> StorageResult<Self::IndexScan> {
        let state = self.state.borrow();
        let index = state.indexes.get(&index.index_id).ok_or_else(|| {
            StorageError::InvalidArgument(InvalidArgumentError::IndexNotFound {
                name: index.name.clone(),
            })
        })?;
        let table_data =
            state.tables.get(&table.table_id).ok_or_else(|| missing_table(&table.name))?;
        let records = index
            .entries
            .iter()
            .filter(|(key, _)| range.contains(key))
            .map(|(_, key)| {
                table_data
                    .rows
                    .get(key)
                    .cloned()
                    .ok_or(StorageError::InvalidArgument(InvalidArgumentError::KeyNotFound))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(scan(records))
    }

    fn insert_table_row(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;
        let table_key = table_key_from_values(table, &values)?;
        let record = OwnedTableRecord {
            table_key,
            record: Tuple::new(values).to_bytes()?.into_boxed_slice(),
        };
        let mut state = self.state.borrow_mut();
        let rows = &mut state
            .tables
            .get_mut(&table.table_id)
            .ok_or_else(|| missing_table(&table.name))?
            .rows;
        if rows.contains_key(&table_key) {
            return Err(StorageError::Constraint(ConstraintError::DuplicateKey));
        }
        rows.insert(table_key, record.clone());
        for index in
            state.indexes.values_mut().filter(|index| index.schema.table_id == table.table_id)
        {
            index.entries.insert(entry_key(table, &index.schema, &record)?, table_key);
        }
        Ok(record)
    }

    fn update_table_row(
        &self,
        table: &TableSchema,
        old: &OwnedTableRecord,
        values: Vec<Value>,
    ) -> StorageResult<OwnedTableRecord> {
        validate_table_row(table, &values)?;
        if table_key_from_values(table, &values)? != old.table_key {
            return Err(StorageError::InvalidArgument(InvalidArgumentError::PrimaryKeyUpdate {
                table: table.name.clone(),
                column: table.row.columns[0].name.clone(),
            }));
        }
        let updated = OwnedTableRecord {
            table_key: old.table_key,
            record: Tuple::new(values).to_bytes()?.into_boxed_slice(),
        };
        let mut state = self.state.borrow_mut();
        state
            .tables
            .get_mut(&table.table_id)
            .ok_or_else(|| missing_table(&table.name))?
            .rows
            .insert(old.table_key, updated.clone());
        for index in
            state.indexes.values_mut().filter(|index| index.schema.table_id == table.table_id)
        {
            index.entries.remove(&entry_key(table, &index.schema, old)?);
            index.entries.insert(entry_key(table, &index.schema, &updated)?, old.table_key);
        }
        Ok(updated)
    }

    fn delete_table_row(
        &self,
        table: &TableSchema,
        record: &OwnedTableRecord,
    ) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        state
            .tables
            .get_mut(&table.table_id)
            .ok_or_else(|| missing_table(&table.name))?
            .rows
            .remove(&record.table_key);
        for index in
            state.indexes.values_mut().filter(|index| index.schema.table_id == table.table_id)
        {
            index.entries.remove(&entry_key(table, &index.schema, record)?);
        }
        Ok(())
    }
}

fn execute<'sql>(
    database: &MemoryStore,
    sql: &'sql str,
) -> Result<ExecutionOutput, DatabaseError<'sql>> {
    let statement = Parser::new(sql).stmt()?;
    let plan = Planner::with_schema(database).plan_physical_statement(&statement)?;
    let before = database.state.borrow().clone();
    match Executor::in_transaction(database).execute(plan) {
        Ok(output) => Ok(output),
        Err(error) => {
            *database.state.borrow_mut() = before;
            Err(error.into())
        }
    }
}

fn row_values(row: &ExecutorRow) -> Vec<Value> {
    row.with_record(|bytes| Tuple::from_bytes(bytes).unwrap().into_values()).unwrap()
}

fn try_rows(output: ExecutionOutput) -> ExecutorResult<Vec<Vec<Value>>> {
    output.into_rows("TEST")?.map(|row| row.map(|row| row_values(&row))).collect()
}

fn query(database: &MemoryStore, sql: &str) -> Vec<Vec<Value>> {
    try_rows(execute(database, sql).unwrap()).unwrap()
}

#[test]
fn values_evaluates_each_literal_row() {
    let database = MemoryStore::default();
    let plan = PhysicalPlan::new(PhysicalPlanNode::Values {
        rows: vec![
            vec![ExecExpr::Literal(Value::Integer(10))],
            vec![ExecExpr::Literal(Value::Integer(20))],
        ],
    });

    let rows = Executor::in_transaction(&database)
        .execute(plan)
        .unwrap()
        .into_rows("TEST")
        .unwrap()
        .collect::<ExecutorResult<Vec<_>>>()
        .unwrap();

    assert_eq!(
        rows.iter().map(row_values).collect::<Vec<_>>(),
        vec![vec![Value::Integer(10)], vec![Value::Integer(20)],]
    );
}

#[test]
fn select_without_from_evaluates_arithmetic() {
    let database = MemoryStore::default();

    assert_eq!(
        query(&database, "SELECT 7 + 5, 7 - 5, 7 * 5, 10 / 5, -5, 1.5 + 0.5;"),
        vec![vec![
            Value::Integer(12),
            Value::Integer(2),
            Value::Integer(35),
            Value::Integer(2),
            Value::Integer(-5),
            Value::Float(2.0),
        ]]
    );
}

#[test]
fn select_without_from_evaluates_boolean_expressions() {
    let database = MemoryStore::default();

    assert_eq!(
        query(
            &database,
            "SELECT NOT FALSE, 1 < 2, 2 <= 2, 3 > 2, 3 >= 3, \
                 1 == 1, 1 != 2, TRUE AND TRUE, FALSE OR TRUE;",
        ),
        vec![vec![Value::Boolean(true); 9]]
    );
}

#[test]
fn boolean_expressions_short_circuit() {
    let database = MemoryStore::default();

    assert_eq!(
        query(&database, "SELECT FALSE AND 1 / 0 == 0, TRUE OR 1 / 0 == 0;"),
        vec![vec![Value::Boolean(false), Value::Boolean(true)]]
    );
}

#[test]
fn invalid_expressions_return_precise_errors() {
    let database = MemoryStore::default();

    assert!(matches!(
        try_rows(execute(&database, "SELECT 1 / 0;").unwrap()),
        Err(ExecutorError::DivisionByZero)
    ));
    assert!(matches!(
        try_rows(execute(&database, "SELECT 2147483647 + 1;").unwrap()),
        Err(ExecutorError::IntegerOverflow { op: Op::Add })
    ));
    assert!(matches!(
        try_rows(execute(&database, "SELECT 1 + 1.0;").unwrap()),
        Err(ExecutorError::UnsupportedBinary { .. })
    ));
    assert!(matches!(
        try_rows(execute(&database, "SELECT 1 == 1.0;").unwrap()),
        Err(ExecutorError::ComparisonTypeMismatch { .. })
    ));
    assert!(matches!(
        try_rows(execute(&database, "SELECT TRUE AND 1;").unwrap()),
        Err(ExecutorError::NonBooleanLogicalOperand { .. })
    ));
}

#[test]
fn select_scans_filters_and_projects_rows() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);").unwrap();
    execute(
        &database,
        "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 20), (2, 'Grace', 10), (3, 'Linus', 30);",
    )
    .unwrap();

    assert_eq!(
        query(&database, "SELECT name, score + 1 FROM users WHERE score > 10 AND name != 'Linus';"),
        vec![vec![Value::String("Ada".into()), Value::Integer(21)]]
    );
}

#[test]
fn select_supports_table_aliases_in_projection_and_filter() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);").unwrap();
    execute(
        &database,
        "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 20), (2, 'Grace', 10), (3, 'Linus', 30);",
    )
    .unwrap();

    assert_eq!(
        query(&database, "SELECT u.name, u.score + 1 FROM users AS u WHERE u.score >= 20;"),
        vec![
            vec![Value::String("Ada".into()), Value::Integer(21)],
            vec![Value::String("Linus".into()), Value::Integer(31)],
        ]
    );
}

#[test]
fn filter_rejects_non_boolean_predicates() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY);").unwrap();
    execute(&database, "INSERT INTO users (id) VALUES (1);").unwrap();

    assert!(matches!(
        try_rows(execute(&database, "SELECT id FROM users WHERE id;").unwrap()),
        Err(ExecutorError::NonBooleanPredicate { value: Value::Integer(1) })
    ));
}

#[test]
fn primary_and_secondary_index_scans_return_matching_rows() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);").unwrap();
    execute(
        &database,
        "INSERT INTO users (id, name, score) VALUES \
             (1, 'Ada', 10), (2, 'Grace', 20), (3, 'Linus', 20), (4, 'Margaret', 30);",
    )
    .unwrap();
    execute(&database, "CREATE INDEX users_score ON users (score);").unwrap();
    execute(&database, "INSERT INTO users (id, name, score) VALUES (5, 'Ken', 20);").unwrap();

    assert_eq!(
        query(&database, "SELECT id FROM users WHERE id >= 2 AND id < 4;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
    assert_eq!(
        query(&database, "SELECT id FROM users WHERE score == 20 AND id > 2;"),
        vec![vec![Value::Integer(3)], vec![Value::Integer(5)]]
    );
    assert_eq!(
        query(&database, "SELECT id FROM users WHERE score >= 20 AND score < 30;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)], vec![Value::Integer(5)],]
    );
}

#[test]
fn limit_and_offset_select_the_requested_window() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE numbers (id INT PRIMARY KEY);").unwrap();
    execute(&database, "INSERT INTO numbers (id) VALUES (1), (2), (3), (4);").unwrap();

    assert_eq!(
        query(&database, "SELECT id FROM numbers LIMIT 2 OFFSET 1;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
    assert!(query(&database, "SELECT id FROM numbers OFFSET 10;").is_empty());
    assert!(query(&database, "SELECT id FROM numbers LIMIT 0;").is_empty());
}

#[test]
fn insert_maps_columns_to_the_table_schema() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT NULLABLE);")
        .unwrap();

    assert!(matches!(
        execute(&database, "INSERT INTO users (name, id) VALUES ('Ada', 1);").unwrap(),
        ExecutionOutput::RowsAffected(1)
    ));
    assert_eq!(
        query(&database, "SELECT id, name, score FROM users;"),
        vec![vec![Value::Integer(1), Value::String("Ada".into()), Value::Null]]
    );
}

#[test]
fn insert_enforces_nullability_and_column_types() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();

    assert!(matches!(
        execute(&database, "INSERT INTO users (id) VALUES (1);"),
        Err(DatabaseError::Executor(ExecutorError::Storage(StorageError::Constraint(
            ConstraintError::NullValue { column }
        )))) if column == "name"
    ));
    assert!(matches!(
        execute(&database, "INSERT INTO users (id, name) VALUES ('two', 'Grace');"),
        Err(DatabaseError::Executor(ExecutorError::Storage(StorageError::Constraint(
            ConstraintError::ColumnTypeMismatch {
                column,
                expected: DataType::Integer,
                actual: "text",
            }
        )))) if column == "id"
    ));
    assert!(query(&database, "SELECT id FROM users;").is_empty());
}

#[test]
fn failed_multi_row_insert_is_atomic() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();

    assert!(
        execute(&database, "INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 99);").is_err()
    );
    assert!(query(&database, "SELECT id FROM users;").is_empty());
}

#[test]
fn update_evaluates_assignments_and_refreshes_indexes() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);").unwrap();
    execute(&database, "CREATE INDEX users_name ON users (name);").unwrap();
    execute(
        &database,
        "INSERT INTO users (id, name, score) VALUES (1, 'Ada', 10), (2, 'Grace', 20);",
    )
    .unwrap();

    assert!(matches!(
        execute(
            &database,
            "UPDATE users SET name = 'Linus', score = score + 1 WHERE name == 'Ada';",
        )
        .unwrap(),
        ExecutionOutput::RowsAffected(1)
    ));
    assert!(query(&database, "SELECT id FROM users WHERE name == 'Ada';").is_empty());
    assert_eq!(
        query(&database, "SELECT id, score FROM users WHERE name == 'Linus';"),
        vec![vec![Value::Integer(1), Value::Integer(11)]]
    );
}

#[test]
fn failed_multi_row_update_is_atomic() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, score INT);").unwrap();
    execute(&database, "INSERT INTO users (id, score) VALUES (1, 10), (2, 20);").unwrap();

    assert!(matches!(
        execute(&database, "UPDATE users SET score = 10 / (2 - id);"),
        Err(DatabaseError::Executor(ExecutorError::DivisionByZero))
    ));
    assert_eq!(
        query(&database, "SELECT score FROM users;"),
        vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]
    );
}

#[test]
fn delete_removes_matching_rows_and_index_entries() {
    let database = MemoryStore::default();
    execute(&database, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);").unwrap();
    execute(&database, "CREATE INDEX users_name ON users (name);").unwrap();
    execute(&database, "INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Ada'), (3, 'Grace');")
        .unwrap();

    assert!(matches!(
        execute(&database, "DELETE FROM users WHERE name == 'Ada';").unwrap(),
        ExecutionOutput::RowsAffected(2)
    ));
    assert!(query(&database, "SELECT id FROM users WHERE name == 'Ada';").is_empty());
    assert_eq!(
        query(&database, "SELECT id FROM users WHERE name == 'Grace';"),
        vec![vec![Value::Integer(3)]]
    );
}
