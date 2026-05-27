//! Internal schema catalog definitions and bootstrap helpers.

use std::fmt;

use crate::{
    core::{PageId, RowId, Tuple, TupleRef, Value, ValueRef},
    sql_parser::parser::stmt::{
        create_index::CreateIndexQuery,
        create_table::{ColumnConstraint, ColumnType, CreateTableQuery},
    },
};

/// Root page id of the `sys_tables` catalog table.
pub const SYS_TABLES_ROOT_PAGE_ID: PageId = 1;
/// Root page id of the `sys_indexes` catalog table.
pub const SYS_INDEXES_ROOT_PAGE_ID: PageId = 2;
/// Root page id of the `sys_columns` catalog table.
pub const SYS_COLUMNS_ROOT_PAGE_ID: PageId = 3;

/// Stable object id assigned to the `sys_tables` catalog table.
pub const SYS_TABLES_TABLE_ID: RowId = 1;
/// Stable object id assigned to the `sys_indexes` catalog table.
pub const SYS_INDEXES_TABLE_ID: RowId = 2;
/// Stable object id assigned to the `sys_columns` catalog table.
pub const SYS_COLUMNS_TABLE_ID: RowId = 3;

const SYS_TABLES_NAME: &str = "sys_tables";
const SYS_INDEXES_NAME: &str = "sys_indexes";
const SYS_COLUMNS_NAME: &str = "sys_columns";

/// Kind of schema object described by a row in `sys_columns`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogObjectKind {
    /// A table row schema.
    Table,
    /// A secondary-index key schema.
    Index,
}

impl CatalogObjectKind {
    /// Returns the integer tag stored in `sys_columns.object_kind`.
    pub fn catalog_tag(self) -> i32 {
        match self {
            Self::Table => 1,
            Self::Index => 2,
        }
    }

    /// Decodes an integer tag stored in `sys_columns.object_kind`.
    pub fn from_catalog_tag(tag: i32) -> Result<Self, CatalogError> {
        match tag {
            1 => Ok(Self::Table),
            2 => Ok(Self::Index),
            _ => Err(CatalogError::InvalidObjectKind { actual: tag }),
        }
    }
}

/// Logical column type recorded in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// Signed 32-bit integer value.
    Integer,
    /// Floating-point value.
    Float,
    /// UTF-8 text value.
    Text,
    /// Boolean value.
    Boolean,
    /// Unsigned 64-bit integer value used for internal identifiers.
    UnsignedInteger,
}

impl DataType {
    /// Returns the integer tag stored in `sys_columns.data_type`.
    pub fn catalog_tag(self) -> i32 {
        match self {
            Self::Integer => 1,
            Self::Float => 2,
            Self::Text => 3,
            Self::Boolean => 4,
            Self::UnsignedInteger => 5,
        }
    }

    /// Decodes an integer tag stored in `sys_columns.data_type`.
    pub fn from_catalog_tag(tag: i32) -> Result<Self, CatalogError> {
        match tag {
            1 => Ok(Self::Integer),
            2 => Ok(Self::Float),
            3 => Ok(Self::Text),
            4 => Ok(Self::Boolean),
            5 => Ok(Self::UnsignedInteger),
            _ => Err(CatalogError::InvalidDataType { actual: tag }),
        }
    }

    /// Maps a parsed SQL column type onto the storage catalog type.
    pub fn from_sql(column_type: &ColumnType) -> Self {
        match column_type {
            ColumnType::Int => Self::Integer,
            ColumnType::Float => Self::Float,
            ColumnType::Text => Self::Text,
        }
    }
}

/// Schema metadata for one table or index column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    /// Column name as exposed to SQL.
    pub name: String,
    /// Logical type stored for values in this column.
    pub data_type: DataType,
    /// Whether this column accepts `NULL` values.
    pub nullable: bool,
    /// Whether this column participates in the object's primary key.
    pub primary_key: bool,
}

/// Ordered schema for one encoded tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSchema {
    /// Columns in tuple field order.
    pub columns: Vec<ColumnSchema>,
}

/// Catalog schema for a table B+-tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// Stable row id for this table object in `sys_tables`.
    pub table_id: RowId,
    /// Table name.
    pub name: String,
    /// Root page id of the table's B+-tree.
    pub root_page_id: PageId,
    /// Largest row id allocated for this table.
    pub last_row_id: RowId,
    /// Schema of table rows stored in the tree values.
    pub row: TupleSchema,
}

/// One column of a secondary-index key and its source table column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnSchema {
    /// Ordinal of the source column in the indexed table schema.
    pub source_column_ordinal: u64,
    /// Column metadata copied into the index key schema.
    pub column: ColumnSchema,
}

/// Catalog schema for a secondary-index B+-tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSchema {
    /// Stable row id for this index object in `sys_indexes`.
    pub index_id: RowId,
    /// Index name.
    pub name: String,
    /// Table object id that this index belongs to.
    pub table_id: RowId,
    /// Root page id of the index B+-tree.
    pub root_page_id: PageId,
    /// Whether duplicate key tuples are rejected.
    pub unique: bool,
    /// Ordered key columns stored in the index.
    pub columns: Vec<IndexColumnSchema>,
}

/// Decoded row from `sys_tables`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCatalogRow {
    /// Stable table object id.
    pub table_id: RowId,
    /// Table name.
    pub name: String,
    /// Root page id of the table B+-tree.
    pub root_page_id: PageId,
    /// Largest row id allocated for this table.
    pub last_row_id: RowId,
}

/// Decoded row from `sys_indexes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCatalogRow {
    /// Stable index object id.
    pub index_id: RowId,
    /// Index name.
    pub name: String,
    /// Table object id that this index belongs to.
    pub table_id: RowId,
    /// Root page id of the index B+-tree.
    pub root_page_id: PageId,
    /// Whether duplicate key tuples are rejected.
    pub unique: bool,
}

/// Decoded row from `sys_columns`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCatalogRow {
    /// Stable catalog row id for this column record.
    pub column_id: RowId,
    /// Kind of object whose tuple schema owns this column.
    pub object_kind: CatalogObjectKind,
    /// Object id from `sys_tables` or `sys_indexes`.
    pub object_id: RowId,
    /// Zero-based position of the column in its tuple schema.
    pub ordinal: u64,
    /// Column name.
    pub name: String,
    /// Logical data type.
    pub data_type: DataType,
    /// Whether this column accepts `NULL` values.
    pub nullable: bool,
    /// Whether this column participates in the object's primary key.
    pub primary_key: bool,
    /// Source table id for index columns, or `None` for table columns.
    pub source_table_id: Option<RowId>,
    /// Source table column ordinal for index columns, or `None` for table columns.
    pub source_column_ordinal: Option<u64>,
}

/// Borrowed column schema used to define built-in system tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemColumnSchema<'a> {
    /// Column name.
    pub name: &'a str,
    /// Logical data type.
    pub data_type: DataType,
    /// Whether this column accepts `NULL` values.
    pub nullable: bool,
    /// Whether this column participates in the object's primary key.
    pub primary_key: bool,
}

/// Borrowed tuple schema used to define built-in system tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemTupleSchema<'a> {
    /// Columns in tuple field order.
    pub columns: &'a [SystemColumnSchema<'a>],
}

/// Borrowed schema for one built-in system table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemTableSchema<'a> {
    /// Stable table object id.
    pub table_id: RowId,
    /// System table name.
    pub name: &'a str,
    /// Root page id reserved for this system table.
    pub root_page_id: PageId,
    /// Largest row id allocated while bootstrapping this system table.
    pub last_row_id: RowId,
    /// Row schema of the system table.
    pub row: SystemTupleSchema<'a>,
}

/// Borrowed row written to `sys_tables` while bootstrapping the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemTableCatalogRow<'a> {
    /// Stable table object id.
    pub table_id: RowId,
    /// System table name.
    pub name: &'a str,
    /// Root page id reserved for this system table.
    pub root_page_id: PageId,
    /// Largest row id allocated while bootstrapping this system table.
    pub last_row_id: RowId,
}

/// Borrowed row written to `sys_columns` while bootstrapping the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemColumnCatalogRow<'a> {
    /// Stable catalog row id for this column record.
    pub column_id: RowId,
    /// Kind of object whose tuple schema owns this column.
    pub object_kind: CatalogObjectKind,
    /// Object id from `sys_tables` or `sys_indexes`.
    pub object_id: RowId,
    /// Zero-based position of the column in its tuple schema.
    pub ordinal: u64,
    /// Column name.
    pub name: &'a str,
    /// Logical data type.
    pub data_type: DataType,
    /// Whether this column accepts `NULL` values.
    pub nullable: bool,
    /// Whether this column participates in the object's primary key.
    pub primary_key: bool,
    /// Source table id for index columns, or `None` for table columns.
    pub source_table_id: Option<RowId>,
    /// Source table column ordinal for index columns, or `None` for table columns.
    pub source_column_ordinal: Option<u64>,
}

/// Errors raised while decoding or constructing catalog metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    /// A catalog tuple had the wrong number of fields.
    InvalidFieldCount { expected: usize, actual: usize },
    /// A catalog tuple field had an unexpected value type.
    InvalidFieldType { index: usize, expected: &'static str },
    /// A `sys_columns.data_type` tag is not recognized.
    InvalidDataType { actual: i32 },
    /// A `sys_columns.object_kind` tag is not recognized.
    InvalidObjectKind { actual: i32 },
    /// An index definition referenced a column absent from the target table.
    UnknownIndexColumn { table: String, column: String },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFieldCount { expected, actual } => {
                write!(f, "invalid catalog field count: expected {expected}, got {actual}")
            }
            Self::InvalidFieldType { index, expected } => {
                write!(f, "invalid catalog field {index}: expected {expected}")
            }
            Self::InvalidDataType { actual } => write!(f, "invalid catalog data type {actual}"),
            Self::InvalidObjectKind { actual } => {
                write!(f, "invalid catalog object kind {actual}")
            }
            Self::UnknownIndexColumn { table, column } => {
                write!(f, "index column {column} does not exist on table {table}")
            }
        }
    }
}

impl std::error::Error for CatalogError {}

impl TableSchema {
    /// Builds a table schema from a parsed `CREATE TABLE` statement.
    pub fn from_create_table(
        table_id: RowId,
        root_page_id: PageId,
        query: &CreateTableQuery<'_>,
    ) -> Self {
        Self {
            table_id,
            name: query.table_name.to_owned(),
            root_page_id,
            last_row_id: 0,
            row: TupleSchema::from_create_table_query(query),
        }
    }

    /// Returns the `sys_tables` row describing this table.
    pub fn catalog_row(&self) -> TableCatalogRow {
        TableCatalogRow {
            table_id: self.table_id,
            name: self.name.clone(),
            root_page_id: self.root_page_id,
            last_row_id: self.last_row_id,
        }
    }
}

impl TupleSchema {
    /// Builds a tuple schema from a parsed `CREATE TABLE` statement.
    pub fn from_create_table_query(query: &CreateTableQuery<'_>) -> Self {
        let columns = query
            .columns
            .iter()
            .map(|column| ColumnSchema {
                name: column.name.to_owned(),
                data_type: DataType::from_sql(&column.column_type),
                nullable: column.constraints.contains(&ColumnConstraint::Nullable),
                primary_key: column.constraints.contains(&ColumnConstraint::PrimaryKey),
            })
            .collect();

        Self { columns }
    }
}

impl IndexSchema {
    /// Builds an index schema from a parsed `CREATE INDEX` statement and source table schema.
    pub fn from_create_index(
        index_id: RowId,
        root_page_id: PageId,
        table: &TableSchema,
        query: &CreateIndexQuery<'_>,
    ) -> Result<Self, CatalogError> {
        let mut columns = Vec::new();
        for column_name in &query.columns.0 {
            let (source_column_ordinal, column) = table
                .row
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == *column_name)
                .ok_or_else(|| CatalogError::UnknownIndexColumn {
                    table: table.name.clone(),
                    column: (*column_name).to_owned(),
                })?;
            columns.push(IndexColumnSchema {
                source_column_ordinal: source_column_ordinal as u64,
                column: column.clone(),
            });
        }

        Ok(Self {
            index_id,
            name: query.index_name.to_owned(),
            table_id: table.table_id,
            root_page_id,
            unique: false,
            columns,
        })
    }

    /// Returns the tuple schema used to encode keys in this index.
    pub fn key_schema(&self) -> TupleSchema {
        TupleSchema { columns: self.columns.iter().map(|column| column.column.clone()).collect() }
    }

    /// Returns the `sys_indexes` row describing this index.
    pub fn catalog_row(&self) -> IndexCatalogRow {
        IndexCatalogRow {
            index_id: self.index_id,
            name: self.name.clone(),
            table_id: self.table_id,
            root_page_id: self.root_page_id,
            unique: self.unique,
        }
    }
}

impl TableCatalogRow {
    /// Encodes this catalog row as a storage tuple.
    pub fn encode(&self) -> Tuple {
        Tuple::new(vec![
            Value::UnsignedInteger(self.table_id),
            Value::String(self.name.clone()),
            Value::UnsignedInteger(self.root_page_id),
            Value::UnsignedInteger(self.last_row_id),
        ])
    }

    /// Decodes a `sys_tables` storage tuple.
    pub fn decode(tuple: &Tuple) -> Result<Self, CatalogError> {
        expect_field_count(tuple, 4)?;
        Ok(Self {
            table_id: expect_unsigned(tuple, 0)?,
            name: expect_string(tuple, 1)?.to_owned(),
            root_page_id: expect_unsigned(tuple, 2)?,
            last_row_id: expect_unsigned(tuple, 3)?,
        })
    }
}

impl IndexCatalogRow {
    /// Encodes this catalog row as a storage tuple.
    pub fn encode(&self) -> Tuple {
        Tuple::new(vec![
            Value::UnsignedInteger(self.index_id),
            Value::String(self.name.clone()),
            Value::UnsignedInteger(self.table_id),
            Value::UnsignedInteger(self.root_page_id),
            Value::Boolean(self.unique),
        ])
    }

    /// Decodes a `sys_indexes` storage tuple.
    pub fn decode(tuple: &Tuple) -> Result<Self, CatalogError> {
        expect_field_count(tuple, 5)?;
        Ok(Self {
            index_id: expect_unsigned(tuple, 0)?,
            name: expect_string(tuple, 1)?.to_owned(),
            table_id: expect_unsigned(tuple, 2)?,
            root_page_id: expect_unsigned(tuple, 3)?,
            unique: expect_bool(tuple, 4)?,
        })
    }
}

impl ColumnCatalogRow {
    /// Encodes this catalog row as a storage tuple.
    pub fn encode(&self) -> Tuple {
        Tuple::new(vec![
            Value::UnsignedInteger(self.column_id),
            Value::Integer(self.object_kind.catalog_tag()),
            Value::UnsignedInteger(self.object_id),
            Value::UnsignedInteger(self.ordinal),
            Value::String(self.name.clone()),
            Value::Integer(self.data_type.catalog_tag()),
            Value::Boolean(self.nullable),
            Value::Boolean(self.primary_key),
            optional_unsigned(self.source_table_id),
            optional_unsigned(self.source_column_ordinal),
        ])
    }

    /// Decodes a `sys_columns` storage tuple.
    pub fn decode(tuple: &Tuple) -> Result<Self, CatalogError> {
        expect_field_count(tuple, 10)?;
        Ok(Self {
            column_id: expect_unsigned(tuple, 0)?,
            object_kind: CatalogObjectKind::from_catalog_tag(expect_integer(tuple, 1)?)?,
            object_id: expect_unsigned(tuple, 2)?,
            ordinal: expect_unsigned(tuple, 3)?,
            name: expect_string(tuple, 4)?.to_owned(),
            data_type: DataType::from_catalog_tag(expect_integer(tuple, 5)?)?,
            nullable: expect_bool(tuple, 6)?,
            primary_key: expect_bool(tuple, 7)?,
            source_table_id: expect_optional_unsigned(tuple, 8)?,
            source_column_ordinal: expect_optional_unsigned(tuple, 9)?,
        })
    }
}

impl SystemTableSchema<'_> {
    pub(crate) fn catalog_row(&self) -> SystemTableCatalogRow<'_> {
        SystemTableCatalogRow {
            table_id: self.table_id,
            name: self.name,
            root_page_id: self.root_page_id,
            last_row_id: self.last_row_id,
        }
    }
}

impl SystemTableCatalogRow<'_> {
    pub(crate) fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        let values = [
            ValueRef::UnsignedInteger(self.table_id),
            ValueRef::String(self.name),
            ValueRef::UnsignedInteger(self.root_page_id),
            ValueRef::UnsignedInteger(self.last_row_id),
        ];
        TupleRef::new(&values).to_bytes()
    }
}

impl SystemColumnCatalogRow<'_> {
    pub(crate) fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        let values = [
            ValueRef::UnsignedInteger(self.column_id),
            ValueRef::Integer(self.object_kind.catalog_tag()),
            ValueRef::UnsignedInteger(self.object_id),
            ValueRef::UnsignedInteger(self.ordinal),
            ValueRef::String(self.name),
            ValueRef::Integer(self.data_type.catalog_tag()),
            ValueRef::Boolean(self.nullable),
            ValueRef::Boolean(self.primary_key),
            optional_unsigned_ref(self.source_table_id),
            optional_unsigned_ref(self.source_column_ordinal),
        ];
        TupleRef::new(&values).to_bytes()
    }
}

static SYS_TABLES_COLUMNS: &[SystemColumnSchema<'static>] = &[
    column("table_id", DataType::UnsignedInteger, false, true),
    column("name", DataType::Text, false, false),
    column("root_page_id", DataType::UnsignedInteger, false, false),
    column("last_row_id", DataType::UnsignedInteger, true, false),
];

static SYS_INDEXES_COLUMNS: &[SystemColumnSchema<'static>] = &[
    column("index_id", DataType::UnsignedInteger, false, true),
    column("name", DataType::Text, false, false),
    column("table_id", DataType::UnsignedInteger, false, false),
    column("root_page_id", DataType::UnsignedInteger, false, false),
    column("unique", DataType::Boolean, false, false),
];

static SYS_COLUMNS_COLUMNS: &[SystemColumnSchema<'static>] = &[
    column("column_id", DataType::UnsignedInteger, false, true),
    column("object_kind", DataType::Integer, false, false),
    column("object_id", DataType::UnsignedInteger, false, false),
    column("ordinal", DataType::UnsignedInteger, false, false),
    column("name", DataType::Text, false, false),
    column("data_type", DataType::Integer, false, false),
    column("nullable", DataType::Boolean, false, false),
    column("primary_key", DataType::Boolean, false, false),
    column("source_table_id", DataType::UnsignedInteger, true, false),
    column("source_column_ordinal", DataType::UnsignedInteger, true, false),
];

const SYSTEM_TABLE_ROW_COUNT: RowId = 3;
const SYSTEM_COLUMN_ROW_COUNT: RowId =
    (SYS_TABLES_COLUMNS.len() + SYS_INDEXES_COLUMNS.len() + SYS_COLUMNS_COLUMNS.len()) as RowId;

/// Fixed schemas of all built-in system catalog tables.
pub static SYSTEM_TABLE_SCHEMAS: &[SystemTableSchema<'static>] = &[
    SystemTableSchema {
        table_id: SYS_TABLES_TABLE_ID,
        name: SYS_TABLES_NAME,
        root_page_id: SYS_TABLES_ROOT_PAGE_ID,
        last_row_id: SYSTEM_TABLE_ROW_COUNT,
        row: SystemTupleSchema { columns: SYS_TABLES_COLUMNS },
    },
    SystemTableSchema {
        table_id: SYS_INDEXES_TABLE_ID,
        name: SYS_INDEXES_NAME,
        root_page_id: SYS_INDEXES_ROOT_PAGE_ID,
        last_row_id: 0,
        row: SystemTupleSchema { columns: SYS_INDEXES_COLUMNS },
    },
    SystemTableSchema {
        table_id: SYS_COLUMNS_TABLE_ID,
        name: SYS_COLUMNS_NAME,
        root_page_id: SYS_COLUMNS_ROOT_PAGE_ID,
        last_row_id: SYSTEM_COLUMN_ROW_COUNT,
        row: SystemTupleSchema { columns: SYS_COLUMNS_COLUMNS },
    },
];

/// Returns the fixed schemas of all built-in system catalog tables.
pub fn system_table_schemas() -> &'static [SystemTableSchema<'static>] {
    SYSTEM_TABLE_SCHEMAS
}

/// Returns the fixed `sys_columns` rows that describe the system catalog tables.
pub fn system_column_rows() -> Vec<SystemColumnCatalogRow<'static>> {
    let mut column_id = 1;
    let mut rows = Vec::new();
    for schema in system_table_schemas() {
        for (ordinal, column) in schema.row.columns.iter().enumerate() {
            rows.push(SystemColumnCatalogRow {
                column_id,
                object_kind: CatalogObjectKind::Table,
                object_id: schema.table_id,
                ordinal: ordinal as u64,
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
                primary_key: column.primary_key,
                source_table_id: None,
                source_column_ordinal: None,
            });
            column_id += 1;
        }
    }
    rows
}

const fn column(
    name: &'static str,
    data_type: DataType,
    nullable: bool,
    primary_key: bool,
) -> SystemColumnSchema<'static> {
    SystemColumnSchema { name, data_type, nullable, primary_key }
}

fn optional_unsigned(value: Option<u64>) -> Value {
    value.map(Value::UnsignedInteger).unwrap_or(Value::Null)
}

fn optional_unsigned_ref(value: Option<u64>) -> ValueRef<'static> {
    value.map(ValueRef::UnsignedInteger).unwrap_or(ValueRef::Null)
}

fn expect_field_count(tuple: &Tuple, expected: usize) -> Result<(), CatalogError> {
    let actual = tuple.len();
    if actual == expected {
        Ok(())
    } else {
        Err(CatalogError::InvalidFieldCount { expected, actual })
    }
}

fn expect_value(tuple: &Tuple, index: usize) -> Result<&Value, CatalogError> {
    tuple
        .values()
        .get(index)
        .ok_or(CatalogError::InvalidFieldCount { expected: index + 1, actual: tuple.len() })
}

fn expect_string(tuple: &Tuple, index: usize) -> Result<&str, CatalogError> {
    match expect_value(tuple, index)? {
        Value::String(value) => Ok(value),
        _ => Err(CatalogError::InvalidFieldType { index, expected: "text" }),
    }
}

fn expect_bool(tuple: &Tuple, index: usize) -> Result<bool, CatalogError> {
    match expect_value(tuple, index)? {
        Value::Boolean(value) => Ok(*value),
        _ => Err(CatalogError::InvalidFieldType { index, expected: "boolean" }),
    }
}

fn expect_integer(tuple: &Tuple, index: usize) -> Result<i32, CatalogError> {
    match expect_value(tuple, index)? {
        Value::Integer(value) => Ok(*value),
        _ => Err(CatalogError::InvalidFieldType { index, expected: "integer" }),
    }
}

fn expect_unsigned(tuple: &Tuple, index: usize) -> Result<u64, CatalogError> {
    match expect_value(tuple, index)? {
        Value::UnsignedInteger(value) => Ok(*value),
        _ => Err(CatalogError::InvalidFieldType { index, expected: "unsigned integer" }),
    }
}

fn expect_optional_unsigned(tuple: &Tuple, index: usize) -> Result<Option<u64>, CatalogError> {
    match expect_value(tuple, index)? {
        Value::Null => Ok(None),
        Value::UnsignedInteger(value) => Ok(Some(*value)),
        _ => Err(CatalogError::InvalidFieldType { index, expected: "nullable unsigned integer" }),
    }
}
