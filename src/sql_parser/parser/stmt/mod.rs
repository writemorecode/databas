use std::fmt::Display;

pub mod create_index;
pub mod create_table;
pub mod delete;
pub mod insert;
pub mod select;

pub mod lists;

use create_index::CreateIndexQuery;
use create_table::CreateTableQuery;
use delete::DeleteQuery;
use insert::InsertQuery;
use select::SelectQuery;

#[derive(Debug, PartialEq)]
pub enum Statement<'a> {
    Explain(Box<Statement<'a>>),
    Select(SelectQuery<'a>),
    Delete(DeleteQuery<'a>),
    Insert(InsertQuery<'a>),
    CreateTable(CreateTableQuery<'a>),
    CreateIndex(CreateIndexQuery<'a>),
}

impl Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain(statement) => write!(f, "EXPLAIN {statement}"),
            Statement::Select(query) => query.fmt(f),
            Statement::Delete(query) => query.fmt(f),
            Statement::Insert(query) => query.fmt(f),
            Statement::CreateTable(query) => query.fmt(f),
            Statement::CreateIndex(query) => query.fmt(f),
        }
    }
}
