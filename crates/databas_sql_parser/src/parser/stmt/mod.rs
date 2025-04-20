use std::fmt::Display;

pub mod create_table;
pub mod insert;
pub mod select;

pub mod lists;

use create_table::CreateTableQuery;
use insert::InsertQuery;
use select::SelectQuery;

#[derive(Debug, PartialEq)]
pub enum Statement<'a> {
    Select(SelectQuery<'a>),
    Insert(InsertQuery<'a>),
    CreateTable(CreateTableQuery<'a>),
}

impl Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Select(query) => query.fmt(f),
            Statement::Insert(query) => query.fmt(f),
            Statement::CreateTable(query) => query.fmt(f),
        }
    }
}
