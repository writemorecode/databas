use std::fmt::Display;

pub mod insert;
pub mod select;

use select::SelectQuery;

use insert::InsertQuery;

#[derive(Debug, PartialEq)]
pub enum Statement<'a> {
    Select(SelectQuery<'a>),
    Insert(InsertQuery<'a>),
}

impl Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Select(query) => query.fmt(f),
            Statement::Insert(query) => query.fmt(f),
        }
    }
}
