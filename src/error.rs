use std::io;

use thiserror::Error;

use crate::{
    core::error::StorageError, executor::ExecutorError, planner::PlannerError,
    sql_parser::error::SQLError,
};

#[derive(Debug, Error)]
pub enum DatabaseError<'a> {
    #[error("{0}")]
    Parser(SQLError<'a>),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Planner(#[from] PlannerError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<'a> From<SQLError<'a>> for DatabaseError<'a> {
    fn from(error: SQLError<'a>) -> Self {
        Self::Parser(error)
    }
}
