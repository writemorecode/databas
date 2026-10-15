use std::io;

use thiserror::Error;

use crate::executor::ExecutorError;
use crate::planner::PlannerError;
use crate::relational::error::StorageError;
use crate::session::SessionError;
use crate::sql_parser::error::SQLError;

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
    Session(#[from] SessionError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<'a> From<SQLError<'a>> for DatabaseError<'a> {
    fn from(error: SQLError<'a>) -> Self {
        Self::Parser(error)
    }
}
