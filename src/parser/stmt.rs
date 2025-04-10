use std::fmt::Display;

use super::expr::Expression;

#[derive(Debug, PartialEq)]
pub enum Statement<'a> {
    Select(SelectQuery<'a>),
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub columns: Vec<Expression<'a>>,
    pub table: Option<&'a str>,
    pub where_clause: Option<Expression<'a>>,
    pub order_by: Option<OrderBy<'a>>,
    pub limit: Option<u32>,
}

#[derive(Debug, PartialEq)]
pub enum Ordering {
    Ascending,
    Descending,
}

impl Display for Ordering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ordering::Ascending => write!(f, "ASC"),
            Ordering::Descending => write!(f, "DESC"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct OrderBy<'a> {
    pub terms: Vec<Expression<'a>>,
    pub order: Option<Ordering>,
}

impl Display for OrderBy<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, col) in self.terms.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }

        if let Some(ref order) = self.order {
            write!(f, " {}", order)?;
        }
        Ok(())
    }
}

impl Display for SelectQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT ")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }

        if let Some(table) = self.table {
            write!(f, " FROM {}", table)?;
        }
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }

        if let Some(ref order_by_clause) = self.order_by {
            write!(f, " ORDER BY {}", order_by_clause)?;
        }

        writeln!(f, ";")
    }
}

impl Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Select(query) => query.fmt(f),
        }
    }
}
