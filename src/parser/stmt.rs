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
    pub order_by: Option<&'a str>,
    pub limit: Option<u32>,
}
