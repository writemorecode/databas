use std::fmt::Display;

use crate::sql_parser::parser::expr::Expression;

#[derive(Debug, PartialEq, Default)]
pub struct ExpressionList<'a>(pub Vec<Expression<'a>>);
impl Display for ExpressionList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, expression) in self.0.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expression}")?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct IdentifierList<'a>(pub Vec<&'a str>);
impl Display for IdentifierList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let joined = self.0.join(", ");
        write!(f, "{joined}")
    }
}
