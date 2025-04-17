use std::fmt::Display;

use crate::parser::expr::Expression;

#[derive(Debug, PartialEq, Default)]
pub struct ExpressionList<'a>(pub Vec<Expression<'a>>);
impl Display for ExpressionList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let strings: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        let joined = strings.join(", ");
        write!(f, "{joined}")
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
