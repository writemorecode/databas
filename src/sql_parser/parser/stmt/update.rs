use std::fmt::Display;

use crate::sql_parser::{
    error::SQLError,
    lexer::{
        token::Token,
        token_kind::{Keyword, TokenKind},
    },
    parser::{Parser, expr::Expression},
};

#[derive(Debug, PartialEq)]
pub struct Assignment<'a> {
    pub column: &'a str,
    pub expression: Expression<'a>,
}

impl Display for Assignment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.column, self.expression)
    }
}

#[derive(Debug, PartialEq)]
pub struct AssignmentList<'a>(pub Vec<Assignment<'a>>);

impl Display for AssignmentList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let joined = self.0.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
        write!(f, "{joined}")
    }
}

#[derive(Debug, PartialEq)]
pub struct UpdateQuery<'a> {
    pub table: &'a str,
    pub assignments: AssignmentList<'a>,
    pub where_clause: Option<Expression<'a>>,
}

impl Display for UpdateQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET {}", self.table, self.assignments)?;

        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }

        write!(f, ";")
    }
}

impl<'a> Parser<'a> {
    pub fn parse_update_query(&mut self) -> Result<UpdateQuery<'a>, SQLError<'a>> {
        let table = self.parse_identifier()?;
        self.lexer.expect_token(TokenKind::Keyword(Keyword::Set))?;
        let assignments = AssignmentList(self.parse_comma_separated_list(|p| {
            let column = p.parse_identifier()?;
            p.lexer.expect_token(TokenKind::Equals)?;
            let expression = p.expr_bp(0)?;
            Ok(Assignment { column, expression })
        })?);

        let where_clause =
            if let Some(Ok(Token { kind: TokenKind::Keyword(Keyword::Where), .. })) =
                self.lexer.peek()
            {
                self.lexer.next();
                Some(self.expr_bp(0)?)
            } else {
                None
            };

        self.lexer.expect_token(TokenKind::Semicolon)?;
        Ok(UpdateQuery { table, assignments, where_clause })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::parser::{Parser, SqlItem, expr::Expression, op::Op, stmt::Statement};

    #[test]
    fn test_parse_update_query() {
        let s = "UPDATE users SET name = 'Ada';";
        let mut parser = Parser::new(s);
        let expected = UpdateQuery {
            table: "users",
            assignments: AssignmentList(vec![Assignment {
                column: "name",
                expression: Expression::Literal(crate::sql_parser::parser::expr::Literal::String(
                    "Ada",
                )),
            }]),
            where_clause: None,
        };

        assert_eq!(Some(Ok(SqlItem::Statement(Statement::Update(expected)))), parser.next());
    }

    #[test]
    fn test_parse_update_query_with_where_clause() {
        let s = "UPDATE users SET name = 'Ada', active = TRUE WHERE id == 1;";
        let mut parser = Parser::new(s);
        let expected = UpdateQuery {
            table: "users",
            assignments: AssignmentList(vec![
                Assignment {
                    column: "name",
                    expression: Expression::Literal(
                        crate::sql_parser::parser::expr::Literal::String("Ada"),
                    ),
                },
                Assignment { column: "active", expression: Expression::from(true) },
            ]),
            where_clause: Some(Expression::BinaryOp((
                Box::new(Expression::Identifier("id")),
                Op::EqualsEquals,
                Box::new(Expression::from(1)),
            ))),
        };

        assert_eq!(Some(Ok(SqlItem::Statement(Statement::Update(expected)))), parser.next());
    }
}
