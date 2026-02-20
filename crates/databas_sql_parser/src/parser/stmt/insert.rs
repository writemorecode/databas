use std::fmt::Display;

use crate::{
    error::SQLError,
    lexer::token_kind::{Keyword, TokenKind},
    parser::{
        Parser,
        stmt::lists::{ExpressionList, IdentifierList},
    },
};

#[derive(Debug, PartialEq)]
pub struct Values<'a>(pub Vec<ExpressionList<'a>>);

impl Display for Values<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.0.iter().map(|v| format!("({v})")).collect::<Vec<_>>().join(", ");
        write!(f, "{s}")
    }
}

impl<'a> Parser<'a> {
    fn parse_values(&mut self) -> Result<Values<'a>, SQLError<'a>> {
        Ok(Values(self.parse_comma_separated_list_in_parenthesis(|p| p.parse_expression_list())?))
    }
}

#[derive(Debug, PartialEq)]
pub struct InsertQuery<'a> {
    pub table: &'a str,
    pub columns: IdentifierList<'a>,
    pub values: Values<'a>,
}

impl Display for InsertQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSERT INTO {} ({}) VALUES {};", self.table, self.columns, self.values)
    }
}

impl<'a> Parser<'a> {
    pub fn parse_insert_query(&mut self) -> Result<InsertQuery<'a>, SQLError<'a>> {
        self.lexer.expect_token(TokenKind::Keyword(Keyword::Into))?;
        let table = self.parse_identifier()?;

        self.lexer.expect_token(TokenKind::LeftParen)?;
        let columns = self.parse_identifier_list()?;
        self.lexer.expect_token(TokenKind::RightParen)?;

        self.lexer.expect_token(TokenKind::Keyword(Keyword::Values))?;

        let values = self.parse_values()?;
        self.lexer.expect_token(TokenKind::Semicolon)?;
        Ok(InsertQuery { table, columns, values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{
        Parser,
        expr::{Expression, Literal},
        stmt::{
            Statement,
            lists::{ExpressionList, IdentifierList},
        },
    };

    #[test]
    fn test_parse_insert_query() {
        let s = "INSERT INTO products (id, name, price) VALUES (123, 'Cake', 45.67), (789, 'Waffles', 10.00);";
        let mut parser = Parser::new(s);
        let got = parser.next();
        let expected = InsertQuery {
            table: "products",
            columns: IdentifierList(vec!["id", "name", "price"]),
            values: Values(vec![
                ExpressionList(vec![
                    Expression::from(123),
                    Expression::Literal(Literal::String("Cake")),
                    Expression::from(45.67f32),
                ]),
                ExpressionList(vec![
                    Expression::from(789),
                    Expression::Literal(Literal::String("Waffles")),
                    Expression::from(10.00f32),
                ]),
            ]),
        };
        assert_eq!(Some(Ok(Statement::Insert(expected))), got);
    }
}
