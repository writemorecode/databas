pub mod expr;
pub mod op;
pub mod stmt;

use expr::{AggregateFunction, AggregateFunctionKind, Expression, Literal};
use op::Op;
use stmt::Statement;
use stmt::lists::{ExpressionList, IdentifierList};

use crate::error::{SQLError, SQLErrorKind};
use crate::lexer::Lexer;
use crate::lexer::token::Token;
use crate::lexer::token_kind::{Aggregate, Keyword, NumberKind, TokenKind};

#[derive(Debug)]
pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Statement<'a>, SQLError<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt() {
            Err(SQLError { kind: SQLErrorKind::UnexpectedEnd, .. }) => None,
            other => Some(other),
        }
    }
}

impl<'a> Parser<'a> {
    pub fn new(source: &'a str) -> Self {
        Self { lexer: Lexer::new(source) }
    }

    fn parse_non_negative_integer(&mut self) -> Result<Option<u32>, SQLError<'a>> {
        let tok = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
        match tok.kind {
            TokenKind::Number(NumberKind::Integer(num)) => Ok(num.try_into().ok()),
            TokenKind::Minus => {
                if let Some(Ok(Token {
                    kind: TokenKind::Number(NumberKind::Integer(num)), ..
                })) = self.lexer.next()
                {
                    Err(SQLError::new(
                        SQLErrorKind::ExpectedNonNegativeInteger { got: -num },
                        tok.offset,
                    ))
                } else {
                    Err(SQLError::new(SQLErrorKind::Other(TokenKind::Minus), tok.offset))
                }
            }
            other => Err(SQLError::new(SQLErrorKind::ExpectedInteger { got: other }, tok.offset)),
        }
    }

    fn parse_comma_separated_list_in_parenthesis<T>(
        &mut self,
        mut parse_item: impl FnMut(&mut Self) -> Result<T, SQLError<'a>>,
    ) -> Result<Vec<T>, SQLError<'a>>
    where
        T: 'a,
    {
        self.parse_comma_separated_list(|p| {
            p.lexer.expect_token(TokenKind::LeftParen)?;
            let item = parse_item(p)?;
            p.lexer.expect_token(TokenKind::RightParen)?;
            Ok(item)
        })
    }

    fn parse_comma_separated_list<T>(
        &mut self,
        mut parse_item: impl FnMut(&mut Self) -> Result<T, SQLError<'a>>,
    ) -> Result<Vec<T>, SQLError<'a>>
    where
        T: 'a,
    {
        let mut list = vec![parse_item(self)?];
        while let Some(Ok(Token { kind: TokenKind::Comma, .. })) = self.lexer.peek() {
            self.lexer.next();
            list.push(parse_item(self)?);
        }
        Ok(list)
    }

    fn parse_expression_list(&mut self) -> Result<ExpressionList<'a>, SQLError<'a>> {
        Ok(ExpressionList(self.parse_comma_separated_list(|p| p.expr_bp(0))?))
    }

    fn parse_identifier_list(&mut self) -> Result<IdentifierList<'a>, SQLError<'a>> {
        Ok(IdentifierList(self.parse_comma_separated_list(|p| p.parse_identifier())?))
    }

    fn parse_identifier(&mut self) -> Result<&'a str, SQLError<'a>> {
        self.lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })
            .and_then(|tok| {
                tok.map(|tok| match tok.kind {
                    TokenKind::Identifier(id) => Ok(id),
                    other => Err(SQLError::new(
                        SQLErrorKind::ExpectedIdentifier { got: other },
                        self.lexer.position,
                    )),
                })
            })?
    }

    pub fn stmt(&mut self) -> Result<Statement<'a>, SQLError<'a>> {
        let token = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
        match token.kind {
            TokenKind::Keyword(Keyword::Select) => {
                Ok(Statement::Select(self.parse_select_query()?))
            }
            TokenKind::Keyword(Keyword::Insert) => {
                Ok(Statement::Insert(self.parse_insert_query()?))
            }
            TokenKind::Keyword(Keyword::Create) => {
                Ok(Statement::CreateTable(self.parse_create_table_query()?))
            }
            other => Err(SQLError::new(SQLErrorKind::Other(other), token.offset)),
        }
    }

    pub fn parse_unary_op(&mut self, tok: Token<'a>) -> Result<Expression<'a>, SQLError<'a>> {
        let op: Op = tok.try_into()?;
        let ((), r_bp) = op.prefix_binding_power().ok_or(SQLError::new(
            SQLErrorKind::InvalidPrefixOperator { op: tok.kind },
            tok.offset,
        ))?;
        let rhs = self.expr_bp(r_bp)?;
        Ok(Expression::UnaryOp((op, Box::new(rhs))))
    }

    pub fn expr(mut self) -> Result<Expression<'a>, SQLError<'a>> {
        self.expr_bp(0)
    }

    fn expr_bp(&mut self, min_bp: u8) -> Result<Expression<'a>, SQLError<'a>> {
        let token = self
            .lexer
            .next()
            .ok_or(SQLError { kind: SQLErrorKind::UnexpectedEnd, pos: self.lexer.position })??;
        let mut lhs = match token.kind {
            TokenKind::String(lit) => Expression::Literal(Literal::String(lit)),
            TokenKind::Number(num) => Expression::Literal(Literal::Number(num)),
            TokenKind::Keyword(Keyword::True) => Expression::Literal(Literal::Boolean(true)),
            TokenKind::Keyword(Keyword::False) => Expression::Literal(Literal::Boolean(false)),
            TokenKind::Identifier(id) => Expression::Identifier(id),
            TokenKind::Asterisk => Expression::Wildcard,
            TokenKind::LeftParen => {
                let lhs = self
                    .expr_bp(0)
                    .map_err(|_| SQLError::new(SQLErrorKind::UnclosedParenthesis, token.offset))?;
                self.lexer.expect_token(TokenKind::RightParen)?;
                lhs
            }
            TokenKind::Minus | TokenKind::Keyword(Keyword::Not) => self.parse_unary_op(token)?,
            TokenKind::Keyword(Keyword::Aggregate(agg)) => self.parse_aggregate_function(agg)?,
            other => {
                return Err(SQLError::new(SQLErrorKind::Other(other), token.offset));
            }
        };

        while let Some(Ok(token)) = self.lexer.peek() {
            if {
                matches!(
                    token.kind,
                    TokenKind::Comma
                        | TokenKind::RightParen
                        | TokenKind::Semicolon
                        | TokenKind::Keyword(
                            Keyword::From
                                | Keyword::Where
                                | Keyword::Order
                                | Keyword::Desc
                                | Keyword::Asc
                                | Keyword::Limit
                                | Keyword::Offset,
                        ),
                )
            } {
                break;
            }
            let op = Op::try_from(*token)?;
            let (l_bp, r_bp) = op.infix_binding_power().ok_or(SQLError::new(
                SQLErrorKind::InvalidOperator { op: token.kind },
                token.offset,
            ))?;
            if l_bp < min_bp {
                break;
            }
            self.lexer.next();
            let rhs = self.expr_bp(r_bp)?;
            lhs = Expression::BinaryOp((Box::new(lhs), op, Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_aggregate_function(&mut self, agg: Aggregate) -> Result<Expression<'a>, SQLError<'a>> {
        self.lexer.expect_token(TokenKind::LeftParen)?;
        let expr = self.expr_bp(0)?;
        self.lexer.expect_token(TokenKind::RightParen)?;
        let kind = match agg {
            Aggregate::Count => AggregateFunctionKind::Count,
            Aggregate::Sum => AggregateFunctionKind::Sum,
            Aggregate::Avg => AggregateFunctionKind::Avg,
            Aggregate::StdDev => AggregateFunctionKind::StdDev,
            Aggregate::Min => AggregateFunctionKind::Min,
            Aggregate::Max => AggregateFunctionKind::Max,
        };
        Ok(Expression::AggregateFunction(AggregateFunction {
            kind,
            expr: Box::new(expr),
        }))
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;
    use crate::{
        error::{SQLError, SQLErrorKind},
        lexer::token_kind::TokenKind,
    };

    #[test]
    fn test_parse_plus_exp() {
        let s = "12 + 34";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            Expression::BinaryOp((a, Op::Add, b))
        };
        assert_eq!(Ok(expected), parser.expr())
    }

    #[test]
    fn test_parse_mul_and_plus_exp() {
        let s = "12 + 34 * 56";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            let c = Box::new(Expression::from(56));
            Expression::BinaryOp((a, Op::Add, Box::new(Expression::BinaryOp((b, Op::Mul, c)))))
        };

        assert_eq!(Ok(expected), parser.expr())
    }

    #[test]
    fn test_parse_mul_and_plus_exp_with_parens() {
        let s = "12 + (34 * 56)";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            let c = Box::new(Expression::from(56));
            Expression::BinaryOp((a, Op::Add, Box::new(Expression::BinaryOp((b, Op::Mul, c)))))
        };
        assert_eq!(Ok(expected), parser.expr())
    }

    #[test]
    fn test_parse_not_exp() {
        let s = "not true";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(true));
            Expression::UnaryOp((Op::Not, a))
        };
        assert_eq!(Ok(expected), parser.expr());

        let s = "not false";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(false));
            Expression::UnaryOp((Op::Not, a))
        };
        assert_eq!(Ok(expected), parser.expr());

        let s = "not (a AND (b != c))";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::Identifier("a"));
            let b = Box::new(Expression::Identifier("b"));
            let c = Box::new(Expression::Identifier("c"));
            let d = Box::new(Expression::BinaryOp((b, Op::NotEquals, c)));
            let e = Box::new(Expression::BinaryOp((a, Op::And, d)));
            Expression::UnaryOp((Op::Not, e))
        };
        assert_eq!(Ok(expected), parser.expr());
    }

    #[test]
    fn test_negative_exp() {
        let s = "-12";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            Expression::UnaryOp((Op::Sub, a))
        };
        assert_eq!(Ok(expected), parser.expr());
    }

    #[test]
    fn test_invalid_operator() {
        let s = "operand invalid_operator";
        let parser = Parser::new(s);
        let expected_err = SQLError::new(
            SQLErrorKind::InvalidOperator { op: TokenKind::Identifier("invalid_operator") },
            8,
        );
        assert_eq!(Err(expected_err), parser.expr());
    }

    #[test]
    fn test_parse_inequality_operators() {
        let s = "12 < 34";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            Expression::BinaryOp((a, Op::LessThan, b))
        };
        assert_eq!(Ok(expected), parser.expr());

        let s = "12 <= 34";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            Expression::BinaryOp((a, Op::LessThanOrEqual, b))
        };
        assert_eq!(Ok(expected), parser.expr());

        let s = "12 > 34";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            Expression::BinaryOp((a, Op::GreaterThan, b))
        };
        assert_eq!(Ok(expected), parser.expr());

        let s = "12 >= 34";
        let parser = Parser::new(s);
        let expected = {
            let a = Box::new(Expression::from(12));
            let b = Box::new(Expression::from(34));
            Expression::BinaryOp((a, Op::GreaterThanOrEqual, b))
        };
        assert_eq!(Ok(expected), parser.expr());
    }

    #[test]
    fn test_parse_non_negative_integer() {
        let mut parser = Parser::new("123");
        assert_eq!(parser.parse_non_negative_integer(), Ok(Some(123)));

        let mut parser = Parser::new("-123");
        assert_eq!(
            parser.parse_non_negative_integer(),
            Err(SQLError { kind: SQLErrorKind::ExpectedNonNegativeInteger { got: -123 }, pos: 0 })
        );

        let mut parser = Parser::new("abc");
        assert_eq!(
            parser.parse_non_negative_integer(),
            Err(SQLError {
                kind: SQLErrorKind::ExpectedInteger { got: TokenKind::Identifier("abc") },
                pos: 0
            })
        );
    }
}
