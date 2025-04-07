use databas::parser::{Expression, Op, Parser};

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
    let s = "!true";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(true));
        Expression::UnaryOp((Op::Not, a))
    };
    assert_eq!(Ok(expected), parser.expr());

    let s = "!false";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(false));
        Expression::UnaryOp((Op::Not, a))
    };
    assert_eq!(Ok(expected), parser.expr());
}

#[test]
fn test_negative_exp() {
    let s = "-12";
    let parser = Parser::new(s);
    let expected = {
        let a = Box::new(Expression::from(12));
        Expression::UnaryOp((Op::Neg, a))
    };
    assert_eq!(Ok(expected), parser.expr());
}
