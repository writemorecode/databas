use std::fmt::Display;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum NumberKind {
    Integer(i32),
    Float(f32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TokenKind<'a> {
    String(&'a str),
    Identifier(&'a str),
    Keyword(Keyword),
    Number(NumberKind),
    LeftParen,
    RightParen,
    Plus,
    Minus,
    Equals,
    NotEquals,
    EqualsEquals,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Asterisk,
    Comma,
    Semicolon,
    Slash,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    True,
    False,
    And,
    Or,
    Not,
    Limit,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keyword::Select => write!(f, "SELECT"),
            Keyword::From => write!(f, "FROM"),
            Keyword::Where => write!(f, "WHERE"),
            Keyword::Order => write!(f, "ORDER"),
            Keyword::By => write!(f, "BY"),
            Keyword::Asc => write!(f, "ASC"),
            Keyword::Desc => write!(f, "DESC"),
            Keyword::And => write!(f, "AND"),
            Keyword::Or => write!(f, "OR"),
            Keyword::True => write!(f, "TRUE"),
            Keyword::False => write!(f, "FALSE"),
            Keyword::Not => write!(f, "NOT"),
            Keyword::Limit => write!(f, "LIMIT"),
        }
    }
}

impl Display for TokenKind<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::String(s) => write!(f, "STRING ('{s}')"),
            TokenKind::Number(NumberKind::Integer(n)) => write!(f, "INTEGER ({n})"),
            TokenKind::Number(NumberKind::Float(n)) => write!(f, "FLOAT ({n})"),
            TokenKind::Identifier(id) => write!(f, "IDENT ('{id}')"),
            TokenKind::Keyword(keyword) => keyword.fmt(f),
            TokenKind::LeftParen => write!(f, "LP"),
            TokenKind::RightParen => write!(f, "RP"),
            TokenKind::Plus => write!(f, "PLUS"),
            TokenKind::Minus => write!(f, "MINUS"),
            TokenKind::Equals => write!(f, "EQ"),
            TokenKind::NotEquals => write!(f, "NEQ"),
            TokenKind::EqualsEquals => write!(f, "EQEQ"),
            TokenKind::LessThan => write!(f, "LT"),
            TokenKind::GreaterThan => write!(f, "GT"),
            TokenKind::LessThanOrEqual => write!(f, "LTEQ"),
            TokenKind::GreaterThanOrEqual => write!(f, "GTEQ"),
            TokenKind::Asterisk => write!(f, "ASTERISK"),
            TokenKind::Comma => write!(f, "COMMA"),
            TokenKind::Semicolon => write!(f, "SEMICOLON"),
            TokenKind::Slash => write!(f, "SLASH"),
        }
    }
}

impl<'a> From<&'a str> for TokenKind<'a> {
    fn from(value: &'a str) -> Self {
        let kind: TokenKind = if value.eq_ignore_ascii_case("SELECT") {
            TokenKind::Keyword(Keyword::Select)
        } else if value.eq_ignore_ascii_case("FROM") {
            TokenKind::Keyword(Keyword::From)
        } else if value.eq_ignore_ascii_case("WHERE") {
            TokenKind::Keyword(Keyword::Where)
        } else if value.eq_ignore_ascii_case("AND") {
            TokenKind::Keyword(Keyword::And)
        } else if value.eq_ignore_ascii_case("OR") {
            TokenKind::Keyword(Keyword::Or)
        } else if value.eq_ignore_ascii_case("TRUE") {
            TokenKind::Keyword(Keyword::True)
        } else if value.eq_ignore_ascii_case("FALSE") {
            TokenKind::Keyword(Keyword::False)
        } else if value.eq_ignore_ascii_case("NOT") {
            TokenKind::Keyword(Keyword::Not)
        } else if value.eq_ignore_ascii_case("ORDER") {
            TokenKind::Keyword(Keyword::Order)
        } else if value.eq_ignore_ascii_case("BY") {
            TokenKind::Keyword(Keyword::By)
        } else if value.eq_ignore_ascii_case("ASC") {
            TokenKind::Keyword(Keyword::Asc)
        } else if value.eq_ignore_ascii_case("DESC") {
            TokenKind::Keyword(Keyword::Desc)
        } else if value.eq_ignore_ascii_case("LIMIT") {
            TokenKind::Keyword(Keyword::Limit)
        } else {
            TokenKind::Identifier(value)
        };
        kind
    }
}

impl Display for NumberKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberKind::Integer(value) => write!(f, "{}", value),
            NumberKind::Float(value) => write!(f, "{}", value),
        }
    }
}
