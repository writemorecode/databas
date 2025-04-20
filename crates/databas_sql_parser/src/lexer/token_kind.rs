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
    Offset,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Int,
    Float,
    Text,
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
            Keyword::Offset => write!(f, "OFFSET"),
            Keyword::Insert => write!(f, "INSERT"),
            Keyword::Into => write!(f, "INTO"),
            Keyword::Values => write!(f, "VALUES"),
            Keyword::Create => write!(f, "CREATE"),
            Keyword::Table => write!(f, "TABLE"),
            Keyword::Int => write!(f, "INT"),
            Keyword::Float => write!(f, "FLOAT"),
            Keyword::Text => write!(f, "TEXT"),
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
        let uppercase = value.to_uppercase();
        match uppercase.as_ref() {
            "SELECT" => TokenKind::Keyword(Keyword::Select),
            "FROM" => TokenKind::Keyword(Keyword::From),
            "WHERE" => TokenKind::Keyword(Keyword::Where),
            "ORDER" => TokenKind::Keyword(Keyword::Order),
            "BY" => TokenKind::Keyword(Keyword::By),
            "ASC" => TokenKind::Keyword(Keyword::Asc),
            "DESC" => TokenKind::Keyword(Keyword::Desc),
            "AND" => TokenKind::Keyword(Keyword::And),
            "OR" => TokenKind::Keyword(Keyword::Or),
            "TRUE" => TokenKind::Keyword(Keyword::True),
            "FALSE" => TokenKind::Keyword(Keyword::False),
            "NOT" => TokenKind::Keyword(Keyword::Not),
            "LIMIT" => TokenKind::Keyword(Keyword::Limit),
            "OFFSET" => TokenKind::Keyword(Keyword::Offset),
            "INSERT" => TokenKind::Keyword(Keyword::Insert),
            "INTO" => TokenKind::Keyword(Keyword::Into),
            "VALUES" => TokenKind::Keyword(Keyword::Values),
            "CREATE" => TokenKind::Keyword(Keyword::Create),
            "TABLE" => TokenKind::Keyword(Keyword::Table),
            "INT" => TokenKind::Keyword(Keyword::Int),
            "FLOAT" => TokenKind::Keyword(Keyword::Float),
            "TEXT" => TokenKind::Keyword(Keyword::Text),
            _ => TokenKind::Identifier(value),
        }
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
