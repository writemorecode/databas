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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
            Self::Select => write!(f, "SELECT"),
            Self::From => write!(f, "FROM"),
            Self::Where => write!(f, "WHERE"),
            Self::Order => write!(f, "ORDER"),
            Self::By => write!(f, "BY"),
            Self::Asc => write!(f, "ASC"),
            Self::Desc => write!(f, "DESC"),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::True => write!(f, "TRUE"),
            Self::False => write!(f, "FALSE"),
            Self::Not => write!(f, "NOT"),
            Self::Limit => write!(f, "LIMIT"),
            Self::Offset => write!(f, "OFFSET"),
            Self::Insert => write!(f, "INSERT"),
            Self::Into => write!(f, "INTO"),
            Self::Values => write!(f, "VALUES"),
            Self::Create => write!(f, "CREATE"),
            Self::Table => write!(f, "TABLE"),
            Self::Int => write!(f, "INT"),
            Self::Float => write!(f, "FLOAT"),
            Self::Text => write!(f, "TEXT"),
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
            Self::Integer(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
        }
    }
}
