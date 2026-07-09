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
    Explain,
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
    Update,
    Set,
    Delete,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Index,
    On,
    Int,
    Float,
    Text,
    Aggregate(Aggregate),
    Primary,
    Key,
    Nullable,
    Begin,
    Commit,
    Rollback,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Aggregate {
    Sum,
    Avg,
    StdDev,
    Min,
    Max,
    Count,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keyword::Explain => write!(f, "EXPLAIN"),
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
            Keyword::Update => write!(f, "UPDATE"),
            Keyword::Set => write!(f, "SET"),
            Keyword::Delete => write!(f, "DELETE"),
            Keyword::Insert => write!(f, "INSERT"),
            Keyword::Into => write!(f, "INTO"),
            Keyword::Values => write!(f, "VALUES"),
            Keyword::Create => write!(f, "CREATE"),
            Keyword::Table => write!(f, "TABLE"),
            Keyword::Index => write!(f, "INDEX"),
            Keyword::On => write!(f, "ON"),
            Keyword::Int => write!(f, "INT"),
            Keyword::Float => write!(f, "FLOAT"),
            Keyword::Text => write!(f, "TEXT"),
            Keyword::Aggregate(aggregate) => match aggregate {
                Aggregate::Sum => write!(f, "SUM"),
                Aggregate::Avg => write!(f, "AVG"),
                Aggregate::StdDev => write!(f, "STDDEV"),
                Aggregate::Min => write!(f, "MIN"),
                Aggregate::Max => write!(f, "MAX"),
                Aggregate::Count => write!(f, "COUNT"),
            },
            Keyword::Primary => write!(f, "PRIMARY"),
            Keyword::Key => write!(f, "KEY"),
            Keyword::Nullable => write!(f, "NULLABLE"),
            Keyword::Begin => write!(f, "BEGIN"),
            Keyword::Commit => write!(f, "COMMIT"),
            Keyword::Rollback => write!(f, "ROLLBACK"),
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
        keyword_from_str(value).map_or(TokenKind::Identifier(value), TokenKind::Keyword)
    }
}

fn keyword_from_str(value: &str) -> Option<Keyword> {
    match value.len() {
        2 if value.eq_ignore_ascii_case("BY") => Some(Keyword::By),
        2 if value.eq_ignore_ascii_case("ON") => Some(Keyword::On),
        2 if value.eq_ignore_ascii_case("OR") => Some(Keyword::Or),
        3 if value.eq_ignore_ascii_case("AND") => Some(Keyword::And),
        3 if value.eq_ignore_ascii_case("ASC") => Some(Keyword::Asc),
        3 if value.eq_ignore_ascii_case("AVG") => Some(Keyword::Aggregate(Aggregate::Avg)),
        3 if value.eq_ignore_ascii_case("INT") => Some(Keyword::Int),
        3 if value.eq_ignore_ascii_case("KEY") => Some(Keyword::Key),
        3 if value.eq_ignore_ascii_case("MAX") => Some(Keyword::Aggregate(Aggregate::Max)),
        3 if value.eq_ignore_ascii_case("MIN") => Some(Keyword::Aggregate(Aggregate::Min)),
        3 if value.eq_ignore_ascii_case("NOT") => Some(Keyword::Not),
        3 if value.eq_ignore_ascii_case("SET") => Some(Keyword::Set),
        3 if value.eq_ignore_ascii_case("SUM") => Some(Keyword::Aggregate(Aggregate::Sum)),
        4 if value.eq_ignore_ascii_case("DESC") => Some(Keyword::Desc),
        4 if value.eq_ignore_ascii_case("FROM") => Some(Keyword::From),
        4 if value.eq_ignore_ascii_case("INTO") => Some(Keyword::Into),
        4 if value.eq_ignore_ascii_case("TEXT") => Some(Keyword::Text),
        4 if value.eq_ignore_ascii_case("TRUE") => Some(Keyword::True),
        5 if value.eq_ignore_ascii_case("BEGIN") => Some(Keyword::Begin),
        5 if value.eq_ignore_ascii_case("COUNT") => Some(Keyword::Aggregate(Aggregate::Count)),
        5 if value.eq_ignore_ascii_case("FALSE") => Some(Keyword::False),
        5 if value.eq_ignore_ascii_case("FLOAT") => Some(Keyword::Float),
        5 if value.eq_ignore_ascii_case("INDEX") => Some(Keyword::Index),
        5 if value.eq_ignore_ascii_case("LIMIT") => Some(Keyword::Limit),
        5 if value.eq_ignore_ascii_case("ORDER") => Some(Keyword::Order),
        5 if value.eq_ignore_ascii_case("TABLE") => Some(Keyword::Table),
        5 if value.eq_ignore_ascii_case("WHERE") => Some(Keyword::Where),
        6 if value.eq_ignore_ascii_case("COMMIT") => Some(Keyword::Commit),
        6 if value.eq_ignore_ascii_case("CREATE") => Some(Keyword::Create),
        6 if value.eq_ignore_ascii_case("DELETE") => Some(Keyword::Delete),
        6 if value.eq_ignore_ascii_case("INSERT") => Some(Keyword::Insert),
        6 if value.eq_ignore_ascii_case("OFFSET") => Some(Keyword::Offset),
        6 if value.eq_ignore_ascii_case("SELECT") => Some(Keyword::Select),
        6 if value.eq_ignore_ascii_case("STDDEV") => Some(Keyword::Aggregate(Aggregate::StdDev)),
        6 if value.eq_ignore_ascii_case("UPDATE") => Some(Keyword::Update),
        6 if value.eq_ignore_ascii_case("VALUES") => Some(Keyword::Values),
        7 if value.eq_ignore_ascii_case("EXPLAIN") => Some(Keyword::Explain),
        7 if value.eq_ignore_ascii_case("PRIMARY") => Some(Keyword::Primary),
        8 if value.eq_ignore_ascii_case("NULLABLE") => Some(Keyword::Nullable),
        8 if value.eq_ignore_ascii_case("ROLLBACK") => Some(Keyword::Rollback),
        _ => None,
    }
}

impl Display for NumberKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberKind::Integer(value) => write!(f, "{}", value),
            NumberKind::Float(value) if value.fract() == 0.0 => write!(f, "{value:.1}"),
            NumberKind::Float(value) => write!(f, "{}", value),
        }
    }
}
