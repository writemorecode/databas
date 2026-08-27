//! Synchronous TCP client for a Databas server.
//!
//! A client owns one TCP connection and executes one query at a time. Databas
//! currently has no pipelining or concurrent-query support.

use std::{
    fmt,
    net::{TcpStream, ToSocketAddrs},
};

use thiserror::Error;

use crate::{
    core::Value,
    protocol::{
        self, COMPLETE, COMPLETE_COMMAND_OK, COMPLETE_EXPLAIN, COMPLETE_ROWS,
        COMPLETE_ROWS_AFFECTED, COMPLETE_SCHEMA_AFFECTED, ERROR, ErrorCode, Frame, QUERY, READY,
        ROW, STARTUP,
    },
};

/// A server-reported database error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("server error {code:?} ({}): {message}", code.as_u16())]
pub struct ServerError {
    /// Stable, machine-readable error category.
    pub code: ErrorCode,
    /// Human-readable diagnostic supplied by the server.
    pub message: String,
}

/// Errors returned by the network client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The TCP connection or wire encoding failed.
    #[error(transparent)]
    Protocol(#[from] protocol::ProtocolError),
    /// The server rejected a startup or query request.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// The server sent a valid frame in an invalid protocol state.
    #[error("unexpected server message: {0}")]
    UnexpectedMessage(&'static str),
    /// An `EXPLAIN` response contained invalid UTF-8.
    #[error("unexpected server message: EXPLAIN result is not UTF-8")]
    InvalidExplainUtf8 {
        /// The underlying UTF-8 decoding failure.
        #[source]
        source: std::str::Utf8Error,
    },
    /// The database name is empty or exceeds the protocol limit.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(&'static str),
}

/// Complete result of one SQL request.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum QueryResult {
    /// Textual physical plan returned by `EXPLAIN`.
    Explain(String),
    /// Typed rows returned by a row-producing statement.
    Rows(Vec<Vec<Value>>),
    /// Number of rows changed by a data-modification statement.
    RowsAffected(u64),
    /// A schema statement completed successfully.
    SchemaAffected,
    /// A transaction command completed successfully.
    CommandOk,
}

impl fmt::Display for QueryResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Explain(plan) => formatter.write_str(plan),
            Self::Rows(rows) => write!(formatter, "{} rows returned.", rows.len()),
            Self::RowsAffected(count) => write!(formatter, "{count} rows affected."),
            Self::SchemaAffected => formatter.write_str("Schema affected."),
            Self::CommandOk => formatter.write_str("Command executed."),
        }
    }
}

/// One synchronous connection to a Databas server.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connects to `address` and selects `database_name`.
    ///
    /// The server hosts exactly one configured database in protocol version 1;
    /// a different name is rejected with [`ErrorCode::DatabaseNotFound`].
    ///
    /// # Errors
    ///
    /// Returns an error if the name is invalid, TCP connection fails, the
    /// handshake is malformed, or the server rejects the database name.
    pub fn connect(address: impl ToSocketAddrs, database_name: &str) -> Result<Self, ClientError> {
        validate_database_name(database_name)?;
        let mut stream = TcpStream::connect(address).map_err(protocol::ProtocolError::from)?;
        stream.set_nodelay(true).map_err(protocol::ProtocolError::from)?;
        protocol::write_frame(&mut stream, STARTUP, database_name.as_bytes())?;

        let frame = read_required_frame(&mut stream, "server closed during startup")?;
        match frame.kind {
            READY if frame.payload.is_empty() => Ok(Self { stream }),
            READY => Err(ClientError::UnexpectedMessage("READY payload must be empty")),
            ERROR => Err(decode_server_error(&frame.payload)?.into()),
            _ => Err(ClientError::UnexpectedMessage("expected READY or ERROR during startup")),
        }
    }

    /// Executes one SQL item and collects its complete result.
    ///
    /// Row frames are streamed by the server but collected into memory by this
    /// convenience API. Calls are strictly sequential on this connection.
    ///
    /// # Errors
    ///
    /// Returns an error if SQL execution fails, the connection is interrupted,
    /// or the server sends a malformed response. A server execution error does
    /// not close the connection, so the client may issue another query.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, ClientError> {
        protocol::write_frame(&mut self.stream, QUERY, sql.as_bytes())?;
        let mut rows = Vec::new();

        loop {
            let frame = read_required_frame(&mut self.stream, "server closed during query")?;
            match frame.kind {
                ROW => rows.push(protocol::decode_row(&frame.payload)?),
                COMPLETE => return decode_complete(&frame.payload, rows),
                ERROR => return Err(decode_server_error(&frame.payload)?.into()),
                _ => {
                    return Err(ClientError::UnexpectedMessage(
                        "expected ROW, COMPLETE, or ERROR during query",
                    ));
                }
            }
        }
    }
}

fn validate_database_name(name: &str) -> Result<(), ClientError> {
    if name.is_empty() {
        return Err(ClientError::InvalidDatabaseName("name cannot be empty"));
    }
    if name.len() > 255 {
        return Err(ClientError::InvalidDatabaseName("name cannot exceed 255 bytes"));
    }
    if name.as_bytes().contains(&0) {
        return Err(ClientError::InvalidDatabaseName("name cannot contain NUL"));
    }
    Ok(())
}

fn read_required_frame(
    stream: &mut TcpStream,
    eof_message: &'static str,
) -> Result<Frame, ClientError> {
    protocol::read_frame(stream)?.ok_or(ClientError::UnexpectedMessage(eof_message))
}

fn decode_server_error(payload: &[u8]) -> Result<ServerError, ClientError> {
    let (code, message) = protocol::decode_error(payload)?;
    Ok(ServerError { code, message })
}

fn decode_complete(payload: &[u8], rows: Vec<Vec<Value>>) -> Result<QueryResult, ClientError> {
    let Some((&kind, data)) = payload.split_first() else {
        return Err(ClientError::UnexpectedMessage("COMPLETE payload is empty"));
    };
    match kind {
        COMPLETE_ROWS => {
            let expected = decode_completion_count(data, "row completion count is invalid")?;
            if expected != rows.len() as u64 {
                return Err(ClientError::UnexpectedMessage("row completion count does not match"));
            }
            Ok(QueryResult::Rows(rows))
        }
        COMPLETE_EXPLAIN => {
            require_no_rows(&rows)?;
            let plan = std::str::from_utf8(data)
                .map_err(|source| ClientError::InvalidExplainUtf8 { source })?;
            Ok(QueryResult::Explain(plan.to_owned()))
        }
        COMPLETE_ROWS_AFFECTED => {
            require_no_rows(&rows)?;
            let count = decode_completion_count(data, "rows-affected count is invalid")?;
            Ok(QueryResult::RowsAffected(count))
        }
        COMPLETE_SCHEMA_AFFECTED if data.is_empty() => {
            require_no_rows(&rows)?;
            Ok(QueryResult::SchemaAffected)
        }
        COMPLETE_COMMAND_OK if data.is_empty() => {
            require_no_rows(&rows)?;
            Ok(QueryResult::CommandOk)
        }
        COMPLETE_SCHEMA_AFFECTED | COMPLETE_COMMAND_OK => {
            Err(ClientError::UnexpectedMessage("completion payload has trailing bytes"))
        }
        _ => Err(ClientError::UnexpectedMessage("unknown completion kind")),
    }
}

fn decode_completion_count(data: &[u8], invalid_message: &'static str) -> Result<u64, ClientError> {
    let [b0, b1, b2, b3, b4, b5, b6, b7] = data else {
        return Err(ClientError::UnexpectedMessage(invalid_message));
    };
    Ok(u64::from_be_bytes([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]))
}

fn require_no_rows(rows: &[Vec<Value>]) -> Result<(), ClientError> {
    if rows.is_empty() {
        Ok(())
    } else {
        Err(ClientError::UnexpectedMessage("non-row result followed ROW frames"))
    }
}
