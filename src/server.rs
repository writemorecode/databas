//! Concurrent Databas TCP server.
//!
//! Each accepted connection owns a session on a worker thread. Transactions
//! coordinate through shared storage and table leases.

use std::{
    io,
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
};

use thiserror::Error;

use crate::{
    core::{Database, Tuple, error::StorageError},
    error::DatabaseError,
    executor::{ExecutionOutput, ExecutorError},
    planner::PlannerError,
    protocol::{
        self, COMPLETE, COMPLETE_COMMAND_OK, COMPLETE_EXPLAIN, COMPLETE_ROWS,
        COMPLETE_ROWS_AFFECTED, COMPLETE_SCHEMA_AFFECTED, ERROR, ErrorCode, QUERY, READY, ROW,
        STARTUP,
    },
    session::Session,
};

/// Failure that prevents the server from continuing.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Listening for a connection failed.
    #[error("server network error: {0}")]
    Network(#[from] io::Error),
    /// Flushing the database after a client disconnected failed.
    #[error("database flush failed: {0}")]
    Storage(#[from] StorageError),
    /// The configured logical database name is invalid.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(&'static str),
}

/// A single-database, single-connection-at-a-time TCP server.
pub struct Server {
    listener: TcpListener,
    database: Arc<Database>,
    database_name: String,
}

impl Server {
    /// Creates a server from an already-open database and bound listener.
    ///
    /// Production callers should open the database first so WAL recovery has
    /// completed before binding the listener.
    ///
    /// # Errors
    ///
    /// Returns an error if `database_name` is empty, longer than 255 bytes, or
    /// contains NUL.
    pub fn new(
        listener: TcpListener,
        database: Database,
        database_name: impl Into<String>,
    ) -> Result<Self, ServerError> {
        let database_name = database_name.into();
        validate_database_name(&database_name)?;
        Ok(Self { listener, database: Arc::new(database), database_name })
    }

    /// Accepts connections forever and serves each on a worker thread.
    ///
    /// Connection-level I/O and protocol errors close only that connection.
    ///
    /// # Errors
    ///
    /// Returns if accepting a connection or flushing the database fails.
    pub fn serve(self) -> Result<(), ServerError> {
        loop {
            let (mut stream, _) = self.listener.accept()?;
            let database = Arc::clone(&self.database);
            let database_name = self.database_name.clone();
            thread::spawn(move || {
                let _ = stream.set_nodelay(true);
                let _ = handle_connection(&mut stream, &database, &database_name);
                let _ = database.flush();
            });
        }
    }

    #[cfg(test)]
    fn serve_one(&self) -> Result<(), ServerError> {
        let (mut stream, _) = self.listener.accept()?;
        let _ = stream.set_nodelay(true);
        let _ = handle_connection(&mut stream, &self.database, &self.database_name);
        self.database.flush()?;
        Ok(())
    }
}

fn validate_database_name(name: &str) -> Result<(), ServerError> {
    if name.is_empty() {
        return Err(ServerError::InvalidDatabaseName("name cannot be empty"));
    }
    if name.len() > 255 {
        return Err(ServerError::InvalidDatabaseName("name cannot exceed 255 bytes"));
    }
    if name.as_bytes().contains(&0) {
        return Err(ServerError::InvalidDatabaseName("name cannot contain NUL"));
    }
    Ok(())
}

fn handle_connection(
    stream: &mut TcpStream,
    database: &Database,
    database_name: &str,
) -> Result<(), protocol::ProtocolError> {
    let startup = match protocol::read_frame(stream) {
        Ok(Some(frame)) => frame,
        Ok(None) => return Ok(()),
        Err(error) => {
            send_protocol_error(stream, &error)?;
            return Ok(());
        }
    };
    if startup.kind != STARTUP {
        send_error(stream, ErrorCode::InvalidRequest, "first client message must be STARTUP")?;
        return Ok(());
    }
    let requested_name = match std::str::from_utf8(&startup.payload) {
        Ok(name) => name,
        Err(_) => {
            send_error(stream, ErrorCode::InvalidRequest, "database name is not UTF-8")?;
            return Ok(());
        }
    };
    if requested_name.is_empty()
        || requested_name.len() > 255
        || requested_name.as_bytes().contains(&0)
    {
        send_error(stream, ErrorCode::InvalidRequest, "database name is invalid")?;
        return Ok(());
    }
    if requested_name != database_name {
        send_error(
            stream,
            ErrorCode::DatabaseNotFound,
            "requested database is not served by this endpoint",
        )?;
        return Ok(());
    }
    protocol::write_frame(stream, READY, &[])?;

    let mut session = Session::new(database);
    loop {
        let frame = match protocol::read_frame(stream) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                send_protocol_error(stream, &error)?;
                return Ok(());
            }
        };
        if frame.kind != QUERY {
            send_error(stream, ErrorCode::InvalidRequest, "expected a QUERY message")?;
            continue;
        }
        let sql = match std::str::from_utf8(&frame.payload) {
            Ok(sql) => sql,
            Err(_) => {
                send_error(stream, ErrorCode::InvalidRequest, "query is not UTF-8")?;
                continue;
            }
        };
        match session.execute_sql(sql) {
            Ok(output) => send_output(stream, output)?,
            Err(error) => {
                let code = database_error_code(&error);
                send_error(stream, code, &error.to_string())?;
            }
        }
    }
}

fn send_output(
    stream: &mut TcpStream,
    output: ExecutionOutput,
) -> Result<(), protocol::ProtocolError> {
    match output {
        ExecutionOutput::Rows { rows } => {
            let mut count = 0_u64;
            for row in rows {
                let row = match row {
                    Ok(row) => row,
                    Err(error) => {
                        send_error(stream, executor_error_code(&error), &error.to_string())?;
                        return Ok(());
                    }
                };
                let tuple = match row.with_record(Tuple::from_bytes) {
                    Ok(Ok(tuple)) => tuple,
                    Ok(Err(error)) => {
                        send_error(stream, ErrorCode::ExecutionError, &error.to_string())?;
                        return Ok(());
                    }
                    Err(error) => {
                        send_error(stream, storage_error_code(&error), &error.to_string())?;
                        return Ok(());
                    }
                };
                let payload = protocol::encode_row(tuple.values())?;
                protocol::write_frame(stream, ROW, &payload)?;
                count = count
                    .checked_add(1)
                    .ok_or(protocol::ProtocolError::Malformed("query returned too many rows"))?;
            }
            let mut payload = Vec::with_capacity(9);
            payload.push(COMPLETE_ROWS);
            payload.extend_from_slice(&count.to_be_bytes());
            protocol::write_frame(stream, COMPLETE, &payload)
        }
        ExecutionOutput::Explain(plan) => {
            let mut payload = Vec::with_capacity(1 + plan.len());
            payload.push(COMPLETE_EXPLAIN);
            payload.extend_from_slice(plan.as_bytes());
            protocol::write_frame(stream, COMPLETE, &payload)
        }
        ExecutionOutput::RowsAffected(count) => {
            let mut payload = Vec::with_capacity(9);
            payload.push(COMPLETE_ROWS_AFFECTED);
            payload.extend_from_slice(&count.to_be_bytes());
            protocol::write_frame(stream, COMPLETE, &payload)
        }
        ExecutionOutput::SchemaAffected => {
            protocol::write_frame(stream, COMPLETE, &[COMPLETE_SCHEMA_AFFECTED])
        }
        ExecutionOutput::CommandOk => {
            protocol::write_frame(stream, COMPLETE, &[COMPLETE_COMMAND_OK])
        }
    }
}

fn send_protocol_error(
    stream: &mut TcpStream,
    error: &protocol::ProtocolError,
) -> Result<(), protocol::ProtocolError> {
    let code = if matches!(error, protocol::ProtocolError::UnsupportedVersion(_)) {
        ErrorCode::UnsupportedProtocolVersion
    } else {
        ErrorCode::ProtocolError
    };
    send_error(stream, code, &error.to_string())
}

fn send_error(
    stream: &mut TcpStream,
    code: ErrorCode,
    message: &str,
) -> Result<(), protocol::ProtocolError> {
    let payload = protocol::encode_error(code, message);
    protocol::write_frame(stream, ERROR, &payload)
}

fn database_error_code(error: &DatabaseError<'_>) -> ErrorCode {
    match error {
        DatabaseError::Parser(_) => ErrorCode::SyntaxError,
        DatabaseError::Storage(error) => storage_error_code(error),
        DatabaseError::Planner(PlannerError::Storage(error)) => storage_error_code(error),
        DatabaseError::Planner(_) => ErrorCode::PlanningError,
        DatabaseError::Executor(error) => executor_error_code(error),
        DatabaseError::Session(_) => ErrorCode::TransactionError,
        DatabaseError::Io(_) => ErrorCode::StorageIoError,
    }
}

fn executor_error_code(error: &ExecutorError) -> ErrorCode {
    match error {
        ExecutorError::Storage(error) => storage_error_code(error),
        _ => ErrorCode::ExecutionError,
    }
}

fn storage_error_code(error: &StorageError) -> ErrorCode {
    match error {
        StorageError::Io(_) => ErrorCode::StorageIoError,
        StorageError::Corruption(_) => ErrorCode::DatabaseCorruption,
        StorageError::Constraint(_) => ErrorCode::ConstraintViolation,
        StorageError::InvalidArgument(_) => ErrorCode::InvalidArgument,
        StorageError::LimitExceeded(_) => ErrorCode::LimitExceeded,
        StorageError::Lock(_) => ErrorCode::ExecutionError,
        StorageError::Internal(_) => ErrorCode::InternalError,
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        client::{Client, ClientError, QueryResult},
        core::Value,
    };

    #[test]
    fn client_and_server_execute_queries_over_tcp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("network.db");
        let (address_sender, address_receiver) = mpsc::sync_channel(1);

        let server_thread = thread::spawn(move || {
            let database = Database::create(path).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            address_sender.send(listener.local_addr().unwrap()).unwrap();
            let server = Server::new(listener, database, "main").unwrap();
            server.serve_one().unwrap();
        });

        let address = address_receiver.recv().unwrap();
        let mut client = Client::connect(address, "main").unwrap();
        assert_eq!(
            client.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT);").unwrap(),
            QueryResult::SchemaAffected
        );
        assert_eq!(
            client.execute("INSERT INTO items (id, name) VALUES (1, 'one');").unwrap(),
            QueryResult::RowsAffected(1)
        );
        assert_eq!(
            client.execute("SELECT id, name FROM items;").unwrap(),
            QueryResult::Rows(vec![vec![Value::Integer(1), Value::String("one".to_owned())]])
        );
        let error = client.execute("SELECT FROM;").unwrap_err();
        assert!(matches!(
            error,
            ClientError::Server(crate::client::ServerError { code: ErrorCode::SyntaxError, .. })
        ));

        drop(client);
        server_thread.join().unwrap();
    }

    #[test]
    fn server_rejects_a_different_database_name() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("network.db");
        let (address_sender, address_receiver) = mpsc::sync_channel(1);

        let server_thread = thread::spawn(move || {
            let database = Database::create(path).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            address_sender.send(listener.local_addr().unwrap()).unwrap();
            let server = Server::new(listener, database, "main").unwrap();
            server.serve_one().unwrap();
        });

        let error = Client::connect(address_receiver.recv().unwrap(), "other").unwrap_err();
        assert!(matches!(
            error,
            ClientError::Server(crate::client::ServerError {
                code: ErrorCode::DatabaseNotFound,
                ..
            })
        ));
        server_thread.join().unwrap();
    }
}
