//! Concurrent Databas TCP server.
//!
//! Each accepted connection owns a session on a worker thread. Transactions
//! coordinate through shared storage and table leases.

use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, TryRecvError},
    },
    thread,
    time::Duration,
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
    thread_pool::{ThreadPool, ThreadPoolError},
};

const SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(30);
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Socket cancellation is independent of worker progress and wakes blocked I/O.
#[derive(Default)]
struct Cancellation {
    stopped: AtomicBool,
    sockets: Mutex<Vec<Weak<TcpStream>>>,
}

impl Cancellation {
    fn register(&self, stream: &TcpStream) -> io::Result<Arc<TcpStream>> {
        let socket = Arc::new(stream.try_clone()?);
        let mut sockets = self.sockets.lock().unwrap_or_else(|error| error.into_inner());
        sockets.retain(|socket| socket.strong_count() != 0);
        if self.stopped.load(Ordering::Acquire) {
            socket.shutdown(Shutdown::Both)?;
        } else {
            sockets.push(Arc::downgrade(&socket));
        }
        Ok(socket)
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let mut sockets = self.sockets.lock().unwrap_or_else(|error| error.into_inner());
        for socket in sockets.drain(..).filter_map(|socket| socket.upgrade()) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

/// Failure that prevents the server from continuing.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Listening for a connection failed.
    #[error("server network error: {0}")]
    Network(#[from] io::Error),
    /// A worker encountered a critical database failure.
    #[error("critical database error: {0}")]
    Storage(#[from] StorageError),
    /// The worker pool failed or a worker stopped unexpectedly.
    #[error("server worker pool error: {0}")]
    ThreadPool(#[from] ThreadPoolError),
    /// A worker stopped while the server was running.
    #[error("database worker {worker_id} stopped unexpectedly")]
    WorkerStopped { worker_id: usize },
    /// The configured logical database name is invalid.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(&'static str),
}

/// A single-database TCP server backed by a fixed-size thread pool.
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

    /// Accepts connections forever and submits each connection as one pool job.
    /// Connections exceeding the bounded pool queue capacity are closed immediately,
    /// keeping the supervisor responsive to failures even under overload.
    ///
    /// Connection-level I/O and malformed protocol messages close only that
    /// connection. Critical storage, corruption, and internal errors stop the
    /// complete server.
    ///
    /// # Errors
    ///
    /// Returns if accepting a connection fails, a worker stops, or a worker
    /// encounters a critical database error.
    pub fn serve(self) -> Result<(), ServerError> {
        let worker_count = thread::available_parallelism().map_or(1, |count| count.get());
        self.serve_with_worker_count(worker_count)
    }

    fn serve_with_worker_count(self, worker_count: usize) -> Result<(), ServerError> {
        let pool = ThreadPool::new(worker_count)?;
        let (failure_sender, failure_receiver) = mpsc::channel();
        let shutdown = Arc::new(Cancellation::default());

        let result = self.accept_connections(&pool, &failure_sender, &failure_receiver, &shutdown);
        shutdown.stop();
        // Wake table-lock waiters before joining. In-flight storage syscalls
        // cannot be interrupted safely; shutdown waits for them to return.
        let stop_result = self.database.stop();
        drop(failure_sender);
        let shutdown_result = pool.shutdown();

        match result {
            Err(error) => Err(error),
            Ok(()) => {
                stop_result?;
                shutdown_result.map_err(Into::into)
            }
        }
    }

    fn accept_connections(
        &self,
        pool: &ThreadPool,
        failures: &mpsc::Sender<StorageError>,
        failure_receiver: &Receiver<StorageError>,
        shutdown: &Arc<Cancellation>,
    ) -> Result<(), ServerError> {
        self.listener.set_nonblocking(true)?;
        loop {
            check_workers(pool, failure_receiver)?;
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let database = Arc::clone(&self.database);
                    let database_name = self.database_name.clone();
                    let failures = failures.clone();
                    let shutdown = Arc::clone(shutdown);
                    let submission = pool.try_execute(move || {
                        if shutdown.stopped.load(Ordering::Acquire) {
                            return;
                        }
                        if let Err(error) =
                            serve_connection(stream, &database, &database_name, &shutdown)
                        {
                            shutdown.stop();
                            let _ = failures.send(error);
                        }
                    });
                    match submission {
                        Ok(()) | Err(ThreadPoolError::QueueFull) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    match failure_receiver.recv_timeout(SUPERVISOR_POLL_INTERVAL) {
                        Ok(error) => return Err(error.into()),
                        Err(RecvTimeoutError::Timeout) => check_stopped_worker(pool)?,
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(ThreadPoolError::QueueClosed.into());
                        }
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    #[cfg(test)]
    fn serve_one(&self) -> Result<(), ServerError> {
        let (stream, _) = self.listener.accept()?;
        serve_connection(stream, &self.database, &self.database_name, &Cancellation::default())?;
        Ok(())
    }
}

fn check_workers(pool: &ThreadPool, failures: &Receiver<StorageError>) -> Result<(), ServerError> {
    match failures.try_recv() {
        Ok(error) => return Err(error.into()),
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => return Err(ThreadPoolError::QueueClosed.into()),
    }
    check_stopped_worker(pool)
}

fn check_stopped_worker(pool: &ThreadPool) -> Result<(), ServerError> {
    if let Some(worker_id) = pool.stopped_worker() {
        return Err(ServerError::WorkerStopped { worker_id });
    }
    Ok(())
}

fn serve_connection(
    mut stream: TcpStream,
    database: &Database,
    database_name: &str,
    shutdown: &Cancellation,
) -> Result<(), StorageError> {
    let Ok(_registration) = shutdown.register(&stream) else {
        return Ok(());
    };
    if configure_stream(&stream).is_err() {
        return Ok(());
    }
    match handle_connection(&mut stream, database, database_name, shutdown) {
        Ok(()) | Err(ConnectionError::Protocol(_)) => Ok(()),
        Err(ConnectionError::Storage(error)) => Err(error),
    }
}

fn configure_stream(stream: &TcpStream) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    if cfg!(debug_assertions) {
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;
    } else {
        stream.set_read_timeout(Some(SOCKET_READ_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_WRITE_TIMEOUT))?;
    }
    Ok(())
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

#[derive(Debug, Error)]
enum ConnectionError {
    #[error(transparent)]
    Protocol(#[from] protocol::ProtocolError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

fn handle_connection(
    stream: &mut TcpStream,
    database: &Database,
    database_name: &str,
    shutdown: &Cancellation,
) -> Result<(), ConnectionError> {
    let startup = match protocol::read_frame(stream) {
        Ok(Some(frame)) => frame,
        Ok(None) => return Ok(()),
        Err(error @ protocol::ProtocolError::Io(_)) => return Err(error.into()),
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
    let result = handle_queries(stream, &mut session, shutdown);
    let cleanup = session.close();
    // Preserve the original critical failure, but never hide failed cleanup
    // behind an ordinary disconnect, malformed frame, or socket timeout.
    if matches!(result, Err(ConnectionError::Storage(_))) {
        return result;
    }
    cleanup?;
    result
}

fn handle_queries(
    stream: &mut TcpStream,
    session: &mut Session<'_>,
    shutdown: &Cancellation,
) -> Result<(), ConnectionError> {
    loop {
        if shutdown.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        let frame = match protocol::read_frame(stream) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error @ protocol::ProtocolError::Io(_)) => return Err(error.into()),
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
        // A statement already admitted before cancellation may finish, but
        // buffered requests must not start another statement after shutdown.
        if shutdown.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        match session.execute_sql(sql) {
            Ok(output) => send_output(stream, output)?,
            Err(error) => send_database_error(stream, error)?,
        }
    }
}

fn send_output(stream: &mut TcpStream, output: ExecutionOutput) -> Result<(), ConnectionError> {
    match output {
        ExecutionOutput::Rows { rows } => {
            let mut count = 0_u64;
            for row in rows {
                let row = match row {
                    Ok(row) => row,
                    Err(ExecutorError::Storage(error)) => {
                        send_storage_error(stream, error)?;
                        return Ok(());
                    }
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
                        send_storage_error(stream, error)?;
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
            protocol::write_frame(stream, COMPLETE, &payload)?;
            Ok(())
        }
        ExecutionOutput::Explain(plan) => {
            let mut payload = Vec::with_capacity(1 + plan.len());
            payload.push(COMPLETE_EXPLAIN);
            payload.extend_from_slice(plan.as_bytes());
            protocol::write_frame(stream, COMPLETE, &payload)?;
            Ok(())
        }
        ExecutionOutput::RowsAffected(count) => {
            let mut payload = Vec::with_capacity(9);
            payload.push(COMPLETE_ROWS_AFFECTED);
            payload.extend_from_slice(&count.to_be_bytes());
            protocol::write_frame(stream, COMPLETE, &payload)?;
            Ok(())
        }
        ExecutionOutput::SchemaAffected => {
            protocol::write_frame(stream, COMPLETE, &[COMPLETE_SCHEMA_AFFECTED])?;
            Ok(())
        }
        ExecutionOutput::CommandOk => {
            protocol::write_frame(stream, COMPLETE, &[COMPLETE_COMMAND_OK])?;
            Ok(())
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

fn send_database_error(
    stream: &mut TcpStream,
    error: DatabaseError<'_>,
) -> Result<(), ConnectionError> {
    match error {
        DatabaseError::Storage(error)
        | DatabaseError::Planner(PlannerError::Storage(error))
        | DatabaseError::Executor(ExecutorError::Storage(error)) => {
            send_storage_error(stream, error)
        }
        DatabaseError::Io(error) => Err(StorageError::Io(error).into()),
        error => {
            let code = database_error_code(&error);
            send_error(stream, code, &error.to_string())?;
            Ok(())
        }
    }
}

fn send_storage_error(stream: &mut TcpStream, error: StorageError) -> Result<(), ConnectionError> {
    if matches!(
        error,
        StorageError::Io(_) | StorageError::Corruption(_) | StorageError::Internal(_)
    ) {
        return Err(error.into());
    }
    send_error(stream, storage_error_code(&error), &error.to_string())?;
    Ok(())
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
    fn fatal_error_closes_other_connections_and_joins_workers() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("shutdown.db")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::new(listener, database, "main").unwrap();
        let database = Arc::clone(&server.database);
        let (done_tx, done_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            done_tx.send(server.serve_with_worker_count(2)).unwrap();
        });
        let mut idle = Client::connect(address, "main").unwrap();
        let mut failing = Client::connect(address, "main").unwrap();
        // A recoverable error must not stop either connection.
        assert!(failing.execute("SELECT FROM;").is_err());
        idle.execute("BEGIN;").unwrap();
        idle.execute("ROLLBACK;").unwrap();
        database.fail_next_wal_flush_for_test();
        assert!(failing.execute("CREATE TABLE broken (id INT PRIMARY KEY);").is_err());
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            Err(ServerError::Storage(StorageError::Io(_)))
        ));
        assert!(idle.execute("BEGIN;").is_err());
        server_thread.join().unwrap();
    }

    #[test]
    fn disconnect_rollback_failure_reaches_supervisor() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("cleanup.db")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::new(listener, database, "main").unwrap();
        let database = Arc::clone(&server.database);
        let (done_tx, done_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            done_tx.send(server.serve_with_worker_count(2)).unwrap();
        });
        let mut client = Client::connect(address, "main").unwrap();
        let mut idle = Client::connect(address, "main").unwrap();
        client.execute("BEGIN;").unwrap();
        database.fail_next_wal_flush_for_test();
        drop(client);
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            Err(ServerError::Storage(StorageError::Io(_)))
        ));
        assert!(idle.execute("BEGIN;").is_err());
        server_thread.join().unwrap();
    }

    #[test]
    fn session_close_reports_rollback_failure_without_retrying_in_drop() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("session-close.db")).unwrap();
        let mut session = Session::new(&database);
        session.execute_sql("BEGIN;").unwrap();
        let txn_id = session.active_transaction_id_for_test().unwrap();
        database.fail_next_wal_flush_for_test();
        assert!(matches!(session.close(), Err(StorageError::Io(_))));
        // Drop must not silently retry and mask the failed rollback outcome.
        assert!(database.transaction_is_active(txn_id).unwrap());
        database.rollback_transaction(txn_id).unwrap();
        Session::new(&database).close().unwrap();
    }

    #[test]
    fn supervisor_detects_panicked_worker_with_a_full_queue() {
        let dir = tempdir().unwrap();
        let database = Database::create(dir.path().join("panic.db")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::new(listener, database, "main").unwrap();
        let pool = ThreadPool::new(2).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (panic_tx, panic_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let started = started_tx.clone();
        pool.execute(move || {
            started.send(()).unwrap();
            panic_rx.recv().unwrap();
            panic!("injected worker failure");
        })
        .unwrap();
        pool.execute(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        for _ in 0..2 {
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            pool.execute(|| {}).unwrap();
        }
        let mut excess = TcpStream::connect(address).unwrap();
        excess.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let supervisor = thread::spawn(move || {
            let (failures, receiver) = mpsc::channel();
            let result = server.accept_connections(
                &pool,
                &failures,
                &receiver,
                &Arc::new(Cancellation::default()),
            );
            done_tx.send(result).unwrap();
            pool.shutdown()
        });
        // Prove the supervisor attempted submission while every worker and
        // queue slot was occupied, rather than detecting the panic first.
        assert!(protocol::read_frame(&mut excess).unwrap().is_none());
        panic_tx.send(()).unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(ServerError::WorkerStopped { .. })
        ));
        release_tx.send(()).unwrap();
        assert!(matches!(supervisor.join().unwrap(), Err(ThreadPoolError::WorkerPanicked { .. })));
    }

    #[test]
    fn cancellation_closes_sockets_registered_after_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let (stream, _) = listener.accept().unwrap();
        let cancellation = Cancellation::default();
        cancellation.stop();
        let _registration = cancellation.register(&stream).unwrap();
        assert!(protocol::read_frame(&mut client).unwrap().is_none());
    }

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
