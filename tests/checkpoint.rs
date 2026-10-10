#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use databas::{
    core::{CheckpointOutcome, Database},
    session::Session,
};

#[test]
fn checkpoint_retaining_active_reservations_does_not_poison_wal() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("active.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, body TEXT);").unwrap();
    let body = "x".repeat(12000);
    session.execute_sql("BEGIN;").unwrap();
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (1, '{body}');")).unwrap();

    assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Partial);
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (2, '{body}');")).unwrap();
    session.execute_sql("COMMIT;").unwrap();
    session.close().unwrap();
    database.flush().unwrap();
}
