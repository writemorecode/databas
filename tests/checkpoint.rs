#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use databas::{
    core::{CheckpointOutcome, Database, Value},
    executor::ExecutionOutput,
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

fn query_rows(database: &Database) -> Vec<Vec<Value>> {
    let mut session = Session::new(database);
    let ExecutionOutput::Rows { rows, .. } =
        session.execute_sql("SELECT id, body FROM items;").unwrap()
    else {
        panic!("expected rows");
    };
    rows.into_iter().map(|row| row.unwrap().values().to_vec()).collect()
}

#[test]
fn completed_checkpoint_preserves_allocator_for_later_overflow_insert_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completed.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, body TEXT);").unwrap();
    assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Completed);
    assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Completed);

    let body = "x".repeat(12000);
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (1, '{body}');")).unwrap();
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(query_rows(&reopened), vec![vec![Value::Integer(1), Value::String(body)]]);
}

#[test]
fn checkpointed_frees_can_be_reused_and_recovered_after_trunks_are_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reuse.db");
    let body = "x".repeat(12000);
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, body TEXT);").unwrap();
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (1, '{body}');")).unwrap();
    session.execute_sql("DELETE FROM items WHERE id == 1;").unwrap();
    assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Completed);
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);
    let original_len = std::fs::metadata(&path).unwrap().len();

    // Recovery writes a nonempty on-disk freelist. Subsequent reservations
    // consume its trunk pages; the replacement WAL snapshot must supersede it.
    let database = Database::open(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (2, '{body}');")).unwrap();
    assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Completed);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
    session.execute_sql(&format!("INSERT INTO items (id, body) VALUES (3, '{body}');")).unwrap();
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        query_rows(&reopened),
        vec![
            vec![Value::Integer(2), Value::String(body.clone())],
            vec![Value::Integer(3), Value::String(body)],
        ]
    );
}

#[test]
fn partial_checkpoint_preserves_active_allocator_history_for_commit_and_rollback() {
    for commit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.db");
        let database = Database::create(&path).unwrap();
        let mut session = Session::new(&database);
        session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, body TEXT);").unwrap();
        let body = "x".repeat(12000);
        session.execute_sql("BEGIN;").unwrap();
        session
            .execute_sql(&format!("INSERT INTO items (id, body) VALUES (1, '{body}');"))
            .unwrap();
        assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Partial);
        assert_eq!(database.checkpoint().unwrap(), CheckpointOutcome::Partial);
        session.execute_sql(if commit { "COMMIT;" } else { "ROLLBACK;" }).unwrap();
        session.close().unwrap();
        database.flush().unwrap();
        drop(database);

        let reopened = Database::open(&path).unwrap();
        let expected =
            if commit { vec![vec![Value::Integer(1), Value::String(body)]] } else { vec![] };
        assert_eq!(query_rows(&reopened), expected);
    }
}
