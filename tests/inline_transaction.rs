#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use databas::{
    core::{Database, Value},
    error::DatabaseError,
    executor::ExecutionOutput,
    session::{Session, SessionError},
};

fn query_rows(session: &mut Session<'_>, sql: &str) -> Vec<Vec<Value>> {
    let ExecutionOutput::Rows { rows, .. } = session.execute_sql(sql).unwrap() else {
        panic!("expected rows from {sql}");
    };
    rows.into_iter().map(|row| row.unwrap().values().to_vec()).collect()
}

#[test]
fn inline_begin_insert_commit_persists_row_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inline-transaction.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, name TEXT);").unwrap();

    session.execute_sql("BEGIN; INSERT INTO items (id, name) VALUES (1, 'one'); COMMIT;").unwrap();
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    let mut session = Session::new(&reopened);
    let ExecutionOutput::Rows { rows, .. } =
        session.execute_sql("SELECT id, name FROM items;").unwrap()
    else {
        panic!("expected rows from SELECT");
    };
    let values = rows.into_iter().map(|row| row.unwrap().values().to_vec()).collect::<Vec<_>>();
    assert_eq!(values, vec![vec![Value::Integer(1), Value::String("one".to_owned())]]);
}

#[test]
fn inline_transaction_with_trailing_comment_commits() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("trailing-comment.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();
    session.execute_sql("BEGIN; INSERT INTO items (id) VALUES (1); COMMIT; -- done").unwrap();

    let ExecutionOutput::Rows { rows, .. } = session.execute_sql("SELECT id FROM items;").unwrap()
    else {
        panic!("expected rows from SELECT");
    };
    let values = rows.into_iter().map(|row| row.unwrap().values().to_vec()).collect::<Vec<_>>();
    assert_eq!(values, vec![vec![Value::Integer(1)]]);
}

#[test]
fn inline_rollback_discards_rows_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inline-rollback.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();
    session.execute_sql("INSERT INTO items (id) VALUES (1);").unwrap();

    assert!(matches!(
        session.execute_sql("BEGIN; INSERT INTO items (id) VALUES (2); ROLLBACK;").unwrap(),
        ExecutionOutput::CommandOk
    ));
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        query_rows(&mut Session::new(&reopened), "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn inline_transaction_executes_every_item_and_returns_final_select() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inline-select.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    assert_eq!(
        query_rows(
            &mut session,
            "BEGIN; INSERT INTO items (id) VALUES (1); INSERT INTO items (id) VALUES (2); \
             COMMIT; SELECT id FROM items;"
        ),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        query_rows(&mut Session::new(&reopened), "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
}

#[test]
fn failed_middle_statement_stops_before_inline_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failed-middle.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    assert!(
        session
            .execute_sql(
                "BEGIN; INSERT INTO items (id) VALUES (1); \
                 INSERT INTO items (id) VALUES (1); COMMIT;"
            )
            .is_err()
    );
    // The first insert ran, but the failure prevented COMMIT from running.
    assert_eq!(query_rows(&mut session, "SELECT id FROM items;"), vec![vec![Value::Integer(1)]]);
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert!(query_rows(&mut Session::new(&reopened), "SELECT id FROM items;").is_empty());
}

#[test]
fn invalid_tail_cannot_execute_earlier_items() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("invalid-tail.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    for sql in [
        "BEGIN; INSERT INTO items (id) VALUES (1); COMMIT", // incomplete command
        "BEGIN; INSERT INTO items (id) VALUES (1)",         // incomplete statement
        "BEGIN; INSERT INTO items (id) VALUES (1); COMMIT; @", // bad token after commit
        "BEGIN; INSERT INTO items (id) VALUES (1); COMMIT; SELECT", // incomplete SELECT
    ] {
        assert!(matches!(session.execute_sql(sql), Err(DatabaseError::Parser(_))), "{sql}");
        assert!(
            matches!(
                session.execute_sql("COMMIT;"),
                Err(DatabaseError::Session(SessionError::NoActiveTransaction))
            ),
            "BEGIN ran for {sql}"
        );
        assert!(query_rows(&mut session, "SELECT id FROM items;").is_empty(), "{sql}");
    }
}

#[test]
fn separate_transaction_requests_still_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("separate-requests.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    session.execute_sql("BEGIN;").unwrap();
    session.execute_sql("INSERT INTO items (id) VALUES (1);").unwrap();
    session.execute_sql("COMMIT;").unwrap();
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        query_rows(&mut Session::new(&reopened), "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn transactions_can_cross_sql_request_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("transaction-boundaries.db");
    let database = Database::create(&path).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    assert!(matches!(
        session.execute_sql("BEGIN; INSERT INTO items (id) VALUES (1);").unwrap(),
        ExecutionOutput::RowsAffected(1)
    ));
    assert!(matches!(
        session.execute_sql("INSERT INTO items (id) VALUES (2); COMMIT;").unwrap(),
        ExecutionOutput::CommandOk
    ));
    session.execute_sql("BEGIN;").unwrap();
    session.execute_sql("INSERT INTO items (id) VALUES (3); COMMIT;").unwrap();
    session.close().unwrap();
    database.flush().unwrap();
    drop(database);

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        query_rows(&mut Session::new(&reopened), "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
}

#[test]
fn multi_item_request_returns_only_final_result() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("final-result.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();
    session.execute_sql("INSERT INTO items (id) VALUES (1);").unwrap();

    assert!(matches!(
        session.execute_sql("SELECT id FROM items; INSERT INTO items (id) VALUES (2);").unwrap(),
        ExecutionOutput::RowsAffected(1)
    ));
    assert_eq!(
        query_rows(&mut session, "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
}

#[test]
fn runtime_error_stops_remaining_implicit_statements() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("implicit-error.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    assert!(
        session
            .execute_sql(
                "INSERT INTO items (id) VALUES (1); INSERT INTO items (id) VALUES (1); \
                 INSERT INTO items (id) VALUES (2);"
            )
            .is_err()
    );
    // Earlier implicit statements commit independently; later ones do not run.
    assert_eq!(query_rows(&mut session, "SELECT id FROM items;"), vec![vec![Value::Integer(1)]]);
    session.execute_sql("INSERT INTO items (id) VALUES (3);").unwrap();
    assert_eq!(
        query_rows(&mut session, "SELECT id FROM items;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn later_items_can_use_schema_created_earlier_in_same_request() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("inline-schema.db")).unwrap();
    let mut session = Session::new(&database);

    assert_eq!(
        query_rows(
            &mut session,
            "CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items (id) VALUES (1); \
             SELECT id FROM items;"
        ),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn empty_and_comment_only_requests_do_not_affect_transaction_state() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path().join("empty-request.db")).unwrap();
    let mut session = Session::new(&database);
    session.execute_sql("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();

    for sql in ["", "   -- comment\n /* another comment */ "] {
        assert!(matches!(session.execute_sql(sql), Err(DatabaseError::Parser(_))), "{sql:?}");
    }
    assert!(matches!(
        session.execute_sql("COMMIT;"),
        Err(DatabaseError::Session(SessionError::NoActiveTransaction))
    ));
    session.execute_sql("BEGIN; INSERT INTO items (id) VALUES (1); COMMIT;").unwrap();
    assert_eq!(query_rows(&mut session, "SELECT id FROM items;"), vec![vec![Value::Integer(1)]]);
}
