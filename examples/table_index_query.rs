use databas::{core::Database, executor::ExecutionOutput, session::Session};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("example.db");
    let database = Database::create(&db_path)?;
    let mut session = Session::new(&database);

    session.execute_sql(
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT
        );",
    )?;
    session.execute_sql("CREATE INDEX idx_users_email ON users (email);")?;
    session.execute_sql(
        "INSERT INTO users (id, name, email) VALUES
            (1, 'Ada Lovelace', 'ada@example.test');",
    )?;

    let output = session.execute_sql("SELECT id, name, email FROM users WHERE id = 1;")?;
    let rows = match output {
        ExecutionOutput::Rows { rows } => rows.collect::<Result<Vec<_>, _>>()?,
        other => panic!("SELECT should return rows, got {other:?}"),
    };

    assert_eq!(rows.len(), 1);
    println!("found user: {}", rows[0]);

    Ok(())
}
