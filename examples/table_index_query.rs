use databas::{
    client::{Client, QueryResult},
    core::Value,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start `server example example.db` before running this example.
    let mut client = Client::connect("127.0.0.1:5432", "example")?;

    client.execute(
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT
        );",
    )?;
    client.execute("CREATE INDEX idx_users_email ON users (email);")?;
    client.execute(
        "INSERT INTO users (id, name, email) VALUES
            (1, 'Ada Lovelace', 'ada@example.test');",
    )?;

    let result = client.execute("SELECT id, name, email FROM users WHERE id = 1;")?;
    let QueryResult::Rows(rows) = result else {
        return Err("SELECT did not return rows".into());
    };
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Ada Lovelace".to_owned()),
            Value::String("ada@example.test".to_owned()),
        ]]
    );

    Ok(())
}
