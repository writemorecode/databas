use databas::core::{
    ColumnSchema, DataType, Database, EncodedTupleView, Tuple, TupleRef, TupleSchema, Value,
    ValueRef,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("example.db");
    let database = Database::create(&db_path)?;
    database.create_table(
        "users",
        TupleSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "email".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                },
            ],
        },
    )?;
    database.create_index("idx_users_email", "users", &["email"])?;

    let mut users = database.table_cursor_by_name("users")?;
    let mut users_by_email = database.index_cursor_by_name("idx_users_email")?;

    let row_id = 1;
    let tuple_row_id = 1;
    let email = b"ada@example.test";
    let user_values = [
        ValueRef::Integer(tuple_row_id),
        ValueRef::String("Ada Lovelace"),
        ValueRef::String("ada@example.test"),
    ];
    let user_tuple = TupleRef::new(&user_values);
    let encoded_user = user_tuple.to_bytes()?;

    users.insert(row_id, &encoded_user)?;
    users_by_email.insert(email, row_id)?;

    let index_entry =
        users_by_email.get_entry(email)?.expect("email index should contain the inserted user");
    let user = users
        .get_record(index_entry.row_id())?
        .expect("table should contain the row referenced by the index");

    assert_eq!(index_entry.row_id(), row_id);
    assert_eq!(user.row_id(), row_id);
    let decoded_user = user.with_record(Tuple::from_bytes)??;
    assert_eq!(decoded_user, Tuple::new(user_values.into_iter().map(Value::from).collect()));

    user.with_record(|record| {
        let tuple = EncodedTupleView::parse(record)?;
        assert_eq!(
            tuple.values().collect::<Vec<_>>(),
            vec![
                ValueRef::Integer(tuple_row_id),
                ValueRef::String("Ada Lovelace"),
                ValueRef::String("ada@example.test"),
            ]
        );

        println!(
            "found row {} via email index: {:?}",
            user.row_id(),
            tuple.values().collect::<Vec<_>>()
        );

        Ok::<(), std::io::Error>(())
    })??;

    Ok(())
}
