use databas_core::Pager;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_file = tempfile::NamedTempFile::new()?;
    let pager = Pager::open(db_file.path())?;

    let mut users = pager.create_table()?;
    let mut users_by_email = pager.create_index()?;

    let row_id = 1;
    let email = b"ada@example.test";
    let encoded_user = b"id=1;name=Ada Lovelace;email=ada@example.test";

    users.insert(row_id, encoded_user)?;
    users_by_email.insert(email, row_id)?;

    let index_entry =
        users_by_email.get(email)?.expect("email index should contain the inserted user");
    let user = users
        .get(index_entry.row_id)?
        .expect("table should contain the row referenced by the index");

    assert_eq!(index_entry.row_id, row_id);
    assert_eq!(user.row_id, row_id);
    assert_eq!(user.record.as_ref(), encoded_user);

    println!(
        "found row {} via email index: {}",
        user.row_id,
        String::from_utf8_lossy(&user.record)
    );

    Ok(())
}
