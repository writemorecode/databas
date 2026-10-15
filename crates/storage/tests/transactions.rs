use databas_storage::Storage;

#[test]
fn committed_raw_tree_recovers_without_a_relational_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raw.db");
    let storage = Storage::create(&path).unwrap();
    let transaction = storage.begin_transaction().unwrap();
    let mut tree = storage.transaction_create_tree(transaction).unwrap();
    let root = tree.root_page_id();
    tree.insert(b"key", b"durable").unwrap();
    drop(tree);
    storage.commit_transaction(transaction).unwrap();
    // No database flush: reopening must recover the committed tree from WAL.
    drop(storage);
    let reopened = Storage::open(&path).unwrap();
    let record = reopened.tree_cursor(root).unwrap().get(b"key").unwrap().unwrap();
    record.with_value(|value| assert_eq!(value, b"durable")).unwrap();
}

#[test]
fn raw_savepoint_preserves_prior_writes_and_full_rollback_restores_committed_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raw.db");
    let storage = Storage::create(&path).unwrap();
    let mut tree = storage.create_tree().unwrap();
    let root = tree.root_page_id();
    tree.insert(b"key", b"original").unwrap();
    drop(tree);
    storage.flush().unwrap();

    let transaction = storage.begin_transaction().unwrap();
    let mut tree = storage.transaction_tree_cursor(transaction, root).unwrap();
    tree.update(b"key", b"first change").unwrap();
    let savepoint = storage.statement_savepoint(transaction).unwrap();
    tree.update(b"key", b"second change").unwrap();
    drop(tree);
    storage.rollback_to_savepoint(savepoint).unwrap();
    storage
        .tree_cursor(root)
        .unwrap()
        .get(b"key")
        .unwrap()
        .unwrap()
        .with_value(|value| assert_eq!(value, b"first change"))
        .unwrap();
    storage.rollback_transaction(transaction).unwrap();
    drop(storage);
    let reopened = Storage::open(&path).unwrap();
    reopened
        .tree_cursor(root)
        .unwrap()
        .get(b"key")
        .unwrap()
        .unwrap()
        .with_value(|value| assert_eq!(value, b"original"))
        .unwrap();
}
