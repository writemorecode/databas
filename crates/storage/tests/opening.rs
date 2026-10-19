//! File ownership is checked in a separate process, using real OS locks.
use std::{fs, path::Path, process::Command};

use databas_storage::{Storage, StorageOptions, error::StorageError};

const OPTIONS: StorageOptions = StorageOptions { cache_frames: 8 };

fn probe(path: &Path, available: bool) -> std::io::Result<()> {
    let output = Command::new(std::env::current_exe()?)
        .args(["--exact", "lock_probe", "--nocapture"])
        .env("DATABAS_LOCK_PROBE_PATH", path)
        .env("DATABAS_LOCK_PROBE_AVAILABLE", available.to_string())
        .output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

#[test]
fn lock_probe() {
    let Some(path) = std::env::var_os("DATABAS_LOCK_PROBE_PATH") else {
        return;
    };
    let available =
        std::env::var("DATABAS_LOCK_PROBE_AVAILABLE").expect("probe expectation") == "true";
    let result = Storage::open_or_create_with_options(path, OPTIONS);
    if available {
        let storage = result.expect("ownership should be released");
        storage.create_tree().expect("reopened file is usable");
        storage.flush().expect("flush reopened file");
    } else {
        assert!(matches!(result, Err(StorageError::Io(_))), "another process obtained ownership");
    }
}

#[test]
fn ownership_follows_the_file_and_lasts_until_the_final_cursor_and_record_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let alias = dir.path().join("alias");
    let storage = Storage::open_or_create_with_options(&path, OPTIONS).unwrap();
    let clone = storage.clone();
    let mut cursor = storage.create_tree().unwrap();
    cursor.insert(b"key", b"value").unwrap();
    storage.flush().unwrap();
    let record = cursor.get(b"key").unwrap().unwrap();
    fs::hard_link(&path, &alias).unwrap();
    probe(&path, false).unwrap();
    probe(&alias, false).unwrap();
    drop(storage);
    probe(&path, false).unwrap();
    drop(clone);
    probe(&path, false).unwrap();
    drop(cursor);
    probe(&path, false).unwrap();
    assert_eq!(record.with_key_value(|_, value| value.to_vec()).unwrap(), b"value");
    drop(record);
    probe(&path, true).unwrap();
}

#[test]
fn filesystem_open_errors_do_not_create_other_files() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing-parent/db");
    for path in [&missing, &dir.path().to_path_buf()] {
        for result in [
            Storage::create(path),
            Storage::open(path),
            Storage::open_or_create_with_options(path, OPTIONS),
        ] {
            assert!(matches!(result, Err(StorageError::Io(_))));
        }
    }
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}
