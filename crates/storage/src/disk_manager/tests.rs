//! The oracle is a sequence of page images, independent of offsets and cursors.
use std::fs;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{DiskManager, DiskManagerError};
use crate::{PAGE_SIZE, PageId};

pub use io::{Control, Effect, FaultFile, Operation};
mod faults;
mod io;
mod limits;

fn image(pattern: &[u8]) -> [u8; PAGE_SIZE] {
    std::array::from_fn(|i| pattern[i % pattern.len()])
}

#[derive(Clone, Debug)]
enum Op {
    Allocate,
    Ensure(u8),
    Read(u8),
    Write(u8, Vec<u8>),
    Sync,
    Reopen,
}

fn operation() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => Just(Op::Allocate),
        2 => (0u8..16).prop_map(Op::Ensure),
        3 => any::<u8>().prop_map(Op::Read),
        4 => (any::<u8>(), prop::collection::vec(any::<u8>(), 1..=32)).prop_map(|(id, bytes)| Op::Write(id, bytes)),
        1 => Just(Op::Sync),
        2 => Just(Op::Reopen),
    ]
}

proptest! {
    #[test]
    fn page_operations_match_a_byte_image_model(ops in prop::collection::vec(operation(), 1..=40)) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pages");
        let mut disk = DiskManager::new(&path)?;
        let mut model: Vec<[u8; PAGE_SIZE]> = vec![];
        for op in ops {
            match op {
                Op::Allocate => {
                    prop_assert_eq!(disk.new_page()?, model.len() as PageId);
                    model.push([0; PAGE_SIZE]);
                }
                Op::Ensure(id) => {
                    disk.ensure_page_exists(u64::from(id))?;
                    model.resize(model.len().max(usize::from(id) + 1), [0; PAGE_SIZE]);
                }
                Op::Read(id) | Op::Write(id, _) => {
                    let id = if id == u8::MAX { PageId::MAX } else {
                        u64::from(id) % (model.len() as u64 + 2)
                    };
                    let result = if let Op::Write(_, ref bytes) = op {
                        disk.write_page(id, &image(bytes))
                    } else {
                        let mut bytes = [0xa5; PAGE_SIZE];
                        let result = disk.read_page(id, &mut bytes);
                        if id < model.len() as u64 {
                            prop_assert_eq!(bytes, model[usize::try_from(id)?]);
                        }
                        result
                    };
                    if id < model.len() as u64 {
                        result?;
                        if let Op::Write(_, bytes) = op { model[usize::try_from(id)?] = image(&bytes); }
                    } else {
                        prop_assert!(matches!(result, Err(DiskManagerError::InvalidPageId { page_id }) if page_id == id), "wrong missing-page error");
                    }
                }
                Op::Sync => disk.sync()?,
                Op::Reopen => {
                    drop(disk);
                    disk = DiskManager::open_existing(&path)?;
                }
            }
            prop_assert_eq!(disk.page_count(), model.len() as u64);
            // Observe independently: a matching offset bug in read and write
            // must not let the implementation agree with itself.
            prop_assert_eq!(fs::read(&path)?, model.concat());
        }
        drop(disk);
        let mut disk = DiskManager::open_existing(&path)?;
        prop_assert_eq!(disk.page_count(), model.len() as u64);
        for (id, expected) in model.iter().enumerate() {
            let mut actual = [0; PAGE_SIZE];
            disk.read_page(id as u64, &mut actual)?;
            prop_assert_eq!(&actual, expected);
        }
    }

    #[test]
    fn partial_pages_are_rejected_without_rewriting(full_pages in 0usize..=3, tail in 1usize..PAGE_SIZE) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pages");
        let bytes = vec![0xa7; full_pages * PAGE_SIZE + tail];
        fs::write(&path, &bytes)?;
        for result in [DiskManager::new(&path), DiskManager::open_existing(&path)] {
            prop_assert!(matches!(result, Err(DiskManagerError::InvalidFileSize { size }) if size == bytes.len() as u64), "wrong file-size error");
            prop_assert_eq!(&fs::read(&path)?, &bytes);
        }
    }
}

#[test]
fn opening_modes_preserve_existing_files_and_missing_paths() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pages");
    prop_assert!(DiskManager::open_existing(&path).is_err());
    prop_assert!(!path.exists());
    let mut disk = DiskManager::create_new(&path)?;
    disk.new_page()?;
    let expected = image(&[1, 3, 5, 7]);
    disk.write_page(0, &expected)?;
    drop(disk);
    prop_assert!(
        matches!(DiskManager::create_new(&path), Err(DiskManagerError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists)
    );
    prop_assert_eq!(DiskManager::new(&path)?.page_count(), 1);
    prop_assert_eq!(fs::read(path)?, expected);
    Ok(())
}
