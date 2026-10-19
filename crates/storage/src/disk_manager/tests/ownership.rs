use std::fs;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{DiskManager, DiskManagerError, image};
use crate::PAGE_SIZE;

proptest! {
    #[test]
    fn acquiring_ownership_observes_growth_since_the_file_was_opened(initial in 0usize..=3, added in 1usize..=4) {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pages");
        let mut expected: Vec<_> = (0..initial).map(|i| image(&[u8::try_from(i).unwrap_or(0), 81])).collect();
        fs::write(&path, expected.concat())?;
        // Explicitly schedule another owner's entire lifetime between opening
        // the descriptor and acquiring ownership. No timing or sleeps needed.
        let mut delayed = DiskManager::open_existing(&path)?;
        let mut owner = DiskManager::open_existing(&path)?;
        owner.lock_exclusive()?;
        for i in 0..added {
            let id = owner.new_page()?;
            let bytes = image(&[u8::try_from(i)?, 97]);
            owner.write_page(id, &bytes)?;
            expected.push(bytes);
        }
        owner.sync()?;
        drop(owner);
        delayed.lock_exclusive()?;
        prop_assert_eq!(delayed.page_count(), expected.len() as u64);
        prop_assert_eq!(delayed.new_page()?, expected.len() as u64);
        expected.push([0; PAGE_SIZE]);
        prop_assert_eq!(fs::read(&path)?, expected.concat());
        // Locking the same handle again is harmless and does not release it.
        delayed.lock_exclusive()?;
        let mut contender = DiskManager::open_existing(&path)?;
        prop_assert!(contender.lock_exclusive().is_err());
    }
}

#[test]
fn acquiring_ownership_revalidates_file_alignment() -> TestCaseResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pages");
    fs::write(&path, [])?;
    let mut delayed = DiskManager::open_existing(&path)?;
    let malformed = vec![0xa5; PAGE_SIZE + 1];
    fs::write(&path, &malformed)?;
    prop_assert!(
        matches!(delayed.lock_exclusive(), Err(DiskManagerError::InvalidFileSize { .. })),
        "accepted a partial page after acquiring ownership"
    );
    prop_assert_eq!(fs::read(&path)?, malformed);
    drop(delayed);
    fs::write(&path, [0; PAGE_SIZE])?;
    DiskManager::open_existing(&path)?.lock_exclusive()?;
    Ok(())
}
