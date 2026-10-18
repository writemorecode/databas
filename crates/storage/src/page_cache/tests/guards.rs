use std::{
    sync::{Arc, Barrier},
    thread,
};

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{PageCache, PageCacheError, disk_image, image, setup, setup_runtime, snapshot};

#[test]
fn readers_share_access_and_writers_exclude_only_their_own_page() -> TestCaseResult {
    let (_dir, cache, model) = setup(2, 2)?;
    let first = cache.fetch_page(0)?;
    let alias = cache.clone().fetch_page(0)?;
    let second = cache.fetch_page(1)?;
    cache.flush_all()?; // Clean pinned pages do not prevent a full flush.
    prop_assert!(
        matches!(cache.flush_page(0), Err(PageCacheError::PinnedPage { page_id: 0 })),
        "single-page flush accepted a pin"
    );
    {
        let read = first.read()?;
        let another_read = alias.read()?;
        prop_assert_eq!(read.page(), another_read.page());
        prop_assert!(
            matches!(
                alias.write(None),
                Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })
            ),
            "write accepted during shared reads"
        );
        second.write(None)?.page_mut().copy_from_slice(&image(99));
    }
    {
        let mut write = alias.write(None)?;
        write.page_mut().copy_from_slice(&image(42));
        prop_assert!(
            matches!(first.read(), Err(PageCacheError::PageImmutableBorrowConflict { page_id: 0 })),
            "read accepted during write"
        );
        prop_assert!(
            matches!(
                first.write(None),
                Err(PageCacheError::PageMutableBorrowConflict { page_id: 0 })
            ),
            "second writer accepted"
        );
        prop_assert_eq!(snapshot(&second)?, image(99));
    }
    prop_assert_eq!(snapshot(&first)?, image(42));
    prop_assert_ne!(snapshot(&first)?, model[0]);
    Ok(())
}

#[test]
fn pins_and_exclusive_borrows_are_shared_across_threads() -> TestCaseResult {
    let (dir, cache, _) = setup(1, 2)?;
    let pin = cache.fetch_page(0)?;
    let mut write = pin.write(None)?;
    write.page_mut().copy_from_slice(&image(73));
    let observer = cache.clone();
    let alias = thread::scope(|scope| {
        scope
            .spawn(move || -> Result<_, TestCaseError> {
                let alias = observer.fetch_page(0)?;
                prop_assert!(
                    matches!(alias.read(), Err(PageCacheError::PageImmutableBorrowConflict { .. })),
                    "read accepted across threads"
                );
                prop_assert!(
                    matches!(
                        alias.write(None),
                        Err(PageCacheError::PageMutableBorrowConflict { .. })
                    ),
                    "write accepted across threads"
                );
                prop_assert!(matches!(
                    observer.fetch_page(1),
                    Err(PageCacheError::NoEvictableFrame)
                ));
                Ok(alias)
            })
            .join()
            .map_err(|_panic| TestCaseError::fail("observer thread panicked"))?
    })?;
    drop(write);
    prop_assert_eq!(snapshot(&alias)?, image(73));
    drop(pin);
    prop_assert!(matches!(cache.fetch_page(1), Err(PageCacheError::NoEvictableFrame)));
    let client = cache.clone();
    drop(cache); // The transferred pin and another handle retain shared state.
    prop_assert_eq!(snapshot(&alias)?, image(73));
    drop(alias);
    // The final pin drop must not itself flush dirty data.
    prop_assert_ne!(disk_image(&dir.path().join("cache.db"), 0)?, image(73).to_vec());
    drop(client.fetch_page(1)?);
    prop_assert_eq!(disk_image(&dir.path().join("cache.db"), 0)?, image(73).to_vec());
    Ok(())
}

#[test]
fn concurrent_fetches_share_bytes_and_retain_every_client_pin() -> TestCaseResult {
    let (dir, cache, _) = setup(1, 2)?;
    let start = Arc::new(Barrier::new(9));
    let mut pins = thread::scope(|scope| -> Result<Vec<_>, TestCaseError> {
        let mut tasks = vec![];
        for _ in 0..8 {
            let start = start.clone();
            let client = cache.clone();
            tasks.push(scope.spawn(move || {
                start.wait();
                client.fetch_page(0)
            }));
        }
        start.wait();
        tasks
            .into_iter()
            .map(|task| {
                Ok(task.join().map_err(|_panic| TestCaseError::fail("fetch thread panicked"))??)
            })
            .collect()
    })?;
    let survivor = pins.pop().ok_or_else(|| TestCaseError::fail("missing client pin"))?;
    survivor.write(None)?.page_mut().copy_from_slice(&image(67));
    for pin in &pins {
        prop_assert_eq!(snapshot(pin)?, image(67));
    }
    drop(pins);
    prop_assert!(matches!(cache.fetch_page(1), Err(PageCacheError::NoEvictableFrame)));
    drop(survivor);
    drop(cache.fetch_page(1)?);
    prop_assert_eq!(disk_image(&dir.path().join("cache.db"), 0)?, image(67).to_vec());
    Ok(())
}

#[test]
fn invalid_cache_capacities_are_rejected() -> TestCaseResult {
    let (_dir, runtime, _) = setup_runtime(0, 0)?;
    prop_assert!(
        matches!(
            PageCache::new(runtime.clone(), 0),
            Err(PageCacheError::InvalidFrameCount { frame_count: 0 })
        ),
        "zero capacity accepted"
    );
    prop_assert!(
        matches!(
            PageCache::new(runtime, usize::MAX),
            Err(PageCacheError::FrameAllocationFailed { .. })
        ),
        "unrepresentable allocation accepted"
    );
    Ok(())
}
