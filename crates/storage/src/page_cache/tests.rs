//! The oracle knows page identities, bytes, and outstanding client pins. It
//! does not track residency, frame IDs, dirty bits, or replacement decisions.

use std::{collections::BTreeSet, fs, path::Path, sync::Arc};

use proptest::{prelude::*, test_runner::TestCaseResult};
use tempfile::TempDir;

use super::{PageCache, PageCacheError, PinGuard};
use crate::{PAGE_SIZE, PageId, disk_manager::DiskManager, storage_runtime::StorageRuntime};

fn image(seed: u8) -> [u8; PAGE_SIZE] {
    std::array::from_fn(|i| seed.wrapping_add(u8::try_from(i % 251).unwrap_or(0)))
}

fn setup(
    capacity: usize,
    pages: usize,
) -> Result<(TempDir, PageCache, Vec<[u8; PAGE_SIZE]>), TestCaseError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("cache.db");
    let mut disk = DiskManager::new(&path)?;
    let mut model = vec![];
    for seed in 0..pages {
        let page_id = disk.new_page()?;
        let bytes = image(u8::try_from(seed)?);
        disk.write_page(page_id, &bytes)?;
        model.push(bytes);
    }
    let runtime = Arc::new(StorageRuntime::new(path, disk)?);
    Ok((dir, PageCache::new(runtime, capacity)?, model))
}

fn snapshot(pin: &PinGuard) -> super::PageCacheResult<[u8; PAGE_SIZE]> {
    Ok(*pin.read()?.page())
}

fn disk_image(path: &Path, page_id: PageId) -> Result<Vec<u8>, TestCaseError> {
    let bytes = fs::read(path)?;
    let start = usize::try_from(page_id)? * PAGE_SIZE;
    Ok(bytes[start..start + PAGE_SIZE].to_vec())
}

fn check_disk(path: &Path, expected: &[[u8; PAGE_SIZE]]) -> TestCaseResult {
    prop_assert_eq!(fs::read(path)?, expected.concat());
    Ok(())
}

#[derive(Debug, Clone)]
enum Op {
    Pin(u8, u8),
    Release(u8),
    Write(u8, usize, Vec<u8>),
    Allocate(u8),
    Flush(u8),
    FlushAll,
    Checkpoint,
    FetchMissing,
}

fn operation() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (any::<u8>(), any::<u8>()).prop_map(|(slot, page)| Op::Pin(slot, page)),
        2 => any::<u8>().prop_map(Op::Release),
        5 => (any::<u8>(), 0usize..PAGE_SIZE, prop::collection::vec(any::<u8>(), 0..=64)).prop_map(|(slot, offset, data)| Op::Write(slot, offset, data)),
        1 => any::<u8>().prop_map(Op::Allocate),
        2 => any::<u8>().prop_map(Op::Flush),
        1 => Just(Op::FlushAll),
        1 => Just(Op::Checkpoint),
        1 => Just(Op::FetchMissing),
    ]
}

proptest! {
    #[test]
    fn shared_cache_operations_preserve_page_identity_bytes_and_pins(
        capacity in 1usize..=4, ops in prop::collection::vec(operation(), 1..=96),
    ) {
        let (dir, cache, mut model) = setup(capacity, 6)?;
        let path = dir.path().join("cache.db");
        let clients = [cache.clone(), cache.clone()];
        let mut pins: [Option<(PageId, PinGuard)>; 8] = std::array::from_fn(|_| None);
        for (step, op) in ops.into_iter().enumerate() {
            let client = &clients[step % clients.len()];
            match op {
                Op::Pin(slot, _) | Op::Allocate(slot) => {
                    let slot = usize::from(slot) % pins.len();
                    pins[slot] = None;
                    let pinned: BTreeSet<_> = pins.iter().flatten().map(|(id, _)| *id).collect();
                    let allocating = matches!(op, Op::Allocate(_));
                    let page_id = if let Op::Pin(_, page) = op {
                        u64::from(page) % u64::try_from(model.len())?
                    } else { model.len() as u64 };
                    let bytes_before = fs::metadata(&path)?.len();
                    let result = if allocating { client.new_page(None) } else {
                        client.fetch_page(page_id).map(|pin| (page_id, pin))
                    };
                    if pinned.len() == capacity && !pinned.contains(&page_id) {
                        prop_assert!(matches!(result, Err(PageCacheError::NoEvictableFrame)), "unexpected result for full pinned cache");
                        prop_assert_eq!(fs::metadata(&path)?.len(), bytes_before);
                    } else {
                        let (actual_id, pin) = result?;
                        prop_assert_eq!(actual_id, page_id);
                        if allocating { model.push([0; PAGE_SIZE]); }
                        prop_assert_eq!(snapshot(&pin)?, model[usize::try_from(page_id)?]);
                        pins[slot] = Some((page_id, pin));
                    }
                }
                Op::Release(slot) => {
                    let before = fs::read(&path)?;
                    let slot = usize::from(slot) % pins.len();
                    pins[slot] = None;
                    prop_assert_eq!(fs::read(&path)?, before);
                }
                Op::Write(slot, offset, data) => {
                    if let Some((page_id, pin)) = &pins[usize::from(slot) % pins.len()] {
                        let before = fs::read(&path)?;
                        let len = data.len().min(PAGE_SIZE - offset);
                        {
                            let mut write = pin.write(None)?;
                            write.page_mut()[offset..offset + len].copy_from_slice(&data[..len]);
                        }
                        model[usize::try_from(*page_id)?][offset..offset + len].copy_from_slice(&data[..len]);
                        prop_assert_eq!(fs::read(&path)?, before);
                    }
                }
                Op::Flush(page) => {
                    let page_id = u64::from(page) % u64::try_from(model.len())?;
                    let result = client.flush_page(page_id);
                    if pins.iter().flatten().any(|(id, _)| *id == page_id) {
                        prop_assert!(matches!(result, Err(PageCacheError::PinnedPage { page_id: id }) if id == page_id), "pinned flush was accepted");
                    } else {
                        result?;
                        prop_assert_eq!(disk_image(&path, page_id)?, model[usize::try_from(page_id)?].to_vec());
                    }
                }
                Op::FlushAll => match client.flush_all() {
                    Ok(()) => check_disk(&path, &model)?,
                    Err(PageCacheError::PinnedPage { page_id }) => {
                        prop_assert!(pins.iter().flatten().any(|(id, _)| *id == page_id));
                    }
                    Err(error) => return Err(error.into()),
                },
                Op::Checkpoint => match client.flush_checkpoint()? {
                    None => check_disk(&path, &model)?,
                    Some(lsn) => {
                        prop_assert_eq!(lsn, 0);
                        prop_assert!(pins.iter().any(Option::is_some));
                    }
                },
                Op::FetchMissing => {
                    let size = fs::metadata(&path)?.len();
                    prop_assert!(client.fetch_page(PageId::MAX).is_err());
                    prop_assert_eq!(fs::metadata(&path)?.len(), size);
                }
            }
            for (page_id, pin) in pins.iter().flatten() {
                prop_assert_eq!(snapshot(pin)?, model[usize::try_from(*page_id)?]);
            }
        }
        drop(pins);
        cache.flush_all()?;
        check_disk(&path, &model)?;
        drop(clients);
        drop(cache);
        let runtime = Arc::new(StorageRuntime::new(path.clone(), DiskManager::open_existing(&path)?)?);
        let reopened = PageCache::new(runtime, capacity)?;
        for (id, bytes) in model.iter().enumerate() {
            prop_assert_eq!(snapshot(&reopened.fetch_page(id as u64)?)?, *bytes);
        }
    }
}
