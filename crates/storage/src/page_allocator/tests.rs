use std::io::ErrorKind;

use super::{BITMAP_OFFSET, PAGES_PER_EXTENT, PageAllocator};
use crate::{
    PAGE_SIZE,
    database_header::DatabaseHeader,
    disk_manager::{
        DiskManager,
        tests::{Effect, Operation},
    },
};

fn full_first_extent(path: &std::path::Path) -> DiskManager {
    let mut disk = DiskManager::create_new(path).unwrap();
    disk.ensure_page_exists(PAGES_PER_EXTENT - 1).unwrap();
    let mut header = DatabaseHeader::encode_page();
    header[BITMAP_OFFSET..].fill(255);
    disk.write_page(0, &header).unwrap();
    disk.sync().unwrap();
    disk
}

#[test]
fn allocation_crosses_bitmap_boundaries_and_reuses_holes_in_each_extent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut disk = full_first_extent(&path);
    let mut allocator = PageAllocator::load(&mut disk).unwrap();
    let first = allocator.candidate(&mut disk).unwrap();
    assert_eq!(first, PAGES_PER_EXTENT + 1);
    allocator.set_allocated(first, true);
    let second = allocator.candidate(&mut disk).unwrap();
    allocator.set_allocated(second, true);
    allocator.set_allocated(PAGES_PER_EXTENT - 1, false);
    allocator.set_allocated(first, false);
    allocator.flush(&mut disk).unwrap();
    drop(disk);
    let mut disk = DiskManager::open_existing(&path).unwrap();
    let mut allocator = PageAllocator::load(&mut disk).unwrap();
    for expected in [PAGES_PER_EXTENT - 1, first, second + 1] {
        let actual = allocator.candidate(&mut disk).unwrap();
        assert_eq!(actual, expected);
        assert!(!PageAllocator::is_metadata(actual));
        allocator.set_allocated(actual, true);
    }
    allocator.flush(&mut disk).unwrap();
    let mut metadata = [0; PAGE_SIZE];
    disk.read_page(PAGES_PER_EXTENT, &mut metadata).unwrap();
    assert_eq!(&metadata[..8], b"DBBITMAP");
    assert_eq!(&metadata[8..16], &PAGES_PER_EXTENT.to_le_bytes());
    assert_eq!(metadata[BITMAP_OFFSET] & 15, 15);
}

#[test]
fn interrupted_extent_initialization_can_be_retried_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    for (index, operation, after) in [
        (0, Operation::Resize, 0),
        (1, Operation::Sync, 0),
        (2, Operation::Write, 0),
        (3, Operation::Write, 9),
        (4, Operation::Write, PAGE_SIZE - 1),
        (5, Operation::Sync, 1),
    ] {
        let path = dir.path().join(format!("db-{index}"));
        let mut disk = full_first_extent(&path);
        let mut allocator = PageAllocator::load(&mut disk).unwrap();
        let faults = disk.faults();
        faults.arm(operation, after, Effect::Error(ErrorKind::Other));
        assert!(allocator.candidate(&mut disk).is_err());
        assert!(faults.consumed());
        drop(disk);
        let mut disk = DiskManager::open_existing(&path).unwrap();
        let mut allocator = PageAllocator::load(&mut disk).unwrap();
        assert_eq!(allocator.candidate(&mut disk).unwrap(), PAGES_PER_EXTENT + 1);
    }
}

#[test]
fn malformed_bitmap_metadata_is_rejected_when_the_extent_contains_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut disk = full_first_extent(&path);
    let mut allocator = PageAllocator::load(&mut disk).unwrap();
    let id = allocator.candidate(&mut disk).unwrap();
    allocator.set_allocated(id, true);
    allocator.flush(&mut disk).unwrap();
    let mut valid = [0; PAGE_SIZE];
    disk.read_page(PAGES_PER_EXTENT, &mut valid).unwrap();
    for (offset, bit) in [(0, 1), (8, 1), (16, 1), (17, 128)] {
        let mut damaged = valid;
        damaged[offset] ^= bit;
        disk.write_page(PAGES_PER_EXTENT, &damaged).unwrap();
        assert!(
            PageAllocator::load(&mut disk)
                .and_then(|allocator| allocator.validate_bounds(disk.page_count()))
                .is_err()
        );
    }
}
