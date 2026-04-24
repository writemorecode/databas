use crate::{
    error::PageStoreResult,
    {PAGE_SIZE, PageId},
};

/// Backend abstraction for fixed-size page allocation and I/O.
pub trait PageStore {
    /// Allocates a new page and returns its page ID.
    fn new_page(&mut self) -> PageStoreResult<PageId>;

    /// Reads page `page_id` into `buf`.
    fn read_page(&mut self, page_id: PageId, buf: &mut [u8; PAGE_SIZE]) -> PageStoreResult<()>;

    /// Writes `buf` to page `page_id`.
    fn write_page(&mut self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> PageStoreResult<()>;

    /// Optional durability barrier.
    fn sync(&mut self) -> PageStoreResult<()> {
        Ok(())
    }
}
