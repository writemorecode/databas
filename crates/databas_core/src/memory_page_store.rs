use crate::{
    error::{PageStoreError, PageStoreResult},
    page_store::PageStore,
    {PAGE_SIZE, PageId},
};

/// In-memory page store for tests and volatile database usage.
pub(crate) struct MemoryPageStore {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl MemoryPageStore {
    pub(crate) fn new() -> Self {
        Self { pages: Vec::new() }
    }
}

impl Default for MemoryPageStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PageStore for MemoryPageStore {
    fn new_page(&mut self) -> PageStoreResult<PageId> {
        let page_id = self.pages.len() as PageId;
        self.pages.push([0u8; PAGE_SIZE]);
        Ok(page_id)
    }

    fn read_page(&mut self, page_id: PageId, buf: &mut [u8; PAGE_SIZE]) -> PageStoreResult<()> {
        let page =
            self.pages.get(page_id as usize).ok_or(PageStoreError::InvalidPageId { page_id })?;
        *buf = *page;
        Ok(())
    }

    fn write_page(&mut self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> PageStoreResult<()> {
        let page = self
            .pages
            .get_mut(page_id as usize)
            .ok_or(PageStoreError::InvalidPageId { page_id })?;
        *page = *buf;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_allocates_sequential_ids() {
        let mut store = MemoryPageStore::new();
        assert_eq!(store.new_page().unwrap(), 0);
        assert_eq!(store.new_page().unwrap(), 1);
        assert_eq!(store.new_page().unwrap(), 2);
    }

    #[test]
    fn newly_allocated_pages_are_zero_initialized() {
        let mut store = MemoryPageStore::new();
        let page_id = store.new_page().unwrap();
        let mut page = [1u8; PAGE_SIZE];
        store.read_page(page_id, &mut page).unwrap();
        assert_eq!(page, [0u8; PAGE_SIZE]);
    }

    #[test]
    fn read_and_write_round_trip() {
        let mut store = MemoryPageStore::new();
        let page_id = store.new_page().unwrap();
        let mut write = [0u8; PAGE_SIZE];
        write[0] = 7;
        write[PAGE_SIZE - 1] = 9;
        store.write_page(page_id, &write).unwrap();

        let mut read = [0u8; PAGE_SIZE];
        store.read_page(page_id, &mut read).unwrap();
        assert_eq!(read, write);
    }

    #[test]
    fn invalid_page_id_returns_error() {
        let mut store = MemoryPageStore::new();
        let mut read = [0u8; PAGE_SIZE];
        let write = [0u8; PAGE_SIZE];

        let read_result = store.read_page(99, &mut read);
        assert!(matches!(read_result, Err(PageStoreError::InvalidPageId { page_id: 99 })));

        let write_result = store.write_page(99, &write);
        assert!(matches!(write_result, Err(PageStoreError::InvalidPageId { page_id: 99 })));
    }
}
