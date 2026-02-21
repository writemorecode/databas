#[derive(Debug)]
pub enum StorageError {
    IO(std::io::Error),
    InvalidPageId(u64),
    InvalidFileSize(u64),
}

pub(crate) type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug)]
pub(crate) enum PageCacheError {
    Storage(StorageError),
    NoEvictableFrame,
    PinnedPage(u64),
    InvalidFrameCount(usize),
    CorruptPageTableEntry { page_id: u64, frame_id: usize, frame_count: usize },
}

pub(crate) type PageCacheResult<T> = Result<T, PageCacheError>;

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<StorageError> for PageCacheError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}
