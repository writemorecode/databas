#[derive(Debug)]
pub enum StorageError {
    IO(std::io::Error),
    InvalidPageId(u64),
    InvalidFileSize(u64),
}

pub(crate) type StorageResult<T> = Result<T, StorageError>;

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}
