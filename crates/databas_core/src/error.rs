use crate::types::RowId;
use std::{error::Error as StdError, fmt};

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

#[derive(Debug)]
pub(crate) enum TablePageError {
    InvalidPageType(u8),
    CorruptPage(&'static str),
    CorruptCell { slot_index: u16 },
    DuplicateRowId(RowId),
    RowIdNotFound(RowId),
    CellTooLarge { len: usize },
    PageFull { needed: usize, available: usize },
}

pub(crate) type TablePageResult<T> = Result<T, TablePageError>;

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

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IO(err) => write!(f, "I/O error: {err}"),
            Self::InvalidPageId(page_id) => write!(f, "invalid page id: {page_id}"),
            Self::InvalidFileSize(size) => {
                write!(f, "invalid file size (not multiple of page size): {size}")
            }
        }
    }
}

impl StdError for StorageError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::IO(err) => Some(err),
            Self::InvalidPageId(_) | Self::InvalidFileSize(_) => None,
        }
    }
}

impl fmt::Display for PageCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "storage error: {err}"),
            Self::NoEvictableFrame => write!(f, "no evictable frame available"),
            Self::PinnedPage(page_id) => write!(f, "page {page_id} is pinned"),
            Self::InvalidFrameCount(frame_count) => {
                write!(f, "invalid frame count: {frame_count}")
            }
            Self::CorruptPageTableEntry { page_id, frame_id, frame_count } => write!(
                f,
                "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
            ),
        }
    }
}

impl StdError for PageCacheError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Storage(err) => Some(err),
            Self::NoEvictableFrame
            | Self::PinnedPage(_)
            | Self::InvalidFrameCount(_)
            | Self::CorruptPageTableEntry { .. } => None,
        }
    }
}

impl fmt::Display for TablePageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPageType(page_type) => write!(f, "invalid page type: {page_type}"),
            Self::CorruptPage(reason) => write!(f, "corrupt page: {reason}"),
            Self::CorruptCell { slot_index } => {
                write!(f, "corrupt cell at slot index {slot_index}")
            }
            Self::DuplicateRowId(row_id) => write!(f, "duplicate row id: {row_id}"),
            Self::RowIdNotFound(row_id) => write!(f, "row id not found: {row_id}"),
            Self::CellTooLarge { len } => write!(f, "cell too large: {len} bytes"),
            Self::PageFull { needed, available } => {
                write!(f, "page full: need {needed} bytes, only {available} bytes available")
            }
        }
    }
}

impl StdError for TablePageError {}
