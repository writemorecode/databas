//! Filesystem operations shared by database pages, WAL, and recovery.

use std::{
    fs, io,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[cfg(feature = "simulation")]
pub mod memory;

#[derive(Clone, Copy, Debug)]
pub enum OpenMode {
    Existing,
    Create,
    CreateNew,
    Truncate,
}

#[derive(Clone, Debug)]
pub enum FileSystem {
    Os,
    #[cfg(feature = "simulation")]
    Memory(memory::MemoryFs),
}

impl FileSystem {
    pub fn open(&self, path: &Path, mode: OpenMode) -> io::Result<File> {
        match self {
            Self::Os => fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(matches!(mode, OpenMode::Create | OpenMode::Truncate))
                .create_new(matches!(mode, OpenMode::CreateNew))
                .truncate(matches!(mode, OpenMode::Truncate))
                .open(path)
                .map(File::Os),
            #[cfg(feature = "simulation")]
            Self::Memory(fs) => fs.open(path, mode).map(File::Memory),
        }
    }

    pub fn exists(&self, path: &Path) -> io::Result<bool> {
        match self {
            Self::Os => path.try_exists(),
            #[cfg(feature = "simulation")]
            Self::Memory(fs) => fs.exists(path),
        }
    }

    pub fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        match self {
            Self::Os => fs::rename(from, to),
            #[cfg(feature = "simulation")]
            Self::Memory(fs) => fs.rename(from, to),
        }
    }

    pub fn sync_parent(&self, path: &Path) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Os => fs::File::open(parent(path))?.sync_all(),
            #[cfg(not(unix))]
            Self::Os => Ok(()),
            #[cfg(feature = "simulation")]
            Self::Memory(fs) => fs.sync_parent(parent(path)),
        }
    }
}

#[cfg(any(unix, feature = "simulation"))]
fn parent(path: &Path) -> &Path {
    path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."))
}

#[derive(Debug)]
pub enum File {
    Os(fs::File),
    #[cfg(feature = "simulation")]
    Memory(memory::MemoryFile),
}

impl File {
    pub fn len(&self) -> io::Result<u64> {
        match self {
            Self::Os(file) => Ok(file.metadata()?.len()),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.len(),
        }
    }

    /// Bind repair files to a filesystem object, not just its pathname.
    pub fn identity(&self) -> io::Result<[u8; 16]> {
        match self {
            #[cfg(unix)]
            Self::Os(file) => {
                use std::os::unix::fs::MetadataExt;
                let metadata = file.metadata()?;
                let mut identity = [0; 16];
                identity[..8].copy_from_slice(&metadata.dev().to_le_bytes());
                identity[8..].copy_from_slice(&metadata.ino().to_le_bytes());
                Ok(identity)
            }
            #[cfg(not(unix))]
            Self::Os(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "doublewrite requires stable filesystem identity",
            )),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => Ok(file.identity()),
        }
    }

    pub fn try_lock(&mut self) -> io::Result<()> {
        match self {
            Self::Os(file) => file.try_lock().map_err(io::Error::from),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.try_lock(),
        }
    }

    pub fn set_len(&self, len: u64) -> io::Result<()> {
        match self {
            Self::Os(file) => file.set_len(len),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.set_len(len),
        }
    }

    pub fn sync_all(&self) -> io::Result<()> {
        match self {
            Self::Os(file) => file.sync_all(),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.sync_all(),
        }
    }
}

impl Read for File {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Os(file) => file.read(bytes),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.read(bytes),
        }
    }
}

impl Write for File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Os(file) => file.write(bytes),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Os(file) => file.flush(),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.flush(),
        }
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Os(file) => file.seek(pos),
            #[cfg(feature = "simulation")]
            Self::Memory(file) => file.seek(pos),
        }
    }
}
