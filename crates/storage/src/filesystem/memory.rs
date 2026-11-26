//! A simulated filesystem with independent file and directory persistence.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use super::{OpenMode, parent};

const SECTOR_SIZE: usize = 512;
const MAX_FILE_LEN: usize = 512 * 1024 * 1024;

type FileId = u64;
type Sector = [u8; SECTOR_SIZE];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Open,
    Exists,
    Rename,
    SyncDirectory,
    Lock,
    Length,
    Resize,
    Read,
    Write,
    Seek,
    SyncFile,
}

#[derive(Clone, Copy, Debug)]
pub enum Effect {
    Short { operation: Operation, limit: usize },
    Error { operation: Operation, kind: io::ErrorKind, after: bool },
    PowerLoss { after: bool },
}

impl Effect {
    fn matches(self, op: Operation) -> bool {
        match self {
            Self::Short { operation, .. } | Self::Error { operation, .. } => operation == op,
            Self::PowerLoss { .. } => true,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Fault {
    /// Number of matching calls before the fault fires.
    pub skip: u64,
    pub effect: Effect,
}

#[derive(Clone, Copy, Debug)]
pub struct Hit {
    pub io: u64,
    pub operation: Operation,
    pub effect: Effect,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryFs(Arc<Mutex<State>>);

impl MemoryFs {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn arm(&self, fault: Fault) {
        self.state().fault = Some(fault);
    }

    pub fn armed(&self) -> bool {
        self.state().fault.is_some()
    }

    pub fn disarm(&self) {
        self.state().fault = None;
    }

    pub fn take_hit(&self) -> Option<Hit> {
        self.state().hit.take()
    }

    pub fn io_count(&self) -> u64 {
        self.state().io
    }

    /// Engine restart keeps the OS cache unless the device lost power.
    /// Freeze the original device before dropping buffered engine handles.
    pub fn restart(&self, power_loss: bool) -> Self {
        let mut state = self.state();
        state.powered_off = true;
        let live_names = if power_loss { &state.durable_names } else { &state.live_names };
        let ids: BTreeSet<_> = live_names.values().chain(state.durable_names.values()).collect();
        let files = ids
            .into_iter()
            .map(|id| {
                let source = &state.files[id];
                let inode = if power_loss {
                    Inode::from_durable(source.durable.clone())
                } else {
                    Inode {
                        live: source.live.clone(),
                        durable: source.durable.clone(),
                        dirty: source.dirty.clone(),
                        ..Inode::default()
                    }
                };
                (*id, inode)
            })
            .collect();
        Self(Arc::new(Mutex::new(State {
            live_names: live_names.clone(),
            durable_names: state.durable_names.clone(),
            files,
            next_id: state.next_id,
            io: state.io,
            ..State::default()
        })))
    }

    /// Write back a bounded run of dirty sectors from one file. Length may
    /// become durable while some data sectors are still pending.
    pub fn writeback(&self, choice: u64, sectors: usize) {
        let mut state = self.state();
        let mut dirty = state
            .files
            .values_mut()
            .filter(|file| !file.dirty.is_empty() || file.live.len != file.durable.len);
        let count = dirty.by_ref().count();
        if count == 0 {
            return;
        }
        let index = bounded_index(choice, count);
        let Some(file) = state
            .files
            .values_mut()
            .filter(|file| !file.dirty.is_empty() || file.live.len != file.durable.len)
            .nth(index)
        else {
            return;
        };
        file.durable.resize(file.live.len);
        let start = if file.dirty.is_empty() {
            0
        } else {
            bounded_index(choice.rotate_left(32), file.dirty.len())
        };
        let selected: Vec<_> = file.dirty.iter().skip(start).take(sectors).copied().collect();
        for sector in selected {
            file.persist_sector(sector);
        }
    }

    /// Background directory writeback can publish the live namespace before
    /// an explicit directory sync. All simulated renames are within one directory.
    pub fn writeback_directory(&self, path: &Path) {
        self.state().persist_directory(parent(path));
    }

    pub fn open(&self, path: &Path, mode: OpenMode) -> io::Result<MemoryFile> {
        self.state().call(Operation::Open, |state, _| {
            let id = match (state.live_names.get(path).copied(), mode) {
                (Some(_), OpenMode::CreateNew) => return Err(io::ErrorKind::AlreadyExists.into()),
                (None, OpenMode::Existing) => return Err(io::ErrorKind::NotFound.into()),
                (Some(id), _) => id,
                (None, _) => {
                    state.next_id += 1;
                    let id = state.next_id;
                    state.live_names.insert(path.to_owned(), id);
                    state.files.insert(id, Inode::default());
                    id
                }
            };
            let file = state.inode(id)?;
            if matches!(mode, OpenMode::Truncate) {
                file.resize(0);
            }
            file.handles += 1;
            Ok(MemoryFile { fs: self.clone(), id, pos: 0, locked: false })
        })
    }

    pub fn exists(&self, path: &Path) -> io::Result<bool> {
        self.state().call(Operation::Exists, |state, _| Ok(state.live_names.contains_key(path)))
    }

    pub fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.state().call(Operation::Rename, |state, _| {
            let id = state.live_names.remove(from).ok_or(io::ErrorKind::NotFound)?;
            state.live_names.insert(to.to_owned(), id);
            state.collect_unlinked();
            Ok(())
        })
    }

    pub fn sync_parent(&self, directory: &Path) -> io::Result<()> {
        self.state().call(Operation::SyncDirectory, |state, _| {
            state.persist_directory(directory);
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
struct State {
    live_names: BTreeMap<PathBuf, FileId>,
    durable_names: BTreeMap<PathBuf, FileId>,
    files: BTreeMap<FileId, Inode>,
    next_id: FileId,
    io: u64,
    powered_off: bool,
    fault: Option<Fault>,
    hit: Option<Hit>,
}

impl State {
    fn call<T>(
        &mut self,
        operation: Operation,
        action: impl FnOnce(&mut Self, usize) -> io::Result<T>,
    ) -> io::Result<T> {
        if self.powered_off {
            return Err(io::Error::other("simulated device is powered off"));
        }
        self.io += 1;
        let effect = self.fault.as_mut().and_then(|fault| {
            if !fault.effect.matches(operation) {
                return None;
            }
            if fault.skip > 0 {
                fault.skip -= 1;
                return None;
            }
            Some(fault.effect)
        });
        let Some(effect) = effect else {
            return action(self, usize::MAX);
        };
        self.fault = None;
        self.hit = Some(Hit { io: self.io, operation, effect });
        match effect {
            Effect::Short { limit, .. } => action(self, limit),
            Effect::Error { kind, after, .. } => {
                if after {
                    action(self, usize::MAX)?;
                }
                Err(kind.into())
            }
            Effect::PowerLoss { after } => {
                // Run the syscall before losing power only when the cut is on
                // its exit boundary. Return its result so RAII handles are
                // dropped outside the device latch, even if buffered writes follow.
                if after {
                    let result = action(self, usize::MAX);
                    self.powered_off = true;
                    result
                } else {
                    self.powered_off = true;
                    Err(io::Error::other("simulated power loss"))
                }
            }
        }
    }

    fn inode(&mut self, id: FileId) -> io::Result<&mut Inode> {
        self.files.get_mut(&id).ok_or_else(|| io::Error::other("simulated file is missing"))
    }

    fn persist_directory(&mut self, directory: &Path) {
        self.durable_names.retain(|path, _| parent(path) != directory);
        self.durable_names.extend(
            self.live_names
                .iter()
                .filter(|(path, _)| parent(path) == directory)
                .map(|(path, id)| (path.clone(), *id)),
        );
        self.collect_unlinked();
    }

    fn collect_unlinked(&mut self) {
        self.files.retain(|id, file| {
            file.handles != 0
                || self.live_names.values().any(|named| named == id)
                || self.durable_names.values().any(|named| named == id)
        });
    }
}

#[derive(Clone, Debug, Default)]
struct Image {
    len: usize,
    sectors: Vec<Option<Arc<Sector>>>,
}

impl Image {
    fn resize(&mut self, len: usize) {
        if len < self.len {
            self.sectors.truncate(len.div_ceil(SECTOR_SIZE));
            let tail = len % SECTOR_SIZE;
            if tail != 0
                && let Some(Some(sector)) = self.sectors.last_mut()
            {
                Arc::make_mut(sector)[tail..].fill(0);
            }
        }
        self.sectors.resize_with(len.div_ceil(SECTOR_SIZE), || None);
        self.len = len;
    }

    fn read(&self, offset: usize, bytes: &mut [u8]) {
        let mut pos = offset;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let start = pos % SECTOR_SIZE;
            let count = remaining.len().min(SECTOR_SIZE - start);
            let (output, rest) = remaining.split_at_mut(count);
            match &self.sectors[pos / SECTOR_SIZE] {
                Some(sector) => output.copy_from_slice(&sector[start..start + count]),
                None => output.fill(0),
            }
            pos += count;
            remaining = rest;
        }
    }
}

#[derive(Debug, Default)]
struct Inode {
    live: Image,
    durable: Image,
    dirty: BTreeSet<usize>,
    handles: usize,
    locked: bool,
}

impl Inode {
    fn from_durable(image: Image) -> Self {
        Self { live: image.clone(), durable: image, ..Self::default() }
    }

    fn resize(&mut self, len: usize) {
        if len < self.live.len {
            self.dirty.extend(len / SECTOR_SIZE..self.live.len.div_ceil(SECTOR_SIZE));
        }
        self.live.resize(len);
    }

    fn write(&mut self, offset: usize, bytes: &[u8]) {
        let end = offset + bytes.len();
        if end > self.live.len {
            self.live.resize(end);
        }
        let mut pos = offset;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let index = pos / SECTOR_SIZE;
            let start = pos % SECTOR_SIZE;
            let count = remaining.len().min(SECTOR_SIZE - start);
            let (input, rest) = remaining.split_at(count);
            let sector = self.live.sectors[index].get_or_insert_with(|| Arc::new([0; SECTOR_SIZE]));
            Arc::make_mut(sector)[start..start + count].copy_from_slice(input);
            self.dirty.insert(index);
            pos += count;
            remaining = rest;
        }
    }

    fn persist_sector(&mut self, sector: usize) {
        if let Some(target) = self.durable.sectors.get_mut(sector) {
            *target = self.live.sectors[sector].clone();
        }
        self.dirty.remove(&sector);
    }

    fn sync(&mut self) {
        self.durable.resize(self.live.len);
        for sector in std::mem::take(&mut self.dirty) {
            if let Some(target) = self.durable.sectors.get_mut(sector) {
                *target = self.live.sectors[sector].clone();
            }
        }
    }
}

#[derive(Debug)]
pub struct MemoryFile {
    fs: MemoryFs,
    id: FileId,
    pos: u64,
    locked: bool,
}

impl MemoryFile {
    pub fn identity(&self) -> [u8; 16] {
        let mut identity = [0; 16];
        identity[8..].copy_from_slice(&self.id.to_le_bytes());
        identity
    }

    pub fn len(&self) -> io::Result<u64> {
        self.fs
            .state()
            .call(Operation::Length, |state, _| Ok(state.inode(self.id)?.live.len as u64))
    }

    pub fn try_lock(&mut self) -> io::Result<()> {
        let mut state = self.fs.state();
        state.call(Operation::Lock, |state, _| {
            let file = state.inode(self.id)?;
            if file.locked && !self.locked {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            file.locked = true;
            self.locked = true;
            Ok(())
        })
    }

    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.fs.state().call(Operation::Resize, |state, _| {
            let len = file_offset(len)?;
            state.inode(self.id)?.resize(len);
            Ok(())
        })
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.fs.state().call(Operation::SyncFile, |state, _| {
            state.inode(self.id)?.sync();
            Ok(())
        })
    }
}

impl Read for MemoryFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.fs.state().call(Operation::Read, |state, limit| {
            let image = &state.inode(self.id)?.live;
            if self.pos >= image.len as u64 {
                return Ok(0);
            }
            let start = usize::try_from(self.pos).map_err(io::Error::other)?;
            let count = bytes.len().min(limit).min(image.len - start);
            image.read(start, &mut bytes[..count]);
            self.pos += count as u64;
            Ok(count)
        })
    }
}

impl Write for MemoryFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.fs.state().call(Operation::Write, |state, limit| {
            let count = bytes.len().min(limit);
            if count > 0 {
                let start = file_offset(self.pos)?;
                file_offset(self.pos.saturating_add(count as u64))?;
                state.inode(self.id)?.write(start, &bytes[..count]);
                self.pos += count as u64;
            }
            Ok(count)
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for MemoryFile {
    fn seek(&mut self, seek: SeekFrom) -> io::Result<u64> {
        self.fs.state().call(Operation::Seek, |state, _| {
            self.pos = match seek {
                SeekFrom::Start(position) => Some(position),
                SeekFrom::Current(offset) => self.pos.checked_add_signed(offset),
                SeekFrom::End(offset) => {
                    (state.inode(self.id)?.live.len as u64).checked_add_signed(offset)
                }
            }
            .ok_or(io::ErrorKind::InvalidInput)?;
            Ok(self.pos)
        })
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        let mut state = self.fs.state();
        if let Some(file) = state.files.get_mut(&self.id) {
            if self.locked {
                file.locked = false;
            }
            file.handles -= 1;
        }
        state.collect_unlinked();
    }
}

fn file_offset(offset: u64) -> io::Result<usize> {
    usize::try_from(offset)
        .ok()
        .filter(|offset| *offset <= MAX_FILE_LEN)
        .ok_or_else(|| io::Error::from(io::ErrorKind::FileTooLarge))
}

#[allow(clippy::cast_possible_truncation)] // The remainder fits in the original bound.
fn bounded_index(choice: u64, bound: usize) -> usize {
    (choice % bound as u64) as usize
}
