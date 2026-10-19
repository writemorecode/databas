//! Unit-test-only adapter: real file bytes and OS locks, one local fault.
use std::{
    fs::{File, Metadata, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

use super::super::DiskManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Read,
    Write,
    Seek,
    Resize,
    Sync,
}

#[derive(Clone, Copy, Debug)]
pub enum Effect {
    Error(io::ErrorKind),
    Zero,
}

#[derive(Debug)]
struct Fault {
    operation: Operation,
    remaining: usize,
    effect: Effect,
}

#[derive(Debug)]
struct Plan {
    chunk: usize,
    fault: Option<Fault>,
}

#[derive(Clone, Debug)]
pub struct Control(Arc<Mutex<Plan>>);

impl Control {
    pub(crate) fn arm(&self, operation: Operation, after: usize, effect: Effect) {
        self.0.lock().unwrap().fault = Some(Fault { operation, remaining: after, effect });
    }

    pub(crate) fn chunk(&self, bytes: usize) {
        assert!(bytes > 0);
        self.0.lock().unwrap().chunk = bytes;
    }

    pub(crate) fn consumed(&self) -> bool {
        self.0.lock().unwrap().fault.is_none()
    }

    fn limit(&self, operation: Operation, requested: usize) -> io::Result<usize> {
        let mut plan = self.0.lock().map_err(|err| io::Error::other(err.to_string()))?;
        let chunk =
            if matches!(operation, Operation::Read | Operation::Write) { plan.chunk } else { 1 };
        let limit = requested.min(chunk);
        let Some(fault) = &plan.fault else {
            return Ok(limit);
        };
        if fault.operation != operation {
            return Ok(limit);
        }
        if fault.remaining != 0 {
            return Ok(limit.min(fault.remaining));
        }
        let effect = fault.effect;
        plan.fault = None;
        match effect {
            Effect::Error(kind) => Err(kind.into()),
            Effect::Zero => Ok(0),
        }
    }

    fn advance(&self, operation: Operation, count: usize) {
        let mut plan = self.0.lock().unwrap();
        if let Some(fault) = &mut plan.fault
            && fault.operation == operation
        {
            fault.remaining -= count;
        }
    }
}

pub struct FaultFile {
    file: File,
    control: Control,
}

impl From<File> for FaultFile {
    fn from(file: File) -> Self {
        Self {
            file,
            control: Control(Arc::new(Mutex::new(Plan { chunk: usize::MAX, fault: None }))),
        }
    }
}

impl FaultFile {
    pub(crate) fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }
    pub(crate) fn try_lock(&self) -> Result<(), TryLockError> {
        self.file.try_lock()
    }

    fn call<T>(
        &self,
        operation: Operation,
        action: impl FnOnce(&File) -> io::Result<T>,
    ) -> io::Result<T> {
        self.control.limit(operation, 1)?;
        let result = action(&self.file)?;
        self.control.advance(operation, 1);
        Ok(result)
    }

    pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
        self.call(Operation::Resize, |file| file.set_len(len))
    }

    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.call(Operation::Sync, File::sync_all)
    }
}

impl Read for FaultFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let limit = self.control.limit(Operation::Read, bytes.len())?;
        let count = self.file.read(&mut bytes[..limit])?;
        self.control.advance(Operation::Read, count);
        Ok(count)
    }
}

impl Write for FaultFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let limit = self.control.limit(Operation::Write, bytes.len())?;
        let count = self.file.write(&bytes[..limit])?;
        self.control.advance(Operation::Write, count);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for FaultFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.control.limit(Operation::Seek, 1)?;
        let offset = self.file.seek(position)?;
        self.control.advance(Operation::Seek, 1);
        Ok(offset)
    }
}

impl DiskManager {
    pub(crate) fn faults(&self) -> Control {
        self.file.control.clone()
    }
}
