use std::io::{self, Write};

use super::DurableWrite;

/// Writer decorator that injects one-shot write and durability failures.
#[derive(Debug)]
pub(super) struct FaultInjectingWriter<W> {
    inner: W,
    fail_next_write_all_after: Option<usize>,
    fail_next_sync: bool,
}

impl<W> FaultInjectingWriter<W> {
    pub(super) fn new(inner: W) -> Self {
        Self { inner, fail_next_write_all_after: None, fail_next_sync: false }
    }

    pub(super) fn fail_next_write_all_after(&mut self, byte_count: usize) {
        self.fail_next_write_all_after = Some(byte_count);
    }

    pub(super) fn fail_next_sync(&mut self) {
        self.fail_next_sync = true;
    }
}

impl<W: Write> Write for FaultInjectingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let Some(byte_count) = self.fail_next_write_all_after.take() else {
            return self.inner.write_all(buf);
        };

        let byte_count = byte_count.min(buf.len());
        self.inner.write_all(&buf[..byte_count])?;
        Err(io::Error::other("injected partial WAL append failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: DurableWrite> DurableWrite for FaultInjectingWriter<W> {
    fn sync_all(&mut self) -> io::Result<()> {
        if std::mem::take(&mut self.fail_next_sync) {
            return Err(io::Error::other("injected WAL flush failure"));
        }
        self.inner.sync_all()
    }
}

mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn write_all_fault_writes_only_bytes_before_boundary() {
        let mut writer = FaultInjectingWriter::new(Cursor::new(Vec::new()));
        writer.fail_next_write_all_after(3);

        let error = writer.write_all(b"frame").unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(writer.inner.into_inner(), b"fra");
    }

    #[test]
    fn write_all_fault_fails_at_exact_boundary() {
        let mut writer = FaultInjectingWriter::new(Cursor::new(Vec::new()));
        writer.fail_next_write_all_after(5);

        let error = writer.write_all(b"frame").unwrap_err();
        writer.write_all(b"!").unwrap();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(writer.inner.into_inner(), b"frame!");
    }

    #[test]
    fn write_all_fault_larger_than_buffer_still_fails_current_write() {
        let mut writer = FaultInjectingWriter::new(Cursor::new(Vec::new()));
        writer.fail_next_write_all_after(100);

        let error = writer.write_all(b"frame").unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(writer.inner.into_inner(), b"frame");
    }
}
