/// Identifier of a page in the database file.
pub type PageId = u64;
pub type SlotId = u16;
/// Monotonic identifier of a physical transaction.
pub type TxnId = u64;
/// Log sequence number.
pub type Lsn = u64;
pub const PAGE_SIZE_U16: u16 = 4096;
pub const PAGE_SIZE: usize = PAGE_SIZE_U16 as usize;

/// Progress of a WAL checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOutcome {
    /// All captured history was reclaimed.
    Completed,
    /// Active transactions, busy pages, or newer records retain history.
    Partial,
}
