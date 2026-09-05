use std::collections::TryReserveError;

use thiserror::Error;

use crate::core::{Lsn, PageId, TxnId};

/// Failure of an internal invariant or allocation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InternalError {
    #[error("{0}")]
    InvariantViolation(#[source] InvariantViolation),
    #[error("allocation failed: {0}")]
    AllocationFailed(#[source] TryReserveError),
    #[error("synchronization lock poisoned: {lock}")]
    SynchronizationPoisoned { lock: &'static str },
}

/// Internal state that should be unreachable through a valid operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvariantViolation {
    #[error("pinned page during flush: {page_id}")]
    PinnedPageDuringFlush { page_id: PageId },
    #[error("page {page_id} cannot be borrowed due to an active conflicting borrow")]
    PageBorrowConflict { page_id: PageId },
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
    #[error("invalid slot index {slot_index} for {slot_count} slots")]
    InvalidSlotIndex { slot_index: u16, slot_count: u16 },
    #[error(
        "requested WAL flush through LSN {requested_lsn}, but highest appended LSN is {highest_appended_lsn:?}"
    )]
    WalFlushLsnNotAppended { requested_lsn: Lsn, highest_appended_lsn: Option<Lsn> },
    #[error("WAL log error: {message}")]
    WalLog { message: String },
    #[error("transaction {txn_id} is already active")]
    ActiveTransaction { txn_id: TxnId },
    #[error("no active transaction")]
    NoActiveTransaction,
    #[error("active transaction mismatch: expected {expected}, got {actual}")]
    TransactionMismatch { expected: TxnId, actual: TxnId },
    #[error(
        "invalid transaction savepoint for transaction {txn_id}: undo length {undo_len} exceeds active undo length {active_undo_len}"
    )]
    InvalidTransactionSavepoint { txn_id: TxnId, undo_len: usize, active_undo_len: usize },
    #[error("transaction id space exhausted")]
    TransactionIdExhausted,
    #[error("transaction {txn_id} failed before commit")]
    TransactionPoisoned { txn_id: TxnId },
    #[error("catalog table {table} cursor had no current record after positioning")]
    CatalogCursorMissingRecord { table: String },
    #[error("leaf split has no left-hand cells")]
    EmptyLeafSplit,
    #[error("leaf split target key is missing")]
    LeafSplitTargetMissing,
    #[error("leaf split target slot {slot_index} exceeds the slot-id range")]
    LeafSplitTargetSlotOutOfRange { slot_index: usize },
    #[error("page {page_id} pin count overflowed")]
    PagePinCountOverflow { page_id: PageId },
}
