//! Validate transaction history and identify the loser chains before redo.

use std::collections::{BTreeMap, BinaryHeap, HashMap};

use crate::error::{StorageError, StorageResult};
use crate::log_manager::{
    LogManagerError, Lsn, RecoveryLogRecord, RecoveryLogRecordKind as Kind, TxnId, ZERO_LSN,
};

pub(super) struct Analysis<'a> {
    records: HashMap<Lsn, &'a RecoveryLogRecord>,
    // Global reverse-LSN order, even when independent transactions interleave.
    losers: BinaryHeap<(Lsn, TxnId)>,
}

struct Transaction {
    begin: Option<Lsn>,
    head: Lsn,
    completed: bool,
}

impl<'a> Analysis<'a> {
    pub(super) fn new(records: &'a [RecoveryLogRecord]) -> StorageResult<Self> {
        let index: HashMap<_, _> = records.iter().map(|record| (record.lsn, record)).collect();
        let oldest = records.first().map_or(ZERO_LSN, |record| record.lsn);
        let mut transactions = BTreeMap::<TxnId, Transaction>::new();
        for record in records {
            let previous = record.prev_lsn;
            validate_link(record, previous, &index, oldest)?;
            if record.txn_id == 0 || (matches!(record.kind, Kind::Begin) != (previous == ZERO_LSN))
            {
                return Err(bad_chain(record, previous));
            }
            let state = transactions.entry(record.txn_id).or_insert(Transaction {
                begin: matches!(record.kind, Kind::Begin).then_some(record.lsn),
                head: previous,
                completed: false,
            });
            if state.completed || previous != state.head {
                return Err(bad_chain(record, previous));
            }
            state.head = record.lsn;
            state.completed = matches!(record.kind, Kind::Commit | Kind::Rollback);

            let next = undo_next(record);
            validate_link(record, next, &index, oldest)?;
            if next > previous
                || (!matches!(record.kind, Kind::Begin)
                    && (next == ZERO_LSN || state.begin.is_some_and(|begin| next < begin)))
            {
                return Err(bad_chain(record, next));
            }
        }

        let mut losers = BinaryHeap::new();
        for (txn_id, state) in transactions {
            if state.completed {
                continue; // Winners may start in a retained suffix; losers may not.
            }
            if state.begin.is_none() {
                return Err(LogManagerError::InvalidChain {
                    txn_id,
                    lsn: state.head,
                    link: oldest,
                }
                .into());
            }
            losers.push((state.head, txn_id));
        }
        Ok(Self { records: index, losers })
    }

    pub(super) fn take_loser(&mut self) -> StorageResult<Option<&'a RecoveryLogRecord>> {
        self.losers
            .pop()
            .map(|(lsn, txn_id)| {
                self.records
                    .get(&lsn)
                    .copied()
                    .ok_or_else(|| LogManagerError::InvalidChain { txn_id, lsn, link: lsn }.into())
            })
            .transpose()
    }

    pub(super) fn resume_at(&mut self, txn_id: TxnId, lsn: Lsn) {
        self.losers.push((lsn, txn_id));
    }
}

pub(super) fn undo_next(record: &RecoveryLogRecord) -> Lsn {
    match record.kind {
        Kind::PageCompensation { undo_next_lsn, .. }
        | Kind::AllocationCompensation { undo_next_lsn, .. } => undo_next_lsn,
        _ => record.prev_lsn,
    }
}

fn validate_link(
    record: &RecoveryLogRecord,
    link: Lsn,
    index: &HashMap<Lsn, &RecoveryLogRecord>,
    oldest: Lsn,
) -> StorageResult<()> {
    if link >= record.lsn
        || (link >= oldest && index.get(&link).is_none_or(|prior| prior.txn_id != record.txn_id))
    {
        return Err(bad_chain(record, link));
    }
    Ok(())
}

fn bad_chain(record: &RecoveryLogRecord, link: Lsn) -> StorageError {
    LogManagerError::InvalidChain { txn_id: record.txn_id, lsn: record.lsn, link }.into()
}
