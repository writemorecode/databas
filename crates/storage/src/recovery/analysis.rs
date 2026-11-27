//! Validate transaction history and identify the loser chains before redo.

use std::collections::{BTreeMap, HashMap};

use crate::error::{StorageError, StorageResult};
use crate::log_manager::{
    LogManagerError, Lsn, RecoveryLogRecord, RecoveryLogRecordKind as Kind, TxnId, ZERO_LSN,
};

pub(super) struct Analysis<'a> {
    records: HashMap<Lsn, &'a RecoveryLogRecord>,
    losers: Vec<(TxnId, Lsn, Lsn)>,
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

        let mut losers = Vec::new();
        for (txn_id, state) in transactions {
            if state.completed {
                continue; // Winners may start in a retained suffix; losers may not.
            }
            let begin = state.begin.ok_or(LogManagerError::InvalidChain {
                txn_id,
                lsn: state.head,
                link: oldest,
            })?;
            losers.push((txn_id, begin, state.head));
        }
        Ok(Self { records: index, losers })
    }

    pub(super) fn restore_losers(
        self,
        transactions: &mut crate::transaction_manager::TransactionManager,
        mut take_pending: impl FnMut(TxnId) -> BTreeMap<crate::PageId, bool>,
    ) -> StorageResult<()> {
        for (txn_id, begin, mut head) in self.losers {
            let mut chain = Vec::new();
            while head != begin {
                let record = self
                    .records
                    .get(&head)
                    .copied()
                    .ok_or(LogManagerError::InvalidChain { txn_id, lsn: head, link: head })?;
                let next = undo_next(record);
                chain.push((record, next));
                head = next;
            }
            transactions.restore_loser(txn_id, begin, take_pending(txn_id), chain);
        }
        Ok(())
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
