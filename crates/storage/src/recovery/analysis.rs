//! Validate retained transaction history before replay.

use std::collections::{BTreeMap, HashMap};

use crate::error::{StorageError, StorageResult};
use crate::log_manager::{
    LogManagerError, Lsn, RecoveryLogRecord, RecoveryLogRecordKind as Kind, TxnId, ZERO_LSN,
};

struct Transaction {
    begin: Option<Lsn>,
    head: Lsn,
    completed: bool,
}

pub(super) fn validate(records: &[RecoveryLogRecord]) -> StorageResult<()> {
    let index: HashMap<_, _> = records.iter().map(|record| (record.lsn, record)).collect();
    let oldest = records.first().map_or(ZERO_LSN, |record| record.lsn);
    let mut transactions = BTreeMap::<TxnId, Transaction>::new();
    for record in records {
        let previous = record.prev_lsn;
        validate_link(record, previous, &index, oldest)?;
        if record.txn_id == 0 || (matches!(record.kind, Kind::Begin) != (previous == ZERO_LSN)) {
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
    }
    for (txn_id, state) in transactions {
        if !state.completed && state.begin.is_none() {
            return Err(
                LogManagerError::InvalidChain { txn_id, lsn: state.head, link: oldest }.into()
            );
        }
    }
    Ok(())
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
