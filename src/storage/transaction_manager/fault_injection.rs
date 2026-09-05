use std::ops::{Deref, DerefMut};

use crate::core::error::StorageResult;

use super::{LogManager, PageRestore, TransactionManager, TransactionSavepoint};

/// Transaction-manager decorator that injects one-shot rollback failures.
#[derive(Debug)]
pub(crate) struct FaultInjectingTransactionManager {
    inner: TransactionManager,
    fail_next_savepoint_rollback: bool,
}

impl FaultInjectingTransactionManager {
    pub(crate) fn new(inner: TransactionManager) -> Self {
        Self { inner, fail_next_savepoint_rollback: false }
    }

    pub(crate) fn fail_next_savepoint_rollback(&mut self) {
        self.fail_next_savepoint_rollback = true;
    }

    pub(crate) fn rollback_to_savepoint(
        &mut self,
        log: &mut LogManager,
        savepoint: TransactionSavepoint,
    ) -> StorageResult<Vec<PageRestore>> {
        if std::mem::take(&mut self.fail_next_savepoint_rollback) {
            log.force_next_lsn_exhausted_for_test();
        }
        self.inner.rollback_to_savepoint(log, savepoint)
    }
}

impl Deref for FaultInjectingTransactionManager {
    type Target = TransactionManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FaultInjectingTransactionManager {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
