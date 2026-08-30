//! Transaction-owned, exclusive table locks.
//!
//! [`LockManager`] implements strict two-phase locking at table granularity.
//! There is one lock mode, so reads and writes of the same table serialize. A
//! lock covers the table tree and all of its secondary indexes; index IDs are
//! therefore not lock resources.
//!
//! # Transaction lifecycle
//!
//! A transaction must first be registered with [`LockManager::begin_transaction`]
//! (or [`LockManager::begin_ddl_transaction`]). During its growing phase it can
//! acquire [`TableLease`] capabilities with [`LockManager::acquire`]. Starting
//! commit or rollback prevents further acquisitions but deliberately retains
//! every granted lock. Only [`LockManager::finish_transaction`] releases locks,
//! after the caller has established the durable commit or rollback outcome.
//! Dropping a lease or a cursor never releases a transaction lock.
//!
//! # Waiting and deadlocks
//!
//! Contended requests wait in a per-table FIFO queue. Queue ownership and waiter
//! order are protected by the same mutex, and release performs a direct handoff
//! before waking waiters. The wait-for graph models the FIFO predecessor chain.
//! A request that closes a cycle is rejected with [`LockError::Deadlock`] and
//! its transaction enters [`TransactionPhase::Aborting`].
//!
//! Queue and graph changes follow one latch order: table queue first, wait-for
//! graph second. The global queue-map mutex is never held while locking a queue
//! or waiting on its condition variable.
//!
//! # Loom testing
//!
//! The synchronization primitives are substituted with Loom's modeled types
//! in test builds using the `loom` feature. Run the focused models with
//! `cargo test --features loom core::lock_manager::loom_tests`.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
};

#[cfg(all(test, feature = "loom"))]
mod sync {
    pub(super) use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
}

#[cfg(not(all(test, feature = "loom")))]
mod sync {
    pub(super) use std::sync::{Arc, Condvar, Mutex, MutexGuard};
}

use sync::{Arc, Condvar, Mutex, MutexGuard};
use thiserror::Error;

use crate::core::{CatalogId, TxnId};

/// Stable identity of a user-table lock resource.
///
/// This newtype keeps table IDs distinct from index and column catalog IDs at
/// the locking boundary. One resource includes the table tree and every
/// secondary index belonging to that table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(CatalogId);

impl TableId {
    /// Creates a table resource identifier from a table catalog identifier.
    pub const fn new(catalog_id: CatalogId) -> Self {
        Self(catalog_id)
    }

    /// Returns the underlying catalog identifier.
    pub const fn catalog_id(self) -> CatalogId {
        self.0
    }
}

impl From<CatalogId> for TableId {
    fn from(value: CatalogId) -> Self {
        Self::new(value)
    }
}

/// Lock-acquisition phase of a transaction registered with [`LockManager`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionPhase {
    /// New table locks may be acquired.
    Growing,
    /// Commit has started; locks await a durable commit outcome.
    Committing,
    /// Full rollback is required or in progress.
    Aborting,
}

impl fmt::Display for TransactionPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Growing => f.write_str("growing"),
            Self::Committing => f.write_str("committing"),
            Self::Aborting => f.write_str("aborting"),
        }
    }
}

/// Typed failures produced by table-lock operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LockError {
    /// The transaction identifier is not registered as active.
    #[error("transaction {txn_id} is not active")]
    TransactionNotActive {
        /// Unregistered or already-completed transaction.
        txn_id: TxnId,
    },
    /// The transaction identifier is already registered.
    #[error("transaction {txn_id} is already active")]
    TransactionAlreadyActive {
        /// Duplicate transaction identifier.
        txn_id: TxnId,
    },
    /// Strict 2PL forbids acquisition after finalization starts.
    #[error("transaction {txn_id} cannot acquire locks while {phase}")]
    TransactionNotGrowing {
        /// Transaction that attempted the operation.
        txn_id: TxnId,
        /// Phase that prevents further acquisition.
        phase: TransactionPhase,
    },
    /// A synchronous transaction attempted a second blocking request.
    #[error("transaction {txn_id} already has an outstanding lock request")]
    OutstandingRequest {
        /// Transaction with an existing queued request.
        txn_id: TxnId,
    },
    /// The request closed a wait-for cycle and its transaction must roll back.
    #[error("deadlock detected; transaction {txn_id} must roll back")]
    Deadlock {
        /// Transaction selected as the deadlock victim.
        txn_id: TxnId,
    },
    /// A queued request was canceled before it acquired its table.
    #[error("lock request for transaction {txn_id} was canceled")]
    Canceled {
        /// Transaction whose request was canceled.
        txn_id: TxnId,
    },
    /// Finalization was invoked before commit or rollback began.
    #[error("transaction {txn_id} has not started finalization")]
    FinalizationNotStarted {
        /// Transaction still in its growing phase.
        txn_id: TxnId,
    },
    /// A lease was presented for a different transaction or table.
    #[error("lock lease does not authorize transaction {txn_id} to access table {table_id:?}")]
    LeaseMismatch {
        /// Transaction requesting access.
        txn_id: TxnId,
        /// Table the caller attempted to access.
        table_id: TableId,
    },
    /// DDL cannot be admitted while another transaction is active.
    #[error("DDL transaction {txn_id} cannot run while another transaction is active")]
    DdlBusy {
        /// DDL transaction that could not be admitted.
        txn_id: TxnId,
    },
    /// A manager mutex was poisoned by a failed thread.
    #[error("lock manager mutex poisoned: {mutex}")]
    Poisoned {
        /// Name of the poisoned manager mutex.
        mutex: &'static str,
    },
    /// Internal queue, graph, or ownership state disagreed.
    #[error("lock manager invariant violated: {message}")]
    Invariant {
        /// Diagnostic describing the inconsistent state.
        message: String,
    },
}

/// A capability proving that one transaction owns one table lock.
///
/// Dropping a lease has no effect.  Code constructing a normal relational
/// cursor should borrow the transaction-owned lease for the cursor's lifetime.
#[derive(Debug, PartialEq, Eq)]
pub struct TableLease {
    txn_id: TxnId,
    table_id: TableId,
}

impl TableLease {
    /// Returns the transaction owning this capability.
    pub const fn transaction_id(&self) -> TxnId {
        self.txn_id
    }

    /// Returns the table authorized by this capability.
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Verifies that this lease authorizes `txn_id` to access `table_id`.
    ///
    /// This capability check avoids a time-of-check/time-of-use race from
    /// consulting manager state with a boolean `is_locked` operation.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::LeaseMismatch`] if either identifier differs from
    /// the capability's owner and resource.
    pub fn authorize(&self, txn_id: TxnId, table_id: impl Into<TableId>) -> Result<(), LockError> {
        let table_id = table_id.into();
        if self.txn_id == txn_id && self.table_id == table_id {
            Ok(())
        } else {
            Err(LockError::LeaseMismatch { txn_id, table_id })
        }
    }
}

/// Owner and FIFO waiters changed atomically under one queue latch.
#[derive(Debug, Default)]
struct QueueState {
    /// Transaction currently holding the exclusive resource.
    owner: Option<TxnId>,
    /// Blocking transactions in arrival order.
    waiters: VecDeque<TxnId>,
}

/// Shared synchronization state for one table resource.
#[derive(Debug, Default)]
struct LockQueue {
    /// Owner and waiter order protected by the queue latch.
    state: Mutex<QueueState>,
    /// Notification used after cancellation or direct ownership handoff.
    changed: Condvar,
}

/// Result of inspecting a request while holding its table queue and graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquireDecision {
    /// The transaction owns the table and can receive its lease.
    Granted,
    /// The transaction was appended to the FIFO queue and must wait.
    Wait,
    /// The new request was removed because it closed a wait-for cycle.
    Deadlock,
}

/// Transaction and resource identifying one acquisition request.
#[derive(Debug, Clone, Copy)]
struct LockRequest {
    txn_id: TxnId,
    table_id: TableId,
}

/// A table queue paired with its currently held state guard.
struct LockedQueue<'a> {
    queue: &'a LockQueue,
    state: MutexGuard<'a, QueueState>,
}

/// Locking state owned by one active transaction.
#[derive(Debug)]
struct TransactionState {
    /// Current strict-2PL phase.
    phase: TransactionPhase,
    /// Granted resources, ordered for deterministic finalization.
    locks: BTreeSet<TableId>,
    /// Resource of the transaction's sole permitted blocking request.
    waiting_for: Option<TableId>,
    /// Whether a canceled waiter still needs to observe its cancellation.
    request_canceled: bool,
    /// Whether finalization completed while that canceled waiter was blocked.
    finalization_finished: bool,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self {
            phase: TransactionPhase::Growing,
            locks: BTreeSet::new(),
            waiting_for: None,
            request_canceled: false,
            finalization_finished: false,
        }
    }
}

/// Directed wait-for graph used for transaction deadlock detection.
///
/// Every vertex has at most one outgoing edge because a transaction may have
/// at most one blocking request. An edge points from a waiter to its FIFO
/// predecessor.
#[derive(Debug, Default)]
struct WaitForGraph {
    edges: HashMap<TxnId, TxnId>,
}

impl WaitForGraph {
    /// Returns the transaction that `txn_id` is waiting for.
    fn predecessor(&self, txn_id: TxnId) -> Option<TxnId> {
        self.edges.get(&txn_id).copied()
    }

    /// Adds or replaces the outgoing edge from `waiter` to `predecessor`.
    fn insert(&mut self, waiter: TxnId, predecessor: TxnId) {
        self.edges.insert(waiter, predecessor);
    }

    /// Removes and returns the outgoing edge from `txn_id`.
    fn remove(&mut self, txn_id: TxnId) -> Option<TxnId> {
        self.edges.remove(&txn_id)
    }

    /// Returns whether `txn_id` has an outgoing edge.
    #[cfg(all(test, not(feature = "loom")))]
    fn contains(&self, txn_id: TxnId) -> bool {
        self.edges.contains_key(&txn_id)
    }

    /// Returns whether the graph has no edges.
    #[cfg(all(test, feature = "loom"))]
    fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Returns whether following edges from `start` reaches `start`.
    fn edge_closes_cycle(&self, start: TxnId) -> bool {
        let mut current = self.predecessor(start);
        let mut visited = HashSet::new();
        while let Some(txn_id) = current {
            if txn_id == start {
                return true;
            }
            if !visited.insert(txn_id) {
                return false;
            }
            current = self.predecessor(txn_id);
        }
        false
    }

    /// Returns whether any transaction participates in a cycle.
    fn has_cycle(&self) -> bool {
        self.edges.keys().copied().any(|start| self.edge_closes_cycle(start))
    }
}

/// Transaction registry and wait-for graph, protected by one mutex.
#[derive(Debug, Default)]
struct GraphState {
    /// Active transactions and their granted/requested resources.
    transactions: HashMap<TxnId, TransactionState>,
    /// FIFO dependencies between waiting transactions.
    waits_for: WaitForGraph,
    /// Sole DDL transaction preventing ordinary transaction admission.
    ddl_owner: Option<TxnId>,
}

/// Shared implementation behind cloneable manager handles.
#[derive(Debug, Default)]
struct LockManagerInner {
    /// Persistent map of lazily created table queues.
    queues: Mutex<HashMap<TableId, Arc<LockQueue>>>,
    /// Transaction registry, wait-for edges, and DDL admission state.
    graph: Mutex<GraphState>,
    /// Test-only synchronization proving that a modeled waiter has enqueued.
    #[cfg(all(test, feature = "loom"))]
    enqueue_signal: Mutex<Option<loom::sync::mpsc::Sender<()>>>,
}

/// Thread-safe manager for FIFO, exclusive table locks.
///
/// Clones refer to the same queues and transaction registry. The manager does
/// not perform WAL or rollback work itself; callers explicitly bracket storage
/// finalization with [`Self::begin_commit`] or [`Self::begin_rollback`] and
/// [`Self::finish_transaction`].
///
/// # Example
///
/// ```
/// use databas::core::{TxnId, lock_manager::{LockManager, TableId}};
///
/// let locks = LockManager::new();
/// let txn_id: TxnId = 1;
/// locks.begin_transaction(txn_id)?;
/// let lease = locks.acquire(txn_id, TableId::new(42))?;
/// lease.authorize(txn_id, TableId::new(42))?;
///
/// locks.begin_commit(txn_id)?;
/// // Persist and flush the commit outcome before releasing locks.
/// locks.finish_transaction(txn_id)?;
/// # Ok::<(), databas::core::lock_manager::LockError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct LockManager {
    inner: Arc<LockManagerInner>,
}

impl LockManager {
    /// Creates an empty lock manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `txn_id` in its growing phase.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::TransactionAlreadyActive`] for a duplicate ID or
    /// [`LockError::DdlBusy`] while the database-wide DDL gate is held.
    pub fn begin_transaction(&self, txn_id: TxnId) -> Result<(), LockError> {
        let mut graph = self.lock_graph()?;
        if graph.transactions.contains_key(&txn_id) {
            return Err(LockError::TransactionAlreadyActive { txn_id });
        }
        if graph.ddl_owner.is_some() {
            return Err(LockError::DdlBusy { txn_id });
        }
        graph.transactions.insert(txn_id, TransactionState::default());
        Ok(())
    }

    /// Registers a database-wide DDL transaction.
    ///
    /// DDL is rejected rather than queued unless there are no active
    /// transactions. While it remains active, ordinary transaction admission
    /// is also rejected. `CREATE INDEX` should additionally acquire its parent
    /// table through [`Self::acquire`].
    ///
    /// # Errors
    ///
    /// Returns [`LockError::TransactionAlreadyActive`] for a duplicate ID or
    /// [`LockError::DdlBusy`] when any transaction is active.
    pub fn begin_ddl_transaction(&self, txn_id: TxnId) -> Result<(), LockError> {
        let mut graph = self.lock_graph()?;
        if graph.transactions.contains_key(&txn_id) {
            return Err(LockError::TransactionAlreadyActive { txn_id });
        }
        if !graph.transactions.is_empty() || graph.ddl_owner.is_some() {
            return Err(LockError::DdlBusy { txn_id });
        }
        graph.transactions.insert(txn_id, TransactionState::default());
        graph.ddl_owner = Some(txn_id);
        Ok(())
    }

    /// Returns the lock-acquisition phase of an active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::TransactionNotActive`] if `txn_id` is unknown or
    /// has already completed.
    pub fn transaction_phase(&self, txn_id: TxnId) -> Result<TransactionPhase, LockError> {
        let graph = self.lock_graph()?;
        graph
            .transactions
            .get(&txn_id)
            .map(|transaction| transaction.phase)
            .ok_or(LockError::TransactionNotActive { txn_id })
    }

    /// Acquires an exclusive table lock, waiting in FIFO order when contended.
    ///
    /// Acquisition is idempotent: an existing owner receives another
    /// capability for the same transaction-owned lease without queueing or
    /// increasing a reference count. A returned lease does not release the
    /// lock when dropped.
    ///
    /// # Blocking
    ///
    /// This method can wait without a timeout until every FIFO predecessor
    /// completes or the request is canceled. Callers must not hold page or
    /// storage latches while invoking it.
    ///
    /// # Errors
    ///
    /// Returns an active-transaction or phase error when acquisition is not
    /// permitted, [`LockError::OutstandingRequest`] for a second concurrent
    /// request in one transaction, [`LockError::Deadlock`] when this request
    /// closes a wait-for cycle, or [`LockError::Canceled`] if the waiter is
    /// removed. Poisoned latches and inconsistent internal state are reported
    /// explicitly rather than panicking.
    pub fn acquire(
        &self,
        txn_id: TxnId,
        table_id: impl Into<TableId>,
    ) -> Result<TableLease, LockError> {
        let request = LockRequest { txn_id, table_id: table_id.into() };
        let queue = self.queue(request.table_id)?;
        let mut locked_queue =
            LockedQueue { queue: &queue, state: lock(&queue.state, "table queue")? };

        match self.prepare_acquire(request, &mut locked_queue.state)? {
            AcquireDecision::Granted => {
                Ok(TableLease { txn_id: request.txn_id, table_id: request.table_id })
            }
            AcquireDecision::Wait => self.wait_for_acquire(request, locked_queue),
            AcquireDecision::Deadlock => {
                drop(locked_queue);
                queue.changed.notify_all();
                Err(LockError::Deadlock { txn_id: request.txn_id })
            }
        }
    }

    /// Grants an uncontended request or appends a contended one to its queue.
    ///
    /// The caller holds the table queue latch; this method acquires the graph
    /// latch second and leaves both queue and graph changes consistent before
    /// returning a decision.
    fn prepare_acquire(
        &self,
        request: LockRequest,
        queue: &mut QueueState,
    ) -> Result<AcquireDecision, LockError> {
        let LockRequest { txn_id, table_id } = request;
        let mut graph = self.lock_graph()?;
        let transaction = active_growing_transaction(&mut graph, txn_id)?;

        if queue.owner == Some(txn_id) {
            if !transaction.locks.contains(&table_id) {
                return Err(invariant("queue owner is absent from its transaction lock set"));
            }
            return Ok(AcquireDecision::Granted);
        }
        if transaction.waiting_for.is_some() {
            return Err(LockError::OutstandingRequest { txn_id });
        }
        if queue.waiters.contains(&txn_id) {
            return Err(invariant("transaction occurs twice in a table wait queue"));
        }
        if queue.owner.is_none() && queue.waiters.is_empty() {
            queue.owner = Some(txn_id);
            transaction.locks.insert(table_id);
            return Ok(AcquireDecision::Granted);
        }

        let predecessor = queue_predecessor(queue, &graph.waits_for)?;
        queue.waiters.push_back(txn_id);
        graph.waits_for.insert(txn_id, predecessor);
        let transaction = graph
            .transactions
            .get_mut(&txn_id)
            .ok_or(LockError::TransactionNotActive { txn_id })?;
        transaction.waiting_for = Some(table_id);
        transaction.request_canceled = false;

        if graph.waits_for.edge_closes_cycle(txn_id) {
            remove_deadlock_request(queue, &mut graph, txn_id)?;
            return Ok(AcquireDecision::Deadlock);
        }

        #[cfg(all(test, feature = "loom"))]
        self.signal_waiter_enqueued()?;
        Ok(AcquireDecision::Wait)
    }

    /// Waits until a queued transaction owns the table or is canceled.
    fn wait_for_acquire(
        &self,
        request: LockRequest,
        mut locked_queue: LockedQueue<'_>,
    ) -> Result<TableLease, LockError> {
        loop {
            locked_queue.state = locked_queue
                .queue
                .changed
                .wait(locked_queue.state)
                .map_err(|_poisoned| LockError::Poisoned { mutex: "table queue" })?;
            let mut graph = self.lock_graph()?;
            let transaction = graph
                .transactions
                .get_mut(&request.txn_id)
                .ok_or(LockError::TransactionNotActive { txn_id: request.txn_id })?;

            if transaction.request_canceled {
                transaction.request_canceled = false;
                let finalization_finished = transaction.finalization_finished;
                if finalization_finished {
                    if !transaction.locks.is_empty() || transaction.waiting_for.is_some() {
                        return Err(invariant(
                            "finalized canceled transaction retains lock-manager state",
                        ));
                    }
                    if graph.waits_for.remove(request.txn_id).is_some() {
                        return Err(invariant(
                            "finalized canceled transaction had an outgoing wait edge",
                        ));
                    }
                    graph.transactions.remove(&request.txn_id);
                    if graph.ddl_owner == Some(request.txn_id) {
                        graph.ddl_owner = None;
                    }
                }
                return Err(LockError::Canceled { txn_id: request.txn_id });
            }
            if locked_queue.state.owner == Some(request.txn_id) {
                validate_handoff(transaction, request)?;
                return Ok(TableLease { txn_id: request.txn_id, table_id: request.table_id });
            }
            if transaction.phase != TransactionPhase::Growing {
                return Err(LockError::Canceled { txn_id: request.txn_id });
            }
            if transaction.waiting_for != Some(request.table_id)
                || !locked_queue.state.waiters.contains(&request.txn_id)
            {
                return Err(invariant("woken transaction is neither owner nor waiter"));
            }
        }
    }

    /// Sends the test-only notification that a Loom waiter is fully queued.
    #[cfg(all(test, feature = "loom"))]
    fn signal_waiter_enqueued(&self) -> Result<(), LockError> {
        if let Some(signal) = lock(&self.inner.enqueue_signal, "Loom enqueue signal")?.take() {
            signal
                .send(())
                .map_err(|error| invariant(format!("failed to signal Loom waiter: {error}")))?;
        }
        Ok(())
    }

    /// Cancels a transaction's outstanding blocking request.
    ///
    /// Returns `true` when a waiter was removed. Already granted locks are not
    /// released; if ownership handoff wins the queue-latch race this returns
    /// `false`, and rollback must release the newly granted lock.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::TransactionNotActive`] for an unknown or completed
    /// transaction, or an explicit poison/invariant error if manager state
    /// cannot be updated safely.
    pub fn cancel_waiting(&self, txn_id: TxnId) -> Result<bool, LockError> {
        let table_id = {
            let graph = self.lock_graph()?;
            graph
                .transactions
                .get(&txn_id)
                .ok_or(LockError::TransactionNotActive { txn_id })?
                .waiting_for
        };
        let Some(table_id) = table_id else {
            return Ok(false);
        };

        let queue = self.queue(table_id)?;
        let mut queue_state = lock(&queue.state, "table queue")?;
        let mut graph = self.lock_graph()?;
        let removed =
            remove_waiter(&mut queue_state, &mut graph, LockRequest { txn_id, table_id })?;
        drop(graph);
        drop(queue_state);
        if removed {
            queue.changed.notify_all();
        }
        Ok(removed)
    }

    /// Starts commit without releasing any granted locks.
    ///
    /// A queued request is canceled and the transaction may no longer acquire
    /// resources. Repeating this call while already committing is idempotent.
    /// Locks remain held until [`Self::finish_transaction`].
    ///
    /// # Errors
    ///
    /// Returns an active-transaction, incompatible-phase, poison, or invariant
    /// error if commit cannot safely begin.
    pub fn begin_commit(&self, txn_id: TxnId) -> Result<(), LockError> {
        self.begin_completion(txn_id, TransactionPhase::Committing)
    }

    /// Starts full rollback without releasing any granted locks.
    ///
    /// A queued request is canceled and the transaction may no longer acquire
    /// resources. Repeating this call while already aborting is idempotent.
    /// Previously granted locks remain held until [`Self::finish_transaction`].
    ///
    /// # Errors
    ///
    /// Returns an active-transaction, incompatible-phase, poison, or invariant
    /// error if rollback cannot safely begin.
    pub fn begin_rollback(&self, txn_id: TxnId) -> Result<(), LockError> {
        self.begin_completion(txn_id, TransactionPhase::Aborting)
    }

    /// Releases all locks and removes a finalized transaction.
    ///
    /// The caller must invoke this only after the commit is durable or all
    /// rollback work has completed. Resources are released in ascending
    /// [`TableId`] order, with each release handing ownership directly to the
    /// FIFO queue front before waking waiters.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::FinalizationNotStarted`] while the transaction is
    /// still growing, [`LockError::TransactionNotActive`] if it is unknown, or
    /// an explicit poison/invariant error. On failure callers must not assume
    /// that the transaction completed or that its locks were safely released.
    pub fn finish_transaction(&self, txn_id: TxnId) -> Result<(), LockError> {
        let tables = {
            let graph = self.lock_graph()?;
            let transaction = graph
                .transactions
                .get(&txn_id)
                .ok_or(LockError::TransactionNotActive { txn_id })?;
            if transaction.phase == TransactionPhase::Growing {
                return Err(LockError::FinalizationNotStarted { txn_id });
            }
            if transaction.waiting_for.is_some() {
                return Err(invariant("finalizing transaction still has a queued request"));
            }
            transaction.locks.iter().copied().collect::<Vec<_>>()
        };

        for table_id in tables {
            self.release_owned(txn_id, table_id)?;
        }

        let mut graph = self.lock_graph()?;
        let transaction =
            graph.transactions.get(&txn_id).ok_or(LockError::TransactionNotActive { txn_id })?;
        if !transaction.locks.is_empty() || transaction.waiting_for.is_some() {
            return Err(invariant("completed transaction remains in lock-manager state"));
        }
        let request_canceled = transaction.request_canceled;
        if graph.waits_for.remove(txn_id).is_some() {
            return Err(invariant("non-waiting transaction had an outgoing wait edge"));
        }
        if request_canceled {
            graph
                .transactions
                .get_mut(&txn_id)
                .ok_or(LockError::TransactionNotActive { txn_id })?
                .finalization_finished = true;
        } else {
            graph.transactions.remove(&txn_id);
            if graph.ddl_owner == Some(txn_id) {
                graph.ddl_owner = None;
            }
        }
        Ok(())
    }

    /// Moves a transaction into a completion phase and cancels its waiter.
    ///
    /// The phase is changed before looking up the queue. If ownership handoff
    /// wins that interval, the granted lock remains in the transaction lock set
    /// for normal finalization.
    fn begin_completion(
        &self,
        txn_id: TxnId,
        requested_phase: TransactionPhase,
    ) -> Result<(), LockError> {
        let waiting_for = {
            let mut graph = self.lock_graph()?;
            let transaction = graph
                .transactions
                .get_mut(&txn_id)
                .ok_or(LockError::TransactionNotActive { txn_id })?;
            match (transaction.phase, requested_phase) {
                (TransactionPhase::Growing, phase) => transaction.phase = phase,
                (TransactionPhase::Aborting, TransactionPhase::Aborting) => {}
                (phase, requested) if phase == requested => {}
                (phase, _) => return Err(LockError::TransactionNotGrowing { txn_id, phase }),
            }
            transaction.waiting_for
        };

        if let Some(table_id) = waiting_for {
            let queue = self.queue(table_id)?;
            let mut queue_state = lock(&queue.state, "table queue")?;
            let mut graph = self.lock_graph()?;
            let removed =
                remove_waiter(&mut queue_state, &mut graph, LockRequest { txn_id, table_id })?;
            drop(graph);
            drop(queue_state);
            if removed {
                queue.changed.notify_all();
            }
        }
        Ok(())
    }

    /// Releases one owned resource and directly hands it to the FIFO front.
    ///
    /// Queue state is latched before graph state. The successor becomes owner,
    /// loses its outgoing edge, and gains the resource in its transaction lock
    /// set before any condition-variable notification.
    fn release_owned(&self, txn_id: TxnId, table_id: TableId) -> Result<(), LockError> {
        let queue = self.queue(table_id)?;
        let mut queue_state = lock(&queue.state, "table queue")?;
        let mut graph = self.lock_graph()?;

        if queue_state.owner != Some(txn_id) {
            return Err(invariant("transaction attempted to release a table owned by another"));
        }
        let transaction = graph
            .transactions
            .get_mut(&txn_id)
            .ok_or(LockError::TransactionNotActive { txn_id })?;
        if !transaction.locks.remove(&table_id) {
            return Err(invariant("released table was absent from transaction lock set"));
        }
        queue_state.owner = None;

        if let Some(next_owner) = queue_state.waiters.pop_front() {
            queue_state.owner = Some(next_owner);
            if graph.waits_for.remove(next_owner).is_none() {
                return Err(invariant("queue-front waiter had no wait-for edge"));
            }
            let next = graph
                .transactions
                .get_mut(&next_owner)
                .ok_or_else(|| invariant("queue-front waiter is not an active transaction"))?;
            if next.waiting_for != Some(table_id) {
                return Err(invariant("queue-front waiter references a different table"));
            }
            next.waiting_for = None;
            next.locks.insert(table_id);
        }

        drop(graph);
        drop(queue_state);
        queue.changed.notify_all();
        Ok(())
    }

    /// Finds or lazily creates the stable queue for `table_id`.
    ///
    /// The map latch is released when this method returns and is never held
    /// while a queue latch is acquired. Empty queues are intentionally retained.
    fn queue(&self, table_id: TableId) -> Result<Arc<LockQueue>, LockError> {
        let mut queues = lock(&self.inner.queues, "lock map")?;
        Ok(Arc::clone(queues.entry(table_id).or_default()))
    }

    /// Acquires the transaction-registry and wait-for-graph latch.
    fn lock_graph(&self) -> Result<MutexGuard<'_, GraphState>, LockError> {
        lock(&self.inner.graph, "wait-for graph")
    }

    /// Arms a Loom-only notification for the next successfully queued waiter.
    #[cfg(all(test, feature = "loom"))]
    fn signal_next_enqueue(&self) -> Result<loom::sync::mpsc::Receiver<()>, LockError> {
        let (send, receive) = loom::sync::mpsc::channel();
        let mut signal = lock(&self.inner.enqueue_signal, "Loom enqueue signal")?;
        if signal.replace(send).is_some() {
            return Err(invariant("a Loom enqueue signal is already armed"));
        }
        Ok(receive)
    }

    /// Verifies queue, ownership, transaction-set, and graph agreement in tests.
    ///
    /// Callers use this only after worker threads join because its diagnostic
    /// multi-queue traversal does not follow the production latch protocol.
    #[cfg(all(test, not(feature = "loom")))]
    fn assert_invariants(&self) {
        let queues = self.inner.queues.lock().unwrap();
        let graph = self.inner.graph.lock().unwrap();
        assert!(!graph.waits_for.has_cycle());
        let mut waiting = HashSet::new();
        for (table_id, queue) in queues.iter() {
            let queue = queue.state.lock().unwrap();
            if !queue.waiters.is_empty() {
                assert!(queue.owner.is_some());
            }
            let mut predecessor = queue.owner;
            for waiter in &queue.waiters {
                assert!(waiting.insert(*waiter));
                assert_eq!(graph.waits_for.predecessor(*waiter), predecessor);
                assert_eq!(graph.transactions[waiter].waiting_for, Some(*table_id));
                predecessor = Some(*waiter);
            }
            if let Some(owner) = queue.owner {
                assert!(graph.transactions[&owner].locks.contains(table_id));
                assert!(!queue.waiters.contains(&owner));
            }
        }
        for (txn_id, transaction) in &graph.transactions {
            assert_eq!(transaction.waiting_for.is_some(), waiting.contains(txn_id));
            assert_eq!(graph.waits_for.contains(*txn_id), waiting.contains(txn_id));
        }
    }
}

/// Returns the predecessor for a new FIFO waiter after validating queue state.
fn queue_predecessor(queue: &QueueState, waits_for: &WaitForGraph) -> Result<TxnId, LockError> {
    if queue.owner.is_none() {
        return Err(invariant("a table has waiters but no owner"));
    }
    if waits_for.has_cycle() {
        return Err(invariant("wait-for graph was cyclic before adding a request"));
    }
    queue
        .waiters
        .back()
        .copied()
        .or(queue.owner)
        .ok_or_else(|| invariant("queued request has no predecessor"))
}

/// Removes a newly appended request that closed a cycle and marks its victim.
fn remove_deadlock_request(
    queue: &mut QueueState,
    graph: &mut GraphState,
    txn_id: TxnId,
) -> Result<(), LockError> {
    if queue.waiters.pop_back() != Some(txn_id) {
        return Err(invariant("new deadlock victim was not the queue tail"));
    }
    graph.waits_for.remove(txn_id);
    let transaction =
        graph.transactions.get_mut(&txn_id).ok_or(LockError::TransactionNotActive { txn_id })?;
    transaction.waiting_for = None;
    transaction.phase = TransactionPhase::Aborting;
    Ok(())
}

/// Validates transaction state installed by direct ownership handoff.
fn validate_handoff(transaction: &TransactionState, request: LockRequest) -> Result<(), LockError> {
    if !transaction.locks.contains(&request.table_id) || transaction.waiting_for.is_some() {
        return Err(invariant("ownership handoff did not update transaction state"));
    }
    if transaction.phase != TransactionPhase::Growing {
        return Err(LockError::Canceled { txn_id: request.txn_id });
    }
    Ok(())
}

/// Returns a mutable active transaction only if it may acquire new locks.
fn active_growing_transaction(
    graph: &mut GraphState,
    txn_id: TxnId,
) -> Result<&mut TransactionState, LockError> {
    let transaction =
        graph.transactions.get_mut(&txn_id).ok_or(LockError::TransactionNotActive { txn_id })?;
    if transaction.phase != TransactionPhase::Growing {
        return Err(LockError::TransactionNotGrowing { txn_id, phase: transaction.phase });
    }
    Ok(transaction)
}

/// Removes a waiter and repairs the FIFO predecessor edge of its successor.
///
/// The caller must hold this table's queue latch followed by the graph latch.
/// Returning `false` means direct handoff won the race and the transaction now
/// owns the resource instead of appearing in the waiter deque.
fn remove_waiter(
    queue: &mut QueueState,
    graph: &mut GraphState,
    request: LockRequest,
) -> Result<bool, LockError> {
    let LockRequest { txn_id, table_id } = request;
    let Some(index) = queue.waiters.iter().position(|waiter| *waiter == txn_id) else {
        // Ownership handoff won the race.  The lock remains transaction-owned.
        return Ok(false);
    };
    let predecessor = if index == 0 { queue.owner } else { queue.waiters.get(index - 1).copied() }
        .ok_or_else(|| invariant("removed waiter had no predecessor"))?;

    queue.waiters.remove(index);
    if let Some(successor) = queue.waiters.get(index).copied() {
        graph.waits_for.insert(successor, predecessor);
    }
    if graph.waits_for.remove(txn_id).is_none() {
        return Err(invariant("removed waiter had no wait-for edge"));
    }
    let transaction =
        graph.transactions.get_mut(&txn_id).ok_or(LockError::TransactionNotActive { txn_id })?;
    if transaction.waiting_for != Some(table_id) {
        return Err(invariant("removed waiter references a different table"));
    }
    transaction.waiting_for = None;
    transaction.request_canceled = true;
    Ok(true)
}

/// Acquires a manager latch and converts poisoning into a typed error.
fn lock<'a, T>(mutex: &'a Mutex<T>, name: &'static str) -> Result<MutexGuard<'a, T>, LockError> {
    mutex.lock().map_err(|_poisoned| LockError::Poisoned { mutex: name })
}

/// Constructs a typed internal-consistency failure.
fn invariant(message: impl Into<String>) -> LockError {
    LockError::Invariant { message: message.into() }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::{
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use super::*;

    fn manager_with_transactions(ids: &[TxnId]) -> Arc<LockManager> {
        let manager = Arc::new(LockManager::new());
        for txn_id in ids {
            manager.begin_transaction(*txn_id).unwrap();
        }
        manager
    }

    fn rollback(manager: &LockManager, txn_id: TxnId) {
        manager.begin_rollback(txn_id).unwrap();
        manager.finish_transaction(txn_id).unwrap();
    }

    fn wait_until_waiting(manager: &LockManager, txn_id: TxnId) {
        for _ in 0..10_000 {
            let waiting = manager
                .inner
                .graph
                .lock()
                .unwrap()
                .transactions
                .get(&txn_id)
                .is_some_and(|transaction| transaction.waiting_for.is_some());
            if waiting {
                return;
            }
            thread::sleep(Duration::from_micros(10));
        }
        panic!("transaction did not enter its wait queue");
    }

    #[test]
    fn uncontended_and_repeated_acquisition_are_idempotent() {
        let manager = manager_with_transactions(&[1]);
        {
            let first = manager.acquire(1, TableId::new(7)).unwrap();
            let second = manager.acquire(1, TableId::new(7)).unwrap();

            assert_eq!(first.table_id(), second.table_id());
            assert_eq!(manager.inner.graph.lock().unwrap().transactions[&1].locks.len(), 1);
        }
        assert_eq!(
            manager.inner.queues.lock().unwrap()[&TableId::new(7)].state.lock().unwrap().owner,
            Some(1)
        );
        manager.assert_invariants();
    }

    #[test]
    fn different_tables_can_be_owned_concurrently() {
        let manager = manager_with_transactions(&[1, 2]);
        let left = manager.acquire(1, 10).unwrap();
        let right = manager.acquire(2, 20).unwrap();
        assert_eq!(left.table_id(), TableId::new(10));
        assert_eq!(right.table_id(), TableId::new(20));
        manager.assert_invariants();
    }

    #[test]
    fn ddl_gate_requires_the_sole_active_transaction() {
        let manager = LockManager::new();
        manager.begin_transaction(1).unwrap();
        assert_eq!(manager.begin_ddl_transaction(2), Err(LockError::DdlBusy { txn_id: 2 }));
        rollback(&manager, 1);

        manager.begin_ddl_transaction(2).unwrap();
        assert_eq!(manager.begin_transaction(3), Err(LockError::DdlBusy { txn_id: 3 }));
        manager.acquire(2, 7).unwrap(); // CREATE INDEX parent-table lock.
        rollback(&manager, 2);
        manager.begin_transaction(3).unwrap();
    }

    #[test]
    fn same_table_requests_are_exclusive_and_fifo_without_barging() {
        let manager = manager_with_transactions(&[1, 2, 3, 4, 5]);
        manager.acquire(1, 7).unwrap();
        let (send, receive) = mpsc::channel();
        let mut threads = Vec::new();

        for txn_id in 2..=4 {
            let child_manager = Arc::clone(&manager);
            let send = send.clone();
            threads.push(thread::spawn(move || {
                let _lease = child_manager.acquire(txn_id, 7).unwrap();
                send.send(txn_id).unwrap();
                rollback(&child_manager, txn_id);
            }));
            wait_until_waiting(&manager, txn_id);
        }

        manager.begin_rollback(1).unwrap();
        manager.finish_transaction(1).unwrap();

        // Arrive during the handoff sequence; transaction 5 cannot pass 2..4.
        let manager_five = Arc::clone(&manager);
        let send_five = send.clone();
        threads.push(thread::spawn(move || {
            manager_five.acquire(5, 7).unwrap();
            send_five.send(5).unwrap();
            rollback(&manager_five, 5);
        }));

        let order = (0..4)
            .map(|_| receive.recv_timeout(Duration::from_secs(2)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(order, vec![2, 3, 4, 5]);
        for thread in threads {
            thread.join().unwrap();
        }
        manager.assert_invariants();
    }

    #[test]
    fn commit_releases_only_after_successful_finalization() {
        let manager = manager_with_transactions(&[1, 2]);
        manager.acquire(1, 7).unwrap();
        manager.begin_commit(1).unwrap();

        let waiting_manager = Arc::clone(&manager);
        let waiter = thread::spawn(move || waiting_manager.acquire(2, 7));
        wait_until_waiting(&manager, 2);
        assert_eq!(
            manager.inner.queues.lock().unwrap()[&TableId::new(7)].state.lock().unwrap().owner,
            Some(1)
        );

        manager.finish_transaction(1).unwrap();
        let lease = waiter.join().unwrap().unwrap();
        assert_eq!(lease.transaction_id(), 2);
        rollback(&manager, 2);
    }

    #[test]
    fn two_transaction_deadlock_selects_closing_request_and_retains_old_lock() {
        let manager = manager_with_transactions(&[1, 2]);
        manager.acquire(1, 10).unwrap();
        manager.acquire(2, 20).unwrap();

        let first_manager = Arc::clone(&manager);
        let first = thread::spawn(move || first_manager.acquire(1, 20));
        wait_until_waiting(&manager, 1);

        assert_eq!(manager.acquire(2, 10), Err(LockError::Deadlock { txn_id: 2 }));
        assert_eq!(manager.transaction_phase(2).unwrap(), TransactionPhase::Aborting);
        assert_eq!(
            manager.inner.queues.lock().unwrap()[&TableId::new(20)].state.lock().unwrap().owner,
            Some(2)
        );

        // Full rollback completion, not deadlock detection, releases table 20.
        manager.begin_rollback(2).unwrap();
        manager.finish_transaction(2).unwrap();
        assert_eq!(first.join().unwrap().unwrap().transaction_id(), 1);
        rollback(&manager, 1);
        manager.assert_invariants();
    }

    #[test]
    fn three_transaction_cycle_is_detected() {
        let manager = manager_with_transactions(&[1, 2, 3]);
        manager.acquire(1, 10).unwrap();
        manager.acquire(2, 20).unwrap();
        manager.acquire(3, 30).unwrap();

        let m1 = Arc::clone(&manager);
        let t1 = thread::spawn(move || m1.acquire(1, 20));
        wait_until_waiting(&manager, 1);
        let m2 = Arc::clone(&manager);
        let t2 = thread::spawn(move || m2.acquire(2, 30));
        wait_until_waiting(&manager, 2);

        assert_eq!(manager.acquire(3, 10), Err(LockError::Deadlock { txn_id: 3 }));
        rollback(&manager, 3);
        assert!(t2.join().unwrap().is_ok());
        rollback(&manager, 2);
        assert!(t1.join().unwrap().is_ok());
        rollback(&manager, 1);
        manager.assert_invariants();
    }

    #[test]
    fn fifo_predecessor_edge_participates_in_deadlock_detection() {
        let manager = manager_with_transactions(&[1, 2, 3]);
        manager.acquire(1, 10).unwrap();
        manager.acquire(3, 20).unwrap();

        let m2 = Arc::clone(&manager);
        let t2 = thread::spawn(move || m2.acquire(2, 10));
        wait_until_waiting(&manager, 2); // 2 -> 1
        let m3 = Arc::clone(&manager);
        let t3 = thread::spawn(move || m3.acquire(3, 10));
        wait_until_waiting(&manager, 3); // 3 -> 2, not directly 3 -> 1

        // The complete cycle is 1 -> 3 -> 2 -> 1.  It exists only when the
        // FIFO predecessor edge is modeled rather than linking every waiter
        // directly to the table owner.
        assert_eq!(manager.acquire(1, 20), Err(LockError::Deadlock { txn_id: 1 }));
        rollback(&manager, 1);
        assert!(t2.join().unwrap().is_ok());
        rollback(&manager, 2);
        assert!(t3.join().unwrap().is_ok());
        rollback(&manager, 3);
        manager.assert_invariants();
    }

    #[test]
    fn canceling_middle_and_front_waiters_repairs_fifo_dependencies() {
        let manager = manager_with_transactions(&[1, 2, 3, 4]);
        manager.acquire(1, 7).unwrap();
        let mut threads = HashMap::new();
        for txn_id in 2..=4 {
            let child = Arc::clone(&manager);
            threads.insert(txn_id, thread::spawn(move || child.acquire(txn_id, 7)));
            wait_until_waiting(&manager, txn_id);
        }

        assert!(manager.cancel_waiting(3).unwrap());
        assert_eq!(manager.inner.graph.lock().unwrap().waits_for.predecessor(4), Some(2));
        assert!(manager.cancel_waiting(2).unwrap());
        assert_eq!(manager.inner.graph.lock().unwrap().waits_for.predecessor(4), Some(1));
        assert!(matches!(
            threads.remove(&2).unwrap().join().unwrap(),
            Err(LockError::Canceled { txn_id: 2 })
        ));
        assert!(matches!(
            threads.remove(&3).unwrap().join().unwrap(),
            Err(LockError::Canceled { txn_id: 3 })
        ));

        rollback(&manager, 1);
        assert!(threads.remove(&4).unwrap().join().unwrap().is_ok());
        rollback(&manager, 4);
        rollback(&manager, 2);
        rollback(&manager, 3);
        manager.assert_invariants();
    }

    #[test]
    fn finalization_retains_a_canceled_transaction_until_its_waiter_exits() {
        let manager = manager_with_transactions(&[1, 2]);
        manager.acquire(1, 7).unwrap();

        let waiter_manager = Arc::clone(&manager);
        let waiter = thread::spawn(move || waiter_manager.acquire(2, 7));
        wait_until_waiting(&manager, 2);

        // A canceled request must inspect its transaction state after waking.
        // If finalization removes that state first, the waiter reports
        // TransactionNotActive instead of Canceled; reusing the ID in this
        // window can make the stale waiter inspect the new transaction.
        // Hold the queue latch so the waiter cannot wake until finalization has
        // exercised this ordering.
        let queue = manager.queue(TableId::new(7)).unwrap();
        let mut queue_state = queue.state.lock().unwrap();
        let mut graph = manager.inner.graph.lock().unwrap();
        graph.transactions.get_mut(&2).unwrap().phase = TransactionPhase::Aborting;
        assert!(
            remove_waiter(
                &mut queue_state,
                &mut graph,
                LockRequest { txn_id: 2, table_id: TableId::new(7) },
            )
            .unwrap()
        );
        drop(graph);

        manager.finish_transaction(2).unwrap();
        queue.changed.notify_all();
        drop(queue_state);

        assert_eq!(waiter.join().unwrap(), Err(LockError::Canceled { txn_id: 2 }));
        rollback(&manager, 1);
        manager.assert_invariants();
    }

    #[test]
    fn cancellation_racing_handoff_leaves_a_single_owner() {
        for iteration in 0..100 {
            let manager = manager_with_transactions(&[1, 2]);
            manager.acquire(1, 7).unwrap();
            let waiter_manager = Arc::clone(&manager);
            let waiter = thread::spawn(move || waiter_manager.acquire(2, 7));
            wait_until_waiting(&manager, 2);

            let barrier = Arc::new(Barrier::new(3));
            let cancel_manager = Arc::clone(&manager);
            let cancel_barrier = Arc::clone(&barrier);
            let cancel = thread::spawn(move || {
                cancel_barrier.wait();
                cancel_manager.cancel_waiting(2)
            });
            let release_manager = Arc::clone(&manager);
            let release_barrier = Arc::clone(&barrier);
            let release = thread::spawn(move || {
                release_barrier.wait();
                rollback(&release_manager, 1);
            });
            barrier.wait();
            release.join().unwrap();
            let canceled = cancel.join().unwrap().unwrap();
            let result = waiter.join().unwrap();

            if canceled {
                assert!(
                    matches!(result, Err(LockError::Canceled { txn_id: 2 })),
                    "iteration {iteration}"
                );
            } else {
                assert!(result.is_ok(), "iteration {iteration}");
            }
            rollback(&manager, 2);
            manager.assert_invariants();
        }
    }

    #[test]
    fn lease_checks_parent_table_and_does_not_release_on_drop_or_savepoint() {
        let manager = manager_with_transactions(&[1]);
        {
            let lease = manager.acquire(1, 42).unwrap();
            assert!(lease.authorize(1, 42).is_ok());
            assert!(matches!(lease.authorize(1, 43), Err(LockError::LeaseMismatch { .. })));
        } // Statement/cursor/savepoint lifetime has no release behavior.
        assert_eq!(
            manager.inner.queues.lock().unwrap()[&TableId::new(42)].state.lock().unwrap().owner,
            Some(1)
        );
        rollback(&manager, 1);
    }
}

#[cfg(all(test, feature = "loom"))]
#[allow(clippy::panic, clippy::unwrap_used)]
mod loom_tests {
    use loom::{
        model::Builder,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };

    use super::*;

    fn check_model(model: impl Fn() + Send + Sync + 'static) {
        let mut builder = Builder::new();
        // These protocols are small, but condition-variable wakeups can create
        // many equivalent schedules. Two preemptions cover the races targeted
        // by these tests while keeping the suite suitable for routine use.
        builder.preemption_bound = Some(2);
        builder.max_branches = 200;
        builder.check(model);
    }

    fn rollback(manager: &LockManager, txn_id: TxnId) {
        manager.begin_rollback(txn_id).unwrap();
        manager.finish_transaction(txn_id).unwrap();
    }

    fn assert_quiescent(manager: &LockManager) {
        let graph = manager.inner.graph.lock().unwrap();
        assert!(graph.transactions.is_empty());
        assert!(graph.waits_for.is_empty());
        drop(graph);

        let queues = manager.inner.queues.lock().unwrap();
        for queue in queues.values() {
            let state = queue.state.lock().unwrap();
            assert_eq!(state.owner, None);
            assert!(state.waiters.is_empty());
        }
    }

    #[test]
    fn concurrent_first_requests_do_not_create_split_queues_or_multiple_owners() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();

            let first = manager.clone();
            let first = thread::spawn(move || {
                let _lease = first.acquire(1, 7).unwrap();
                rollback(&first, 1);
            });
            let second = manager.clone();
            let second = thread::spawn(move || {
                let _lease = second.acquire(2, 7).unwrap();
                rollback(&second, 2);
            });

            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(manager.inner.queues.lock().unwrap().len(), 1);
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn queued_request_is_removed_and_woken_by_cancellation() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();
            manager.acquire(1, 7).unwrap();

            let enqueued = manager.signal_next_enqueue().unwrap();
            let waiter_manager = manager.clone();
            let waiter = thread::spawn(move || waiter_manager.acquire(2, 7));
            enqueued.recv().unwrap();

            assert!(manager.cancel_waiting(2).unwrap());
            assert!(matches!(waiter.join().unwrap(), Err(LockError::Canceled { txn_id: 2 })));
            rollback(&manager, 1);
            rollback(&manager, 2);
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn cancellation_racing_with_handoff_has_no_lost_waiter_or_owner() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();
            manager.acquire(1, 7).unwrap();

            // Ensure cancellation and handoff race over an actual queued
            // request rather than allowing cancellation to run too early.
            let enqueued = manager.signal_next_enqueue().unwrap();
            // Keep transaction 2 active until cancellation has observed the
            // race outcome, even if handoff and acquisition complete first.
            let (cancel_send, cancel_receive) = mpsc::channel();
            let waiter_manager = manager.clone();
            let waiter = thread::spawn(move || {
                let acquired = match waiter_manager.acquire(2, 7) {
                    Ok(_lease) => true,
                    Err(LockError::Canceled { txn_id: 2 }) => false,
                    Err(error) => panic!("unexpected waiter result: {error}"),
                };
                let removed = cancel_receive.recv().unwrap();
                rollback(&waiter_manager, 2);
                (acquired, removed)
            });
            enqueued.recv().unwrap();

            let cancel_manager = manager.clone();
            let cancel = thread::spawn(move || {
                let removed = cancel_manager.cancel_waiting(2).unwrap();
                cancel_send.send(removed).unwrap();
            });
            let release_manager = manager.clone();
            let release = thread::spawn(move || rollback(&release_manager, 1));

            cancel.join().unwrap();
            release.join().unwrap();
            let (acquired, removed) = waiter.join().unwrap();
            assert_ne!(acquired, removed);
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn ddl_and_ordinary_transaction_admission_never_overlap() {
        check_model(|| {
            let manager = LockManager::new();
            let admitted = Arc::new(AtomicUsize::new(0));

            let ordinary_manager = manager.clone();
            let ordinary_admitted = Arc::clone(&admitted);
            let ordinary = thread::spawn(move || match ordinary_manager.begin_transaction(1) {
                Ok(()) => {
                    assert_eq!(ordinary_admitted.fetch_add(1, Ordering::SeqCst), 0);
                    thread::yield_now();
                    ordinary_admitted.fetch_sub(1, Ordering::SeqCst);
                    rollback(&ordinary_manager, 1);
                    true
                }
                Err(LockError::DdlBusy { txn_id: 1 }) => false,
                Err(error) => panic!("unexpected ordinary admission result: {error}"),
            });

            let ddl_manager = manager.clone();
            let ddl_admitted = Arc::clone(&admitted);
            let ddl = thread::spawn(move || match ddl_manager.begin_ddl_transaction(2) {
                Ok(()) => {
                    assert_eq!(ddl_admitted.fetch_add(1, Ordering::SeqCst), 0);
                    thread::yield_now();
                    ddl_admitted.fetch_sub(1, Ordering::SeqCst);
                    rollback(&ddl_manager, 2);
                    true
                }
                Err(LockError::DdlBusy { txn_id: 2 }) => false,
                Err(error) => panic!("unexpected DDL admission result: {error}"),
            });

            let ordinary_won = ordinary.join().unwrap();
            let ddl_won = ddl.join().unwrap();
            assert!(ordinary_won || ddl_won);
            assert_eq!(admitted.load(Ordering::SeqCst), 0);
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn rollback_start_racing_with_handoff_retains_any_granted_lock_until_finish() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();
            manager.acquire(1, 7).unwrap();

            let enqueued = manager.signal_next_enqueue().unwrap();
            let waiter_manager = manager.clone();
            let waiter = thread::spawn(move || match waiter_manager.acquire(2, 7) {
                Ok(_lease) => 0,
                Err(LockError::Canceled { txn_id: 2 }) => 1,
                Err(LockError::TransactionNotGrowing {
                    txn_id: 2,
                    phase: TransactionPhase::Aborting,
                }) => 2,
                Err(error) => panic!("unexpected waiter result during rollback: {error}"),
            });
            enqueued.recv().unwrap();
            let abort_manager = manager.clone();
            let abort = thread::spawn(move || abort_manager.begin_rollback(2));
            let release_manager = manager.clone();
            let release = thread::spawn(move || rollback(&release_manager, 1));

            abort.join().unwrap().unwrap();
            release.join().unwrap();
            let outcome = waiter.join().unwrap();
            assert!(outcome <= 2);
            assert_eq!(manager.transaction_phase(2).unwrap(), TransactionPhase::Aborting);

            // A successful acquisition proves handoff completed before rollback
            // started. Its lock must remain owned until explicit finalization.
            if outcome == 0 {
                assert!(
                    manager.inner.graph.lock().unwrap().transactions[&2]
                        .locks
                        .contains(&TableId::new(7))
                );
                assert_eq!(
                    manager.inner.queues.lock().unwrap()[&TableId::new(7)]
                        .state
                        .lock()
                        .unwrap()
                        .owner,
                    Some(2)
                );
            }
            manager.finish_transaction(2).unwrap();
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn commit_finish_hands_a_contended_table_to_the_waiter() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();
            manager.acquire(1, 7).unwrap();
            manager.begin_commit(1).unwrap();

            let enqueued = manager.signal_next_enqueue().unwrap();
            let waiter_manager = manager.clone();
            let waiter = thread::spawn(move || {
                let lease = waiter_manager.acquire(2, 7).unwrap();
                assert_eq!(lease.transaction_id(), 2);
                rollback(&waiter_manager, 2);
            });
            enqueued.recv().unwrap();
            let commit_manager = manager.clone();
            let commit = thread::spawn(move || commit_manager.finish_transaction(1));

            commit.join().unwrap().unwrap();
            waiter.join().unwrap();
            assert_quiescent(&manager);
        });
    }

    #[test]
    fn concurrent_cycle_requests_abort_exactly_the_closing_request() {
        check_model(|| {
            let manager = LockManager::new();
            manager.begin_transaction(1).unwrap();
            manager.begin_transaction(2).unwrap();
            manager.acquire(1, 10).unwrap();
            manager.acquire(2, 20).unwrap();

            let first_manager = manager.clone();
            let first = thread::spawn(move || {
                let victim = match first_manager.acquire(1, 20) {
                    Ok(_lease) => false,
                    Err(LockError::Deadlock { txn_id: 1 }) => true,
                    Err(error) => panic!("unexpected first request result: {error}"),
                };
                rollback(&first_manager, 1);
                victim
            });
            let second_manager = manager.clone();
            let second = thread::spawn(move || {
                let victim = match second_manager.acquire(2, 10) {
                    Ok(_lease) => false,
                    Err(LockError::Deadlock { txn_id: 2 }) => true,
                    Err(error) => panic!("unexpected second request result: {error}"),
                };
                rollback(&second_manager, 2);
                victim
            });

            let first_victim = first.join().unwrap();
            let second_victim = second.join().unwrap();
            assert_ne!(first_victim, second_victim);
            assert_quiescent(&manager);
        });
    }
}
