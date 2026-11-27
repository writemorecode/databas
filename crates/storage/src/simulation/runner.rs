use std::{io, path::Path};

use crate::{
    PageId, Storage, StorageOptions,
    error::{InternalError, InvariantViolation, StorageError},
    filesystem::{
        FileSystem, OpenMode,
        memory::{Effect, Fault, Hit, MemoryFs, Operation},
    },
};

use super::{
    RunError, RunResult, Trace, oracle,
    workload::{self, CLIENTS, Changes, Client, Command, Contents, Rng, TREES, Transaction},
};

const DATABASE: &str = "storage.db";
const OPTIONS: StorageOptions = StorageOptions { cache_frames: 64 };

pub struct Simulation {
    rng: Rng,
    faults: Rng,
    fs: MemoryFs,
    storage: Option<Storage>,
    roots: Vec<PageId>,
    clients: Vec<Client>,
    model: Vec<Contents>,
    pending_commit: Option<Changes>,
    recovery_cuts_left: u64,
    checkpoint_at: u64,
    writeback_at: u64,
    fault_at: u64,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    gets: u64,
    sets: u64,
    deletes: u64,
    commits: u64,
    rollbacks: u64,
    savepoint_rollbacks: u64,
    checkpoints: u64,
    partial_checkpoints: u64,
    writebacks: u64,
    short_io: u64,
    io_errors: u64,
    power_losses: u64,
    recoveries: u64,
    interrupted_commits_kept: u64,
    interrupted_commits_lost: u64,
    recovery_cuts: u64,
}

impl Simulation {
    pub fn new(seed: u64) -> RunResult<Self> {
        let mut rng = Rng::new(seed);
        let fs = MemoryFs::default();
        let storage = open(&fs, OpenMode::CreateNew)?;
        let transaction = storage.begin_transaction()?;
        let roots = (0..TREES)
            .map(|_| {
                storage.transaction_create_tree(transaction).map(|cursor| cursor.root_page_id())
            })
            .collect::<Result<_, _>>()?;
        storage.commit_transaction(transaction)?;
        storage.checkpoint()?;
        let faults = Rng::new(rng.next());
        let clients = (0..CLIENTS).map(|_| Client::new(rng.next())).collect();
        Ok(Self {
            rng,
            faults,
            fs,
            storage: Some(storage),
            roots,
            clients,
            model: vec![Contents::new(); TREES],
            pending_commit: None,
            recovery_cuts_left: 0,
            checkpoint_at: 1_500,
            writeback_at: 120,
            fault_at: 3_000,
            stats: Stats::default(),
        })
    }

    pub fn step(&mut self, event: u64, trace: &Trace) -> RunResult<()> {
        let (client_at, client) = self
            .clients
            .iter()
            .enumerate()
            .map(|(id, client)| (client.ready, id))
            .min()
            .ok_or_else(|| RunError::Mismatch("simulation has no clients".into()))?;
        let now = client_at.min(self.checkpoint_at).min(self.writeback_at).min(self.fault_at);
        if self.storage.is_none() {
            trace.event(event, now, format_args!("reopen after simulated outage"));
            return self.recover(now, trace);
        }
        if now == self.fault_at {
            self.fault_at = now + 2_500 + self.faults.below(12_000);
            if self.fs.armed() {
                trace.event(event, now, format_args!("fault still armed"));
            } else {
                let fault = self.next_fault();
                trace.event(event, now, format_args!("arm {fault:?}"));
                self.fs.arm(fault);
            }
            return Ok(());
        }
        if now == self.writeback_at {
            self.writeback_at = now + 50 + self.rng.below(250);
            trace.event(event, now, format_args!("background writeback"));
            self.fs.writeback(self.rng.next(), 1 + self.rng.index(56));
            if self.rng.chance(8) {
                self.fs.writeback_directory(Path::new(DATABASE));
            }
            self.stats.writebacks += 1;
            return Ok(());
        }

        if now == self.checkpoint_at {
            self.checkpoint_at = now + 850 + self.rng.below(2_800);
            trace.event(event, now, format_args!("checkpoint"));
            let result = self.engine()?.checkpoint().map(|outcome| {
                self.stats.checkpoints += 1;
                if outcome == crate::CheckpointOutcome::Partial {
                    self.stats.partial_checkpoints += 1;
                }
            });
            return self.complete(result.map_err(RunError::from), None, trace);
        }

        let command = self.clients[client].plan(client);
        trace.event(event, now, format_args!("client {client} {command:?}"));
        self.clients[client].ready = now + self.clients[client].delay(&command);
        let commit = matches!(command, Command::Commit).then_some(client);
        let result = self.execute_command(client, command, trace);
        self.complete(result, commit, trace)
    }

    fn next_fault(&mut self) -> Fault {
        let rng = &mut self.faults;
        let effect = match rng.below(12) {
            0..=4 => Effect::PowerLoss { after: rng.chance(2) },
            5..=7 => Effect::Short {
                operation: if rng.chance(2) { Operation::Read } else { Operation::Write },
                limit: 1 + rng.index(2_048),
            },
            8 => Effect::Error {
                operation: Operation::Read,
                kind: io::ErrorKind::Other,
                after: false,
            },
            9 => Effect::Error {
                operation: Operation::Write,
                kind: io::ErrorKind::StorageFull,
                after: false,
            },
            10 => Effect::Error {
                operation: Operation::SyncFile,
                kind: io::ErrorKind::Other,
                after: rng.chance(2),
            },
            _ => Effect::Error {
                operation: if rng.chance(2) { Operation::Rename } else { Operation::SyncDirectory },
                kind: io::ErrorKind::Other,
                after: false,
            },
        };
        let skip = match effect {
            Effect::Error { operation: Operation::Rename | Operation::SyncDirectory, .. } => 0,
            _ => rng.below(24),
        };
        Fault { skip, effect }
    }

    fn execute_command(&mut self, client: usize, command: Command, trace: &Trace) -> RunResult<()> {
        match command {
            Command::Begin { trees, remaining, read_heavy } => {
                if self
                    .clients
                    .iter()
                    .filter_map(|client| client.transaction.as_ref())
                    .any(|tx| trees.iter().any(|tree| tx.uses(*tree)))
                {
                    trace.note(format_args!("client {client} waiting for a tree"));
                    self.clients[client].ready += 30;
                    return Ok(());
                }
                let storage = self.engine()?;
                let id = storage.begin_transaction()?;
                let cursors = trees
                    .into_iter()
                    .map(|tree| {
                        storage
                            .transaction_tree_cursor(id, self.roots[tree])
                            .map(|cursor| (tree, cursor))
                    })
                    .collect::<Result<_, _>>()?;
                self.clients[client].transaction = Some(Transaction {
                    id,
                    cursors,
                    writes: Changes::new(),
                    savepoint: None,
                    remaining,
                    read_heavy,
                });
            }
            Command::Commit => {
                let id = transaction(&mut self.clients[client])?.id;
                self.engine()?.commit_transaction(id)?;
            }
            Command::Rollback => {
                let id = transaction(&mut self.clients[client])?.id;
                self.engine()?.rollback_transaction(id)?;
                self.clients[client].transaction = None;
                self.stats.rollbacks += 1;
            }
            Command::Savepoint => {
                let id = transaction(&mut self.clients[client])?.id;
                let savepoint = self.engine()?.statement_savepoint(id)?;
                let tx = transaction(&mut self.clients[client])?;
                tx.savepoint = Some((savepoint, tx.writes.clone()));
                tx.remaining -= 1;
            }
            Command::RollbackToSavepoint => {
                let (savepoint, writes) = transaction(&mut self.clients[client])?
                    .savepoint
                    .take()
                    .ok_or_else(|| RunError::Mismatch("client has no savepoint".into()))?;
                self.engine()?.rollback_to_savepoint(savepoint)?;
                let tx = transaction(&mut self.clients[client])?;
                tx.writes = writes;
                tx.remaining -= 1;
                self.stats.savepoint_rollbacks += 1;
            }
            Command::Get { tree, key } => {
                let result = self.access(client, tree, key)?;
                let tx = transaction(&mut self.clients[client])?;
                let encoded = workload::key(key);
                let expected = tx
                    .writes
                    .get(&(tree, encoded.clone()))
                    .map_or_else(|| self.model[tree].get(&encoded), Option::as_ref);
                if result.as_ref() != expected {
                    return Err(RunError::Mismatch(format!(
                        "client {client} GET tree {tree} key {key}, expected length {:?}, got {:?}",
                        expected.map(Vec::len),
                        result.as_ref().map(Vec::len),
                    )));
                }
                self.stats.gets += 1;
            }
            Command::Set { tree, key, len, token } => {
                let prior = self.access(client, tree, key)?;
                let value = workload::value(len, token);
                let key = workload::key(key);
                let tx = transaction(&mut self.clients[client])?;
                let cursor = cursor(tx, tree)?;
                if prior.is_some() {
                    cursor.update(&key, &value)?;
                } else {
                    cursor.insert(&key, &value)?;
                }
                tx.writes.insert((tree, key), Some(value));
                self.stats.sets += 1;
            }
            Command::Delete { tree, key } => {
                let prior = self.access(client, tree, key)?;
                let tx = transaction(&mut self.clients[client])?;
                let key = workload::key(key);
                if prior.is_some() {
                    cursor(tx, tree)?.delete(&key)?;
                }
                tx.writes.insert((tree, key), None);
                self.stats.deletes += 1;
            }
        }
        Ok(())
    }

    fn access(&mut self, client: usize, tree: usize, key: u64) -> RunResult<Option<Vec<u8>>> {
        let tx = transaction(&mut self.clients[client])?;
        tx.remaining -= 1;
        let encoded = workload::key(key);
        let result = cursor(tx, tree)?.get(&encoded)?;
        Ok(result.map(|record| record.with_value(<[u8]>::to_vec)).transpose()?)
    }

    fn engine(&self) -> RunResult<&Storage> {
        self.storage.as_ref().ok_or_else(|| RunError::Mismatch("storage is offline".into()))
    }

    fn complete(
        &mut self,
        result: RunResult<()>,
        commit: Option<usize>,
        trace: &Trace,
    ) -> RunResult<()> {
        let hit = self.take_hit(trace);
        let power_loss = hit.is_some_and(|hit| matches!(hit.effect, Effect::PowerLoss { .. }));
        let io_error = match (&result, hit.map(|hit| hit.effect)) {
            (Err(RunError::Storage(StorageError::Io(error))), Some(Effect::Error { kind, .. })) => {
                error.kind() == kind
            }
            // A write guard can consume the original append error on drop.
            // Later work in the same command may observe either the poisoned
            // transaction or the poisoned WAL (for example during eviction).
            // Accept these only when this event actually injected a write error;
            // restart and let the recovery oracle verify the durable contents.
            (
                Err(RunError::Storage(StorageError::Internal(InternalError::InvariantViolation(
                    InvariantViolation::TransactionPoisoned { .. }
                    | InvariantViolation::WalPoisoned,
                )))),
                Some(Effect::Error { operation: Operation::Write, .. }),
            ) => true,
            _ => false,
        };
        if power_loss || io_error {
            if let Some(client) = commit {
                self.pending_commit = self.clients[client].transaction.take().map(|tx| tx.writes);
            }
            self.recovery_cuts_left =
                if self.faults.chance(9) { 1 + self.faults.below(3) } else { 0 };
            self.restart(power_loss);
            return Ok(());
        }
        result?;
        if let Some(client) = commit {
            let tx = self.clients[client]
                .transaction
                .take()
                .ok_or_else(|| RunError::Mismatch("committing client has no transaction".into()))?;
            oracle::apply(&mut self.model, tx.writes);
            self.stats.commits += 1;
        }
        Ok(())
    }

    fn restart(&mut self, power_loss: bool) {
        self.fs = self.fs.restart(power_loss);
        for client in &mut self.clients {
            client.transaction = None;
        }
        self.storage = None;
    }

    fn recover(&mut self, now: u64, trace: &Trace) -> RunResult<()> {
        if self.recovery_cuts_left > 0 {
            self.recovery_cuts_left -= 1;
            // Measure this restart without mutating the real device. A fixed
            // small skip only exercises opening/scanning, missing redo and undo.
            let probe = self.fs.fork_for_recovery();
            let before = probe.io_count();
            let _storage = open(&probe, OpenMode::Existing)?;
            let calls = probe.io_count() - before;
            let fault = Fault {
                skip: self.faults.below(calls),
                effect: Effect::PowerLoss { after: self.faults.chance(2) },
            };
            trace.note(format_args!("arm during recovery ({calls} calls) {fault:?}"));
            self.fs.arm(fault);
        }
        let opened = open(&self.fs, OpenMode::Existing);
        if self.take_hit(trace).is_some() {
            self.stats.recovery_cuts += 1;
            self.restart(true);
            return Ok(());
        }
        self.fs.disarm();
        self.recovery_cuts_left = 0;
        let storage = opened?;
        let pending = self.pending_commit.take();
        let ambiguous = pending.is_some();
        let kept = oracle::reconcile(&storage, &self.roots, &mut self.model, pending)?;
        if ambiguous {
            if kept {
                self.stats.interrupted_commits_kept += 1;
            } else {
                self.stats.interrupted_commits_lost += 1;
            }
        }
        self.stats.recoveries += 1;
        trace.note(format_args!("recovered {} trees", self.roots.len()));
        self.storage = Some(storage);
        for client in &mut self.clients {
            client.ready = client.ready.max(now) + client.rng.below(250);
        }
        Ok(())
    }

    fn take_hit(&mut self, trace: &Trace) -> Option<Hit> {
        let hit = self.fs.take_hit()?;
        trace.note(format_args!("I/O #{} {:?}, {:?}", hit.io, hit.operation, hit.effect));
        match hit.effect {
            Effect::PowerLoss { .. } => self.stats.power_losses += 1,
            Effect::Short { .. } => self.stats.short_io += 1,
            Effect::Error { .. } => self.stats.io_errors += 1,
        }
        Some(hit)
    }

    pub fn summary(&self) -> String {
        let s = &self.stats;
        format!(
            "{} gets, {} sets, {} deletes, {} commits, {} rollbacks, {} savepoint rollbacks, {} checkpoints ({} partial), {} writeback events, {} I/O calls, {} short I/O faults, {} I/O errors, {} power losses ({} during recovery), {} verified recoveries, {} interrupted commits visible and {} unchanged",
            s.gets,
            s.sets,
            s.deletes,
            s.commits,
            s.rollbacks,
            s.savepoint_rollbacks,
            s.checkpoints,
            s.partial_checkpoints,
            s.writebacks,
            self.fs.io_count(),
            s.short_io,
            s.io_errors,
            s.power_losses,
            s.recovery_cuts,
            s.recoveries,
            s.interrupted_commits_kept,
            s.interrupted_commits_lost,
        )
    }
}

fn open(fs: &MemoryFs, mode: OpenMode) -> Result<Storage, StorageError> {
    Storage::open_in(FileSystem::Memory(fs.clone()), Path::new(DATABASE), OPTIONS, mode)
}

fn transaction(client: &mut Client) -> RunResult<&mut Transaction> {
    client
        .transaction
        .as_mut()
        .ok_or_else(|| RunError::Mismatch("client has no transaction".into()))
}

fn cursor(tx: &mut Transaction, tree: usize) -> RunResult<&mut crate::TreeCursor> {
    tx.cursors
        .iter_mut()
        .find_map(|(id, cursor)| (*id == tree).then_some(cursor))
        .ok_or_else(|| RunError::Mismatch(format!("transaction does not own tree {tree}")))
}
