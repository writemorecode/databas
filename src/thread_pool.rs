//! Pool for running blocking jobs on reusable worker threads.
//! Periodic tasks share one additional, lazily created maintenance worker.
//!
//! Jobs are submitted through a bounded queue. Dropping or explicitly shutting
//! down the pool closes that queue, lets workers finish queued jobs, and joins
//! every worker thread.

use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc::{
            Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError, channel, sync_channel,
        },
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use thiserror::Error;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Failure while creating, using, or shutting down a thread pool.
#[derive(Debug, Error)]
pub enum ThreadPoolError {
    /// A pool must contain at least one worker.
    #[error("thread pool size must be greater than zero")]
    InvalidSize,
    /// Periodic tasks need a nonzero interval.
    #[error("periodic task interval must be greater than zero")]
    InvalidInterval,
    /// The job queue closed before a job could be submitted.
    #[error("thread pool job queue is closed")]
    QueueClosed,
    /// Nonblocking submission found no available queue capacity.
    #[error("thread pool job queue is full")]
    QueueFull,
    /// A worker panicked while executing a job.
    #[error("thread pool worker {worker_id} panicked")]
    WorkerPanicked { worker_id: usize },
}

/// A pool of ordinary workers with an optional reserved maintenance worker.
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<SyncSender<Job>>,
    maintenance_sender: Option<SyncSender<Job>>,
    timers: Arc<Mutex<Vec<Timer>>>,
}

struct Timer {
    cancel: Sender<()>,
    stopped: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

impl ThreadPool {
    /// Creates `size` workers and a bounded queue capable of holding `size` jobs.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadPoolError::InvalidSize`] when `size` is zero.
    pub fn new(size: usize) -> Result<Self, ThreadPoolError> {
        if size == 0 {
            return Err(ThreadPoolError::InvalidSize);
        }

        let (sender, receiver) = sync_channel(size);
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = (0..size).map(|id| Worker::new(id, Arc::clone(&receiver))).collect();
        Ok(Self {
            workers,
            sender: Some(sender),
            maintenance_sender: None,
            timers: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Schedules a task at fixed intervals on a reserved maintenance worker.
    /// Missed ticks are skipped, and invocations of one task never overlap.
    /// Dropping the returned handle cancels future invocations; an invocation
    /// already running is allowed to finish. Cancellation joins the timer thread,
    /// but does not wait for a running invocation. Shutdown cancels all timers.
    ///
    /// # Errors
    /// Returns `InvalidInterval` for a zero interval or `QueueClosed` after shutdown.
    pub fn execute_periodic<F>(
        &mut self,
        interval: Duration,
        job: F,
    ) -> Result<PeriodicTask, ThreadPoolError>
    where
        F: Fn() + Send + Sync + 'static,
    {
        if interval.is_zero() {
            return Err(ThreadPoolError::InvalidInterval);
        }
        if self.sender.is_none() {
            return Err(ThreadPoolError::QueueClosed);
        }
        let sender = self
            .maintenance_sender
            .get_or_insert_with(|| {
                let (sender, receiver) = sync_channel(1);
                self.workers.push(Worker::new(self.workers.len(), Arc::new(Mutex::new(receiver))));
                sender
            })
            .clone();
        let (cancel, cancellation) = channel();
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let pending = Arc::new(AtomicBool::new(false));
        let job = Arc::new(job);
        let timer = thread::spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = cancellation.recv_timeout(interval) {
                if task_stopped.load(Ordering::Acquire) {
                    break;
                }
                if pending.swap(true, Ordering::AcqRel) {
                    continue;
                }
                let pending_job = Arc::clone(&pending);
                let job = Arc::clone(&job);
                let stopped = Arc::clone(&task_stopped);
                let submission = sender.try_send(Box::new(move || {
                    if !stopped.load(Ordering::Acquire) {
                        job();
                    }
                    pending_job.store(false, Ordering::Release);
                }) as Job);
                match submission {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => pending.store(false, Ordering::Release),
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
            task_stopped.store(true, Ordering::Release);
        });
        self.timers.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(Timer {
            cancel: cancel.clone(),
            stopped: Arc::clone(&stopped),
            thread: timer,
        });
        Ok(PeriodicTask { cancel, stopped, timers: Arc::downgrade(&self.timers) })
    }

    /// Queues a job, waiting when the bounded queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadPoolError::QueueClosed`] if all workers have stopped.
    pub fn execute<F>(&self, job: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or(ThreadPoolError::QueueClosed)?;
        sender.send(Box::new(job)).map_err(|_queue_closed| ThreadPoolError::QueueClosed)
    }

    /// Queues a job without blocking. Rejected jobs are dropped.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadPoolError::QueueFull`] when the bounded queue is full,
    /// or [`ThreadPoolError::QueueClosed`] if all workers have stopped.
    pub fn try_execute<F>(&self, job: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or(ThreadPoolError::QueueClosed)?;
        sender.try_send(Box::new(job)).map_err(|error| match error {
            TrySendError::Full(_) => ThreadPoolError::QueueFull,
            TrySendError::Disconnected(_) => ThreadPoolError::QueueClosed,
        })
    }

    /// Returns the identifier of a worker that has stopped unexpectedly.
    pub fn stopped_worker(&self) -> Option<usize> {
        self.workers
            .iter()
            .find(|worker| worker.thread.as_ref().is_some_and(JoinHandle::is_finished))
            .map(|worker| worker.id)
    }

    /// Closes the job queue, finishes queued jobs, and joins every worker.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadPoolError::WorkerPanicked`] if a worker panicked. All
    /// workers are joined before the error is returned.
    pub fn shutdown(mut self) -> Result<(), ThreadPoolError> {
        self.close_and_join()
    }

    fn close_and_join(&mut self) -> Result<(), ThreadPoolError> {
        let timers = {
            let mut timers = self.timers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *timers)
        };
        for timer in &timers {
            timer.stopped.store(true, Ordering::Release);
            let _ = timer.cancel.send(());
        }
        for timer in timers {
            let _ = timer.thread.join();
        }
        self.maintenance_sender.take();
        self.sender.take();
        let mut panicked_worker = None;
        for worker in &mut self.workers {
            let Some(thread) = worker.thread.take() else {
                continue;
            };
            if thread.join().is_err() && panicked_worker.is_none() {
                panicked_worker = Some(worker.id);
            }
        }
        match panicked_worker {
            Some(worker_id) => Err(ThreadPoolError::WorkerPanicked { worker_id }),
            None => Ok(()),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let _ = self.close_and_join();
    }
}

/// Cancellation handle for a periodic task. Drop to cancel future runs.
pub struct PeriodicTask {
    cancel: Sender<()>,
    stopped: Arc<AtomicBool>,
    timers: Weak<Mutex<Vec<Timer>>>,
}

impl Drop for PeriodicTask {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.cancel.send(());
        if let Some(timers) = self.timers.upgrade() {
            let timer = {
                let mut timers = timers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                timers
                    .iter()
                    .position(|timer| Arc::ptr_eq(&timer.stopped, &self.stopped))
                    .map(|index| timers.swap_remove(index))
            };
            // Joining outside the lock lets shutdown and other cancellations proceed.
            if let Some(timer) = timer {
                let _ = timer.thread.join();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || {
            loop {
                let job = {
                    let Ok(receiver) = receiver.lock() else {
                        return;
                    };
                    match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => return,
                    }
                };
                job();
            }
        });
        Self { id, thread: Some(thread) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn nonblocking_submission_rejects_full_queue_and_shutdown_drains_jobs() {
        assert!(matches!(ThreadPool::new(0), Err(ThreadPoolError::InvalidSize)));
        let pool = ThreadPool::new(2).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (release_tx, release_rx) = mpsc::channel();
            releases.push(release_tx);
            let started = started_tx.clone();
            pool.execute(move || {
                started.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .unwrap();
        }
        // Both jobs must run concurrently, outside the receiver mutex.
        for _ in 0..2 {
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        let (finished_tx, finished_rx) = mpsc::channel();
        for _ in 0..2 {
            let finished = finished_tx.clone();
            pool.try_execute(move || finished.send(()).unwrap()).unwrap();
        }
        assert!(matches!(
            pool.try_execute(|| panic!("rejected job ran")),
            Err(ThreadPoolError::QueueFull)
        ));
        for release in releases {
            release.send(()).unwrap();
        }
        pool.shutdown().unwrap();
        for _ in 0..2 {
            finished_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
}
