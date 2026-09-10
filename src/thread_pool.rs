//! Fixed-size pool for running blocking jobs on reusable worker threads.
//!
//! Jobs are submitted through a bounded queue. Dropping or explicitly shutting
//! down the pool closes that queue, lets workers finish queued jobs, and joins
//! every worker thread.

use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread::{self, JoinHandle},
};

use thiserror::Error;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Failure while creating, using, or shutting down a thread pool.
#[derive(Debug, Error)]
pub enum ThreadPoolError {
    /// A pool must contain at least one worker.
    #[error("thread pool size must be greater than zero")]
    InvalidSize,
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

/// A fixed-size pool of worker threads for blocking jobs.
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<SyncSender<Job>>,
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
        Ok(Self { workers, sender: Some(sender) })
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
