use crossbeam::channel::{Receiver, Sender, unbounded};
use std::thread::{self, JoinHandle};

use crate::{Result, KvsError};
use super::{ThreadPool, Job};


struct Worker {
    id: u32,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: u32, receiver: Receiver<Job>) -> Self {

        let thread = thread::spawn(move || loop {
            match receiver.recv() {
                Ok(job) => {
                    job();
                }
                Err(_) => {
                    break;
                }
            }
        });

        Worker { id, thread: Some(thread) }
    }
}

pub struct NaiveThreadPool {
    sender: Option<Sender<Job>>,
    workers: Vec<Worker>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        if threads < 1 { return Err(KvsError::ThreadPool("minimum amount of threads is 1".to_string())); }

        let (sender, receiver) = unbounded();

        let workers = (0..threads)
            .map(|id| Worker::new(id, receiver.clone()))
            .collect();

        Ok(
            NaiveThreadPool { sender: Some(sender), workers }
        )
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let job = Box::new(job);
        if let Some(sender) = &self.sender {
            sender.send(job).unwrap();
        }
    }
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                if let Err(e) = thread.join() {
                    slog::error!(slog_scope::logger(), "Failed to join worker thread"; "worker_id" => worker.id, "error" => format!("{:?}", e));
                }
            }
        }
    }
}