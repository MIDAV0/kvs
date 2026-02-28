pub mod naive_thread_pool;

pub use naive_thread_pool::NaiveThreadPool;

use crate::Result;


pub type Job = Box<dyn FnOnce() + Send + 'static>;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}