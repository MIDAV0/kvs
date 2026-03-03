pub mod shared_queue_thread_pool;
pub mod rayon_thread_pool;

pub use shared_queue_thread_pool::SharedQueueThreadPool;
pub use rayon_thread_pool::RayonThreadPool;

use crate::Result;


pub type Job = Box<dyn FnOnce() + Send + 'static>;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}