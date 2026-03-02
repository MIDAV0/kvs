use rayon::{ThreadPool as RThreadPool};

use crate::{Result, KvsError};
use super::ThreadPool;


pub struct RayonThreadPool {
    pool: RThreadPool,
}

impl ThreadPool for RayonThreadPool {

    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|e| KvsError::ThreadPool(e.to_string()))?;
        Ok(RayonThreadPool { pool })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.pool.spawn(job);
    }
}