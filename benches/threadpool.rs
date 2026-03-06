use std::sync::{Arc, Barrier, atomic::{AtomicUsize, Ordering}};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};


fn threadpools(c: &mut Criterion) {
    let mut group = c.benchmark_group("threadpool_bench");

    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    const NUM_TASKS: usize = 1000;

    for threads in thread_counts {

        let shared_pool = SharedQueueThreadPool::new(threads).unwrap();

        group.bench_with_input(
            BenchmarkId::new("shared_threads", threads), 
            &threads,
            |b, _| {
                let counter = Arc::new(AtomicUsize::new(0));
                b.iter(|| {
                    counter.store(0, Ordering::SeqCst);
                    for _ in 0..NUM_TASKS {
                        let counter = Arc::clone(&counter);
                        shared_pool.spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                    while counter.load(Ordering::Acquire) < NUM_TASKS {
                        std::hint::spin_loop();
                    }
                });
            } 
        );

        let rayon_pool = RayonThreadPool::new(threads).unwrap();

        group.bench_with_input(
            BenchmarkId::new("rayon_threads", threads), 
            &threads,
            |b, _| {
                let counter = Arc::new(AtomicUsize::new(0));
                b.iter(|| {
                    counter.store(0, Ordering::SeqCst);
                    for _ in 0..NUM_TASKS {
                        let counter = Arc::clone(&counter);
                        rayon_pool.spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                    while counter.load(Ordering::Acquire) < NUM_TASKS {
                        std::hint::spin_loop();
                    }
                });
            } 
        );
    }

    group.finish();
}

criterion_group!(benches, threadpools);
criterion_main!(benches);