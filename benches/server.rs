use std::{net::SocketAddr, sync::{Arc, Barrier, atomic::{AtomicU16, Ordering}}, thread};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kvs::{KvClient, KvServer, KvStore, SledKvsEngine, thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool}};
use tempfile::TempDir;

const NUM_REQUESTS: usize = 1000;  // Reduced to stay within OS file descriptor limits
const KEY_LENGTH: usize = 100;

// Global port counter to avoid conflicts between benchmark groups
static PORT_COUNTER: AtomicU16 = AtomicU16::new(6000);

fn generate_keys(count: usize, key_len: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{:0>width$}", i, width = key_len))
        .collect()
}

fn write_queued_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_queued_kvstore");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        let server_pool = SharedQueueThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));
        
        let client_pool = SharedQueueThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let value = Arc::new(value.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let value = (*value).clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.set(key, value);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}



fn read_queued_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_queued_kvstore");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        let server_pool = SharedQueueThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));

        for key in keys.iter() {
            let mut client = KvClient::connect(server_addr.to_string()).unwrap();
            client.set(key.clone(), value.clone()).unwrap();
        }
        
        let client_pool = SharedQueueThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.get(key);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}

fn write_rayon_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_kvstore");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        let server_pool = RayonThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));
        
        let client_pool = RayonThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let value = Arc::new(value.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let value = (*value).clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.set(key, value);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}


fn read_rayon_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_kvstore");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        let server_pool = RayonThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));

        for key in keys.iter() {
            let mut client = KvClient::connect(server_addr.to_string()).unwrap();
            client.set(key.clone(), value.clone()).unwrap();
        }
        
        let client_pool = RayonThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.get(key);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}


fn write_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_sled");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
        let server_pool = RayonThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));
        
        let client_pool = RayonThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let value = Arc::new(value.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let value = (*value).clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.set(key, value);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}

fn read_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_sled");
    
    let num_cpus = num_cpus::get();
    let mut thread_counts: Vec<u32> = vec![1, 2, 4];
    for i in (6..=(num_cpus * 2)).step_by(2) {
        thread_counts.push(i as u32);
    }
    thread_counts.sort();
    thread_counts.dedup();

    let keys = generate_keys(NUM_REQUESTS, KEY_LENGTH);
    let value = "benchmark_value".to_string();
    
    for server_threads in thread_counts {
        let temp_dir = TempDir::new().unwrap();
        let engine = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
        let server_pool = RayonThreadPool::new(server_threads).unwrap();
        let server = KvServer::new(engine, server_pool);
        
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        
        thread::spawn(move || {
            let _ = server.start(server_addr);
        });
        
        thread::sleep(std::time::Duration::from_millis(200));

        for key in keys.iter() {
            let mut client = KvClient::connect(server_addr.to_string()).unwrap();
            client.set(key.clone(), value.clone()).unwrap();
        }
        
        let client_pool = RayonThreadPool::new(NUM_REQUESTS as u32).unwrap();
        let keys = Arc::new(keys.clone());
        let server_addr_str = Arc::new(format!("127.0.0.1:{}", port));
        
        group.bench_with_input(
            BenchmarkId::new("threads", server_threads),
            &server_threads,
            |b, _| {
                b.iter(|| {
                    // Create fresh barrier for each iteration
                    let barrier = Arc::new(Barrier::new(NUM_REQUESTS + 1));
                    
                    for i in 0..NUM_REQUESTS {
                        let key = keys[i].clone();
                        let addr = (*server_addr_str).clone();
                        let barrier = Arc::clone(&barrier);
                        
                        client_pool.spawn(move || {
                            // Create fresh connection for each request
                            // (server closes connection after handling request)
                            let mut client = KvClient::connect(addr).unwrap();
                            let result = client.get(key);
                            assert!(result.is_ok(), "Set failed: {:?}", result);
                            barrier.wait();
                        });
                    }
                    
                    barrier.wait();
                });
            }
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    write_queued_kvstore,
    read_queued_kvstore,
    write_rayon_kvstore,
    read_rayon_kvstore,
    write_rayon_sled,
    read_rayon_sled
);
criterion_main!(benches);