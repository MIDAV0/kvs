use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use sled;
use tempfile::TempDir;

fn random_string(rng: &mut SmallRng, len: usize) -> String {
    (0..len).map(|_| (rng.gen_range(0, 26) as u8 + b'a') as char).collect()
}

fn write(c: &mut Criterion) {

    let mut group = c.benchmark_group("write_bench");

    group.bench_function("kvs_write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::from_seed([0; 16]);
                let data: Vec<(String, String)> = (0..100)
                    .map(|_| {
                        let key_len = rng.gen_range(1, 100001);
                        let val_len = rng.gen_range(1, 100001);
                        (random_string(&mut rng, key_len), random_string(&mut rng, val_len))
                    })
                    .collect();
                (KvStore::open(temp_dir.path()).unwrap(), data, temp_dir)
            },
            |(mut store, data, _temp_dir)| {
                for (key, value) in data {
                    store.set(key, value).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled_write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::from_seed([0; 16]);
                let data: Vec<(String, String)> = (0..100)
                    .map(|_| {
                        let key_len = rng.gen_range(1, 100001);
                        let val_len = rng.gen_range(1, 100001);
                        (random_string(&mut rng, key_len), random_string(&mut rng, val_len))
                    })
                    .collect();
                (SledKvsEngine::new(sled::open(&temp_dir).unwrap()), data, temp_dir)
            },
            |(mut db, data, _temp_dir)| {
                for (key, value) in data {
                    db.set(key, value).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}


fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_bench");

    group.bench_function("kvs_read", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut store = KvStore::open(temp_dir.path()).unwrap();
                let mut rng = SmallRng::from_seed([0; 16]);
                
                // Write 100 random key-value pairs
                let keys: Vec<String> = (0..100)
                    .map(|_| {
                        let key_len = rng.gen_range(1, 100001);
                        let val_len = rng.gen_range(1, 100001);
                        let key = random_string(&mut rng, key_len);
                        let value = random_string(&mut rng, val_len);
                        store.set(key.clone(), value).unwrap();
                        key
                    })
                    .collect();
                let mut read_rng = SmallRng::from_seed([1; 16]);
                let read_indices: Vec<usize> = (0..1000)
                    .map(|_| read_rng.gen_range(0, keys.len()))
                    .collect();
                (store, keys, read_indices, temp_dir)
            },
            |(mut store, keys, read_indices, _temp_dir)| {
                for &index in &read_indices {
                    let key = &keys[index];
                    store.get(key.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled_read", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut db = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
                let mut rng = SmallRng::from_seed([0; 16]);
                
                // Write 100 random key-value pairs
                let keys: Vec<String> = (0..100)
                    .map(|_| {
                        let key_len = rng.gen_range(1, 100001);
                        let val_len = rng.gen_range(1, 100001);
                        let key = random_string(&mut rng, key_len);
                        let value = random_string(&mut rng, val_len);
                        db.set(key.clone(), value).unwrap();
                        key
                    })
                    .collect();
                let mut read_rng = SmallRng::from_seed([1; 16]);
                let read_indices: Vec<usize> = (0..1000)
                    .map(|_| read_rng.gen_range(0, keys.len()))
                    .collect();
                
                (db, keys, read_indices, temp_dir)
            },
            |(mut db, keys, read_indices, _temp_dir)| {
                // Read 1000 times from the written keys
                for &index in &read_indices {
                    let key = &keys[index];
                    db.get(key.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, write, read);
criterion_main!(benches);