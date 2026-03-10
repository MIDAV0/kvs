# A Log-structured key-value store implemented in Rust with concurrent read/write support.

A **log-structured store** treats storage as an append-only log: all writes (inserts, updates, deletes) are sequentially appended to the end of a file rather than updating data in place. This design trades read amplification for write performance—sequential writes are fast, but reads require an in-memory index to locate values. Garbage collection (compaction) periodically reclaims space from obsolete entries.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         KvServer                                │
│  ┌─────────────┐    ┌─────────────────────────────────────────┐ │
│  │ ThreadPool  │───▶│              KvsEngine                  │ │
│  │ (Rayon /    │    │  ┌─────────────────┐  ┌──────────────┐  │ │
│  │  Shared     │    │  │    KvStore      │  │SledKvsEngine │  │ │
│  │  Queue)     │    │  │  (Log-struct)   │  │  (Baseline)  │  │ │
│  └─────────────┘    │  └─────────────────┘  └──────────────┘  │ │
│                     └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

The system consists of three layers:
1. **Network Layer**: TCP server accepting connections via `KvServer`
2. **Concurrency Layer**: ThreadPool implementations (SharedQueue / Rayon)
3. **Storage Layer**: Pluggable engines via `KvsEngine` trait

---

## Data Model

### On-Disk Format

Commands are serialized as JSON and appended to log files named `{epoch}.log`:

```rust
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}
```

Example log file content:
```json
{"Set":{"key":"foo","value":"bar"}}{"Set":{"key":"baz","value":"qux"}}{"Rm":{"key":"foo"}}
```

### In-Memory Index

A **lock-free skip list** (`SkipMap`) maps keys to their position in log files:

```rust
struct CommandPos {
    epoch: u64,   // Which log file
    pos: u64,     // Byte offset in file
    len: u64,     // Length of serialized command
}
```

The skip list enables concurrent reads without locking while the writer holds an exclusive lock.

---

## Write Path

```
          ┌───────────────────────────────────────────────┐
          │                   KvStore                     │
          │  ┌────────────────────────────────────────┐   │
 SET(k,v) │  │ writer: Arc<Mutex<KvsWriter>>          │   │
    │     │  │   ├─ Serializes Command::Set to JSON   │   │
    ▼     │  │   ├─ Appends to current epoch log      │   │
 LOCK ────│──│   ├─ Flushes to disk                   │   │
          │  │   ├─ Updates SkipMap index             │   │
          │  │   └─ Triggers compaction if needed     │   │
          │  └────────────────────────────────────────┘   │
          └───────────────────────────────────────────────┘
```

1. **Acquire writer lock** (single-writer enforced via `Mutex`)
2. **Serialize** command to JSON
3. **Append** to current epoch's log file
4. **Flush** buffer to disk (durability guarantee)
5. **Update index**: Insert new `CommandPos` into `SkipMap`
6. **Track garbage**: Increment `uncompacted` counter if overwriting existing key
7. **Trigger compaction** if `uncompacted >= 1MB`

### Remove Path

Removes follow a similar path but:
- Write a `Command::Rm` tombstone
- Remove key from index
- Mark both the old value AND the tombstone as garbage (both will be cleaned by compaction)

---

## Compaction Strategy

**Threshold-based compaction**: Triggered when stale data exceeds 1MB (`COMPACTION_THRESHOLD`).

```
Before Compaction:                    After Compaction:
┌──────────────────┐                  ┌──────────────────┐
│  epoch 1.log     │                  │  epoch 3.log     │ (compacted)
│  Set(a, 1)  ──┐  │                  │  Set(a, 3) ◄───┐ │
│  Set(b, 2)    │  │   ═══════════▶   │  Set(c, 5) ◄─┐ │ │
│  Set(a, 3) ◄──┘  │   compact()      └──────────────│─│─┘
├──────────────────┤                                 │ │
│  epoch 2.log     │                  ┌──────────────│─│─┐
│  Rm(b)           │                  │  epoch 4.log │ │ │ (new writes)
│  Set(c, 5) ◄────────────────────────────────────────┘  │
└──────────────────┘                  └──────────────────┘
                                      
 Stale epochs 1 & 2 deleted           Index points to epoch 3 & 4
```

### Compaction Algorithm

1. **Allocate new epochs**: `compaction_epoch = current + 1`, `current += 2`
2. **Copy live data**: Iterate index, copy referenced values to compaction epoch
3. **Update index**: Point all entries to new file positions
4. **Update safe_epoch**: Atomic store signals readers that old epochs are stale
5. **Delete old files**: Remove all log files with `epoch < compaction_epoch`

### Reader Coordination

Readers use a **safe_epoch** atomic variable to know when to close file handles:
- Before any read, readers call `clear_stale_epochs()` to close handles to deleted files
- Readers lazily open file handles as needed

---

## Crash Recovery

Recovery is straightforward due to the append-only log structure:

```
┌─────────────────────────────────────────────────────────────────┐
│                      KvStore::open()                            │
│  1. Scan directory for *.log files                              │
│  2. Sort by epoch number                                        │
│  3. For each log file:                                          │
│     ├─ Open file reader                                         │
│     ├─ Deserialize each Command (JSON stream)                   │
│     └─ Rebuild index:                                           │
│         Set(k,v) → index.insert(k, pos)                         │
│         Rm(k)    → index.remove(k)                              │
│  4. Track uncompacted bytes while loading                       │
│  5. Create new epoch for writes (latest + 1)                    │
└─────────────────────────────────────────────────────────────────┘
```

### Guarantees

| Scenario | Recovery Behavior |
|----------|-------------------|
| Crash during write | Incomplete JSON at EOF ignored; last complete command preserved |
| Crash during compaction | Old files still exist; replay reconstructs correct state |
| Partial flush | JSON deserialization fails on incomplete records; skip to next valid |

### Durability

Each write calls `flush()` immediately, ensuring data hits OS buffers. For true fsync durability, the buffer implementation could be extended with explicit `sync_data()` calls.

---

## Concurrency Model

```
┌────────────────────────────────────────────────────────────────┐
│                Multi-Reader, Single-Writer                     │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                 Arc<SkipMap<String, CommandPos>>         │  │
│  │                    (lock-free concurrent reads)          │  │
│  └──────────────────────────────────────────────────────────┘  │
│           ▲                                       ▲            │
│           │ read                            write │            │
│  ┌────────┴────────┐                    ┌─────────┴────────┐   │
│  │    KvsReader    │  (cloneable,       │    KvsWriter     │   │
│  │  (per-thread)   │   thread-local     │   Arc<Mutex<>>   │   │
│  │                 │   file handles)    │  (exclusive)     │   │
│  └─────────────────┘                    └──────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

- **Readers**: Each thread gets its own `KvsReader` clone with thread-local file handles (`RefCell<BTreeMap>`)
- **Writer**: Single writer protected by `Mutex`, ensures sequential writes and consistent index updates
- **Index**: `crossbeam_skiplist::SkipMap` allows concurrent reads/writes without global locking

---

## Benchmarks

Benchmarks compare `KvStore` (log-structured) against `SledKvsEngine` (B-tree based) using Criterion.

### Test Configuration

| Parameter | Value |
|-----------|-------|
| Key size | 1 - 100KB (random) |
| Value size | 1 - 100KB (random) |
| Batch size | 100 operations |
| Read operations | 1000 random reads |

### Write Performance

```
cargo bench --bench kvs -- write_bench
```

| Engine | Description | Characteristics |
|--------|-------------|-----------------|
| **KvStore** | Append-only writes | O(1) write, sequential I/O |
| **Sled** | B-tree insert | O(log n) writes, random I/O |

**Expected**: KvStore should excel at write-heavy workloads due to sequential appends vs. Sled's in-place updates requiring page splits.

### Read Performance

```
cargo bench --bench kvs -- read_bench
```

| Engine | Description | Characteristics |
|--------|-------------|-----------------|
| **KvStore** | Index lookup + random read | O(1) lookup + seek |
| **Sled** | B-tree traversal | O(log n) + cache-friendly |

**Expected**: Sled may perform better for reads due to data locality, while KvStore requires one seek per read.

### ThreadPool Benchmark

```
cargo bench --bench threadpool
```

Compares SharedQueueThreadPool vs RayonThreadPool at various thread counts (1-2x CPU cores).

| Pool Type | Scaling | Best For |
|-----------|---------|----------|
| **SharedQueue** | Linear | I/O-bound tasks |
| **Rayon** | Work-stealing | CPU-bound, varying workloads |

### Running Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench --bench kvs

# View results (HTML reports)
open target/criterion/report/index.html
```

---

## File Layout

```
data/
├── engine          # Persisted engine choice (kvs/sled)
└── <data files>    # Engine-specific data

# For KvStore engine:
data/
├── 1.log           # Epoch 1 commands
├── 3.log           # Epoch 3 (after compaction)
└── 4.log           # Current write epoch
```

---

## Protocol (Client-Server)

Binary protocol using `bincode` serialization with length-prefix framing:

```
┌────────────┬─────────────────────────────┐
│  4 bytes   │       N bytes               │
│  (length)  │    (bincode payload)        │
└────────────┴─────────────────────────────┘
```

Request/Response types:
- `Request::Get(key)` → `GetResponse::Ok(Option<String>)` | `GetResponse::Err(String)`
- `Request::Set(key, value)` → `SetResponse::Ok(())` | `SetResponse::Err(String)`
- `Request::Rm(key)` → `RemoveResponse::Ok(())` | `RemoveResponse::Err(String)`

---