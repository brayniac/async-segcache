# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

async-segcache is a high-performance, log-structured cache library for Rust with async/await support. It uses a segment-based architecture with TTL buckets for efficient expiration and multiple eviction policies.

## Build Commands

```bash
# Build the project (debug mode)
cargo build

# Build for release
cargo build --release

# Run tests (excluding loom tests)
cargo test

# Run specific test
cargo test test_name

# Run loom concurrency tests (SLOW - don't run automatically)
# Example: LOOM_MAX_PREEMPTIONS=1 cargo test --features loom test_name

# Format code
cargo fmt

# Lint with clippy
cargo clippy

# Check code without building
cargo check
```

## Architecture

### Core Components

#### 1. Cache (src/lib.rs)
- Main entry point with async API
- `set()`, `get()`, `delete()` operations
- Configurable via `CacheBuilder` (segment size, heap size, eviction policy)
- Eviction policies: Random, FIFO, CTE, Util

#### 2. Segments (src/segments.rs)
- Log-structured segments storing items sequentially
- States: Free, Reserved, Linking, Live, Sealed, Draining, Locked, Relinking
- Each segment tracks: write_offset, live_items, live_bytes, expire_at, bucket_id
- Reference counting for concurrent readers
- Lock-free free pool for segment allocation

#### 3. TTL Buckets (src/ttlbuckets.rs)
- 1024 buckets organized logarithmically (1-8s, 9-16s, 17-24s, ...)
- Each bucket maintains a doubly-linked list of segments ordered by expiration
- Async operations: `append_item()`, `append_segment()`, `remove_segment()`
- Expiration: `try_expire_head_segment()` for proactive cleanup
- Eviction: `evict_head_segment()` for memory pressure

#### 4. Hashtable (src/hashtable.rs)
- Lock-free chaining hashtable using ASFC (Adaptive Segmented Frequency Counter)
- Frequency tracking for eviction policies
- `reserve_and_link()` handles insertion and updates
- `get()`, `unlink_item()` for lookups and deletions

#### 5. Item Format (src/item.rs)
- Wire format: [Header][Optional][Key][Value][Padding]
- Header includes: magic bytes, flags (deleted bit), lengths, CAS tag, frequency
- Validation support with checksums (optional feature)

### Key Features

#### Async Operations
- `set()`, `delete()` are async to support:
  - Eager removal of empty sealed segments
  - Cache-wide eviction when allocation fails
  - Async mutex serialization for segment operations

#### Allocation Strategy (Priority Order)
1. **Reserve from free pool** - Fast path
2. **Try to expire 1 segment** - Remove expired data (bounded latency)
3. **Evict by policy** - Remove valid data according to configured policy
4. **Retry** - Another thread may have freed space

#### Eager Removal
- `delete()` checks if segment becomes empty and sealed
- `set()` checks when overwriting the last item in a segment
- Proactively removes empty segments from TTL chains and returns to free pool

#### Eviction Policies
- **Random**: Scans segments randomly, evicts first evictable segment
- **FIFO**: Evicts oldest segments first (head of TTL buckets)
- **CTE, Util**: Use frequency tracking from ASFC (to be implemented)
- Always respects cache-wide policy (no per-bucket preference)

#### Concurrency
- Lock-free data structures where possible
- Async mutexes for serialization (TTL bucket operations)
- CAS operations for state transitions
- Loom testing for concurrency verification

### Important Implementation Details

#### Segment States and Transitions
```
Free → Reserved → Linking → Live → Sealed → Draining → Locked → Reserved → Free
                     ↑                                      ↓
                     └──────── Relinking ←─────────────────┘
```

- **Free**: Available in free pool
- **Reserved**: Allocated but not yet linked to TTL bucket
- **Linking**: Being added to TTL bucket chain
- **Live**: Active segment accepting writes
- **Sealed**: Full segment, no more writes
- **Draining**: Being removed from chain, waiting for readers
- **Locked**: No readers, being cleared
- **Relinking**: Chain pointers being updated

#### Test-Only Functions
Many functions are marked `#[cfg(test)]` and only available in test builds:
- `segments::delete_item()`, `segments::expire()`
- `segments::live_items()`, `segments::write_offset()`
- `hashtable::link_item()`, `hashtable::item_freq()`

#### Metrics
Comprehensive metrics via `metriken` crate:
- Segment states (free, live, sealed)
- Item counts (total, live)
- Operation counters (set, get, delete, evict, expire)
- Hashtable stats (collisions, unlinks)

### Testing

#### Unit Tests
- src/lib.rs: Cache operations and integration tests
- src/segments.rs: Segment operations and state transitions
- src/ttlbuckets.rs: TTL bucket operations
- src/hashtable.rs: Hashtable and ASFC operations

#### Integration Tests
- tests/fill_cache_test.rs: Cache fill and eviction scenarios
- tests/proactive_expiration_test.rs: TTL expiration behavior
- tests/ttl_expiration_test.rs: TTL-based expiration

#### Loom Tests
- Concurrency verification using loom
- **IMPORTANT**: Don't run automatically (very slow)
- Use `LOOM_MAX_PREEMPTIONS=0` or `1` to limit state space
- Located in src/tests.rs module

### Common Pitfalls

1. **Async functions**: `delete()` and `set()` are async - always use `.await`
2. **Loom incompatibility**: Loom doesn't support async well - tests using async delete are commented out
3. **Segment states**: Always verify state before transitions using `segment.state()`
4. **Test-only APIs**: Don't use `#[cfg(test)]` functions in production code paths
5. **Reserved segments**: After eviction/expiration, segments are in Reserved state, ready for `append_segment()`