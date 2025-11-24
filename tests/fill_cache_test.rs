/// Integration test to fill the cache with various item sizes
/// This test attempts to reproduce segfault issues by filling segments completely
/// with different value sizes to find edge cases in bounds checking.

use async_segcache::{CacheBuilder, Duration};

/// Test filling cache with a specific value size
async fn test_fill_with_value_size(value_size: usize) {
    println!("\n=== Testing with value_size={} ===", value_size);

    // Use smaller cache for faster testing: 8MB heap, 1MB segments = 8 segments
    let cache = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .hashtable_power(16)
        .build();

    let ttl = Duration::from_secs(3600);
    let value = vec![0xAB; value_size];

    // Calculate expected items per segment
    let key_size = 20; // Typical valkey-bench key size
    let header_size = 5;
    let unpadded_item_size = header_size + key_size + value_size;
    let padded_item_size = (unpadded_item_size + 7) & !7;
    let segment_size = 1024 * 1024;
    let expected_items_per_segment = segment_size / padded_item_size;

    println!("Unpadded item size: {}", unpadded_item_size);
    println!("Padded item size: {}", padded_item_size);
    println!("Expected items per segment: ~{}", expected_items_per_segment);

    let mut success_count = 0;
    let mut fail_count = 0;

    // Try to fill all segments completely
    // 8 segments * expected items per segment * 2 (to ensure we fill completely)
    let total_attempts = 8 * expected_items_per_segment * 2;

    for i in 0..total_attempts {
        let key = format!("key_{:020}", i);

        match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
            Ok(()) => {
                success_count += 1;
                if success_count % 1000 == 0 {
                    println!("  Successfully inserted {} items", success_count);
                }
            }
            Err(e) => {
                fail_count += 1;
                if fail_count == 1 {
                    println!("  First failure at item {}: {:?}", i, e);
                }
            }
        }
    }

    println!("Results: {} successful, {} failed", success_count, fail_count);
    println!("Cache should be full or nearly full");

    // Verify we can still read items
    println!("Verifying reads...");
    let mut read_success = 0;
    let mut buffer = vec![0u8; value_size + 100];

    for i in 0..std::cmp::min(100, success_count) {
        let key = format!("key_{:020}", i);
        if cache.get(key.as_bytes(), &mut buffer).is_ok() {
            read_success += 1;
        }
    }

    println!("Successfully read {}/100 test items", read_success);
}

#[tokio::test]
async fn test_fill_cache_small_values() {
    // Small values (should fit many per segment)
    test_fill_with_value_size(64).await;
}

#[tokio::test]
async fn test_fill_cache_medium_values() {
    // Medium values
    test_fill_with_value_size(256).await;
}

#[tokio::test]
async fn test_fill_cache_large_values() {
    // Large values (1024 bytes like in your production case)
    test_fill_with_value_size(1024).await;
}

#[tokio::test]
async fn test_fill_cache_very_large_values() {
    // Very large values (should only fit a few per segment)
    test_fill_with_value_size(100_000).await;
}

#[tokio::test]
async fn test_fill_cache_edge_case_sizes() {
    // Test various sizes that might trigger padding edge cases
    let edge_sizes = vec![
        1,      // Minimum value size
        7,      // Just before 8-byte boundary
        8,      // On 8-byte boundary
        9,      // Just after 8-byte boundary
        15,     // Just before 16-byte boundary
        16,     // On 16-byte boundary
        255,    // Just below NON_TEMPORAL_THRESHOLD
        256,    // At NON_TEMPORAL_THRESHOLD
        257,    // Just above NON_TEMPORAL_THRESHOLD
        1023,   // Just below 1024
        1024,   // Your production case
        1025,   // Just above 1024
    ];

    for size in edge_sizes {
        println!("\n--- Edge case: value_size={} ---", size);
        test_fill_with_value_size(size).await;
    }
}

#[tokio::test]
async fn test_segment_boundary_items() {
    // This test specifically targets segment boundary conditions
    println!("\n=== Testing segment boundary conditions ===");

    let cache = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .hashtable_power(16)
        .build();

    let ttl = Duration::from_secs(3600);

    // Fill with items designed to leave specific amounts of space at the end
    // Use 1000 byte values (padded to 1032 bytes with 20-byte key)
    let key_size = 20;
    let value_size = 1000;
    let header_size = 5;
    let padded_size = (header_size + key_size + value_size + 7) & !7;

    println!("Padded item size: {}", padded_size);
    println!("Items per segment: {}", 1024 * 1024 / padded_size);
    println!("Remainder per segment: {} bytes", 1024 * 1024 % padded_size);

    let value = vec![0xCD; value_size];
    let mut count = 0;

    // Fill segments until we start getting failures
    for i in 0..10000 {
        let key = format!("boundary_{:020}", i);

        match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
            Ok(()) => {
                count += 1;
            }
            Err(_) => {
                println!("Stopped at {} items (cache full)", count);
                break;
            }
        }
    }

    // Now try to insert items of various sizes to fill the gaps
    println!("Attempting to fill gaps with smaller items...");

    for gap_size in [1, 8, 16, 32, 64, 128, 256, 512] {
        let small_value = vec![0xEF; gap_size];
        let key = format!("gap_{:04}", gap_size);

        match cache.set(key.as_bytes(), &small_value, &[], Some(ttl)).await {
            Ok(()) => println!("  Successfully filled gap with {} byte value", gap_size),
            Err(_) => println!("  Failed to fill gap with {} byte value", gap_size),
        }
    }
}

#[tokio::test]
async fn test_concurrent_fill() {
    use std::sync::Arc;

    println!("\n=== Testing concurrent cache filling ===");

    let cache = Arc::new(CacheBuilder::new()
        .heap_size(16 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .hashtable_power(16)
        .build());

    let ttl = Duration::from_secs(3600);
    let num_threads = 4;
    let items_per_thread = 5000;
    let value_size = 1024;

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let handle = tokio::task::spawn(async move {
            let value = vec![0xAB; value_size];
            let mut success = 0;
            let mut fail = 0;

            for i in 0..items_per_thread {
                let key = format!("thread{}_{:010}", thread_id, i);

                match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
                    Ok(()) => success += 1,
                    Err(_) => fail += 1,
                }
            }

            println!("Thread {} completed: {} success, {} failed", thread_id, success, fail);
            (success, fail)
        });

        handles.push(handle);
    }

    let mut total_success = 0;
    let mut total_fail = 0;

    for handle in handles {
        let (success, fail) = handle.await.unwrap();
        total_success += success;
        total_fail += fail;
    }

    println!("Total: {} successful, {} failed", total_success, total_fail);
}

#[tokio::test]
async fn test_production_scenario() {
    // Simulate the exact production scenario:
    // - Default config (64MB heap, 1MB segments)
    // - 1M keys in keyspace (typical valkey-bench)
    // - 1024 byte values
    // - High concurrency

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    println!("\n=== Production Scenario Test ===");
    println!("64MB heap, 1MB segments, 1024 byte values, 8 threads");

    let cache = Arc::new(CacheBuilder::new()
        .heap_size(64 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .hashtable_power(16)
        .build());

    let ttl = Duration::from_secs(3600);
    let num_threads = 8;
    let operations_per_thread = 200_000; // 1.6M total operations
    let key_space = 1_000_000; // 1M unique keys
    let value_size = 1024;

    let success_counter = Arc::new(AtomicU64::new(0));
    let fail_counter = Arc::new(AtomicU64::new(0));

    println!("Launching {} threads, each doing {} operations", num_threads, operations_per_thread);
    println!("Key space: {} keys", key_space);

    let start = std::time::Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let success_counter = Arc::clone(&success_counter);
        let fail_counter = Arc::clone(&fail_counter);

        let handle = tokio::task::spawn(async move {
            let value = vec![0xAB_u8; value_size];
            let mut local_success = 0;
            let mut local_fail = 0;

            for i in 0..operations_per_thread {
                // Generate key similar to valkey-bench pattern
                let key_id = (thread_id * 1_000_000 + i) % key_space;
                let key = format!("key:{:010}", key_id);

                match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
                    Ok(()) => local_success += 1,
                    Err(_) => local_fail += 1,
                }

                // Progress update every 50k ops
                if i > 0 && i % 50_000 == 0 {
                    println!("Thread {} progress: {}/{}", thread_id, i, operations_per_thread);
                }
            }

            success_counter.fetch_add(local_success, AtomicOrdering::Relaxed);
            fail_counter.fetch_add(local_fail, AtomicOrdering::Relaxed);

            (local_success, local_fail)
        });

        handles.push(handle);
    }

    // Wait for all threads
    for (idx, handle) in handles.into_iter().enumerate() {
        let (success, fail) = handle.await.unwrap();
        println!("Thread {} completed: {} success, {} failed", idx, success, fail);
    }

    let elapsed = start.elapsed();
    let total_success = success_counter.load(AtomicOrdering::Relaxed);
    let total_fail = fail_counter.load(AtomicOrdering::Relaxed);
    let total_ops = total_success + total_fail;

    println!("\n=== Results ===");
    println!("Total operations: {}", total_ops);
    println!("Successful: {} ({:.1}%)", total_success, 100.0 * total_success as f64 / total_ops as f64);
    println!("Failed: {} ({:.1}%)", total_fail, 100.0 * total_fail as f64 / total_ops as f64);
    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.0} ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    // Verify we can still read some items
    println!("\nVerifying reads...");
    let mut buffer = vec![0u8; value_size + 100];
    let mut read_success = 0;
    for i in 0..100 {
        let key = format!("key:{:010}", i);
        if cache.get(key.as_bytes(), &mut buffer).is_ok() {
            read_success += 1;
        }
    }
    println!("Successfully read {}/100 sampled items", read_success);
}

#[tokio::test]
async fn test_eviction_stress() {
    // This is the critical test: working set larger than cache
    // Matches production: 256MB cache, 1M keys with 1KB values = 1GB working set
    // This forces continuous eviction!

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    println!("\n=== EVICTION STRESS TEST ===");
    println!("256MB heap, 1M keys × 1KB values = ~1GB working set");
    println!("Cache can only hold ~25% of working set - HEAVY EVICTION");

    let cache = Arc::new(CacheBuilder::new()
        .heap_size(256 * 1024 * 1024)  // 256MB like production
        .segment_size(1024 * 1024)      // 1MB segments
        .hashtable_power(18)            // 256K buckets like production
        .build());

    let ttl = Duration::from_secs(3600);
    let num_threads = 8;
    let key_space = 1_000_000;     // 1M keys
    let value_size = 1024;          // 1KB values
    let operations_per_thread = 500_000; // 4M total ops with heavy churn

    // Working set = 1M × 1056 bytes ≈ 1GB
    // Cache = 256MB
    // This means ~75% of items must be evicted!

    println!("Total working set: ~1GB");
    println!("Cache capacity: 256MB");
    println!("Expected eviction rate: ~75%");
    println!("Running {}M operations across {} threads\n",
             (num_threads * operations_per_thread) / 1_000_000, num_threads);

    let success_counter = Arc::new(AtomicU64::new(0));
    let fail_counter = Arc::new(AtomicU64::new(0));
    let start = std::time::Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let success_counter = Arc::clone(&success_counter);
        let fail_counter = Arc::clone(&fail_counter);

        let handle = tokio::task::spawn(async move {
            let value = vec![0xCD_u8; value_size];
            let mut local_success = 0;
            let mut local_fail = 0;

            for i in 0..operations_per_thread {
                // Cycle through the full key space to force evictions
                let key_id = (thread_id * operations_per_thread + i) % key_space;
                let key = format!("evict_test_key:{:010}", key_id);

                match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
                    Ok(()) => local_success += 1,
                    Err(_) => {
                        local_fail += 1;
                        if local_fail == 1 {
                            println!("Thread {} first failure at operation {}", thread_id, i);
                        }
                    }
                }

                if i > 0 && i % 100_000 == 0 {
                    println!("Thread {} progress: {}/{} ({} success, {} fail)",
                             thread_id, i, operations_per_thread, local_success, local_fail);
                }
            }

            success_counter.fetch_add(local_success, AtomicOrdering::Relaxed);
            fail_counter.fetch_add(local_fail, AtomicOrdering::Relaxed);

            (local_success, local_fail)
        });

        handles.push(handle);
    }

    // Wait for completion
    for (idx, handle) in handles.into_iter().enumerate() {
        let (success, fail) = handle.await.unwrap();
        println!("Thread {} final: {} success, {} failed", idx, success, fail);
    }

    let elapsed = start.elapsed();
    let total_success = success_counter.load(AtomicOrdering::Relaxed);
    let total_fail = fail_counter.load(AtomicOrdering::Relaxed);
    let total_ops = total_success + total_fail;

    println!("\n=== EVICTION STRESS RESULTS ===");
    println!("Total operations: {}", total_ops);
    println!("Successful: {} ({:.1}%)", total_success, 100.0 * total_success as f64 / total_ops as f64);
    println!("Failed: {} ({:.1}%)", total_fail, 100.0 * total_fail as f64 / total_ops as f64);
    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.0} ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    // Most items should have been evicted, but we should be able to read recent ones
    println!("\nVerifying recent items are still cached...");
    let mut buffer = vec![0u8; value_size + 100];
    let mut read_success = 0;

    // Check the last 100 keys written (most likely to still be in cache)
    for i in (key_space - 100)..key_space {
        let key = format!("evict_test_key:{:010}", i);
        if cache.get(key.as_bytes(), &mut buffer).is_ok() {
            read_success += 1;
        }
    }
    println!("Successfully read {}/100 recent items", read_success);

    // The test passes if we didn't crash!
    println!("\n✓ Eviction stress test completed without crash");
}

#[tokio::test]
async fn test_fill_with_exact_segment_size_items() {
    // Test items that nearly fill an entire segment by themselves
    println!("\n=== Testing with near-segment-size items ===");

    let cache = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .hashtable_power(16)
        .build();

    let ttl = Duration::from_secs(3600);

    // Try values that leave different amounts of space in the segment
    let test_sizes: Vec<usize> = vec![
        1024 * 1024 - 100,  // Leave 100 bytes
        1024 * 1024 - 50,   // Leave 50 bytes
        1024 * 1024 - 25,   // Leave 25 bytes
        1024 * 1024 - 10,   // Leave 10 bytes
        1024 * 1024 - 5,    // Leave 5 bytes
    ];

    for (idx, size) in test_sizes.iter().enumerate() {
        // Account for header and key
        let value_size = size.saturating_sub(5usize + 20usize);
        let value = vec![0xCC; value_size];
        let key = format!("large_{}", idx);

        println!("Attempting to insert item with value_size={}", value_size);
        match cache.set(key.as_bytes(), &value, &[], Some(ttl)).await {
            Ok(()) => println!("  Success"),
            Err(e) => println!("  Failed: {:?}", e),
        }
    }
}
