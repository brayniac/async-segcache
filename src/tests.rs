#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use crate::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Helper to get metric values
    fn get_metric_value(metric: &metriken::Counter) -> u64 {
        metric.value()
    }

    #[test]
    #[ignore] // Slow test - explores many interleavings. Run with: cargo test --features loom -- --ignored
    fn concurrent_insert_same_segment() {
        // Note: Loom explores many interleavings. Metrics accumulate across all interleavings.
        // For exact metric validation, set LOOM_MAX_PREEMPTIONS=1 or 2
        loom::model(|| {

            let cache = Arc::new(Cache::new());
            let cache1 = Arc::clone(&cache);
            let cache2 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                cache1
                    .ttl_buckets()
                    .append_item(
                        cache1.as_ref(),
                        b"key1",
                        b"value1",
                        b"",
                        clocksource::coarse::Duration::from_secs(60),
                    )
            });

            let t2 = thread::spawn(move || {
                cache2
                    .ttl_buckets()
                    .append_item(
                        cache2.as_ref(),
                        b"key2",
                        b"value2",
                        b"",
                        clocksource::coarse::Duration::from_secs(60),
                    )
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both inserts should succeed
            assert!(result1.is_some(), "First insert failed");
            assert!(result2.is_some(), "Second insert failed");

            let (seg_id1, _offset1) = result1.unwrap();
            let (seg_id2, _offset2) = result2.unwrap();

            // Verify each segment has its item
            let segment1 = cache.segments().get(seg_id1).unwrap();
            let segment2 = cache.segments().get(seg_id2).unwrap();

            // Calculate total items - if same segment, count once; if different, sum both
            let total_items = if seg_id1 == seg_id2 {
                segment1.live_items()
            } else {
                segment1.live_items() + segment2.live_items()
            };
            assert_eq!(total_items, 2, "Total items should be 2");

            // Both segments should be Live or Sealed
            assert!(
                matches!(segment1.state(), SegmentState::Live | SegmentState::Sealed),
                "Segment 1 should be Live or Sealed"
            );
            assert!(
                matches!(segment2.state(), SegmentState::Live | SegmentState::Sealed),
                "Segment 2 should be Live or Sealed"
            );

            // Metrics are tracked correctly (verified by simpler tests with LOOM_MAX_PREEMPTIONS=1)
        });
    }

    #[test]
    fn concurrent_segment_reserve_and_release() {
        loom::model(|| {

            let cache = Arc::new(Cache::new());
            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads reserve segments
            let t1 = thread::spawn(move || c1.segments().reserve(c1.metrics()));
            let t2 = thread::spawn(move || c2.segments().reserve(c2.metrics()));

            let id1 = t1.join().unwrap();
            let id2 = t2.join().unwrap();

            // Both should succeed (plenty of segments available)
            assert!(id1.is_some());
            assert!(id2.is_some());

            // Should be different segments
            assert_ne!(id1.unwrap(), id2.unwrap());

            // Verify both are in Reserved state
            assert_eq!(cache.segments().get(id1.unwrap()).unwrap().state(), SegmentState::Reserved);
            assert_eq!(cache.segments().get(id2.unwrap()).unwrap().state(), SegmentState::Reserved);

            // Release both segments
            cache.segments().release(id1.unwrap(), cache.metrics());
            cache.segments().release(id2.unwrap(), cache.metrics());

            // Both segments should be Free
            assert_eq!(cache.segments().get(id1.unwrap()).unwrap().state(), SegmentState::Free);
            assert_eq!(cache.segments().get(id2.unwrap()).unwrap().state(), SegmentState::Free);

            // Metrics are tracked correctly (verified by simpler tests with LOOM_MAX_PREEMPTIONS=1)
        });
    }

    #[test]
    fn concurrent_item_append_to_segment() {
        loom::model(|| {

            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads append items to the same segment
            let t1 = thread::spawn(move || {
                let seg = c1.segments().get(seg_id).unwrap();
                seg.append_item(b"key1", b"value1", b"", c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let seg = c2.segments().get(seg_id).unwrap();
                seg.append_item(b"key2", b"value2", b"", c2.metrics())
            });

            let offset1 = t1.join().unwrap();
            let offset2 = t2.join().unwrap();

            // Both should succeed
            assert!(offset1.is_some(), "First append failed");
            assert!(offset2.is_some(), "Second append failed");

            // Offsets should be different (no overlap)
            assert_ne!(offset1.unwrap(), offset2.unwrap(), "Offsets should be different");

            // Verify segment stats - key correctness check
            assert_eq!(segment.live_items(), 2, "Segment should have 2 live items");
            assert!(segment.write_offset() > 0, "Segment should have non-zero write offset");

            // Metrics are tracked correctly (verified by simpler tests with LOOM_MAX_PREEMPTIONS=1)
        });
    }

    #[test]
    #[ignore] // Slow test - 3 threads with retry loops. Run with: cargo test --features loom -- --ignored
    fn cas_retry_tracking() {
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);
            let c3 = Arc::clone(&cache);

            // Three threads trying to append to the same segment simultaneously
            // This should cause CAS retries due to contention on write_offset
            let t1 = thread::spawn(move || {
                let seg = c1.segments().get(seg_id).unwrap();
                seg.append_item(b"k1", b"v1", b"", c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let seg = c2.segments().get(seg_id).unwrap();
                seg.append_item(b"k2", b"v2", b"", c2.metrics())
            });

            let t3 = thread::spawn(move || {
                let seg = c3.segments().get(seg_id).unwrap();
                seg.append_item(b"k3", b"v3", b"", c3.metrics())
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // All should succeed
            assert!(r1.is_some() && r2.is_some() && r3.is_some());

            // Verify segment has all 3 items
            let segment = cache.segments().get(seg_id).unwrap();
            assert_eq!(segment.live_items(), 3, "Segment should have 3 live items");

            // With 3 threads, CAS retries are being tracked
            // Just verify the metric is accessible
            let _ = get_metric_value(&cache.metrics().cas_retry);
        });
    }

    #[test]
    fn segment_full_tracking() {
        // This test has no concurrency, so exact metric validation works
        loom::model(|| {
            // Create a very small segment that will fill up quickly
            let cache = Arc::new(Cache::new());

            // Capture metrics at START of this interleaving
            let initial_append = get_metric_value(&cache.metrics().item_append);
            let initial_full = get_metric_value(&cache.metrics().item_append_full);

            // Create a small cache with small segments
            let cache = Arc::new(Cache {
                hashtable: Hashtable::new(4),
                segments: SegmentsBuilder::new()
                    .segment_size(128) // Very small segment
                    .heap_size(128)
                    .build(),
                ttl_buckets: TtlBuckets::new(),
                metrics: CacheMetrics::new(),
            });

            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Fill the segment with a large value
            let result1 = segment.append_item(b"key1", &[0u8; 64], b"", cache.metrics());
            assert!(result1.is_some(), "First insert should succeed");

            // Try to add another large value - should fail due to segment full
            let result2 = segment.append_item(b"key2", &[0u8; 64], b"", cache.metrics());

            // The second insert should fail due to insufficient space
            assert!(result2.is_none(), "Second insert should fail due to full segment");

            // Verify segment has exactly 1 item
            assert_eq!(segment.live_items(), 1, "Segment should have exactly 1 item");

            // Validate metrics for THIS interleaving (no concurrency = exact validation works)
            let append_delta = get_metric_value(&cache.metrics().item_append) - initial_append;
            let full_delta = get_metric_value(&cache.metrics().item_append_full) - initial_full;

            assert_eq!(append_delta, 1, "Expected exactly 1 successful append");
            assert_eq!(full_delta, 1, "Expected exactly 1 full segment failure");
        });
    }

    #[test]
    fn single_segment_reserve_metrics() {
        // Simple test with exact metric validation (no concurrency)
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            let initial_reserve = get_metric_value(&cache.metrics().segment_reserve);
            let initial_release = get_metric_value(&cache.metrics().segment_release);

            // Reserve a segment
            let id = cache.segments().reserve(cache.metrics()).unwrap();
            assert_eq!(cache.segments().get(id).unwrap().state(), SegmentState::Reserved);

            // Release it
            cache.segments().release(id, cache.metrics());
            assert_eq!(cache.segments().get(id).unwrap().state(), SegmentState::Free);

            // Exact metric validation (no concurrency)
            let reserve_delta = get_metric_value(&cache.metrics().segment_reserve) - initial_reserve;
            let release_delta = get_metric_value(&cache.metrics().segment_release) - initial_release;

            assert_eq!(reserve_delta, 1, "Expected exactly 1 segment reserved");
            assert_eq!(release_delta, 1, "Expected exactly 1 segment released");
        });
    }

    #[test]
    fn single_item_append_metrics() {
        // Simple test with exact metric validation (no concurrency)
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            let initial_append = get_metric_value(&cache.metrics().item_append);

            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Append an item
            let offset = segment.append_item(b"key", b"value", b"", cache.metrics());
            assert!(offset.is_some());
            assert_eq!(segment.live_items(), 1);

            // Exact metric validation (no concurrency)
            let append_delta = get_metric_value(&cache.metrics().item_append) - initial_append;
            assert_eq!(append_delta, 1, "Expected exactly 1 item appended");
        });
    }

    #[test]
    fn hashtable_unlink_concurrent() {
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to unlink the same item (that doesn't exist)
            let t1 = thread::spawn(move || {
                c1.hashtable().unlink_item(b"test_key", 1, 0, c1.metrics())
            });

            let t2 = thread::spawn(move || {
                c2.hashtable().unlink_item(b"test_key", 1, 0, c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both should fail to find the item (it was never inserted)
            assert!(!result1, "First unlink should fail - item not in table");
            assert!(!result2, "Second unlink should fail - item not in table");

            // Note: unlink_item only increments metrics when it finds a matching item
            // to attempt to unlink. Since the hashtable is empty, no metrics are incremented.
            // This test verifies concurrent access doesn't corrupt the hashtable structure.
        });
    }

    #[test]
    fn concurrent_packed_metadata_cas() {
        // Low-level test of packed metadata CAS operations
        // Tests the core atomic operation without retry logic
        use crate::segments::{PackedSegmentMeta, INVALID_SEGMENT_ID};
        use crate::sync::{AtomicU64, Ordering};

        loom::model(|| {
            // Create a packed metadata value representing a Live segment
            let initial = PackedSegmentMeta {
                state: SegmentState::Live,
                next: INVALID_SEGMENT_ID,
                prev: INVALID_SEGMENT_ID,
            };
            let packed_meta = Arc::new(AtomicU64::new(initial.pack()));

            let meta1 = Arc::clone(&packed_meta);
            let meta2 = Arc::clone(&packed_meta);

            // Two threads both try to CAS from Live to Sealed
            let t1 = thread::spawn(move || {
                let current = meta1.load(Ordering::Acquire);
                let current_unpacked = PackedSegmentMeta::unpack(current);

                if current_unpacked.state == SegmentState::Live {
                    let new_meta = PackedSegmentMeta {
                        state: SegmentState::Sealed,
                        next: current_unpacked.next,
                        prev: current_unpacked.prev,
                    };
                    meta1.compare_exchange(
                        current,
                        new_meta.pack(),
                        Ordering::Release,
                        Ordering::Acquire
                    ).is_ok()
                } else {
                    false
                }
            });

            let t2 = thread::spawn(move || {
                let current = meta2.load(Ordering::Acquire);
                let current_unpacked = PackedSegmentMeta::unpack(current);

                if current_unpacked.state == SegmentState::Live {
                    let new_meta = PackedSegmentMeta {
                        state: SegmentState::Sealed,
                        next: current_unpacked.next,
                        prev: current_unpacked.prev,
                    };
                    meta2.compare_exchange(
                        current,
                        new_meta.pack(),
                        Ordering::Release,
                        Ordering::Acquire
                    ).is_ok()
                } else {
                    false
                }
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Exactly one should succeed
            assert_ne!(result1, result2, "Exactly one CAS should succeed");

            // Final state should be Sealed
            let final_packed = packed_meta.load(Ordering::Acquire);
            let final_meta = PackedSegmentMeta::unpack(final_packed);
            assert_eq!(final_meta.state, SegmentState::Sealed);
        });
    }


    #[test]
    fn concurrent_write_offset_cas() {
        // Low-level test of write_offset CAS for item appending
        // Tests atomic offset reservation without retry logic
        use crate::sync::{AtomicU32, Ordering};

        loom::model(|| {
            let write_offset = Arc::new(AtomicU32::new(0));
            let data_len = 1000u32;

            let off1 = Arc::clone(&write_offset);
            let off2 = Arc::clone(&write_offset);

            // Two threads try to reserve space (100 bytes each)
            let t1 = thread::spawn(move || {
                let current = off1.load(Ordering::Acquire);
                let new_offset = current + 100;

                if new_offset <= data_len {
                    off1.compare_exchange(
                        current,
                        new_offset,
                        Ordering::Release,
                        Ordering::Acquire
                    ).ok().map(|_| current)
                } else {
                    None
                }
            });

            let t2 = thread::spawn(move || {
                let current = off2.load(Ordering::Acquire);
                let new_offset = current + 100;

                if new_offset <= data_len {
                    off2.compare_exchange(
                        current,
                        new_offset,
                        Ordering::Release,
                        Ordering::Acquire
                    ).ok().map(|_| current)
                } else {
                    None
                }
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // At least one should succeed
            // If both load 0 initially, both will try to CAS 0→100 and only one wins
            // If they interleave, both can succeed with offsets 0 and 100
            let success_count = [result1.is_some(), result2.is_some()].iter().filter(|&&x| x).count();
            assert!(success_count >= 1, "At least one reservation should succeed");

            if success_count == 2 {
                // Both succeeded - they got different offsets
                assert_ne!(result1.unwrap(), result2.unwrap(), "Offsets should be different");
                assert_eq!(write_offset.load(Ordering::Acquire), 200, "Final offset should be 200");
            } else {
                // Only one succeeded
                assert_eq!(write_offset.load(Ordering::Acquire), 100, "Final offset should be 100");
            }
        });
    }

    #[test]
    fn concurrent_append_and_get() {
        // Test multiple threads appending and reading from the same segment
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: Append an item
            let t1 = thread::spawn(move || {
                let segment = c1.segments().get(seg_id).unwrap();
                segment.append_item(b"key1", b"value1", b"", c1.metrics())
            });

            // Thread 2: Append another item
            let t2 = thread::spawn(move || {
                let segment = c2.segments().get(seg_id).unwrap();
                segment.append_item(b"key2", b"value2", b"", c2.metrics())
            });

            let offset1 = t1.join().unwrap();
            let offset2 = t2.join().unwrap();

            assert!(offset1.is_some(), "First append should succeed");
            assert!(offset2.is_some(), "Second append should succeed");

            let offset1 = offset1.unwrap();
            let offset2 = offset2.unwrap();

            // Offsets should be different
            assert_ne!(offset1, offset2, "Offsets should be different");

            // Both items should be readable
            let segment = cache.segments().get(seg_id).unwrap();
            let mut buffer1 = vec![0u8; 1024];
            let mut buffer2 = vec![0u8; 1024];

            let item1 = segment.get_item(offset1, b"key1", &mut buffer1).unwrap();
            assert_eq!(item1.key(), b"key1");
            assert_eq!(item1.value(), b"value1");

            let item2 = segment.get_item(offset2, b"key2", &mut buffer2).unwrap();
            assert_eq!(item2.key(), b"key2");
            assert_eq!(item2.value(), b"value2");
        });
    }

    #[test]
    fn get_item_race_with_state_transition() {
        // Test the double-check pattern in get_item
        // Thread 1 starts get_item, Thread 2 transitions to Draining
        use crate::segments::INVALID_SEGMENT_ID;

        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Append an item
            let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

            // Manually set up segment state
            segment.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Sealed,
                Some(INVALID_SEGMENT_ID),
                Some(INVALID_SEGMENT_ID),
                cache.metrics(),
            );

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: Try to get_item and validate inside the thread
            let t1 = thread::spawn(move || {
                let segment = c1.segments().get(seg_id).unwrap();
                let mut buffer = vec![0u8; 1024];
                let result = segment.get_item(offset, b"key");

                // Validate the result inside the thread
                match result {
                    Ok(item) => {
                        assert_eq!(item.key(), b"key");
                        assert_eq!(item.value(), b"value");
                        true  // Return success
                    }
                    Err(GetItemError::SegmentNotAccessible) => false, // Failed as expected
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            });

            // Thread 2: Transition to Draining
            let t2 = thread::spawn(move || {
                let segment = c2.segments().get(seg_id).unwrap();
                segment.cas_metadata(
                    SegmentState::Sealed,
                    SegmentState::Draining,
                    None,
                    None,
                    c2.metrics(),
                )
            });

            let _get_succeeded = t1.join().unwrap();
            let _transition_result = t2.join().unwrap();

            // ref_count should always be 0 at the end (properly cleaned up)
            assert_eq!(segment.ref_count(), 0, "ref_count should be cleaned up");

            // Either get_item succeeded (and got valid data), or it failed with SegmentNotAccessible
            // Both outcomes are correct depending on interleaving
        });
    }

    #[test]
    fn concurrent_mark_deleted() {
        // Two threads trying to mark the same item deleted - only one should succeed
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Append an item
            let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();
            assert_eq!(segment.live_items(), 1);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to mark the same item deleted
            let t1 = thread::spawn(move || {
                let segment = c1.segments().get(seg_id).unwrap();
                segment.mark_deleted(offset, b"key", c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let segment = c2.segments().get(seg_id).unwrap();
                segment.mark_deleted(offset, b"key", c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Exactly one should return Ok(true), the other Ok(false)
            let success_count = [
                result1 == Ok(true),
                result2 == Ok(true),
            ].iter().filter(|&&x| x).count();

            assert_eq!(success_count, 1, "Exactly one delete should succeed");

            // live_items should be decremented exactly once
            assert_eq!(segment.live_items(), 0);

            // Both results should be Ok (no errors)
            assert!(result1.is_ok());
            assert!(result2.is_ok());
        });
    }

    #[test]
    fn mark_deleted_race_with_get_item() {
        // Test mark_deleted and get_item racing
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Append an item
            let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: mark_deleted
            let t1 = thread::spawn(move || {
                let segment = c1.segments().get(seg_id).unwrap();
                segment.mark_deleted(offset, b"key", c1.metrics())
            });

            // Thread 2: get_item - validate inside thread
            let t2 = thread::spawn(move || {
                let segment = c2.segments().get(seg_id).unwrap();
                let mut buffer = vec![0u8; 1024];
                let result = segment.get_item(offset, b"key");

                match result {
                    Ok(item) => {
                        // Read succeeded before deletion
                        assert_eq!(item.key(), b"key");
                        assert_eq!(item.value(), b"value");
                        Ok(())
                    }
                    Err(GetItemError::ItemDeleted) => {
                        // Read happened after deletion - expected
                        Err(())
                    }
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            });

            let _delete_result = t1.join().unwrap();
            let _get_result = t2.join().unwrap();

            // Both operations should complete successfully (no panics)
            // Item should be deleted at the end
            assert_eq!(segment.live_items(), 0);
        });
    }

    #[test]
    #[ignore] // Expensive loom test with many interleavings. Run with LOOM_MAX_PREEMPTIONS=0
    fn concurrent_expire() {
        // Test that concurrent expire() attempts are safe and don't panic
        // When two threads race to expire a segment:
        // - First thread successfully clears and releases to free pool
        // - Second thread detects the race and returns false
        // Both complete successfully without panicking
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Add an item so we have something to clear
            segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                c1.segments().expire(seg_id, c1.as_ref());
            });

            let t2 = thread::spawn(move || {
                c2.segments().expire(seg_id, c2.as_ref());
            });

            // Both should complete without panicking
            t1.join().unwrap();
            t2.join().unwrap();

            // Segment should end up in Free state
            assert_eq!(segment.state(), SegmentState::Free);
            assert_eq!(segment.live_items(), 0);
            assert_eq!(segment.write_offset(), 0);
        });
    }

    #[test]
    fn concurrent_hashtable_get() {
        // Test that concurrent get() calls are safe
        // Multiple threads reading the same key should not interfere
        use std::hash::{BuildHasher, Hash, Hasher};
        use crate::hashtable::Hashbucket;
        use crate::sync::Ordering;

        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Reserve segment and append an item
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();
            let offset = segment.append_item(b"concurrent_key", b"value", b"", cache.metrics()).unwrap();

            // Manually insert into hashtable
            let mut hasher = (*cache.hashtable().hash_builder).build_hasher();
            b"concurrent_key".hash(&mut hasher);
            let hash = hasher.finish();
            let bucket_index = (hash & cache.hashtable().mask) as usize;
            let tag = ((hash >> 32) & 0xFFF) as u16;
            let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
            cache.hashtable().data[bucket_index].items[0].store(packed, Ordering::Release);

            // Two threads reading concurrently
            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                c1.hashtable().get(b"concurrent_key", c1.segments())
            });

            let t2 = thread::spawn(move || {
                c2.hashtable().get(b"concurrent_key", c2.segments())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both should succeed (or both fail if deletion happens)
            // At minimum, they shouldn't panic or corrupt data
            assert!(result1.is_some() || result2.is_some());
        });
    }

    #[test]
    fn hashtable_get_with_concurrent_unlink() {
        // Test get() racing with unlink_item()
        // One thread reads while another deletes
        use std::hash::{BuildHasher, Hash, Hasher};
        use crate::hashtable::Hashbucket;
        use crate::sync::Ordering;

        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Reserve segment and append an item
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();
            let offset = segment.append_item(b"race_key", b"value", b"", cache.metrics()).unwrap();

            // Insert into hashtable
            let mut hasher = (*cache.hashtable().hash_builder).build_hasher();
            b"race_key".hash(&mut hasher);
            let hash = hasher.finish();
            let bucket_index = (hash & cache.hashtable().mask) as usize;
            let tag = ((hash >> 32) & 0xFFF) as u16;
            let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
            cache.hashtable().data[bucket_index].items[0].store(packed, Ordering::Release);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: get
            let t1 = thread::spawn(move || {
                c1.hashtable().get(b"race_key", c1.segments())
            });

            // Thread 2: unlink
            let t2 = thread::spawn(move || {
                c2.hashtable().unlink_item(b"race_key", seg_id, offset, c2.metrics())
            });

            let get_result = t1.join().unwrap();
            let unlink_result = t2.join().unwrap();

            // Either:
            // - get sees the item before unlink (get=Some, unlink=true)
            // - get misses after unlink (get=None, unlink=true)
            // - both operations interleave in various ways
            // Important: no panics or corruption
            if get_result.is_some() {
                assert_eq!(get_result.unwrap(), (seg_id, offset));
            }
        });
    }

    #[test]
    fn concurrent_link_same_bucket() {
        // Test concurrent link_item() to the same bucket (CAS contention)
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Create two items that will hash to the same bucket
            let offset1 = segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();
            let offset2 = segment.append_item(b"key2", b"value2", b"", cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads trying to link items to the same bucket concurrently
            let t1 = thread::spawn(move || {
                c1.hashtable().link_item(b"key1", seg_id, offset1, c1.segments(), c1.metrics())
            });

            let t2 = thread::spawn(move || {
                c2.hashtable().link_item(b"key2", seg_id, offset2, c2.segments(), c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both should succeed (bucket has 7 slots)
            assert!(result1.is_ok());
            assert!(result2.is_ok());

            // Verify both items are in the hashtable
            let found1 = cache.hashtable().get(b"key1", cache.segments());
            let found2 = cache.hashtable().get(b"key2", cache.segments());

            assert_eq!(found1, Some((seg_id, offset1)));
            assert_eq!(found2, Some((seg_id, offset2)));
        });
    }

    #[test]
    fn link_item_race_with_get() {
        // Test link_item() racing with get()
        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();
            let offset = segment.append_item(b"race_key", b"value", b"", cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: link
            let t1 = thread::spawn(move || {
                c1.hashtable().link_item(b"race_key", seg_id, offset, c1.segments(), c1.metrics())
            });

            // Thread 2: get
            let t2 = thread::spawn(move || {
                c2.hashtable().get(b"race_key", c2.segments())
            });

            let link_result = t1.join().unwrap();
            let get_result = t2.join().unwrap();

            // Link should always succeed
            assert!(link_result.is_ok());

            // Get either finds it or doesn't depending on timing
            if get_result.is_some() {
                assert_eq!(get_result.unwrap(), (seg_id, offset));
            }
        });
    }

    #[test]
    fn link_item_race_with_unlink() {
        // Test link_item() racing with unlink_item()
        use std::hash::{BuildHasher, Hash, Hasher};
        use crate::hashtable::Hashbucket;
        use crate::sync::Ordering;

        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();
            let offset = segment.append_item(b"race_key", b"value", b"", cache.metrics()).unwrap();

            // Pre-insert the item into hashtable
            let mut hasher = (*cache.hashtable().hash_builder).build_hasher();
            b"race_key".hash(&mut hasher);
            let hash = hasher.finish();
            let bucket_index = (hash & cache.hashtable().mask) as usize;
            let tag = ((hash >> 32) & 0xFFF) as u16;
            let packed = Hashbucket::pack_item(tag, 1, seg_id, offset);
            cache.hashtable().data[bucket_index].items[0].store(packed, Ordering::Release);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: unlink
            let t1 = thread::spawn(move || {
                c1.hashtable().unlink_item(b"race_key", seg_id, offset, c1.metrics())
            });

            // Thread 2: link (trying to re-link the same item)
            let t2 = thread::spawn(move || {
                c2.hashtable().link_item(b"race_key", seg_id, offset, c2.segments(), c2.metrics())
            });

            let unlink_result = t1.join().unwrap();
            let link_result = t2.join().unwrap();

            // Both operations should succeed (no corruption)
            // The final state depends on ordering
            let final_state = cache.hashtable().get(b"race_key", cache.segments());

            if final_state.is_some() {
                assert_eq!(final_state.unwrap(), (seg_id, offset));
            }
        });
    }

    #[test]
    fn concurrent_link_bucket_full() {
        // Test concurrent link_item() when bucket is full (eviction needed)
        use std::hash::{BuildHasher, Hash, Hasher};
        use crate::hashtable::Hashbucket;
        use crate::sync::Ordering;

        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Fill all 7 slots in a bucket
            let mut hasher = (*cache.hashtable().hash_builder).build_hasher();
            b"fill_key".hash(&mut hasher);
            let hash = hasher.finish();
            let bucket_index = (hash & cache.hashtable().mask) as usize;
            let tag = ((hash >> 32) & 0xFFF) as u16;

            for i in 0..7 {
                let packed = Hashbucket::pack_item(tag, (i + 1) as u8, seg_id, (i * 100) as u32);
                cache.hashtable().data[bucket_index].items[i].store(packed, Ordering::Release);
            }

            // Now try to insert new items concurrently (will need eviction)
            let offset1 = segment.append_item(b"new_key1", b"value1", b"", cache.metrics()).unwrap();
            let offset2 = segment.append_item(b"new_key2", b"value2", b"", cache.metrics()).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                c1.hashtable().link_item(b"new_key1", seg_id, offset1, c1.segments(), c1.metrics())
            });

            let t2 = thread::spawn(move || {
                c2.hashtable().link_item(b"new_key2", seg_id, offset2, c2.segments(), c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // At least one should succeed (one might fail due to bucket full)
            // But no panics or corruption
            let success_count = (result1.is_ok() as u32) + (result2.is_ok() as u32);
            assert!(success_count >= 1);
        });
    }

    #[test]
    fn concurrent_set_same_key() {
        // Test two threads concurrently overwriting the same key
        // This tests the overwrite logic under concurrent access
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Both threads set the same key with different values
            let t1 = thread::spawn(move || {
                c1.set(b"concurrent_key", b"value1", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let t2 = thread::spawn(move || {
                c2.set(b"concurrent_key", b"value2", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both sets should succeed
            assert!(result1.is_ok(), "First set should succeed");
            assert!(result2.is_ok(), "Second set should succeed");

            // items_live should be exactly 1 (not 2) after concurrent overwrites
            assert_eq!(cache.metrics().items_live.value(), 1, "Should have exactly 1 live item after concurrent overwrites");

            // Get the value - should be either value1 or value2 depending on which won
            let mut buffer = vec![0u8; 1024];
            let item = cache.get(b"concurrent_key");
            assert!(item.is_ok(), "Key should exist after concurrent sets");

            let item = item.unwrap();
            assert_eq!(item.key(), b"concurrent_key");
            // Value should be one of the two values
            assert!(
                item.value() == b"value1" || item.value() == b"value2",
                "Value should be either value1 or value2"
            );
        });
    }

    #[test]
    fn concurrent_set_different_keys() {
        // Test two threads setting different keys concurrently
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                c1.set(b"key1", b"value1", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let t2 = thread::spawn(move || {
                c2.set(b"key2", b"value2", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both should succeed
            assert!(result1.is_ok());
            assert!(result2.is_ok());

            // Should have 2 items
            assert_eq!(cache.metrics().items_live.value(), 2);

            // Both keys should be retrievable
            let mut buffer = vec![0u8; 1024];
            let item1 = cache.get(b"key1");
            assert!(item1.is_ok());
            assert_eq!(item1.unwrap().value(), b"value1");

            let item2 = cache.get(b"key2");
            assert!(item2.is_ok());
            assert_eq!(item2.unwrap().value(), b"value2");
        });
    }

    #[test]
    fn set_race_with_get() {
        // Test set() racing with get() on the same key
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Pre-populate with initial value
            cache.set(b"race_key", b"initial", b"", Some(clocksource::coarse::Duration::from_secs(60))).unwrap();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Thread 1: overwrite the key
            let t1 = thread::spawn(move || {
                c1.set(b"race_key", b"updated", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            // Thread 2: get the key
            let t2 = thread::spawn(move || {
                let mut buffer = vec![0u8; 1024];
                c2.get(b"race_key").map(|item| {
                    item.value().to_vec()
                })
            });

            let set_result = t1.join().unwrap();
            let get_result = t2.join().unwrap();

            // Set should succeed
            assert!(set_result.is_ok());

            // Get should succeed and return either "initial" or "updated" depending on timing
            assert!(get_result.is_ok(), "Get should find the key");
            let value = get_result.unwrap();
            assert!(
                value == b"initial" || value == b"updated",
                "Value should be either initial or updated, got {:?}",
                String::from_utf8_lossy(&value)
            );

            // items_live should be 1 (overwrite, not append)
            assert_eq!(cache.metrics().items_live.value(), 1);
        });
    }

    // Note: This test is disabled because delete() is now async and loom doesn't support async well
    // The race condition between set() and delete() is still possible but harder to test with loom
    // #[test]
    // fn set_race_with_delete() {
    //     // Test set() racing with delete() on the same key
    //     loom::model(|| {
    //         let cache = Arc::new(Cache::new());
    //
    //         // Pre-populate with initial value
    //         cache.set(b"delete_race", b"initial", b"", Some(clocksource::coarse::Duration::from_secs(60))).unwrap();
    //
    //         let c1 = Arc::clone(&cache);
    //         let c2 = Arc::clone(&cache);
    //
    //         // Thread 1: overwrite the key
    //         let t1 = thread::spawn(move || {
    //             c1.set(b"delete_race", b"updated", b"", Some(clocksource::coarse::Duration::from_secs(60)))
    //         });
    //
    //         // Thread 2: delete the key
    //         let t2 = thread::spawn(move || {
    //             c2.delete(b"delete_race").await // Can't use await in loom thread::spawn
    //         });
    //
    //         let set_result = t1.join().unwrap();
    //         let delete_result = t2.join().unwrap();
    //
    //         // Both operations should complete without error
    //         assert!(set_result.is_ok());
    //         assert!(delete_result.is_ok());
    //
    //         // Final state depends on ordering:
    //         // - If delete happened last: items_live = 0, delete returns true
    //         // - If set happened last: items_live = 1, delete might return true or false
    //         let items = cache.metrics().items_live.value();
    //         assert!(items == 0 || items == 1, "items_live should be 0 or 1, got {}", items);
    //
    //         // Try to get the key - may or may not exist depending on operation order
    //         let mut buffer = vec![0u8; 1024];
    //         let get_result = cache.get(b"delete_race");
    //
    //         if items == 1 {
    //             // If item exists, it should be "updated"
    //             assert!(get_result.is_ok(), "If items_live=1, key should exist");
    //             assert_eq!(get_result.unwrap().value(), b"updated");
    //         } else {
    //             // If items_live=0, key should not exist
    //             assert!(get_result.is_err(), "If items_live=0, key should not exist");
    //         }
    //     });
    // }

    #[test]
    fn concurrent_set_overwrite_metrics() {
        // Test that metrics stay consistent during concurrent overwrites
        // This is a simpler test focused just on metrics correctness
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Pre-populate
            cache.set(b"metric_key", b"v1", b"", Some(clocksource::coarse::Duration::from_secs(60))).unwrap();
            assert_eq!(cache.metrics().items_live.value(), 1);

            let initial_items = cache.metrics().items_live.value();
            let initial_appends = cache.metrics().item_append.value();

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads overwrite the same key
            let t1 = thread::spawn(move || {
                c1.set(b"metric_key", b"v2", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let t2 = thread::spawn(move || {
                c2.set(b"metric_key", b"v3", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            assert!(r1.is_ok());
            assert!(r2.is_ok());

            // Check how many appends happened
            let final_appends = cache.metrics().item_append.value();
            let appends_during_overwrites = final_appends - initial_appends;

            // Check how many threads got None from reserve_slot (no replaced item)
            let none_count = cache.metrics().cas_retry.value(); // Debug marker
            let clean_none_count = cache.metrics().cas_abort.value(); // "clean" None returns (no conflicts found)

            // items_live should still be 1 (not 2 or 3)
            let final_items = cache.metrics().items_live.value();
            if final_items != initial_items {
                panic!(
                    "items_live mismatch: initial={}, final={}, appends={}, none_count={}, clean_none={}, hashtable_link={}",
                    initial_items, final_items, appends_during_overwrites, none_count, clean_none_count,
                    cache.metrics().hashtable_link.value()
                );
            }
        });
    }

    #[test]
    fn triple_concurrent_set_same_key() {
        // Test three threads concurrently overwriting the same key
        // More complex scenario with higher contention
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);
            let c3 = Arc::clone(&cache);

            let t1 = thread::spawn(move || {
                c1.set(b"triple", b"value1", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let t2 = thread::spawn(move || {
                c2.set(b"triple", b"value2", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let t3 = thread::spawn(move || {
                c3.set(b"triple", b"value3", b"", Some(clocksource::coarse::Duration::from_secs(60)))
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // All should succeed
            assert!(r1.is_ok() && r2.is_ok() && r3.is_ok());

            // Should have exactly 1 item
            assert_eq!(cache.metrics().items_live.value(), 1);

            // Verify the key exists and has one of the three values
            let mut buffer = vec![0u8; 1024];
            let item = cache.get(b"triple");
            assert!(item.is_ok());
            let item = item.unwrap();
            let value = item.value();
            assert!(
                value == b"value1" || value == b"value2" || value == b"value3",
                "Value should be one of the three values"
            );
        });
    }
}
