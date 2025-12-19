use crate::*;
use crate::sync::*;
use clocksource::coarse::Duration;

/// Error type for try_append_segment operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendSegmentError {
    InvalidSegmentId,
    SegmentNotReserved,
    FailedToTransitionToLinking,
    BucketModified,
    InvalidTailSegmentId,
    TailInUnexpectedState,
}

/// Error type for try_append_item operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendItemError {
    NoTailSegment,
    InvalidTailSegmentId,
    TailNotLive,
    TailSegmentFull,
}

/// Error type for try_remove_segment operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoveSegmentError {
    InvalidSegmentId,
    FailedToTransitionToDraining,
    #[allow(dead_code)]
    SegmentHasActiveReaders,
    FailedToTransitionToLocked,
    InvalidPrevSegmentId,
    InvalidNextSegmentId,
    FailedToLockPrevSegment,
    FailedToLockNextSegment,
    BucketModified,
}

// TTL bucket configuration constants
const N_BUCKET_PER_STEP_N_BIT: usize = 8;
const N_BUCKET_PER_STEP: usize = 1 << N_BUCKET_PER_STEP_N_BIT; // 256 buckets per step

const TTL_BUCKET_INTERVAL_N_BIT_1: usize = 3; // 8 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_2: usize = 7; // 128 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_3: usize = 11; // 2048 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_4: usize = 15; // 32768 second intervals

const TTL_BUCKET_INTERVAL_1: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_1; // 8
const TTL_BUCKET_INTERVAL_2: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_2; // 128
const TTL_BUCKET_INTERVAL_3: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_3; // 2048
const TTL_BUCKET_INTERVAL_4: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_4; // 32768

const TTL_BOUNDARY_1: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_1 + N_BUCKET_PER_STEP_N_BIT); // 2048
const TTL_BOUNDARY_2: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_2 + N_BUCKET_PER_STEP_N_BIT); // 32768
const TTL_BOUNDARY_3: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_3 + N_BUCKET_PER_STEP_N_BIT); // 524288

const MAX_N_TTL_BUCKET: usize = N_BUCKET_PER_STEP * 4; // 1024 total buckets
const MAX_TTL_BUCKET_IDX: usize = MAX_N_TTL_BUCKET - 1;

/// TTL buckets for organizing segments by expiration time
pub struct TtlBuckets {
    buckets: Box<[TtlBucket]>,
    #[allow(dead_code)]
    last_expired: clocksource::coarse::AtomicInstant,
}

impl TtlBuckets {
    /// Create a new set of `TtlBuckets` which cover the full range of TTLs.
    /// Uses logarithmic bucketing for efficient coverage of wide TTL ranges.
    pub fn new() -> Self {
        let intervals = [
            TTL_BUCKET_INTERVAL_1,
            TTL_BUCKET_INTERVAL_2,
            TTL_BUCKET_INTERVAL_3,
            TTL_BUCKET_INTERVAL_4,
        ];

        let mut buckets = Vec::with_capacity(intervals.len() * N_BUCKET_PER_STEP);

        for (i, interval) in intervals.iter().enumerate() {
            for j in 0..N_BUCKET_PER_STEP {
                // Buckets use the minimum TTL of their range to ensure we expire early, not late
                // Shift all ranges by 1 to avoid TTL=0:
                // Bucket 0 (range 1-8s): TTL=1s
                // Bucket 1 (range 9-16s): TTL=9s
                // Bucket 2 (range 17-24s): TTL=17s
                let ttl_secs = (interval * j + 1) as u32;
                let ttl = Duration::from_secs(ttl_secs);
                let index = (i * N_BUCKET_PER_STEP + j) as u16;
                let bucket = TtlBucket::new(ttl, index);
                buckets.push(bucket);
            }
        }

        let buckets = buckets.into_boxed_slice();
        let last_expired = clocksource::coarse::AtomicInstant::now();

        Self {
            buckets,
            last_expired,
        }
    }

    /// Get the index of the `TtlBucket` for the given TTL.
    /// Uses branchless operations for better performance.
    fn get_bucket_index(&self, ttl: Duration) -> usize {
        let ttl_secs = ttl.as_secs() as i32;

        // Handle negative/zero TTL - this is a bug condition
        if ttl_secs <= 0 {
            panic!("TTL must be positive, got {} seconds", ttl_secs);
        }

        // Branchless bucket index calculation using bit manipulation
        // We compute all possible indices and select the correct one
        let idx1 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_1) as usize;
        let idx2 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_2) as usize + N_BUCKET_PER_STEP;
        let idx3 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_3) as usize + N_BUCKET_PER_STEP * 2;
        let idx4 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_4) as usize + N_BUCKET_PER_STEP * 3;

        // Create masks for each range (1 if in range, 0 otherwise)
        let mask1 = ((ttl_secs < TTL_BOUNDARY_1) as usize).wrapping_neg();
        let mask2 =
            (((ttl_secs >= TTL_BOUNDARY_1) & (ttl_secs < TTL_BOUNDARY_2)) as usize).wrapping_neg();
        let mask3 =
            (((ttl_secs >= TTL_BOUNDARY_2) & (ttl_secs < TTL_BOUNDARY_3)) as usize).wrapping_neg();
        let mask4 = ((ttl_secs >= TTL_BOUNDARY_3) as usize).wrapping_neg();

        // Select the appropriate index using bitwise operations
        let bucket_idx = (idx1 & mask1) | (idx2 & mask2) | (idx3 & mask3) | (idx4 & mask4);

        // Clamp to maximum bucket index
        std::cmp::min(bucket_idx, MAX_TTL_BUCKET_IDX)
    }

    /// Get the bucket for a given TTL value
    pub fn get_bucket(&self, ttl: Duration) -> &TtlBucket {
        let idx = self.get_bucket_index(ttl);
        &self.buckets[idx]
    }

    /// Get the bucket for a given TTL in seconds
    #[cfg(test)] // Used in tests
    pub(crate) fn get_bucket_for_seconds(&self, ttl_seconds: u32) -> &TtlBucket {
        let ttl = Duration::from_secs(ttl_seconds);
        self.get_bucket(ttl)
    }

    /// Get a bucket by its index
    pub fn get_bucket_by_index(&self, index: u16) -> &TtlBucket {
        &self.buckets[index as usize]
    }

    /// Append an item to the appropriate TTL bucket based on duration
    ///
    /// # Loom Test Coverage
    /// - `concurrent_insert_same_segment` (ignored) - Full end-to-end test through this path
    pub async fn append_item(
        &self,
        cache: &impl CacheOps,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        duration: Duration,
    ) -> Option<(u32, u32)> {
        let bucket = self.get_bucket(duration);
        bucket.append_item(cache, key, value, optional, cache.metrics()).await
    }
}

/// A single TTL bucket containing a doubly-linked list of segments
pub struct TtlBucket {
    // Use AtomicU64 to pack head and tail for atomic updates
    // Layout: [16 bits unused][24 bits tail][24 bits head]
    head_tail: AtomicU64,

    // Async mutex for serializing segment append/remove operations
    // This eliminates CAS retry loops and allows other tasks to run while waiting
    write_lock: tokio::sync::Mutex<()>,

    ttl: Duration,
    index: u16, // Bucket index in the TtlBuckets array
}

impl TtlBucket {
    const INVALID_ID: u32 = 0xFFFFFF;

    pub fn new(ttl: Duration, index: u16) -> Self {
        // Initially empty - both head and tail are invalid
        let initial = Self::pack_head_tail(Self::INVALID_ID, Self::INVALID_ID);
        Self {
            head_tail: AtomicU64::new(initial),
            write_lock: tokio::sync::Mutex::new(()),
            ttl,
            index,
        }
    }

    fn pack_head_tail(head: u32, tail: u32) -> u64 {
        let head_24 = (head & 0xFFFFFF) as u64;
        let tail_24 = (tail & 0xFFFFFF) as u64;
        (tail_24 << 24) | head_24
    }

    fn unpack_head_tail(packed: u64) -> (u32, u32) {
        let head = (packed & 0xFFFFFF) as u32;
        let tail = ((packed >> 24) & 0xFFFFFF) as u32;
        (head, tail)
    }

    /// Get the head segment ID of this bucket
    pub fn head(&self) -> Option<u32> {
        let packed = self.head_tail.load(Ordering::Acquire);
        let (head, _) = Self::unpack_head_tail(packed);
        if head == Self::INVALID_ID {
            None
        } else {
            Some(head)
        }
    }

    /// Get the tail segment ID of this bucket
    #[cfg(test)] // Used in tests
    pub(crate) fn tail(&self) -> Option<u32> {
        let packed = self.head_tail.load(Ordering::Acquire);
        let (_, tail) = Self::unpack_head_tail(packed);
        if tail == Self::INVALID_ID {
            None
        } else {
            Some(tail)
        }
    }

    /// Test-only helper to set bucket head and tail pointers directly
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn test_set_head_tail(&self, head: u32, tail: u32) {
        let packed = Self::pack_head_tail(head, tail);
        self.head_tail.store(packed, Ordering::Release);
    }

    /// Try to append a segment to the end of this TTL bucket chain (single attempt, no retry)
    ///
    /// This transitions the segment from Reserved to Live state and adds it to the bucket's
    /// linked list. If the bucket is empty, the segment becomes both head and tail.
    ///
    /// Returns Ok(()) on success, Err with reason on failure.
    ///
    /// # Loom Test Coverage
    /// - `try_append_segment` - Low-level single-attempt append without retry loops
    pub(crate) fn try_append_segment(
        &self,
        cache: &impl CacheOps,
        segment_id: u32,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(), AppendSegmentError> {
        let segment = cache
            .segments()
            .get(segment_id)
            .ok_or(AppendSegmentError::InvalidSegmentId)?;

        // Verify segment is in Reserved state
        if segment.state() != SegmentState::Reserved {
            return Err(AppendSegmentError::SegmentNotReserved);
        }

        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (current_head, current_tail) = Self::unpack_head_tail(current_packed);

        if current_head == Self::INVALID_ID {
            // List is empty - try to make this segment both head and tail
            let new_packed = Self::pack_head_tail(segment_id, segment_id);

            // First, transition segment to Linking state
            if !segment.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Linking,
                None, // next stays invalid
                None, // prev stays invalid
                metrics,
            ) {
                return Err(AppendSegmentError::FailedToTransitionToLinking);
            }

            // Set bucket ID now that we own the segment in Linking state
            segment.set_bucket_id(self.index);

            // Try to update head and tail atomically
            match self.head_tail.compare_exchange(
                current_packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully updated bucket, now transition segment to Live
                    // We own this segment in Linking state, this CAS must succeed
                    assert!(
                        segment.cas_metadata(
                            SegmentState::Linking,
                            SegmentState::Live,
                            None, // next stays invalid
                            None, // prev stays invalid
                            metrics,
                        ),
                        "Failed to transition owned segment to Live in empty bucket - data structure corruption detected"
                    );
                    metrics.ttl_append_segment.increment();
                    Ok(())
                }
                Err(_) => {
                    // Someone else modified the bucket
                    // Clear bucket ID and revert from Linking to Reserved first
                    segment.clear_bucket_id();
                    segment.cas_metadata(
                        SegmentState::Linking,
                        SegmentState::Reserved,
                        None,
                        None,
                        metrics,
                    );
                    // Now return the segment to the free pool
                    cache.segments().release(segment_id, metrics);
                    metrics.ttl_append_segment_error.increment();
                    Err(AppendSegmentError::BucketModified)
                }
            }
        } else {
            // List is not empty - append to tail
            let tail_segment = cache
                .segments()
                .get(current_tail)
                .ok_or(AppendSegmentError::InvalidTailSegmentId)?;

            // First, transition to Linking state and set prev pointer
            if !segment.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Linking,
                None,               // next stays invalid (it's the new tail)
                Some(current_tail), // prev points to old tail
                metrics,
            ) {
                return Err(AppendSegmentError::FailedToTransitionToLinking);
            }

            // Set bucket ID now that we own the segment in Linking state
            segment.set_bucket_id(self.index);

            // Seal the old tail and update it to point forward to new segment
            // Try Live first (common case), then Sealed (if already sealed)
            // Pass None for prev to preserve current value (race-free)
            let sealed = tail_segment.cas_metadata(
                SegmentState::Live,
                SegmentState::Sealed,
                Some(segment_id), // next now points to new segment
                None,             // Preserve current prev
                metrics,
            ) || tail_segment.cas_metadata(
                SegmentState::Sealed,
                SegmentState::Sealed,
                Some(segment_id), // next now points to new segment
                None,             // Preserve current prev
                metrics,
            );

            if !sealed {
                // Tail is in unexpected state, abort
                segment.clear_bucket_id();
                segment.cas_metadata(SegmentState::Linking, SegmentState::Reserved, None, None, metrics);
                metrics.ttl_append_segment_error.increment();
                return Err(AppendSegmentError::TailInUnexpectedState);
            }

            // Finally, update the bucket's tail pointer
            let new_packed = Self::pack_head_tail(current_head, segment_id);
            match self.head_tail.compare_exchange(
                current_packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully updated bucket tail, now transition to Live
                    // We own this segment in Linking state, this CAS must succeed
                    assert!(
                        segment.cas_metadata(
                            SegmentState::Linking,
                            SegmentState::Live,
                            None,               // next stays invalid
                            Some(current_tail), // prev stays set
                            metrics,
                        ),
                        "Failed to transition owned segment to Live after linking - data structure corruption detected"
                    );
                    metrics.ttl_append_segment.increment();
                    Ok(())
                }
                Err(_) => {
                    // Someone else modified the bucket - they won the race
                    // We speculatively updated the old tail's next pointer, but another thread
                    // may have also done so and won the CAS. We can't reliably unlink because
                    // the old tail's next pointer may have been overwritten multiple times.

                    // Try to unlink: restore old tail's next to invalid, but don't panic on failure
                    // Failure just means another thread won and has properly linked the chain
                    let _unlink_succeeded = tail_segment.cas_metadata(
                        SegmentState::Sealed,
                        SegmentState::Sealed,
                        Some(INVALID_SEGMENT_ID), // Unlink from new segment
                        None,                      // Preserve current prev
                        metrics,
                    );

                    // If unlink failed, the segment we tried to link is orphaned but that's OK -
                    // it will be cleaned up when the segment pool runs low or during eviction

                    // Clear bucket ID and revert new segment from Linking back to Reserved
                    segment.clear_bucket_id();
                    segment.cas_metadata(
                        SegmentState::Linking,
                        SegmentState::Reserved,
                        None,
                        None,
                        metrics,
                    );

                    // Return segment to free pool
                    cache.segments().release(segment_id, metrics);
                    metrics.ttl_append_segment_error.increment();
                    Err(AppendSegmentError::BucketModified)
                }
            }
        }
    }

    /// Append a segment to the end of this TTL bucket chain
    /// The segment must be in Reserved state
    ///
    /// # Loom Test Coverage
    /// - `concurrent_ttl_bucket_append` - Two threads appending different segments to same bucket
    /// - `concurrent_insert_same_segment` (ignored) - Full end-to-end insertion path
    pub async fn append_segment(
        &self,
        cache: &impl CacheOps,
        segment_id: u32,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(), &'static str> {
        let segment = cache
            .segments()
            .get(segment_id)
            .ok_or("Invalid segment ID")?;

        // Set the expiration time for this segment (only once)
        let expire_at = clocksource::coarse::Instant::now() + self.ttl;
        segment.set_expire_at(expire_at);

        // Acquire async mutex to serialize append operations
        // This eliminates CAS retry loops and allows other tasks to run while waiting
        let _guard = self.write_lock.lock().await;

        // With mutex held, try_append_segment should succeed on first attempt
        // (unless there's a genuine error like invalid segment ID)
        match self.try_append_segment(cache, segment_id, metrics) {
            Ok(()) => Ok(()),
            Err(error) => {
                match error {
                    AppendSegmentError::InvalidSegmentId => Err("Invalid segment ID"),
                    AppendSegmentError::SegmentNotReserved => Err("Segment must be in Reserved state"),
                    AppendSegmentError::BucketModified => Err("Bucket was concurrently modified"),
                    AppendSegmentError::InvalidTailSegmentId => Err("Invalid tail segment ID"),
                    AppendSegmentError::TailInUnexpectedState => Err("Tail segment in unexpected state"),
                    AppendSegmentError::FailedToTransitionToLinking => Err("Failed to transition segment to Linking state"),
                }
            }
        }
    }

    /// Try to evict the head segment from this TTL bucket (single attempt, no retry)
    /// Returns Ok(segment_id) if successful, Err with reason if not
    ///
    /// # Loom Test Coverage
    /// - `try_evict_head_segment` - Low-level single-attempt eviction without retry loops
    pub(crate) fn try_evict_head_segment(&self, cache: &impl CacheOps, metrics: &crate::metrics::CacheMetrics) -> Result<u32, &'static str> {
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (head, tail) = Self::unpack_head_tail(current_packed);

        if head == Self::INVALID_ID || head == tail {
            // Empty chain or only one segment, can't evict
            return Err("Cannot evict: empty or single segment");
        }

        let segment = cache.segments().get(head).ok_or("Invalid head segment ID")?;
        let next_id = segment.next();

        // First transition to Draining state
        if !segment.cas_metadata(
            SegmentState::Sealed,
            SegmentState::Draining,
            None,
            None,
            metrics,
        ) {
            // Segment might still be Live or in another state
            let state = segment.state();
            match state {
                SegmentState::Live => return Err("Cannot evict Live segment"),
                SegmentState::Draining | SegmentState::Locked => return Err("Segment already being evicted"),
                SegmentState::Linking => return Err("Segment in Linking state"),
                SegmentState::Reserved | SegmentState::Free => {
                    // Segment is already freed - this can happen due to races in append_segment
                    // where orphaned segments create transient inconsistencies. Another thread
                    // likely already completed the eviction. Return error to retry.
                    return Err("Segment already freed");
                }
                _ => return Err("Segment in unexpected state"),
            }
        }

        // Wait for readers to finish - bounded spinning for async compatibility
        #[cfg(not(feature = "loom"))]
        {
            const MAX_SPINS: u32 = 100_000;
            let mut spin_count = 0;

            while segment.ref_count() > 0 {
                if spin_count >= MAX_SPINS {
                    // Readers taking too long - abort removal
                    segment.cas_metadata(
                        SegmentState::Draining,
                        SegmentState::Sealed, // Restore to Sealed
                        None,
                        None,
                        metrics,
                    );
                    return Err("Segment has active readers after timeout");
                }

                // Exponential backoff: 1 spin for first 10k, then 2 spins
                if spin_count < 10_000 {
                    spin_loop();
                } else {
                    spin_loop();
                    spin_loop();
                }
                spin_count += 1;
            }
        }

        // Transition to Locked state
        if !segment.cas_metadata(
            SegmentState::Draining,
            SegmentState::Locked,
            None,
            None,
            metrics,
        ) {
            return Err("Failed to lock segment");
        }

        // Update the bucket's head pointer
        let new_head = next_id.unwrap_or(Self::INVALID_ID);
        let new_packed = Self::pack_head_tail(new_head, tail);

        if self
            .head_tail
            .compare_exchange(
                current_packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Someone else modified the bucket - restore state
            segment.cas_metadata(SegmentState::Locked, SegmentState::Sealed, next_id, None, metrics);
            return Err("Bucket head pointer was modified");
        }

        // Clear the evicted segment's chain links
        // Must do this before clear() since it checks for no links
        segment.cas_metadata(
            SegmentState::Locked,
            SegmentState::Locked,
            Some(INVALID_SEGMENT_ID), // Clear next pointer
            Some(INVALID_SEGMENT_ID), // Clear prev pointer
            metrics,
        );

        // AFTER successfully updating bucket head, update the next segment's prev pointer
        // Use Relinking protocol to ensure atomic update without silent failures
        if let Some(next_id) = next_id {
            if let Some(next_segment) = cache.segments().get(next_id) {
                // Only update if prev still points to the evicted head
                if next_segment.prev() == Some(head) {
                    // Retry loop to handle state changes (bounded)
                    for _ in 0..8 {
                        let next_state = next_segment.state();

                        // Skip if segment is in a transient state
                        if matches!(next_state, SegmentState::Draining | SegmentState::Locked | SegmentState::Relinking) {
                            break; // Another operation is handling this segment
                        }

                        // Lock: (Live | Sealed) -> Relinking
                        if next_segment.cas_metadata(
                            next_state,
                            SegmentState::Relinking,
                            None,
                            None,
                            metrics,
                        ) {
                            // Update prev pointer while locked
                            next_segment.cas_metadata(
                                SegmentState::Relinking,
                                SegmentState::Relinking,
                                None,
                                Some(INVALID_SEGMENT_ID),
                                metrics,
                            );
                            // Unlock: Relinking -> original state
                            next_segment.cas_metadata(
                                SegmentState::Relinking,
                                next_state,
                                None,
                                None,
                                metrics,
                            );
                            break;
                        }
                        // CAS failed, state changed - retry with new state
                    }
                }
            }
        }

        // Clear the segment and prepare it for reuse
        let (items_cleared, bytes_cleared) = cache.segments().clear(head, cache.hashtable(), metrics)
            .expect("Failed to clear segment which is in Locked state - data structure corruption");

        // Decrement global metrics for the evicted items
        for _ in 0..items_cleared {
            metrics.items_live.decrement();
        }
        metrics.bytes_live.sub(bytes_cleared as i64);

        // Return the evicted segment ID for reuse
        metrics.ttl_evict_head.increment();
        metrics.segment_evict.increment();

        Ok(head)
    }

    /// Evict the head segment from this TTL bucket with retry logic
    /// Returns the segment ID if successful, None if unable to evict
    ///
    /// # Implementation
    /// Calls `try_evict_head_segment` with retry logic for transient failures.
    ///
    /// # Loom Test Coverage
    /// - `try_evict_head_segment` (ignored) - Tests the underlying single-attempt logic
    pub(crate) fn evict_head_segment(&self, cache: &impl CacheOps, metrics: &crate::metrics::CacheMetrics) -> Option<u32> {
        for attempt in 1..=16 {
            if attempt > 1 {
                metrics.ttl_evict_head_retry.increment();
            }

            match self.try_evict_head_segment(cache, metrics) {
                Ok(segment_id) => return Some(segment_id),
                Err(reason) => {
                    // Give up immediately for certain errors
                    match reason {
                        "Cannot evict: empty or single segment" => return None,
                        "Cannot evict Live segment" => return None,
                        "Bucket head {} points to segment in {:?} state - data structure corruption" => return None,
                        _ => {
                            // Retry for transient failures - no yield for async compatibility
                            // Just continue with next attempt
                            continue;
                        }
                    }
                }
            }
        }

        metrics.ttl_evict_head_give_up.increment();
        None
    }

    /// Expire the head segment from this TTL bucket (single attempt, no retry).
    /// This is used for proactive TTL expiration.
    ///
    /// Unlike evict_head_segment which returns the segment for immediate reuse,
    /// this function releases the segment back to the free pool.
    ///
    /// # Process
    /// 1. Get head segment and verify it exists
    /// 2. Transition head to Draining state
    /// 3. Wait for readers to finish
    /// 4. Transition to Locked state
    /// 5. Update bucket's head pointer to next segment
    /// 6. Clear the segment's chain links
    /// 7. Update next segment's prev pointer
    /// 8. Clear all items from the segment (unlink from hashtable)
    /// 9. Release segment to free pool
    ///
    /// # Returns
    /// - `Ok(())` if segment was successfully expired
    /// - `Err(&str)` describing why expiration failed
    pub(crate) fn try_expire_head_segment(
        &self,
        cache: &impl CacheOps,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(), &'static str> {
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (head, tail) = Self::unpack_head_tail(current_packed);

        if head == Self::INVALID_ID {
            return Err("Empty bucket");
        }

        let segment = cache.segments().get(head).ok_or("Invalid head segment ID")?;
        let next_id = segment.next();

        // Transition to Draining state (can be from Live, Sealed, or Reserved)
        let initial_state = segment.state();
        match initial_state {
            SegmentState::Free => return Err("Segment already freed"),
            SegmentState::Draining | SegmentState::Locked => return Err("Segment already being processed"),
            _ => {}
        }

        if !segment.cas_metadata(
            initial_state,
            SegmentState::Draining,
            None,
            None,
            metrics,
        ) {
            return Err("Failed to transition to Draining state");
        }

        // Wait for readers to finish - bounded spinning for async compatibility
        #[cfg(not(feature = "loom"))]
        {
            const MAX_SPINS: u32 = 100_000;
            let mut spin_count = 0;

            while segment.ref_count() > 0 {
                if spin_count >= MAX_SPINS {
                    // Readers taking too long - abort expiration
                    segment.cas_metadata(
                        SegmentState::Draining,
                        initial_state,
                        None,
                        None,
                        metrics,
                    );
                    return Err("Segment has active readers after timeout");
                }

                if spin_count < 10_000 {
                    spin_loop();
                } else {
                    spin_loop();
                    spin_loop();
                }
                spin_count += 1;
            }
        }

        #[cfg(feature = "loom")]
        {
            if segment.ref_count() > 0 {
                return Err("Segment has active readers");
            }
        }

        // Transition to Locked state
        if !segment.cas_metadata(
            SegmentState::Draining,
            SegmentState::Locked,
            None,
            None,
            metrics,
        ) {
            return Err("Failed to lock segment");
        }

        // Update the bucket's head pointer
        let new_head = next_id.unwrap_or(Self::INVALID_ID);
        let new_tail = if head == tail {
            // This was the only segment, bucket becomes empty
            Self::INVALID_ID
        } else {
            tail
        };
        let new_packed = Self::pack_head_tail(new_head, new_tail);

        if self
            .head_tail
            .compare_exchange(
                current_packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Someone else modified the bucket - restore state
            segment.cas_metadata(SegmentState::Locked, initial_state, next_id, None, metrics);
            return Err("Bucket head pointer was modified");
        }

        // Clear the expired segment's chain links
        segment.cas_metadata(
            SegmentState::Locked,
            SegmentState::Locked,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
            metrics,
        );

        // Update the next segment's prev pointer
        // Use Relinking protocol to ensure atomic update without silent failures
        if let Some(next_id) = next_id {
            if let Some(next_segment) = cache.segments().get(next_id) {
                // Only update if prev still points to the expired head
                if next_segment.prev() == Some(head) {
                    // Retry loop to handle state changes (bounded)
                    for _ in 0..8 {
                        let next_state = next_segment.state();

                        // Skip if segment is in a transient state
                        if matches!(next_state, SegmentState::Draining | SegmentState::Locked | SegmentState::Relinking) {
                            break; // Another operation is handling this segment
                        }

                        // Lock: (Live | Sealed) -> Relinking
                        if next_segment.cas_metadata(
                            next_state,
                            SegmentState::Relinking,
                            None,
                            None,
                            metrics,
                        ) {
                            // Update prev pointer while locked
                            next_segment.cas_metadata(
                                SegmentState::Relinking,
                                SegmentState::Relinking,
                                None,
                                Some(INVALID_SEGMENT_ID),
                                metrics,
                            );
                            // Unlock: Relinking -> original state
                            next_segment.cas_metadata(
                                SegmentState::Relinking,
                                next_state,
                                None,
                                None,
                                metrics,
                            );
                            break;
                        }
                        // CAS failed, state changed - retry with new state
                    }
                }
            }
        }

        // Clear all items from the segment and release to free pool
        // The segment is already in Locked state with links cleared
        let (items_cleared, bytes_cleared) = cache.segments().clear(head, cache.hashtable(), metrics)
            .expect("Failed to clear segment which is in Locked state");

        // Decrement global metrics for the expired items
        for _ in 0..items_cleared {
            metrics.items_live.decrement();
        }
        metrics.bytes_live.sub(bytes_cleared as i64);

        // Release segment to free pool
        cache.segments().release(head, metrics);

        Ok(())
    }

    /// Count the number of segments in the chain from head, up to max_count.
    ///
    /// # Returns
    /// The number of segments in the chain (at most max_count).
    fn chain_len(&self, cache: &impl CacheOps, max_count: usize) -> usize {
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (head, _) = Self::unpack_head_tail(current_packed);

        if head == Self::INVALID_ID {
            return 0;
        }

        let mut count = 0;
        let mut current = head;

        while current != Self::INVALID_ID && count < max_count {
            count += 1;
            if let Some(segment) = cache.segments().get(current) {
                match segment.next() {
                    Some(next) if next != INVALID_SEGMENT_ID => current = next,
                    _ => break,
                }
            } else {
                break;
            }
        }

        count
    }

    /// Perform merge eviction on this TTL bucket.
    ///
    /// Merge eviction combines multiple segments from the head of the chain,
    /// prunes low-frequency items, and copies surviving high-frequency items
    /// into fewer destination segments. This frees up segments while retaining
    /// popular data.
    ///
    /// # Process
    /// 1. Check if we have enough segments to merge (at least merge_ratio + target_ratio)
    /// 2. Calculate frequency threshold based on utilization
    /// 3. For each source segment (merge_ratio segments from head):
    ///    a. Prune low-frequency items (mark as deleted)
    ///    b. Copy surviving items to destination segment(s)
    ///    c. Unlink source segment and release to free pool
    /// 4. Return the number of segments freed
    ///
    /// # Parameters
    /// - `cache`: Cache operations interface
    /// - `merge_ratio`: Number of segments to merge
    /// - `target_ratio`: Target number of segments after merging
    /// - `metrics`: Cache metrics
    ///
    /// # Returns
    /// - `Some(freed_count)`: Number of segments freed
    /// - `None`: Not enough segments to merge or merge failed
    pub async fn merge_evict(
        &self,
        cache: &impl CacheOps,
        merge_ratio: usize,
        target_ratio: usize,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Option<usize> {
        // We need at least 2 segments to merge (the destination + at least 1 source)
        // Plus we need to keep the tail Live, so min 3 segments in the chain
        let min_segments_needed = 3;
        let chain_length = self.chain_len(cache, min_segments_needed);

        if chain_length < min_segments_needed {
            return None;
        }

        // Acquire write lock for the entire merge operation
        let _guard = self.write_lock.lock().await;

        // Re-check chain length after acquiring lock
        let chain_length = self.chain_len(cache, min_segments_needed);
        if chain_length < min_segments_needed {
            return None;
        }

        // Collect the segment IDs we'll be merging from the head
        // First segment becomes the destination, rest are sources
        let segments_to_merge = merge_ratio.min(chain_length - 1); // Keep at least 1 segment (tail)
        let mut all_segments = Vec::with_capacity(segments_to_merge);
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (mut current_id, _) = Self::unpack_head_tail(current_packed);

        while all_segments.len() < segments_to_merge && current_id != Self::INVALID_ID {
            all_segments.push(current_id);
            if let Some(segment) = cache.segments().get(current_id) {
                match segment.next() {
                    Some(next) if next != INVALID_SEGMENT_ID => current_id = next,
                    _ => break,
                }
            } else {
                break;
            }
        }

        if all_segments.len() < 2 {
            return None; // Need at least dest + 1 source
        }

        // First segment is the destination, rest are sources
        let dest_seg_id = all_segments[0];
        let source_segments = &all_segments[1..];

        // Calculate target retention ratio based on merge parameters
        // If we're merging N segments into ~N/target_ratio segments, we want to retain
        // approximately target_ratio/merge_ratio of the data from each segment.
        // For shorter chains, be less aggressive (retain more).
        let effective_target_ratio = if chain_length < merge_ratio {
            // Short chain - retain more to avoid over-pruning
            1.0 / chain_length as f64
        } else {
            // Normal operation - use configured ratio
            // merge_ratio=4, target_ratio=2 means 4 segments -> ~2 segments
            // So we want to retain about 50% of total data
            target_ratio as f64 / merge_ratio as f64
        };

        // Start with cutoff = 1.0 (most permissive - only prune items with freq < 1)
        // The cutoff will adapt based on actual retention vs target
        let mut cutoff = 1.0f64;

        // Phase 1: Prune the destination segment and compact it
        // This creates free space at the end for items from source segments
        let dest_segment = cache.segments().get(dest_seg_id)?;
        let (new_cutoff, _, _, _) = dest_segment.prune(cache.hashtable(), cutoff, effective_target_ratio, metrics);
        cutoff = new_cutoff;
        let dest_write_offset = dest_segment.compact(cache.hashtable(), metrics) as usize;
        let dest_capacity = dest_segment.data_len();
        let mut dest_free_space = dest_capacity - dest_write_offset;

        // Phase 2: Process each source segment - prune, then copy survivors to destination
        let mut segments_freed = 0usize;

        for &src_seg_id in source_segments {
            let src_segment = match cache.segments().get(src_seg_id) {
                Some(seg) => seg,
                None => continue,
            };

            // Prune the source segment using the adaptive cutoff
            let (new_cutoff, items_retained, _, _) = src_segment.prune(cache.hashtable(), cutoff, effective_target_ratio, metrics);
            cutoff = new_cutoff;

            if items_retained == 0 {
                // Source segment is empty after pruning - remove from chain and release
                match self.try_remove_segment(cache, src_seg_id, metrics) {
                    Ok(()) => {
                        // Segment is now in Locked state with links cleared
                        // Clear it (no items left, but this resets stats and transitions to Reserved)
                        let _ = cache.segments().clear(src_seg_id, cache.hashtable(), metrics);
                        // Release to free pool
                        cache.segments().release(src_seg_id, metrics);
                        segments_freed += 1;
                        metrics.segment_evict.increment();
                        metrics.merge_evict_segments.increment();
                    }
                    Err(_) => {}
                }
                continue;
            }

            // Copy surviving items from source to destination
            // Note: copy_live_items_into updates the hashtable as it goes
            match src_segment.copy_live_items_into(
                dest_segment,
                cache.hashtable(),
                cache.segments(),
                dest_seg_id,
                metrics,
            ) {
                Some(_) => {
                    // Successfully copied all items - remove from chain and release
                    match self.try_remove_segment(cache, src_seg_id, metrics) {
                        Ok(()) => {
                            // Segment is now in Locked state with links cleared
                            // Clear it (items already copied, but this resets stats and transitions to Reserved)
                            let _ = cache.segments().clear(src_seg_id, cache.hashtable(), metrics);
                            // Release to free pool
                            cache.segments().release(src_seg_id, metrics);
                            segments_freed += 1;
                            metrics.segment_evict.increment();
                            metrics.merge_evict_segments.increment();
                        }
                        Err(_) => {}
                    }

                    // Update free space estimate
                    let new_write_offset = dest_segment.current_write_offset() as usize;
                    dest_free_space = dest_capacity - new_write_offset;
                }
                None => {
                    // Destination is full - stop merging
                    // Items that survived pruning but couldn't be copied remain in the source
                    // segment which stays in the chain. This is safe because:
                    // 1. Pruned items were already unlinked from hashtable
                    // 2. Surviving items still have valid hashtable entries pointing to source
                    break;
                }
            }

            // If destination is getting full, stop merging more segments
            if dest_free_space < dest_capacity / 4 {
                break;
            }
        }

        if segments_freed > 0 {
            metrics.merge_evict.increment();
            Some(segments_freed)
        } else {
            None
        }
    }

    /// Try to remove a segment from the chain (single attempt, no retry).
    /// The segment must be in Sealed state.
    ///
    /// # Chain Update Protocol
    ///
    /// When removing segment B from chain A <-> B <-> C:
    /// 1. Lock target B: Sealed → Draining → Locked
    /// 2. Lock prev A: Sealed → Relinking (A cannot be Live since B exists after it)
    /// 3. Update A's next pointer to C
    /// 4. Unlock A: Relinking → Sealed
    /// 5. Lock next C: Sealed | Live → Relinking (remembering original state)
    /// 6. Update C's prev pointer to A
    /// 7. Unlock C: Relinking → (original state)
    /// 8. Update bucket head/tail if needed
    /// 9. Clear B's links and release
    ///
    /// The Relinking state prevents concurrent modifications to chain pointers while
    /// still allowing reads of segment data.
    pub(crate) fn try_remove_segment(
        &self,
        cache: &impl CacheOps,
        segment_id: u32,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(), RemoveSegmentError> {
        let segment = cache
            .segments()
            .get(segment_id)
            .ok_or(RemoveSegmentError::InvalidSegmentId)?;

        // Step 1: Lock target segment B: Sealed → Draining → Locked
        if !segment.cas_metadata(
            SegmentState::Sealed,
            SegmentState::Draining,
            None, // Preserve next
            None, // Preserve prev
            metrics,
        ) {
            return Err(RemoveSegmentError::FailedToTransitionToDraining);
        }

        // Wait for readers to finish
        #[cfg(not(feature = "loom"))]
        {
            const MAX_SPINS: u32 = 100_000;
            let mut spin_count = 0;

            while segment.ref_count() > 0 {
                if spin_count >= MAX_SPINS {
                    // Readers taking too long - abort removal
                    segment.cas_metadata(
                        SegmentState::Draining,
                        SegmentState::Sealed,
                        None,
                        None,
                        metrics,
                    );
                    return Err(RemoveSegmentError::SegmentHasActiveReaders);
                }

                // Exponential backoff: 1 spin for first 10k, then 2 spins
                if spin_count < 10_000 {
                    spin_loop();
                } else {
                    spin_loop();
                    spin_loop();
                }
                spin_count += 1;
            }
        }

        #[cfg(feature = "loom")]
        {
            if segment.ref_count() > 0 {
                return Err(RemoveSegmentError::SegmentHasActiveReaders);
            }
        }

        // Transition to Locked state
        if !segment.cas_metadata(
            SegmentState::Draining,
            SegmentState::Locked,
            None, // Preserve next
            None, // Preserve prev
            metrics,
        ) {
            return Err(RemoveSegmentError::FailedToTransitionToLocked);
        }

        let next_id = segment.next();
        let prev_id = segment.prev();

        // CRITICAL: Lock both neighbors BEFORE making any changes to ensure atomicity.
        // If we update prev->next before locking next, and then fail to lock next,
        // the chain is left corrupted (prev->next points to next, but next->prev
        // still points to the removed segment).

        // Step 2: Lock prev segment if it exists
        let prev_segment = if let Some(prev_id) = prev_id {
            let prev_seg = cache
                .segments()
                .get(prev_id)
                .ok_or(RemoveSegmentError::InvalidPrevSegmentId)?;

            // Verify chain integrity
            let prev_next = prev_seg.next();
            if prev_next != Some(segment_id) {
                panic!(
                    "Chain corruption detected: segment {} prev pointer points to {}, but segment {} next pointer is {:?}",
                    segment_id, prev_id, prev_id, prev_next
                );
            }

            // Lock prev segment: Sealed → Relinking
            // (prev cannot be Live since segment_id exists after it)
            if !prev_seg.cas_metadata(
                SegmentState::Sealed,
                SegmentState::Relinking,
                None,
                None,
                metrics,
            ) {
                return Err(RemoveSegmentError::FailedToLockPrevSegment);
            }

            Some(prev_seg)
        } else {
            None
        };

        // Step 3: Lock next segment if it exists (before making any changes to prev)
        let (next_segment, original_next_state) = if let Some(next_id) = next_id {
            let next_seg = match cache.segments().get(next_id) {
                Some(seg) => seg,
                None => {
                    // Unlock prev before returning error
                    if let Some(prev_seg) = prev_segment {
                        prev_seg.cas_metadata(
                            SegmentState::Relinking,
                            SegmentState::Sealed,
                            None,
                            None,
                            metrics,
                        );
                    }
                    return Err(RemoveSegmentError::InvalidNextSegmentId);
                }
            };

            // Verify chain integrity
            let next_prev = next_seg.prev();
            if next_prev != Some(segment_id) {
                // Unlock prev before panicking
                if let Some(prev_seg) = prev_segment {
                    prev_seg.cas_metadata(
                        SegmentState::Relinking,
                        SegmentState::Sealed,
                        None,
                        None,
                        metrics,
                    );
                }
                panic!(
                    "Chain corruption detected: segment {} next pointer points to {}, but segment {} prev pointer is {:?}",
                    segment_id, next_id, next_id, next_prev
                );
            }

            // Remember original state (could be Live or Sealed)
            let original_state = next_seg.state();

            // Lock next segment: (Sealed | Live) → Relinking
            if !next_seg.cas_metadata(
                original_state,
                SegmentState::Relinking,
                None,
                None,
                metrics,
            ) {
                // Failed to lock next - unlock prev first (no changes made yet)
                if let Some(prev_seg) = prev_segment {
                    prev_seg.cas_metadata(
                        SegmentState::Relinking,
                        SegmentState::Sealed,
                        None,
                        None,
                        metrics,
                    );
                }
                return Err(RemoveSegmentError::FailedToLockNextSegment);
            }

            (Some(next_seg), Some(original_state))
        } else {
            (None, None)
        };

        // Step 4: Now that both neighbors are locked, make the changes atomically
        // Update prev's next pointer to skip over removed segment
        if let Some(prev_seg) = prev_segment {
            prev_seg.cas_metadata(
                SegmentState::Relinking,
                SegmentState::Relinking,
                Some(next_id.unwrap_or(INVALID_SEGMENT_ID)),
                None,
                metrics,
            );

            // Unlock prev segment: Relinking → Sealed
            prev_seg.cas_metadata(
                SegmentState::Relinking,
                SegmentState::Sealed,
                None,
                None,
                metrics,
            );
        }

        // Step 5: Update next's prev pointer to skip over removed segment
        if let (Some(next_seg), Some(original_state)) = (next_segment, original_next_state) {
            next_seg.cas_metadata(
                SegmentState::Relinking,
                SegmentState::Relinking,
                None,
                Some(prev_id.unwrap_or(INVALID_SEGMENT_ID)),
                metrics,
            );

            // Unlock next segment: Relinking → (original state)
            next_seg.cas_metadata(
                SegmentState::Relinking,
                original_state,
                None,
                None,
                metrics,
            );
        }

        // Step 8: Update bucket head/tail if necessary (single attempt)
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (current_head, current_tail) = Self::unpack_head_tail(current_packed);

        let new_head = if current_head == segment_id {
            next_id.unwrap_or(Self::INVALID_ID)
        } else {
            current_head
        };

        let new_tail = if current_tail == segment_id {
            prev_id.unwrap_or(Self::INVALID_ID)
        } else {
            current_tail
        };

        // Only update if necessary
        if new_head != current_head || new_tail != current_tail {
            let new_packed = Self::pack_head_tail(new_head, new_tail);
            if self
                .head_tail
                .compare_exchange(
                    current_packed,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return Err(RemoveSegmentError::BucketModified);
            }
        }

        // Step 9: Clear segment's chain links (stay in Locked state)
        // Caller is responsible for clearing items and releasing segment
        if !segment.cas_metadata(
            SegmentState::Locked,
            SegmentState::Locked,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
            metrics,
        ) {
            panic!(
                "Failed to clear segment {} links - segment in unexpected state",
                segment_id
            );
        }

        Ok(())
    }

    /// Remove a segment from the chain with async mutex for serialization.
    /// The segment must be in Sealed state.
    ///
    /// This function:
    /// 1. Unlinks the segment from the TTL bucket chain
    /// 2. Clears all items from the segment (unlinking from hashtable)
    /// 3. Releases the segment back to the free pool
    ///
    /// # Returns
    /// - `Ok((items_cleared, bytes_cleared))` on success
    /// - `Err` with reason if removal failed
    pub async fn remove_segment(
        &self,
        cache: &impl CacheOps,
        segment_id: u32,
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(u32, u64), &'static str> {
        // Acquire async mutex to serialize remove operations
        let _guard = self.write_lock.lock().await;

        // With mutex held, try_remove_segment should succeed on first attempt
        match self.try_remove_segment(cache, segment_id, metrics) {
            Ok(()) => {
                // Segment is now in Locked state with links cleared
                // Clear items from hashtable and reset segment
                let (items_cleared, bytes_cleared) = cache.segments()
                    .clear(segment_id, cache.hashtable(), metrics)
                    .expect("Failed to clear segment after successful removal from chain");

                // Release segment back to free pool
                cache.segments().release(segment_id, metrics);

                Ok((items_cleared, bytes_cleared))
            }
            Err(error) => {
                match error {
                    RemoveSegmentError::InvalidSegmentId => Err("Invalid segment ID"),
                    RemoveSegmentError::FailedToTransitionToDraining => {
                        Err("Segment not in Sealed state")
                    }
                    RemoveSegmentError::SegmentHasActiveReaders => {
                        Err("Segment still has active readers")
                    }
                    RemoveSegmentError::FailedToTransitionToLocked => {
                        Err("Failed to transition from Draining to Locked")
                    }
                    RemoveSegmentError::InvalidPrevSegmentId => {
                        Err("Invalid prev segment ID")
                    }
                    RemoveSegmentError::InvalidNextSegmentId => {
                        Err("Invalid next segment ID")
                    }
                    RemoveSegmentError::BucketModified => Err("Bucket was concurrently modified"),
                    RemoveSegmentError::FailedToLockPrevSegment => Err("Failed to lock previous segment"),
                    RemoveSegmentError::FailedToLockNextSegment => Err("Failed to lock next segment"),
                }
            }
        }
    }

    /// Try to append an item to the current tail segment (single attempt, no retry)
    ///
    /// Returns Ok((segment_id, offset)) if the item was successfully appended to the current tail.
    /// Returns Err with reason if:
    /// - No tail segment exists
    /// - Tail segment is not in Live state
    /// - Tail segment is full
    ///
    /// # Loom Test Coverage
    /// - `try_append_item` - Low-level single-attempt item append without retry loops
    pub(crate) fn try_append_item(
        &self,
        cache: &impl CacheOps,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        metrics: &crate::metrics::CacheMetrics,
    ) -> Result<(u32, u32), AppendItemError> {
        let current_packed = self.head_tail.load(Ordering::Acquire);
        let (_head, tail) = Self::unpack_head_tail(current_packed);

        // Check if we have a tail segment
        if tail == Self::INVALID_ID {
            return Err(AppendItemError::NoTailSegment);
        }

        let segment = cache.segments().get(tail).ok_or(AppendItemError::InvalidTailSegmentId)?;

        // Only append to Live segments
        if segment.state() != SegmentState::Live {
            return Err(AppendItemError::TailNotLive);
        }

        // Try to append the item
        match segment.append_item(key, value, optional, metrics, false) {
            Some(offset) => Ok((tail, offset)),
            None => Err(AppendItemError::TailSegmentFull),
        }
    }

    /// Append an item to this TTL bucket
    /// Returns (segment_id, offset) if successful
    ///
    /// # Loom Test Coverage
    /// - `concurrent_insert_same_segment` (ignored) - Two threads inserting through full path with retry loops
    pub async fn append_item(
        &self,
        cache: &impl CacheOps,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        metrics: &crate::metrics::CacheMetrics,
    ) -> Option<(u32, u32)> {
        let mut attempts = 0;

        loop {
            attempts += 1;
            if attempts > 16 {
                // Tried 16 times, give up
                return None;
            }

            // Add backoff after first few attempts to reduce contention
            if attempts > 4 {
                // Use async yield instead of spin loop
                tokio::task::yield_now().await;
            }

            // Step 1: Try to append to current tail if it's Live
            match self.try_append_item(cache, key, value, optional, metrics) {
                Ok((segment_id, offset)) => return Some((segment_id, offset)),
                Err(error) => {
                    // If tail is full, try to seal it before proceeding
                    if error == AppendItemError::TailSegmentFull {
                        let current_packed = self.head_tail.load(Ordering::Acquire);
                        let (_head, tail) = Self::unpack_head_tail(current_packed);
                        if tail != Self::INVALID_ID {
                            let segment = cache.segments().get(tail).expect("Invalid tail segment ID");
                            // Try to seal it - preserving links
                            segment.cas_metadata(
                                SegmentState::Live,
                                SegmentState::Sealed,
                                None,  // Preserve current next
                                None,  // Preserve current prev
                                metrics,
                            );
                        }
                    }
                    // Fall through to try allocating a new segment
                }
            }

            let current_packed = self.head_tail.load(Ordering::Acquire);
            let (_head, tail) = Self::unpack_head_tail(current_packed);

            // Step 2: Try to get a free segment
            match cache.segments().reserve(metrics) {
                Some(new_segment_id) => {
                    // Try to append the new segment to this bucket
                    match self.append_segment(cache, new_segment_id, metrics).await {
                        Ok(_) => {
                            // New segment added to bucket, now append the item
                            let segment = cache.segments().get(new_segment_id).expect("Invalid segment ID");
                            if let Some(offset) = segment.append_item(key, value, optional, metrics, false) {
                                return Some((new_segment_id, offset));
                            }
                            // This shouldn't happen with a fresh segment, but handle gracefully
                            // The segment is already linked, so we can't release it back
                            // Just continue to try another segment
                        }
                        Err(_) => {
                            // Failed to append segment to bucket
                            // append_segment already released the segment back to the pool
                            // Just retry with a new segment
                            continue;
                        }
                    }
                }
                None => {
                    // Step 3: No free segments, try to expire one segment first (cheaper than eviction)
                    let segment_id = if let Some(id) = cache.try_expire_one_segment() {
                        id
                    } else {
                        // No expired segments, use cache-wide eviction according to policy
                        match cache.evict_segment_by_policy().await {
                        Some(id) => id,
                        None => {
                            // Eviction failed, try to append to tail as last resort
                            // (another thread might have made space available)
                            if tail != Self::INVALID_ID {
                                let segment = cache.segments().get(tail).expect("Invalid tail segment ID");
                                if segment.state() == SegmentState::Live {
                                    if let Some(offset) = segment.append_item(key, value, optional, metrics, false) {
                                        return Some((tail, offset));
                                    }
                                }
                            }
                            // Continue retry without yield for async compatibility
                            continue;
                        }
                        }
                    };

                    // Now append the expired/evicted segment as the new tail
                    match self.append_segment(cache, segment_id, metrics).await {
                        Ok(_) => {
                            // Successfully appended the segment, now append the item
                            let segment = cache.segments().get(segment_id).expect("Invalid segment ID");
                            if let Some(offset) = segment.append_item(key, value, optional, metrics, false) {
                                return Some((segment_id, offset));
                            }
                            // This shouldn't happen with a freshly cleared segment
                            return None;
                        }
                        Err(_) => {
                            // Failed to append the segment
                            // append_segment already released the segment back to the pool
                            // Just return None
                            return None;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::{Cache, SegmentState};
    use crate::segments::INVALID_SEGMENT_ID;

    #[test]
    fn test_item_append_increments_live_items() {
        let cache = Cache::new();

        // Reserve a segment
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Initially no items
        assert_eq!(segment.live_items(), 0);

        // Append an item
        let offset = segment.append_item(b"key1", b"value1", b"", cache.metrics(), false);
        assert!(offset.is_some(), "Should successfully append item");
        assert_eq!(segment.live_items(), 1);

        // Append another item
        let offset2 = segment.append_item(b"key2", b"value2", b"", cache.metrics(), false);
        assert!(offset2.is_some(), "Should successfully append second item");
        assert_eq!(segment.live_items(), 2);

        // Offsets should be different
        assert_ne!(offset.unwrap(), offset2.unwrap());
    }

    #[test]
    fn test_segment_clear_reduces_live_items() {
        let cache = Cache::new();

        // Reserve and append items
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        segment.append_item(b"key1", b"value1", b"", cache.metrics(), false).unwrap();
        segment.append_item(b"key2", b"value2", b"", cache.metrics(), false).unwrap();
        assert_eq!(segment.live_items(), 2);

        // Transition to Locked state (required for clear)
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Locked,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
            cache.metrics(),
        );

        // Clear the segment
        let cleared = cache.segments().clear(seg_id, cache.hashtable(), cache.metrics());
        assert!(cleared.is_some(), "Should successfully clear segment");

        // Live items should be reset
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.write_offset(), 0);
    }

    #[tokio::test]
    async fn test_ttl_bucket_append_segment() {
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Initially empty
        assert_eq!(bucket.head(), None);
        assert_eq!(bucket.tail(), None);

        // Reserve and append a segment
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let result = bucket.append_segment(&cache, seg_id, cache.metrics());
        assert!(result.await.is_ok(), "Should successfully append segment");

        // Bucket should have segment as both head and tail
        assert_eq!(bucket.head(), Some(seg_id));
        assert_eq!(bucket.tail(), Some(seg_id));

        // Segment should be in Live state
        let segment = cache.segments().get(seg_id).unwrap();
        assert_eq!(segment.state(), SegmentState::Live);
    }

    #[tokio::test]
    async fn test_ttl_bucket_append_multiple_segments() {
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Append first segment
        let seg_id1 = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_id1, cache.metrics()).await.unwrap();

        // Append second segment
        let seg_id2 = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_id2, cache.metrics()).await.unwrap();

        // Head should be first, tail should be second
        assert_eq!(bucket.head(), Some(seg_id1));
        assert_eq!(bucket.tail(), Some(seg_id2));

        // Verify linked list structure
        let seg1 = cache.segments().get(seg_id1).unwrap();
        let seg2 = cache.segments().get(seg_id2).unwrap();

        assert_eq!(seg1.prev(), None);
        assert_eq!(seg1.next(), Some(seg_id2));
        assert_eq!(seg2.prev(), Some(seg_id1));
        assert_eq!(seg2.next(), None);

        // First segment should be sealed, second should be live
        assert_eq!(seg1.state(), SegmentState::Sealed);
        assert_eq!(seg2.state(), SegmentState::Live);
    }

    #[tokio::test]
    async fn test_ttl_bucket_append_item() {
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Append an item (should allocate a segment automatically)
        let result = bucket.append_item(&cache, b"key1", b"value1", b"", cache.metrics()).await;
        assert!(result.is_some(), "Should successfully append item");

        let (seg_id, offset) = result.unwrap();

        // Verify segment was created
        assert_eq!(bucket.tail(), Some(seg_id));

        // Verify item was added
        let segment = cache.segments().get(seg_id).unwrap();
        assert_eq!(segment.live_items(), 1);
        assert!(offset < segment.write_offset());
    }

    #[tokio::test]
    async fn test_segment_full_triggers_new_segment() {
        // Create a cache with small segments
        let cache = crate::CacheBuilder::new()
            .hashtable_power(4) // Small hashtable (16 buckets)
            .segment_size(256) // Small segments
            .heap_size(1024)   // Room for multiple segments
            .build()
            .unwrap();

        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Append a large item to fill most of the first segment
        let result1 = bucket.append_item(&cache, b"key1", &[0u8; 128], b"", cache.metrics()).await;
        assert!(result1.is_some());
        let (seg_id1, _) = result1.unwrap();

        // Append another large item - should trigger new segment
        let result2 = bucket.append_item(&cache, b"key2", &[0u8; 128], b"", cache.metrics()).await;
        assert!(result2.is_some());
        let (seg_id2, _) = result2.unwrap();

        // Should be in different segments
        assert_ne!(seg_id1, seg_id2);

        // First segment should be sealed, second should be live
        assert_eq!(cache.segments().get(seg_id1).unwrap().state(), SegmentState::Sealed);
        assert_eq!(cache.segments().get(seg_id2).unwrap().state(), SegmentState::Live);
    }

    #[tokio::test]
    async fn test_evict_head_segment() {
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Append two segments
        let seg_id1 = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_id1, cache.metrics()).await.unwrap();

        let seg_id2 = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_id2, cache.metrics()).await.unwrap();

        assert_eq!(bucket.head(), Some(seg_id1));
        assert_eq!(bucket.tail(), Some(seg_id2));

        // Evict the head
        let evicted = bucket.evict_head_segment(&cache, cache.metrics());
        assert_eq!(evicted, Some(seg_id1));

        // Head should now be seg_id2
        assert_eq!(bucket.head(), Some(seg_id2));
        assert_eq!(bucket.tail(), Some(seg_id2));

        // Evicted segment should be back in Reserved state
        assert_eq!(cache.segments().get(seg_id1).unwrap().state(), SegmentState::Reserved);
    }

    #[tokio::test]
    async fn test_remove_middle_segment() {
        // Test removing segment B from chain A <-> B <-> C
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Create 3-segment chain: A <-> B <-> C
        let seg_a = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_a, cache.metrics()).await.unwrap();

        let seg_b = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_b, cache.metrics()).await.unwrap();

        let seg_c = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_c, cache.metrics()).await.unwrap();

        // Verify initial chain structure
        assert_eq!(bucket.head(), Some(seg_a));
        assert_eq!(bucket.tail(), Some(seg_c));

        let segment_a = cache.segments().get(seg_a).unwrap();
        let segment_b = cache.segments().get(seg_b).unwrap();
        let segment_c = cache.segments().get(seg_c).unwrap();

        assert_eq!(segment_a.prev(), None);
        assert_eq!(segment_a.next(), Some(seg_b));
        assert_eq!(segment_a.state(), SegmentState::Sealed);

        assert_eq!(segment_b.prev(), Some(seg_a));
        assert_eq!(segment_b.next(), Some(seg_c));
        assert_eq!(segment_b.state(), SegmentState::Sealed);

        assert_eq!(segment_c.prev(), Some(seg_b));
        assert_eq!(segment_c.next(), None);
        assert_eq!(segment_c.state(), SegmentState::Live);

        // Remove middle segment B
        let result = bucket.remove_segment(&cache, seg_b, cache.metrics()).await;
        assert!(result.is_ok(), "Should successfully remove middle segment");

        // Verify chain is now A <-> C
        assert_eq!(bucket.head(), Some(seg_a));
        assert_eq!(bucket.tail(), Some(seg_c));

        // A should now point to C
        assert_eq!(segment_a.prev(), None);
        assert_eq!(segment_a.next(), Some(seg_c));
        assert_eq!(segment_a.state(), SegmentState::Sealed);

        // C should now point back to A
        assert_eq!(segment_c.prev(), Some(seg_a));
        assert_eq!(segment_c.next(), None);
        assert_eq!(segment_c.state(), SegmentState::Live);

        // B should have no links and be in Free state (after release)
        assert_eq!(segment_b.prev(), None);
        assert_eq!(segment_b.next(), None);
        assert_eq!(segment_b.state(), SegmentState::Free);
    }

    #[tokio::test]
    async fn test_remove_segment_from_two_segment_chain() {
        // Test removing first segment from a 2-segment chain
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Create 2-segment chain: A <-> B
        let seg_a = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_a, cache.metrics()).await.unwrap();

        let seg_b = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_b, cache.metrics()).await.unwrap();

        assert_eq!(bucket.head(), Some(seg_a));
        assert_eq!(bucket.tail(), Some(seg_b));

        let segment_a = cache.segments().get(seg_a).unwrap();
        let segment_b = cache.segments().get(seg_b).unwrap();

        // Remove A (which only has next, no prev)
        let result = bucket.remove_segment(&cache, seg_a, cache.metrics()).await;
        assert!(result.is_ok(), "Should successfully remove first segment");

        // Head should now be B
        assert_eq!(bucket.head(), Some(seg_b));
        assert_eq!(bucket.tail(), Some(seg_b));

        // B should have no prev link
        assert_eq!(segment_b.prev(), None);
        assert_eq!(segment_b.next(), None);
        assert_eq!(segment_b.state(), SegmentState::Live);

        // A should be freed
        assert_eq!(segment_a.state(), SegmentState::Free);
    }

    #[tokio::test]
    async fn test_remove_segment_preserves_live_state() {
        // Test that the next segment remains Live after removal if it was Live
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Create 2-segment chain: A <-> B (B is Live)
        let seg_a = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_a, cache.metrics()).await.unwrap();

        let seg_b = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_b, cache.metrics()).await.unwrap();

        let segment_b = cache.segments().get(seg_b).unwrap();
        assert_eq!(segment_b.state(), SegmentState::Live, "B should start Live");

        // Remove A
        let result = bucket.remove_segment(&cache, seg_a, cache.metrics()).await;
        assert!(result.is_ok());

        // B should still be Live
        assert_eq!(segment_b.state(), SegmentState::Live, "B should remain Live after removal");
    }

    #[tokio::test]
    #[should_panic(expected = "Chain corruption detected")]
    async fn test_remove_segment_detects_chain_corruption() {
        // Test that remove_segment panics on corrupted chain
        let cache = Cache::new();
        let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

        // Create 3-segment chain
        let seg_a = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_a, cache.metrics()).await.unwrap();

        let seg_b = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_b, cache.metrics()).await.unwrap();

        let seg_c = cache.segments().reserve(cache.metrics()).unwrap();
        bucket.append_segment(&cache, seg_c, cache.metrics()).await.unwrap();

        // Corrupt the chain by breaking A's next pointer
        let segment_a = cache.segments().get(seg_a).unwrap();
        segment_a.cas_metadata(
            SegmentState::Sealed,
            SegmentState::Sealed,
            Some(INVALID_SEGMENT_ID), // Break the link to B
            None,
            cache.metrics(),
        );

        // This should panic when it detects B's prev points to A but A's next doesn't point to B
        bucket.remove_segment(&cache, seg_b, cache.metrics()).await.unwrap();
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;
    use crate::{Cache, SegmentState};

    #[test]
    fn try_append_item() {
        // Test low-level item append to existing live segment without retry loops
        // Set up a live segment at the tail to avoid testing append_segment/evict_head_segment
        use crate::segments::INVALID_SEGMENT_ID;

        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Reserve a segment and manually set it up as a Live tail
            let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
            let segment = cache.segments().get(seg_id).unwrap();

            // Manually transition to Live state with no links (it's both head and tail)
            segment.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Live,
                Some(INVALID_SEGMENT_ID), // next = invalid
                Some(INVALID_SEGMENT_ID), // prev = invalid
                cache.metrics(),
            );

            // Set expiration time
            let expire_at = clocksource::coarse::Instant::now() + clocksource::coarse::Duration::from_secs(60);
            segment.set_expire_at(expire_at);

            // Manually set up the bucket to have this segment as both head and tail
            let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);
            bucket.test_set_head_tail(seg_id, seg_id);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to append items to the same live segment
            let t1 = thread::spawn(move || {
                let bucket = c1.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_append_item(c1.as_ref(), b"key1", b"value1", b"", c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let bucket = c2.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_append_item(c2.as_ref(), b"key2", b"value2", b"", c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Both should succeed (the segment should have space for both items)
            assert!(result1.is_ok(), "First append should succeed");
            assert!(result2.is_ok(), "Second append should succeed");

            let (seg1, offset1) = result1.unwrap();
            let (seg2, offset2) = result2.unwrap();

            // Both should be to the same segment
            assert_eq!(seg1, seg_id);
            assert_eq!(seg2, seg_id);

            // Offsets should be different
            assert_ne!(offset1, offset2, "Offsets should be different");

            // Segment should have 2 items
            assert_eq!(segment.live_items(), 2, "Segment should have 2 live items");
        });
    }

    #[test]
    fn try_append_segment() {
        // Test low-level segment append without retry loops
        // Two threads racing to append to an empty bucket
        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Reserve 2 segments
            let seg_id1 = cache.segments().reserve(cache.metrics()).unwrap();
            let seg_id2 = cache.segments().reserve(cache.metrics()).unwrap();

            // Set expiration times manually (since try_append_segment doesn't do this)
            let expire_at = clocksource::coarse::Instant::now() + clocksource::coarse::Duration::from_secs(60);
            cache.segments().get(seg_id1).unwrap().set_expire_at(expire_at);
            cache.segments().get(seg_id2).unwrap().set_expire_at(expire_at);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to append to the same empty bucket
            let t1 = thread::spawn(move || {
                let bucket = c1.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_append_segment(c1.as_ref(), seg_id1, c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let bucket = c2.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_append_segment(c2.as_ref(), seg_id2, c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // At least one should succeed
            let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
            assert!(success_count >= 1, "At least one append should succeed");

            if success_count == 2 {
                // Both succeeded - they should be linked
                let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);
                let head = bucket.head().unwrap();
                let tail = bucket.tail().unwrap();
                assert_ne!(head, tail, "Head and tail should be different when both appends succeed");

                // Verify the linked list structure
                let head_segment = cache.segments().get(head).unwrap();
                let tail_segment = cache.segments().get(tail).unwrap();

                assert_eq!(head_segment.prev(), None, "Head should have no prev");
                assert_eq!(tail_segment.next(), None, "Tail should have no next");
                assert_eq!(head_segment.next(), Some(tail), "Head should point to tail");
                assert_eq!(tail_segment.prev(), Some(head), "Tail should point to head");
            } else {
                // Only one succeeded - it should be both head and tail
                let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);
                let head = bucket.head().unwrap();
                let tail = bucket.tail().unwrap();
                assert_eq!(head, tail, "When only one append succeeds, head should equal tail");

                // The failed segment's state depends on where the failure occurred:
                // - "Failed to transition to Linking state" → stays Reserved
                // - "Bucket was concurrently modified" → released to Free
                let (success_id, failed_id) = if result1.is_ok() {
                    (seg_id1, seg_id2)
                } else {
                    (seg_id2, seg_id1)
                };

                assert_eq!(head, success_id);
                let failed_state = cache.segments().get(failed_id).unwrap().state();
                assert!(
                    matches!(failed_state, SegmentState::Reserved | SegmentState::Free),
                    "Failed segment should be Reserved or Free, got {:?}",
                    failed_state
                );
            }
        });
    }

    #[test]
    fn try_evict_head_segment() {
        // Test low-level head eviction without retry loops
        // Set up state manually to avoid going through append_segment's retry logic
        use crate::segments::INVALID_SEGMENT_ID;

        loom::model(|| {
            let cache = Arc::new(Cache::new());

            // Reserve 2 segments
            let seg_id1 = cache.segments().reserve(cache.metrics()).unwrap();
            let seg_id2 = cache.segments().reserve(cache.metrics()).unwrap();

            let seg1 = cache.segments().get(seg_id1).unwrap();
            let seg2 = cache.segments().get(seg_id2).unwrap();

            // Manually set up the chain: seg1 (Sealed) -> seg2 (Live)
            // seg1: head, Sealed state, next=seg2, prev=None
            seg1.cas_metadata(SegmentState::Reserved, SegmentState::Sealed, Some(seg_id2), Some(INVALID_SEGMENT_ID), cache.metrics());

            // seg2: tail, Live state, next=None, prev=seg1
            seg2.cas_metadata(SegmentState::Reserved, SegmentState::Live, Some(INVALID_SEGMENT_ID), Some(seg_id1), cache.metrics());

            // Manually set up the bucket's head and tail pointers using test helper
            let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);
            bucket.test_set_head_tail(seg_id1, seg_id2);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to evict the head
            let t1 = thread::spawn(move || {
                let bucket = c1.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_evict_head_segment(c1.as_ref(), c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let bucket = c2.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_evict_head_segment(c2.as_ref(), c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Exactly one should succeed
            let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
            assert_eq!(success_count, 1, "Exactly one eviction should succeed");

            // The successful eviction should return seg_id1
            if result1.is_ok() {
                assert_eq!(result1.unwrap(), seg_id1);
            } else {
                assert_eq!(result2.unwrap(), seg_id1);
            }

            // After eviction, bucket head should be seg_id2
            let new_head = bucket.head().unwrap();
            assert_eq!(new_head, seg_id2);

            // seg_id1 should be in Reserved state (ready for reuse)
            assert_eq!(cache.segments().get(seg_id1).unwrap().state(), SegmentState::Reserved);
        });
    }

    #[test]
    fn concurrent_ttl_bucket_append() {
        loom::model(|| {

            let cache = Arc::new(Cache::new());

            // Reserve two segments
            let seg_id1 = cache.segments().reserve(cache.metrics()).unwrap();
            let seg_id2 = cache.segments().reserve(cache.metrics()).unwrap();

            let cache1 = Arc::clone(&cache);
            let cache2 = Arc::clone(&cache);

            // Two threads try to append segments to the same bucket
            let t1 = thread::spawn(move || {
                let bucket = cache1.ttl_buckets().get_bucket_for_seconds(60);
                bucket.append_segment(cache1.as_ref(), seg_id1, cache1.metrics())
            });
            let t2 = thread::spawn(move || {
                let bucket = cache2.ttl_buckets().get_bucket_for_seconds(60);
                bucket.append_segment(cache2.as_ref(), seg_id2, cache2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // In some interleavings, one may fail due to concurrent modification
            // At least one should succeed
            let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
            assert!(success_count >= 1, "At least one append should succeed");

            if success_count == 2 {
                // Both succeeded - verify linked list structure
                let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);
                let head = bucket.head().unwrap();
                let tail = bucket.tail().unwrap();
                assert_ne!(head, tail, "Head and tail should be different");

                let head_segment = cache.segments().get(head).unwrap();
                let tail_segment = cache.segments().get(tail).unwrap();

                // Head should have no prev, tail should have no next
                assert_eq!(head_segment.prev(), None, "Head should have no prev");
                assert_eq!(tail_segment.next(), None, "Tail should have no next");

                // They should be linked to each other
                assert_eq!(head_segment.next(), Some(tail), "Head should point to tail");
                assert_eq!(tail_segment.prev(), Some(head), "Tail should point to head");

                // Both should be in Live or Sealed state
                assert!(
                    matches!(head_segment.state(), SegmentState::Live | SegmentState::Sealed),
                    "Head segment in unexpected state: {:?}",
                    head_segment.state()
                );
                assert!(
                    matches!(tail_segment.state(), SegmentState::Live | SegmentState::Sealed),
                    "Tail segment in unexpected state: {:?}",
                    tail_segment.state()
                );
            }

            // Metrics are tracked correctly (verified by simpler tests with LOOM_MAX_PREEMPTIONS=1)
        });
    }

    #[test]
    fn try_remove_segment() {
        // Test low-level segment removal with Relinking protocol
        // Two threads trying to remove the same segment - only one should succeed
        use crate::segments::INVALID_SEGMENT_ID;

        loom::model(|| {
            let cache = Arc::new(Cache::new());
            let bucket = cache.ttl_buckets().get_bucket_for_seconds(60);

            // Create 3-segment chain: A <-> B <-> C
            let seg_a = cache.segments().reserve(cache.metrics()).unwrap();
            let seg_b = cache.segments().reserve(cache.metrics()).unwrap();
            let seg_c = cache.segments().reserve(cache.metrics()).unwrap();

            // Set up the chain manually to avoid testing append_segment
            let segment_a = cache.segments().get(seg_a).unwrap();
            let segment_b = cache.segments().get(seg_b).unwrap();
            let segment_c = cache.segments().get(seg_c).unwrap();

            let expire_at = clocksource::coarse::Instant::now() + clocksource::coarse::Duration::from_secs(60);
            segment_a.set_expire_at(expire_at);
            segment_b.set_expire_at(expire_at);
            segment_c.set_expire_at(expire_at);

            // A: Sealed, no prev, next -> B
            segment_a.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Sealed,
                Some(seg_b),
                Some(INVALID_SEGMENT_ID),
                cache.metrics(),
            );

            // B: Sealed, prev -> A, next -> C
            segment_b.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Sealed,
                Some(seg_c),
                Some(seg_a),
                cache.metrics(),
            );

            // C: Live, prev -> B, no next
            segment_c.cas_metadata(
                SegmentState::Reserved,
                SegmentState::Live,
                Some(INVALID_SEGMENT_ID),
                Some(seg_b),
                cache.metrics(),
            );

            // Set bucket head/tail
            bucket.test_set_head_tail(seg_a, seg_c);

            let c1 = Arc::clone(&cache);
            let c2 = Arc::clone(&cache);

            // Two threads try to remove B concurrently
            let t1 = thread::spawn(move || {
                let bucket = c1.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_remove_segment(c1.as_ref(), seg_b, c1.metrics())
            });

            let t2 = thread::spawn(move || {
                let bucket = c2.ttl_buckets().get_bucket_for_seconds(60);
                bucket.try_remove_segment(c2.as_ref(), seg_b, c2.metrics())
            });

            let result1 = t1.join().unwrap();
            let result2 = t2.join().unwrap();

            // Exactly one should succeed
            let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
            assert_eq!(success_count, 1, "Exactly one removal should succeed");

            // After removal, A should link to C
            assert_eq!(segment_a.next(), Some(seg_c), "A should link to C");
            assert_eq!(segment_c.prev(), Some(seg_a), "C should link back to A");

            // B should be in Locked state with links cleared (caller must clear and release)
            assert_eq!(segment_b.state(), SegmentState::Locked, "B should be Locked");
            assert_eq!(segment_b.next(), Some(INVALID_SEGMENT_ID), "B should have cleared next link");
            assert_eq!(segment_b.prev(), Some(INVALID_SEGMENT_ID), "B should have cleared prev link");

            // Bucket head/tail should still be correct
            assert_eq!(bucket.head(), Some(seg_a));
            assert_eq!(bucket.tail(), Some(seg_c));
        });
    }
}
