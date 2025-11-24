mod hashtable;
mod item;
mod segments;
mod ttlbuckets;

pub(crate) mod metrics;
pub(crate) mod sync;
pub(crate) mod util;

#[cfg(test)]
mod tests;

use hashtable::*;
use item::*;
use segments::*;
use ttlbuckets::*;

pub use metrics::CacheMetrics;
pub use std::time::Duration;

use std::sync::Arc;

/// Eviction policy for the cache
///
/// Determines which segment to evict when the cache runs out of memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Random eviction - picks a random segment from the segment array
    ///
    /// Fast and simple. Scans the segment array from a random starting position
    /// and evicts the first Live or Sealed segment found. Good for workloads
    /// where no particular access pattern dominates.
    Random,

    /// Random FIFO eviction - selects a random TTL bucket weighted by segment count,
    /// and evicts the oldest segment in that bucket
    ///
    /// This strategy helps preserve the TTL distribution of the working set and
    /// prioritizes keeping newer items. By weighting bucket selection by the number
    /// of segments, buckets with more data are more likely to be selected for eviction,
    /// which helps balance memory usage across different TTL ranges.
    RandomFifo,

    /// Closest to expiration (CTE) - evicts the segment that would expire soonest
    ///
    /// This strategy walks through all TTL buckets and finds the segment with the
    /// earliest expiration time, then evicts it. This is unique to segcache and
    /// effectively implements early TTL expiration to free memory. Good for workloads
    /// where respecting TTL ordering is important even under memory pressure.
    Cte,

    /// Least utilized segment - evicts the segment with the fewest live bytes
    ///
    /// As segments are append-only, when an item is replaced or removed, the segment
    /// containing that item accumulates dead bytes. This strategy evicts the segment
    /// with the lowest number of live bytes, minimizing the impact on the total number
    /// of live bytes in the cache. Good for workloads with high update rates where
    /// fragmentation is a concern.
    Util,

    /// No automatic eviction - fail allocations when out of memory
    ///
    /// Useful for testing or when you want explicit control over eviction
    /// via manual calls to expire_segments() or evict_random_segment().
    None,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        EvictionPolicy::Random
    }
}

/// Error type for set operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetItemError {
    /// Failed to allocate space (out of memory after eviction attempts)
    OutOfMemory,
    /// Failed to insert into hashtable (CAS failure, very rare)
    HashTableInsertFailed,
}

/// Error type for delete operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteItemError {
    /// Item not found in hashtable
    NotFound,
    /// Segment ID was invalid
    InvalidSegment,
    /// Item was already deleted
    AlreadyDeleted,
    /// Failed to mark item as deleted
    MarkDeletedFailed,
}

/// Core cache data shared between the public API
struct CacheCore {
    hashtable: Hashtable,
    segments: Segments,
    ttl_buckets: TtlBuckets,
    metrics: CacheMetrics,
    eviction_policy: EvictionPolicy,
}

pub struct Cache {
    core: Arc<CacheCore>,
}

/// Builder for constructing a Cache with custom configuration
pub struct CacheBuilder {
    hashtable_power: u8,
    segment_size: usize,
    heap_size: usize,
    eviction_policy: EvictionPolicy,
}

impl CacheBuilder {
    /// Create a new CacheBuilder with default settings
    pub fn new() -> Self {
        Self {
            hashtable_power: 16, // 2^16 = 65536 buckets
            segment_size: 1024 * 1024, // 1MB
            heap_size: 64 * 1024 * 1024, // 64MB
            eviction_policy: EvictionPolicy::default(),
        }
    }

    /// Set the hashtable size as a power of 2
    ///
    /// The hashtable will have 2^power buckets. Default is 16 (65536 buckets).
    /// Larger values provide better distribution but use more memory.
    pub fn hashtable_power(mut self, power: u8) -> Self {
        self.hashtable_power = power;
        self
    }

    /// Set the size of each segment in bytes
    ///
    /// Default is 1MB. Larger segments reduce metadata overhead but may
    /// increase fragmentation. Must be at most heap_size.
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the total heap size in bytes
    ///
    /// Default is 64MB. This determines the total cache capacity.
    /// The number of segments will be heap_size / segment_size.
    pub fn heap_size(mut self, size: usize) -> Self {
        self.heap_size = size;
        self
    }

    /// Set the eviction policy
    ///
    /// Default is Random eviction. Use EvictionPolicy::None to disable
    /// automatic eviction and require manual management.
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Build the Cache with the configured settings
    pub fn build(self) -> Cache {
        assert!(
            self.heap_size >= self.segment_size,
            "Heap size must be at least as large as segment size"
        );

        let segments = SegmentsBuilder::new()
            .segment_size(self.segment_size)
            .heap_size(self.heap_size)
            .build();

        let metrics = CacheMetrics::new();

        // Initialize segments_free gauge to the total number of segments
        let num_segments = self.heap_size / self.segment_size;
        metrics.segments_free.set(num_segments as i64);

        let core = Arc::new(CacheCore {
            hashtable: Hashtable::new(self.hashtable_power),
            segments,
            ttl_buckets: TtlBuckets::new(),
            metrics,
            eviction_policy: self.eviction_policy,
        });

        Cache {
            core,
        }
    }
}

impl Default for CacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    /// Create a new Cache with default settings
    ///
    /// Default configuration:
    /// - Hashtable: 2^16 (65536) buckets
    /// - Segment size: 1MB
    /// - Heap size: 64MB
    ///
    /// For custom configuration, use CacheBuilder:
    /// ```
    /// use async_segcache::CacheBuilder;
    ///
    /// let cache = CacheBuilder::new()
    ///     .heap_size(256 * 1024 * 1024) // 256MB
    ///     .segment_size(2 * 1024 * 1024) // 2MB segments
    ///     .hashtable_power(18) // 262K buckets
    ///     .build();
    /// ```
    pub fn new() -> Self {
        CacheBuilder::new().build()
    }

    /// Get a reference to the cache metrics
    pub fn metrics(&self) -> &CacheMetrics {
        &self.core.metrics
    }

    /// Set an item in the cache with optional TTL
    ///
    /// If ttl is None, uses the largest TTL bucket (effectively ~8.9 million seconds)
    ///
    /// This is an async operation that only awaits when allocating new segments,
    /// allowing other tasks to run while waiting for the TTL bucket mutex.
    pub async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), SetItemError> {
        // Use largest TTL bucket if no TTL specified
        let std_duration = ttl.unwrap_or_else(|| {
            // Largest TTL bucket is at index 1023 (32768 * 256 = ~8.4M seconds)
            Duration::from_secs(32768 * 256)
        });

        // Convert std::time::Duration to clocksource::coarse::Duration
        // Clamp to u32::MAX if duration is too large
        let secs = std_duration.as_secs().min(u32::MAX as u64) as u32;
        let duration = clocksource::coarse::Duration::from_secs(secs);

        // Directly call reserve_and_link (no queue/worker needed)
        // The async mutex in TTL buckets serializes segment allocation
        self.core.hashtable.reserve_and_link(
            key,
            value,
            optional,
            duration,
            &*self.core,
        ).await
            .map_err(|_| SetItemError::HashTableInsertFailed)
    }

    /// Get an item from the cache into a provided buffer
    ///
    /// This method is useful when you want to reuse buffers across multiple gets
    /// to avoid allocations. For most use cases, prefer `get()` which returns
    /// a zero-copy ItemGuard.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to look up
    /// * `buffer` - Buffer to copy the item data into (must be large enough)
    ///
    /// # Returns
    ///
    /// - `Ok(Item)` - Item with data copied into the provided buffer
    /// - `Err(GetItemError::BufferTooSmall)` - Buffer is too small for the item
    /// - `Err(GetItemError::NotFound)` - Key not found or item expired
    pub fn get_with_buffer<'a>(&self, key: &[u8], buffer: &'a mut [u8]) -> Result<Item<'a>, GetItemError> {
        // Step 1: Look up item in hashtable
        let (segment_id, offset) = self
            .core.hashtable
            .get(key, &self.core.segments)
            .ok_or(GetItemError::NotFound)?;

        // Step 2: Check if segment has expired
        if let Some(segment) = self.core.segments.get(segment_id) {
            let now = clocksource::coarse::Instant::now();
            if now >= segment.expire_at() {
                // Segment has expired - mark item as deleted and unlink
                self.core.metrics.item_expire.increment();

                // Mark deleted in segment (ignore errors - item may already be deleted)
                let _ = segment.mark_deleted(offset, key, &self.core.metrics);

                // Unlink from hashtable
                self.core.hashtable.unlink_item(key, segment_id, offset, &self.core.metrics);

                return Err(GetItemError::NotFound);
            }
        }

        // Step 3: Read item from segment
        self.core.segments.get_item(segment_id, offset, key, buffer)
    }

    /// Get an item from the cache with zero-copy access.
    ///
    /// Returns an ItemGuard that provides direct access to the item's data in the segment
    /// without any allocations or copies. The guard holds a reference to the segment,
    /// preventing eviction while the guard exists.
    ///
    /// This is the most efficient way to read from the cache, ideal for hot paths where
    /// you want to minimize overhead. The caller can access the value directly or clone it
    /// if needed.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// - `Ok(ItemGuard)` - Guard providing zero-copy access to the item
    /// - `Err(GetItemError::NotFound)` - Key not found or item expired
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Zero-copy read
    /// let guard = cache.get_with_buffer(b"key")?;
    /// let value = guard.value();
    /// process_data(value);
    ///
    /// // Serialize directly to socket
    /// socket.write_all(guard.value())?;
    ///
    /// // Clone if needed
    /// let owned = guard.value().to_vec();
    /// ```
    pub fn get<'a>(&'a self, key: &[u8]) -> Result<crate::item::ItemGuard<'a>, GetItemError> {
        // Step 1: Look up item in hashtable
        let (segment_id, offset) = self
            .core.hashtable
            .get(key, &self.core.segments)
            .ok_or(GetItemError::NotFound)?;

        // Step 2: Check if segment has expired
        if let Some(segment) = self.core.segments.get(segment_id) {
            let now = clocksource::coarse::Instant::now();
            if now >= segment.expire_at() {
                // Segment has expired - mark item as deleted and unlink
                self.core.metrics.item_expire.increment();

                // Mark deleted in segment (ignore errors - item may already be deleted)
                let _ = segment.mark_deleted(offset, key, &self.core.metrics);

                // Unlink from hashtable
                self.core.hashtable.unlink_item(key, segment_id, offset, &self.core.metrics);

                return Err(GetItemError::NotFound);
            }
        }

        // Step 3: Get guard with zero-copy access to item
        self.core.segments.get_item_guard(segment_id, offset, key)
    }

    /// Delete an item from the cache
    ///
    /// If the deletion causes a sealed segment to become empty, it will be
    /// eagerly removed from its TTL bucket and returned to the free pool.
    pub async fn delete(&self, key: &[u8]) -> Result<(), DeleteItemError> {
        // Step 1: Look up item in hashtable
        let (segment_id, offset) = self
            .core.hashtable
            .get(key, &self.core.segments)
            .ok_or(DeleteItemError::NotFound)?;

        // Step 2: Get segment and mark item as deleted
        let segment = self
            .core.segments
            .get(segment_id)
            .ok_or(DeleteItemError::InvalidSegment)?;

        match segment.mark_deleted(offset, key, &self.core.metrics) {
            Ok(true) => {
                // Successfully marked as deleted
            }
            Ok(false) => {
                // Already deleted
                return Err(DeleteItemError::AlreadyDeleted);
            }
            Err(_) => {
                // Mark deleted failed (key mismatch or other error)
                return Err(DeleteItemError::MarkDeletedFailed);
            }
        }

        // Step 3: Unlink from hashtable
        self.core.hashtable.unlink_item(key, segment_id, offset, &self.core.metrics);

        // Step 4: Check for eager removal - if segment is now empty and sealed,
        // remove it from the TTL bucket chain and return it to the free pool
        let remaining_items = segment.live_items();
        if remaining_items == 0 {
            let state = segment.state();
            if state == SegmentState::Sealed {
                // Segment is empty and sealed - try to remove it from the bucket
                if let Some(bucket_id) = segment.bucket_id() {
                    let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_id);
                    // Try to remove - don't fail the delete if removal fails
                    // (another thread might have already removed it, or it's being evicted)
                    let _ = bucket.remove_segment(self, segment_id, &self.core.metrics).await;
                }
            }
        }

        Ok(())
    }

    /// Proactively expire segments whose TTL has passed
    ///
    /// Walks through TTL buckets from shortest to longest TTL, expiring
    /// segments that have exceeded their expiration time. This is useful for:
    /// - Background expiration threads to reduce memory pressure
    /// - Ensuring timely cleanup of expired data
    /// - Keeping metrics accurate
    ///
    /// Returns the number of segments expired.
    ///
    /// # Example
    /// ```no_run
    /// use async_segcache::Cache;
    /// use std::time::Duration;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = Cache::new();
    ///
    /// // Insert some items with short TTL
    /// cache.set(b"key1", b"value1", &[], Some(Duration::from_secs(1))).await.unwrap();
    /// cache.set(b"key2", b"value2", &[], Some(Duration::from_secs(1))).await.unwrap();
    ///
    /// // Wait for expiration
    /// std::thread::sleep(Duration::from_secs(2));
    ///
    /// // Proactively expire segments
    /// let expired_count = cache.expire_segments();
    /// println!("Expired {} segments", expired_count);
    /// # }
    /// ```
    pub fn expire_segments(&self) -> usize {
        let now = clocksource::coarse::Instant::now();
        let mut expired_count = 0;

        // Walk through all TTL buckets in order (shortest to longest TTL)
        // We can stop when we find a bucket with no expired segments
        for bucket_idx in 0..1024 {
            let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_idx);

            // Keep expiring segments from the head of this bucket while they're expired
            loop {
                // Get the head of this bucket's segment chain
                let head_id = match bucket.head() {
                    Some(id) => id,
                    None => break, // Empty bucket, move to next bucket
                };

                // Check if the head segment has expired
                let segment = match self.core.segments.get(head_id) {
                    Some(seg) => seg,
                    None => break, // Invalid segment, move to next bucket
                };

                let expire_at = segment.expire_at();

                if now < expire_at {
                    // Head hasn't expired yet. Since segments are ordered by expiration time
                    // within each bucket, and buckets are ordered by TTL, no segments in this
                    // bucket or higher buckets have expired. We can stop entirely.
                    return expired_count;
                }

                // Head segment is expired - try to expire it
                match bucket.try_expire_head_segment(self, &self.core.metrics) {
                    Ok(()) => {
                        expired_count += 1;
                        // Continue loop to check if the new head is also expired
                    }
                    Err(_) => {
                        // Failed to expire (concurrent modification, active readers, etc.)
                        // Move to next bucket to avoid getting stuck
                        break;
                    }
                }
            }
        }

        expired_count
    }

    /// Attempt to evict a random segment to free up memory.
    ///
    /// This method scans the segment array starting from a random position,
    /// looking for a segment in Live or Sealed state that can be evicted.
    /// It makes a single pass through the array and returns on the first
    /// successful eviction or after scanning all segments.
    ///
    /// The evicted segment is cleared (all items unlinked from hashtable)
    /// and transitioned to Locked state, ready for immediate reuse.
    ///
    /// # Returns
    ///
    /// - `Ok(segment_id)` - Successfully evicted segment, caller can reuse it
    /// - `Err(&str)` - No segment could be evicted (all segments are Free, Reserved,
    ///   or currently being processed by other threads)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use async_segcache::Cache;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = Cache::new();
    ///
    /// // Try to evict a random segment when under memory pressure
    /// match cache.evict_random_segment().await {
    ///     Ok(segment_id) => println!("Evicted segment {}", segment_id),
    ///     Err(e) => println!("Eviction failed: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn evict_random_segment(&self) -> Result<u32, &'static str> {
        use rand::Rng;

        let num_segments = self.core.segments.segments.len();
        if num_segments == 0 {
            return Err("No segments available");
        }

        // Pick a random starting point
        #[cfg(not(feature = "loom"))]
        let start_id = rand::thread_rng().r#gen::<u32>() % (num_segments as u32);

        #[cfg(feature = "loom")]
        let start_id = 0u32;

        // Scan for a Live or Sealed segment (with wraparound)
        // Make exactly one pass through the array
        for offset in 0..num_segments {
            let id = (start_id + offset as u32) % (num_segments as u32);

            if let Some(segment) = self.core.segments.get(id) {
                let state = segment.state();

                // Only evict segments that are in a bucket chain with data
                if state == SegmentState::Sealed || state == SegmentState::Live {
                    // Get the bucket this segment belongs to
                    if let Some(bucket_id) = segment.bucket_id() {
                        let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_id);

                        // Try to remove from chain (may fail due to concurrent modifications)
                        if bucket.remove_segment(self, id, &self.core.metrics).await.is_ok() {
                            // Successfully removed from chain, now evict
                            // evict() clears the segment and leaves it in Locked state
                            if self.core.segments.evict(id, self) {
                                return Ok(id);
                            }
                        }
                        // If remove_segment or evict failed, continue scanning
                        // Don't retry the same segment - let caller retry the append
                    }
                }
            }
        }

        // Made a full pass, no segment could be evicted
        Err("No evictable segments found")
    }

    /// Attempt to evict the head (oldest) segment from a random TTL bucket.
    ///
    /// This method implements weighted random selection by picking a random segment
    /// from the segment array, looking up its TTL bucket, and evicting the oldest
    /// segment in that bucket. This naturally weights selection toward buckets with
    /// more segments without requiring any chain walking or counting.
    ///
    /// The algorithm:
    /// 1. Pick a random segment ID from the segment array
    /// 2. Check if it's Live or Sealed (in a TTL bucket chain)
    /// 3. Get its bucket ID
    /// 4. Evict the head (oldest) segment from that bucket
    /// 5. If the random segment wasn't in a bucket, try the next segment (with wraparound)
    ///
    /// # Returns
    ///
    /// - `Ok(segment_id)` - Successfully evicted segment, ready for reuse in Locked state
    /// - `Err(&str)` - No segment could be evicted (all buckets empty or eviction failed)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use async_segcache::Cache;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = Cache::new();
    ///
    /// // Evict oldest segment from a randomly selected bucket
    /// match cache.evict_random_fifo_segment().await {
    ///     Ok(segment_id) => println!("Evicted segment {}", segment_id),
    ///     Err(e) => println!("Eviction failed: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn evict_random_fifo_segment(&self) -> Result<u32, &'static str> {
        use rand::Rng;

        let num_segments = self.core.segments.segments.len();
        if num_segments == 0 {
            return Err("No segments available");
        }

        // Pick a random starting point
        #[cfg(not(feature = "loom"))]
        let start_id = rand::thread_rng().r#gen::<u32>() % (num_segments as u32);

        #[cfg(feature = "loom")]
        let start_id = 0u32;

        // Scan for a Live or Sealed segment to determine the bucket
        // Make exactly one pass through the array
        for offset in 0..num_segments {
            let id = (start_id + offset as u32) % (num_segments as u32);

            if let Some(segment) = self.core.segments.get(id) {
                let state = segment.state();

                // Only consider segments that are in a bucket chain
                if state == SegmentState::Sealed || state == SegmentState::Live {
                    // Get the bucket this segment belongs to
                    if let Some(bucket_id) = segment.bucket_id() {
                        let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_id);

                        // Evict the head (oldest) segment from this bucket
                        if let Some(head_id) = bucket.head() {
                            // Try to remove from chain
                            if bucket.remove_segment(self, head_id, &self.core.metrics).await.is_ok() {
                                // Successfully removed from chain, now evict
                                if self.core.segments.evict(head_id, self) {
                                    return Ok(head_id);
                                }
                            }
                        }
                        // If removal or eviction failed, continue scanning
                        // to find another bucket
                    }
                }
            }
        }

        Err("No evictable segments found in any bucket")
    }

    /// Attempt to evict the segment closest to expiration (CTE).
    ///
    /// This method walks through all segments in the segment array and finds the one
    /// with the earliest expiration time, then evicts it. This is effectively early
    /// TTL expiration - we're evicting the segment that would naturally expire first
    /// anyway.
    ///
    /// This strategy is unique to segcache and respects TTL ordering even under memory
    /// pressure, making it ideal for workloads where maintaining TTL semantics is
    /// important.
    ///
    /// The algorithm:
    /// 1. Walk through all segments in the segment array
    /// 2. For each Live or Sealed segment, check its expiration time
    /// 3. Track the segment with the earliest expiration time
    /// 4. Evict that segment
    ///
    /// # Returns
    ///
    /// - `Ok(segment_id)` - Successfully evicted segment, ready for reuse in Locked state
    /// - `Err(&str)` - No segment could be evicted (no segments found or eviction failed)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use async_segcache::Cache;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = Cache::new();
    ///
    /// // Evict the segment that would expire first
    /// match cache.evict_cte_segment().await {
    ///     Ok(segment_id) => println!("Evicted segment {}", segment_id),
    ///     Err(e) => println!("Eviction failed: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn evict_cte_segment(&self) -> Result<u32, &'static str> {
        use clocksource::coarse::Instant;

        let num_segments = self.core.segments.segments.len();
        if num_segments == 0 {
            return Err("No segments available");
        }

        let mut earliest_expiration: Option<Instant> = None;
        let mut earliest_segment_id: Option<u32> = None;
        let mut earliest_bucket_id: Option<u16> = None;

        // Walk through all segments to find the one with earliest expiration
        for id in 0..num_segments {
            if let Some(segment) = self.core.segments.get(id as u32) {
                let state = segment.state();

                // Only consider segments that are in a bucket chain with data
                if state == SegmentState::Sealed || state == SegmentState::Live {
                    let expire_at = segment.expire_at();

                    // Check if this segment expires earlier than our current earliest
                    if earliest_expiration.is_none() || expire_at < earliest_expiration.unwrap() {
                        earliest_expiration = Some(expire_at);
                        earliest_segment_id = Some(id as u32);
                        earliest_bucket_id = segment.bucket_id();
                    }
                }
            }
        }

        // If we found a candidate, try to evict it
        if let (Some(segment_id), Some(bucket_id)) = (earliest_segment_id, earliest_bucket_id) {
            let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_id);

            // Try to remove from chain
            if bucket.remove_segment(self, segment_id, &self.core.metrics).await.is_ok() {
                // Successfully removed from chain, now evict
                if self.core.segments.evict(segment_id, self) {
                    return Ok(segment_id);
                }
            }

            // If eviction failed, return error and let caller retry
            return Err("Failed to evict segment closest to expiration");
        }

        Err("No evictable segments found")
    }

    /// Attempt to evict the least utilized segment.
    ///
    /// This method walks through all segments in the segment array and finds the one
    /// with the fewest live bytes, then evicts it. Since segments are append-only,
    /// updates and deletes create fragmentation (dead bytes). This strategy minimizes
    /// the impact on total live bytes by evicting the most fragmented segments first.
    ///
    /// The algorithm:
    /// 1. Walk through all segments in the segment array
    /// 2. For each Live or Sealed segment, check its live_bytes count
    /// 3. Track the segment with the fewest live bytes
    /// 4. Evict that segment
    ///
    /// # Returns
    ///
    /// - `Ok(segment_id)` - Successfully evicted segment, ready for reuse in Locked state
    /// - `Err(&str)` - No segment could be evicted (no segments found or eviction failed)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use async_segcache::Cache;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = Cache::new();
    ///
    /// // Evict the segment with the most fragmentation (fewest live bytes)
    /// match cache.evict_util_segment().await {
    ///     Ok(segment_id) => println!("Evicted segment {}", segment_id),
    ///     Err(e) => println!("Eviction failed: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn evict_util_segment(&self) -> Result<u32, &'static str> {
        let num_segments = self.core.segments.segments.len();
        if num_segments == 0 {
            return Err("No segments available");
        }

        let mut least_live_bytes: Option<u32> = None;
        let mut least_utilized_id: Option<u32> = None;
        let mut least_utilized_bucket_id: Option<u16> = None;

        // Walk through all segments to find the one with fewest live bytes
        for id in 0..num_segments {
            if let Some(segment) = self.core.segments.get(id as u32) {
                let state = segment.state();

                // Only consider segments that are in a bucket chain with data
                if state == SegmentState::Sealed || state == SegmentState::Live {
                    let live_bytes = segment.live_bytes();

                    // Check if this segment has fewer live bytes than our current minimum
                    if least_live_bytes.is_none() || live_bytes < least_live_bytes.unwrap() {
                        least_live_bytes = Some(live_bytes);
                        least_utilized_id = Some(id as u32);
                        least_utilized_bucket_id = segment.bucket_id();
                    }
                }
            }
        }

        // If we found a candidate, try to evict it
        if let (Some(segment_id), Some(bucket_id)) = (least_utilized_id, least_utilized_bucket_id) {
            let bucket = self.core.ttl_buckets.get_bucket_by_index(bucket_id);

            // Try to remove from chain
            if bucket.remove_segment(self, segment_id, &self.core.metrics).await.is_ok() {
                // Successfully removed from chain, now evict
                if self.core.segments.evict(segment_id, self) {
                    return Ok(segment_id);
                }
            }

            // If eviction failed, return error and let caller retry
            return Err("Failed to evict least utilized segment");
        }

        Err("No evictable segments found")
    }

    /// Get the configured eviction policy
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.core.eviction_policy
    }

    /// Internal method to evict a segment according to the configured policy.
    ///
    /// Called automatically when allocations fail. Returns Ok(segment_id) on success,
    /// Err if eviction is disabled or no segment could be evicted.
    #[allow(dead_code)] // Will be used when automatic eviction on allocation failure is implemented
    pub(crate) async fn evict_by_policy(&self) -> Result<u32, &'static str> {
        match self.core.eviction_policy {
            EvictionPolicy::Random => self.evict_random_segment().await,
            EvictionPolicy::RandomFifo => self.evict_random_fifo_segment().await,
            EvictionPolicy::Cte => self.evict_cte_segment().await,
            EvictionPolicy::Util => self.evict_util_segment().await,
            EvictionPolicy::None => Err("Eviction disabled by policy"),
        }
    }
}

pub(crate) trait CacheOps {
    fn segments(&self) -> &Segments;
    fn ttl_buckets(&self) -> &TtlBuckets;
    fn hashtable(&self) -> &Hashtable;
    fn metrics(&self) -> &CacheMetrics;
    fn eviction_policy(&self) -> EvictionPolicy;

    /// Try to expire a single segment
    /// Returns the freed segment ID on success, None if no expired segments found
    /// This is cheaper than eviction since expired segments are already invalid
    fn try_expire_one_segment(&self) -> Option<u32>;

    /// Evict a segment using the configured eviction policy
    /// Returns the evicted segment ID on success, None if eviction failed
    async fn evict_segment_by_policy(&self) -> Option<u32>;
}

impl CacheOps for CacheCore {
    fn segments(&self) -> &Segments {
        &self.segments
    }
    fn ttl_buckets(&self) -> &TtlBuckets {
        &self.ttl_buckets
    }
    fn hashtable(&self) -> &Hashtable {
        &self.hashtable
    }
    fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
    fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    fn try_expire_one_segment(&self) -> Option<u32> {
        let now = clocksource::coarse::Instant::now();

        // Walk through all TTL buckets in order (shortest to longest TTL)
        // Stop as soon as we expire one segment or find a non-expired head
        for bucket_idx in 0..1024 {
            let bucket = self.ttl_buckets.get_bucket_by_index(bucket_idx);

            // Get the head of this bucket's segment chain
            let head_id = match bucket.head() {
                Some(id) => id,
                None => continue, // Empty bucket, try next bucket
            };

            // Check if the head segment has expired
            let segment = match self.segments.get(head_id) {
                Some(seg) => seg,
                None => continue, // Invalid segment, try next bucket
            };

            let expire_at = segment.expire_at();

            if now < expire_at {
                // Head hasn't expired yet. Since segments are ordered by expiration time
                // within each bucket, and buckets are ordered by TTL, no segments in this
                // bucket or higher buckets have expired. We can stop entirely.
                return None;
            }

            // Head segment is expired - try to expire it
            if let Ok(()) = bucket.try_expire_head_segment(self, &self.metrics) {
                // Successfully expired one segment, reserve it and return
                return self.segments.reserve(&self.metrics);
            }
            // If expiration failed, continue to next bucket
        }

        None
    }

    async fn evict_segment_by_policy(&self) -> Option<u32> {
        use rand::Rng;

        let num_segments = self.segments.segments.len();
        if num_segments == 0 {
            return None;
        }

        // Pick a random starting point
        #[cfg(not(feature = "loom"))]
        let start_id = rand::thread_rng().r#gen::<u32>() % (num_segments as u32);

        #[cfg(feature = "loom")]
        let start_id = 0u32;

        // Scan for a Live or Sealed segment (with wraparound)
        // Make exactly one pass through the array
        for offset in 0..num_segments {
            let id = (start_id + offset as u32) % (num_segments as u32);

            if let Some(segment) = self.segments.get(id) {
                let state = segment.state();

                // Only evict segments that are in a bucket chain with data
                if state == SegmentState::Sealed || state == SegmentState::Live {
                    // Get the bucket this segment belongs to
                    if let Some(bucket_id) = segment.bucket_id() {
                        let bucket = self.ttl_buckets.get_bucket_by_index(bucket_id);

                        // Try to evict head if this is the head segment
                        if let Some(head) = bucket.head() {
                            if head == id {
                                // This is the head - use evict_head_segment which returns segment in Reserved state
                                if let Some(evicted_id) = bucket.evict_head_segment(self, &self.metrics) {
                                    return Some(evicted_id);
                                }
                            }
                        }
                        // Either not the head, or eviction failed - skip this segment
                        // If remove_segment or reserve failed, continue scanning
                        // Don't retry the same segment - let caller retry the append
                    }
                }
            }
        }

        // Made a full pass, no segment could be evicted
        None
    }
}

impl CacheOps for Arc<CacheCore> {
    fn segments(&self) -> &Segments {
        &self.segments
    }
    fn ttl_buckets(&self) -> &TtlBuckets {
        &self.ttl_buckets
    }
    fn hashtable(&self) -> &Hashtable {
        &self.hashtable
    }
    fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
    fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    fn try_expire_one_segment(&self) -> Option<u32> {
        (**self).try_expire_one_segment()
    }

    async fn evict_segment_by_policy(&self) -> Option<u32> {
        (**self).evict_segment_by_policy().await
    }
}

impl CacheOps for Cache {
    fn segments(&self) -> &Segments {
        &self.core.segments
    }
    fn ttl_buckets(&self) -> &TtlBuckets {
        &self.core.ttl_buckets
    }
    fn hashtable(&self) -> &Hashtable {
        &self.core.hashtable
    }
    fn metrics(&self) -> &CacheMetrics {
        &self.core.metrics
    }
    fn eviction_policy(&self) -> EvictionPolicy {
        self.core.eviction_policy
    }

    fn try_expire_one_segment(&self) -> Option<u32> {
        self.core.try_expire_one_segment()
    }

    async fn evict_segment_by_policy(&self) -> Option<u32> {
        self.core.evict_segment_by_policy().await
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_get() {
        let cache = Cache::new();

        // Set an item with TTL
        let ttl = Some(Duration::from_secs(60));
        cache.set(b"test_key", b"test_value", b"", ttl).await.unwrap();

        // Get the item
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(b"test_key", &mut buffer).unwrap();

        assert_eq!(item.key(), b"test_key");
        assert_eq!(item.value(), b"test_value");
    }

    #[tokio::test]
    async fn test_get_with_guard() {
        let cache = Cache::new();

        // Set an item with TTL
        let ttl = Some(Duration::from_secs(60));
        cache.set(b"test_key", b"test_value", b"", ttl).await.unwrap();

        // Get the item using get() which returns ItemGuard (zero-copy)
        let guard = cache.get(b"test_key").unwrap();
        assert_eq!(guard.value(), b"test_value");
        assert_eq!(guard.key(), b"test_key");
        assert_eq!(guard.optional(), b"");

        // Test that we can clone if needed
        let value_cloned = guard.value().to_vec();
        assert_eq!(value_cloned, b"test_value");
        drop(guard); // Explicitly drop to show guard lifetime

        // Test with a larger value
        let large_value = vec![0x42u8; 8192]; // 8KB value
        cache.set(b"large_key", &large_value, b"", ttl).await.unwrap();

        let guard = cache.get(b"large_key").unwrap();
        assert_eq!(guard.value(), &large_value[..]);

        // Test not found
        let result = cache.get(b"nonexistent");
        assert!(matches!(result, Err(GetItemError::NotFound)));
    }

    #[tokio::test]
    async fn test_set_without_ttl() {
        let cache = Cache::new();

        // Set an item without TTL (uses largest bucket)
        cache.set(b"no_ttl_key", b"some_value", b"", None).await.unwrap();

        // Verify we can get it back
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(b"no_ttl_key", &mut buffer).unwrap();

        assert_eq!(item.key(), b"no_ttl_key");
        assert_eq!(item.value(), b"some_value");
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let cache = Cache::new();

        let mut buffer = vec![0u8; 1024];
        let result = cache.get_with_buffer(b"nonexistent_key", &mut buffer);

        assert!(matches!(result, Err(GetItemError::NotFound)));
    }

    #[tokio::test]
    async fn test_delete() {
        let cache = Cache::new();

        // Set an item
        cache.set(b"delete_me", b"value", b"", Some(Duration::from_secs(60))).await.unwrap();

        // Verify it exists
        let mut buffer = vec![0u8; 1024];
        assert!(cache.get_with_buffer(b"delete_me", &mut buffer).is_ok());

        // Delete it
        cache.delete(b"delete_me").await.unwrap();

        // Verify it's gone
        let result = cache.get_with_buffer(b"delete_me", &mut buffer);
        assert!(matches!(result, Err(GetItemError::NotFound)));
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let cache = Cache::new();

        let result = cache.delete(b"nonexistent").await;
        assert_eq!(result, Err(DeleteItemError::NotFound));
    }

    #[tokio::test]
    async fn test_delete_already_deleted() {
        let cache = Cache::new();

        // Set and delete an item
        cache.set(b"key", b"value", b"", Some(Duration::from_secs(60))).await.unwrap();
        cache.delete(b"key").await.unwrap();

        // Try to delete again - should fail because it's not in hashtable anymore
        let result = cache.delete(b"key").await;
        assert_eq!(result, Err(DeleteItemError::NotFound));
    }

    #[tokio::test]
    async fn test_set_get_with_optional() {
        let cache = Cache::new();

        // Set an item with optional data
        let optional = b"optional_data";
        cache.set(b"key_with_opt", b"value", optional, Some(Duration::from_secs(60))).await.unwrap();

        // Get the item
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(b"key_with_opt", &mut buffer).unwrap();

        assert_eq!(item.key(), b"key_with_opt");
        assert_eq!(item.value(), b"value");
        assert_eq!(item.optional(), optional);
    }

    #[tokio::test]
    async fn test_multiple_items() {
        let cache = Cache::new();

        // Set multiple items
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.set(key.as_bytes(), value.as_bytes(), b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        // Get all items back
        for i in 0..10 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);

            let mut buffer = vec![0u8; 1024];
            let item = cache.get_with_buffer(key.as_bytes(), &mut buffer).unwrap();

            assert_eq!(item.key(), key.as_bytes());
            assert_eq!(item.value(), expected_value.as_bytes());
        }
    }

    #[tokio::test]
    async fn test_overwrite_key() {
        let cache = Cache::new();

        // Set a key
        cache.set(b"overwrite", b"value1", b"", Some(Duration::from_secs(60))).await.unwrap();

        let items_after_first = cache.metrics().items_live.value();
        let bytes_after_first = cache.metrics().bytes_live.value();

        // Overwrite it
        cache.set(b"overwrite", b"value2", b"", Some(Duration::from_secs(60))).await.unwrap();

        let items_after_overwrite = cache.metrics().items_live.value();
        let bytes_after_overwrite = cache.metrics().bytes_live.value();

        // Try to get the value
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(b"overwrite", &mut buffer).unwrap();
        let retrieved_value = item.value();

        println!(
            "Overwrite test:\n  \
             - items_live after first set: {}\n  \
             - items_live after overwrite: {}\n  \
             - bytes_live after first set: {}\n  \
             - bytes_live after overwrite: {}\n  \
             - Retrieved value: {:?}\n  \
             - Expected value: {:?}",
            items_after_first, items_after_overwrite,
            bytes_after_first, bytes_after_overwrite,
            String::from_utf8_lossy(retrieved_value),
            String::from_utf8_lossy(b"value2")
        );

        // Verify overwrite behavior is correct:
        // 1. items_live should stay the same (old item deleted, new item added)
        // 2. bytes_live should stay the same (assuming same size values)
        // 3. get() should return the new value

        assert_eq!(
            items_after_overwrite, items_after_first,
            "items_live should stay constant on overwrite"
        );

        assert_eq!(
            bytes_after_overwrite, bytes_after_first,
            "bytes_live should stay constant when overwriting with same size value"
        );

        assert_eq!(
            retrieved_value, b"value2",
            "get() should return the new value after overwrite"
        );
    }

    #[tokio::test]
    async fn test_cache_builder() {
        // Create a larger cache with custom settings
        let cache = CacheBuilder::new()
            .heap_size(128 * 1024 * 1024) // 128MB
            .segment_size(2 * 1024 * 1024) // 2MB segments
            .hashtable_power(18) // 2^18 = 262144 buckets
            .build();

        // Insert some items
        cache.set(b"test1", b"value1", b"", Some(Duration::from_secs(60))).await.unwrap();
        cache.set(b"test2", b"value2", b"", Some(Duration::from_secs(60))).await.unwrap();

        // Verify they can be retrieved
        let mut buffer = vec![0u8; 1024];
        let item1 = cache.get_with_buffer(b"test1", &mut buffer).unwrap();
        assert_eq!(item1.value(), b"value1");

        let item2 = cache.get_with_buffer(b"test2", &mut buffer).unwrap();
        assert_eq!(item2.value(), b"value2");
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache = Cache::new();

        let key = b"test_key";
        let value = b"test_value";
        let short_ttl = Duration::from_secs(1);

        // Set item with 1 second TTL
        cache.set(key, value, &[], Some(short_ttl)).await.unwrap();

        // Should be able to get it immediately
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(key, &mut buffer).unwrap();
        assert_eq!(item.value(), value);

        // Check initial expiration state
        if let Some((seg_id, _offset)) = cache.hashtable().get(key, &cache.segments()) {
            if let Some(segment) = cache.segments().get(seg_id) {
                let now = clocksource::coarse::Instant::now();
                let expire_at = segment.expire_at();
                // Should not be expired yet
                assert!(now < expire_at, "Item should not be expired immediately after insert");
            }
        }

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Should not be able to get it after expiration
        let result = cache.get_with_buffer(key, &mut buffer);
        assert!(result.is_err(), "Item should have expired");

        // Verify expiration metric was incremented
        assert!(cache.metrics().item_expire.value() > 0, "item_expire metric should be incremented");
    }

    #[tokio::test]
    async fn test_builder_larger_cache() {
        // Create a 256MB cache to verify it can store more data
        let cache = CacheBuilder::new()
            .heap_size(256 * 1024 * 1024) // 256MB
            .segment_size(1024 * 1024) // 1MB segments
            .build();

        let large_value = vec![b'x'; 100 * 1024]; // 100KB
        let initial_evictions = cache.metrics().segment_evict.value();

        // Insert enough items to fill beyond the default 64MB cache
        // but within the 256MB cache (should not evict as much)
        let items_to_insert = 2000;

        for i in 0..items_to_insert {
            let key = format!("big_key_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let final_evictions = cache.metrics().segment_evict.value();
        let eviction_count = final_evictions - initial_evictions;

        println!("256MB cache evictions: {}", eviction_count);

        // With 256MB (256 segments @ 1MB each), inserting 2000 items should
        // trigger evictions, but fewer than a 64MB cache would
        // We expect roughly: 256 segments * 10 items/segment = 2560 items capacity
        // So 2000 items should fit with minimal eviction
        assert!(eviction_count < 100, "Should have relatively few evictions with larger cache");
    }

    #[tokio::test]
    async fn test_cache_fills_and_evicts() {
        let cache = Cache::new();

        // Fill cache with large items to trigger eviction
        // Use 100KB values to fill segments faster (1MB segments / 100KB = ~10 items per segment)
        // With 64 segments, we should be able to store ~640 items before needing eviction
        let large_value = vec![b'x'; 100 * 1024]; // 100KB

        let initial_segments_free = cache.metrics().segments_free.value();
        let initial_evictions = cache.metrics().segment_evict.value();

        let mut success_count = 0;
        let items_to_insert = 1000; // More than cache capacity

        for i in 0..items_to_insert {
            let key = format!("large_key_{}", i);

            match cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))) .await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    println!("Failed at iteration {} with error: {:?}", i, e);
                    break;
                }
            }
        }

        let final_evictions = cache.metrics().segment_evict.value();
        let eviction_count = final_evictions - initial_evictions;

        println!("Inserted {} items", success_count);
        println!("Evictions: {}", eviction_count);
        println!("Initial free segments: {}", initial_segments_free);
        println!("Final free segments: {}", cache.metrics().segments_free.value());

        // Should have successfully inserted all items
        assert_eq!(success_count, items_to_insert, "Should insert all {} items", items_to_insert);

        // Should have triggered eviction since we inserted more than capacity
        assert!(eviction_count > 0, "Should have triggered at least one eviction");

        // Verify we can still read items (at least recent ones should be available)
        let mut buffer = vec![0u8; 200 * 1024]; // Buffer large enough for items
        let recent_key = format!("large_key_{}", items_to_insert - 1);
        let item = cache.get_with_buffer(recent_key.as_bytes(), &mut buffer)
            .expect("Should be able to read most recent item");

        assert_eq!(item.value().len(), large_value.len());
    }

    #[tokio::test]
    async fn test_segment_state_transitions() {
        // Test the full segment lifecycle through normal cache operations
        let cache = Cache::new();

        // Initially all 64 segments should be free
        let initial_free = cache.metrics().segments_free.value();
        assert_eq!(initial_free, 64, "All segments should start free");
        assert_eq!(cache.metrics().segments_live.value(), 0, "No live segments initially");
        assert_eq!(cache.metrics().segments_sealed.value(), 0, "No sealed segments initially");

        // Reserve a segment by triggering allocation
        cache.set(b"trigger_alloc", b"value", b"", Some(Duration::from_secs(60))).await.unwrap();

        // Verify segment was reserved (Free → Reserved → Linking → Live)
        assert_eq!(cache.metrics().segment_reserve.value(), 1);
        assert_eq!(cache.metrics().segments_free.value(), 63, "One segment should be reserved");
        assert_eq!(cache.metrics().segments_live.value(), 1, "Should have one live segment");

        // Fill segments to trigger sealing (Live → Sealed when full)
        let large_value = vec![b'x'; 500 * 1024]; // 500KB
        for i in 0..3 {
            let key = format!("fill_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        // Multiple segments should now be allocated
        assert!(cache.metrics().segment_reserve.value() >= 2,
            "Should have reserved multiple segments after filling");

        // Verify we have both live (current tail) and sealed segments
        let live_count = cache.metrics().segments_live.value();
        let sealed_count = cache.metrics().segments_sealed.value();
        assert!(live_count >= 1, "Should have at least one live segment (tail)");
        assert!(sealed_count >= 1, "Should have sealed segments after filling");

        // Trigger eviction by filling cache
        let eviction_start = cache.metrics().segment_evict.value();
        for i in 0..200 {
            let key = format!("evict_trigger_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        // Verify eviction happened (segment went through Draining → Locked → Reserved)
        let evictions = cache.metrics().segment_evict.value() - eviction_start;
        assert!(evictions > 0, "Should have evicted segments");

        // Verify segment_clear was called during eviction
        assert!(cache.metrics().segment_clear.value() > 0, "Should have cleared segments");

        // State gauges should remain consistent
        let final_free = cache.metrics().segments_free.value();
        let final_live = cache.metrics().segments_live.value();
        let final_sealed = cache.metrics().segments_sealed.value();

        println!("Final state: free={}, live={}, sealed={}", final_free, final_live, final_sealed);

        // All segments should be accounted for (free + live + sealed + other states = 64)
        // Note: some segments might be in transient states (Reserved, Linking, etc.)
        assert!(final_free >= 0, "Free count should be non-negative");
        assert!(final_live >= 1, "Should have at least one live tail segment");
    }

    #[tokio::test]
    async fn test_ttl_bucket_chaining() {
        // Test that multiple segments can chain in a single TTL bucket
        let cache = Cache::new();

        // Use large items to force multiple segments in the same bucket
        let large_value = vec![b'x'; 800 * 1024]; // 800KB per item

        // Insert items with same TTL - should go to same bucket
        // Each segment is 1MB, so ~1 item per segment
        let num_items = 5;
        for i in 0..num_items {
            let key = format!("chain_key_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        // Should have multiple segments reserved and appended to bucket
        let segments_reserved = cache.metrics().segment_reserve.value();
        assert!(segments_reserved >= num_items,
            "Should reserve at least {} segments, got {}", num_items, segments_reserved);

        // Verify we can read all items (chain integrity maintained)
        let mut buffer = vec![0u8; 1024 * 1024];
        for i in 0..num_items {
            let key = format!("chain_key_{}", i);
            let item = cache.get_with_buffer(key.as_bytes(), &mut buffer)
                .expect(&format!("Should be able to read item {}", i));
            assert_eq!(item.value().len(), large_value.len());
        }

        // Verify segments were appended to bucket (chained)
        let ttl_appends = cache.metrics().ttl_append_segment.value();
        assert!(ttl_appends >= num_items as u64,
            "Should have appended at least {} segments to bucket, got {}",
            num_items, ttl_appends);
    }

    #[tokio::test]
    async fn test_ttl_bucket_chain_eviction() {
        // Test evicting from a chain with multiple segments
        let cache = CacheBuilder::new()
            .heap_size(10 * 1024 * 1024) // Small 10MB cache
            .segment_size(1024 * 1024) // 1MB segments = 10 segments total
            .build();

        let large_value = vec![b'x'; 800 * 1024]; // 800KB

        // Fill the cache with a chain of segments in one bucket
        let initial_evictions = cache.metrics().segment_evict.value();
        let num_items = 20; // More than capacity, will trigger eviction

        for i in 0..num_items {
            let key = format!("evict_chain_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60)))
                .await.expect(&format!("Failed to set item {}", i));
        }

        let evictions = cache.metrics().segment_evict.value() - initial_evictions;

        // With 10 segments and 20 items (~1 per segment), we should evict ~10 times
        assert!(evictions >= 10,
            "Should have evicted at least 10 segments from chain, got {}", evictions);

        // Verify recent items are still accessible
        let mut buffer = vec![0u8; 1024 * 1024];
        for i in (num_items - 5)..num_items {
            let key = format!("evict_chain_{}", i);
            cache.get_with_buffer(key.as_bytes(), &mut buffer)
                .expect(&format!("Recent item {} should be accessible", i));
        }

        // Old items should be evicted
        let old_key = b"evict_chain_0";
        let result = cache.get_with_buffer(old_key, &mut buffer);
        assert!(matches!(result, Err(GetItemError::NotFound)),
            "Old items should be evicted");
    }

    #[tokio::test]
    async fn test_segment_chain_integrity() {
        // Test that segment chains maintain proper next/prev pointers
        let cache = Cache::new();

        let value = vec![b'x'; 900 * 1024]; // Force multiple segments

        // Create a chain of 3 segments in the same bucket
        for i in 0..3 {
            let key = format!("chain_{}", i);
            cache.set(key.as_bytes(), &value, b"", Some(Duration::from_secs(100))).await.unwrap();
        }

        // All items should be retrievable (proves chain is intact)
        let mut buffer = vec![0u8; 1024 * 1024];
        for i in 0..3 {
            let key = format!("chain_{}", i);
            assert!(cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok(),
                "Item {} should be in chain", i);
        }

        // Add more items to trigger sealing and new segment appending
        for i in 3..6 {
            let key = format!("chain_{}", i);
            cache.set(key.as_bytes(), &value, b"", Some(Duration::from_secs(100))).await.unwrap();
        }

        // All items should still be retrievable
        for i in 0..6 {
            let key = format!("chain_{}", i);
            assert!(cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok(),
                "Item {} should be in extended chain", i);
        }

        // Verify metrics show proper chaining
        let ttl_appends = cache.metrics().ttl_append_segment.value();
        assert!(ttl_appends >= 6,
            "Should have appended at least 6 segments to bucket, got {}", ttl_appends);
    }

    #[tokio::test]
    async fn test_segment_reuse_after_eviction() {
        // Test that evicted segments are properly cleaned and reused
        let cache = CacheBuilder::new()
            .heap_size(5 * 1024 * 1024) // 5MB = 5 segments
            .build();

        let value = vec![b'x'; 800 * 1024];

        // Fill all segments
        for i in 0..5 {
            let key = format!("fill_{}", i);
            cache.set(key.as_bytes(), &value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let segments_after_fill = cache.metrics().segment_reserve.value();
        assert_eq!(segments_after_fill, 5, "Should have reserved all 5 segments");

        // Trigger eviction and reuse
        let evict_start = cache.metrics().segment_evict.value();
        for i in 5..10 {
            let key = format!("fill_{}", i);
            cache.set(key.as_bytes(), &value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let evictions = cache.metrics().segment_evict.value() - evict_start;
        assert!(evictions >= 5, "Should have evicted at least 5 segments");

        // Should not have reserved more segments (reused evicted ones)
        let segments_after_reuse = cache.metrics().segment_reserve.value();
        // Might reserve a few more due to concurrency, but not 5 more
        assert!(segments_after_reuse < segments_after_fill + 3,
            "Should reuse evicted segments, not reserve many new ones. Reserved: {}",
            segments_after_reuse);

        // Verify old items are gone
        let mut buffer = vec![0u8; 1024 * 1024];
        for i in 0..5 {
            let key = format!("fill_{}", i);
            let result = cache.get_with_buffer(key.as_bytes(), &mut buffer);
            assert!(matches!(result, Err(GetItemError::NotFound)),
                "Old item {} should be evicted", i);
        }

        // Verify new items are accessible
        for i in 5..10 {
            let key = format!("fill_{}", i);
            assert!(cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok(),
                "New item {} should be accessible", i);
        }
    }

    #[tokio::test]
    async fn test_cache_wide_item_gauges() {
        // Test that items_live and bytes_live track correctly
        let cache = Cache::new();

        // Initially should be zero
        assert_eq!(cache.metrics().items_live.value(), 0, "Should start with 0 items");
        assert_eq!(cache.metrics().bytes_live.value(), 0, "Should start with 0 bytes");

        // Add first item
        cache.set(b"key1", b"value1", b"", Some(Duration::from_secs(60))).await.unwrap();
        assert_eq!(cache.metrics().items_live.value(), 1, "Should have 1 item");
        let bytes_after_1 = cache.metrics().bytes_live.value();
        assert!(bytes_after_1 > 0, "Should have non-zero bytes");

        // Add second item
        cache.set(b"key2", b"value2_longer", b"", Some(Duration::from_secs(60))).await.unwrap();
        assert_eq!(cache.metrics().items_live.value(), 2, "Should have 2 items");
        let bytes_after_2 = cache.metrics().bytes_live.value();
        assert!(bytes_after_2 > bytes_after_1, "Bytes should increase");

        // Delete first item
        cache.delete(b"key1").await.unwrap();
        assert_eq!(cache.metrics().items_live.value(), 1, "Should have 1 item after delete");
        let bytes_after_delete = cache.metrics().bytes_live.value();
        assert!(bytes_after_delete < bytes_after_2, "Bytes should decrease after delete");
        assert!(bytes_after_delete > 0, "Should still have bytes for remaining item");

        // Delete second item
        cache.delete(b"key2").await.unwrap();
        assert_eq!(cache.metrics().items_live.value(), 0, "Should have 0 items after deleting all");
        assert_eq!(cache.metrics().bytes_live.value(), 0, "Should have 0 bytes after deleting all");
    }

    #[tokio::test]
    async fn test_operation_counters() {
        // Test that operation counters increment correctly
        let cache = Cache::new();

        let initial_append = cache.metrics().item_append.value();
        let initial_unlink = cache.metrics().item_unlink.value();
        let _initial_unlink_not_found = cache.metrics().item_unlink_not_found.value();

        // Set items - should increment item_append
        for i in 0..5 {
            let key = format!("counter_key_{}", i);
            cache.set(key.as_bytes(), b"value", b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        assert_eq!(cache.metrics().item_append.value(), initial_append + 5,
            "Should have appended 5 items");

        // Delete 3 items - should increment item_unlink
        for i in 0..3 {
            let key = format!("counter_key_{}", i);
            cache.delete(key.as_bytes()).await.unwrap();
        }

        assert_eq!(cache.metrics().item_unlink.value(), initial_unlink + 3,
            "Should have unlinked 3 items");

        // Try to delete non-existent item - should increment item_unlink_not_found
        let result = cache.delete(b"nonexistent").await;
        assert!(matches!(result, Err(DeleteItemError::NotFound)));

        // Note: item_unlink_not_found is incremented in hashtable.unlink_item()
        // when the item isn't found in the hashtable, not in Cache::delete()
        // So we need to verify it through the full eviction/clearing path

        // Verify final state
        assert_eq!(cache.metrics().items_live.value(), 2, "Should have 2 items remaining");
    }

    #[tokio::test]
    async fn test_item_gauges_with_eviction() {
        // Test that gauges remain accurate during eviction
        let cache = CacheBuilder::new()
            .heap_size(5 * 1024 * 1024) // 5MB = 5 segments
            .build();

        let large_value = vec![b'x'; 800 * 1024]; // 800KB

        // Fill all segments
        for i in 0..5 {
            let key = format!("fill_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let items_before_eviction = cache.metrics().items_live.value();
        let bytes_before_eviction = cache.metrics().bytes_live.value();
        assert_eq!(items_before_eviction, 5, "Should have 5 items before eviction");
        assert!(bytes_before_eviction > 0, "Should have bytes before eviction");

        // Trigger eviction
        for i in 5..8 {
            let key = format!("fill_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        // During eviction, old items are removed (gauges decremented)
        // and new items are added (gauges incremented)
        // We should still have items, but count may vary depending on eviction
        let items_after_eviction = cache.metrics().items_live.value();
        let bytes_after_eviction = cache.metrics().bytes_live.value();

        // Should have at least some items remaining (recent ones)
        assert!(items_after_eviction > 0, "Should have items after eviction");
        assert!(items_after_eviction <= 8, "Should not exceed total items inserted");
        assert!(bytes_after_eviction > 0, "Should have bytes after eviction");

        println!("Items: before={}, after={}", items_before_eviction, items_after_eviction);
        println!("Bytes: before={}, after={}", bytes_before_eviction, bytes_after_eviction);
    }

    #[tokio::test]
    async fn test_append_counter_with_full_segment() {
        // Test item_append counter increments even when segment fills
        let cache = Cache::new();

        let initial_append = cache.metrics().item_append.value();
        let _initial_append_full = cache.metrics().item_append_full.value();

        // Insert many items to fill segments
        let large_value = vec![b'y'; 500 * 1024]; // 500KB
        for i in 0..10 {
            let key = format!("large_{}", i);
            cache.set(key.as_bytes(), &large_value, b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let final_append = cache.metrics().item_append.value();
        assert_eq!(final_append, initial_append + 10,
            "Should have incremented item_append for all items");

        // item_append_full is incremented when append_item returns None due to full segment
        // This happens internally in the TTL bucket retry logic, hard to test directly here
        // but we've verified it increments during segment operations
    }

    #[tokio::test]
    async fn test_ttl_bucket_assignment() {
        let cache = Cache::new();

        // Items with different TTLs should go to different buckets
        // TTL buckets are organized in ranges: [0-256s], [257-512s], etc.

        // Short TTL (60 seconds)
        cache.set(b"short_ttl", b"value1", b"", Some(Duration::from_secs(60))).await.unwrap();

        // Medium TTL (300 seconds = 5 minutes)
        cache.set(b"medium_ttl", b"value2", b"", Some(Duration::from_secs(300))).await.unwrap();

        // Long TTL (3600 seconds = 1 hour)
        cache.set(b"long_ttl", b"value3", b"", Some(Duration::from_secs(3600))).await.unwrap();

        // Verify all items are accessible
        let mut buffer = [0u8; 256];

        let item1 = cache.get_with_buffer(b"short_ttl", &mut buffer).unwrap();
        assert_eq!(item1.value(), b"value1");

        let item2 = cache.get_with_buffer(b"medium_ttl", &mut buffer).unwrap();
        assert_eq!(item2.value(), b"value2");

        let item3 = cache.get_with_buffer(b"long_ttl", &mut buffer).unwrap();
        assert_eq!(item3.value(), b"value3");

        // Verify they went to different TTL buckets by checking segment distribution
        // Items in the same TTL bucket will often (but not always) share segments
        let (seg1, _) = cache.hashtable().get(b"short_ttl", &cache.segments()).unwrap();
        let (seg2, _) = cache.hashtable().get(b"medium_ttl", &cache.segments()).unwrap();
        let (seg3, _) = cache.hashtable().get(b"long_ttl", &cache.segments()).unwrap();

        // With a fresh cache and distinct TTL ranges, these should be in different segments
        assert_ne!(seg1, seg2, "Short and medium TTL should use different segments");
        assert_ne!(seg2, seg3, "Medium and long TTL should use different segments");
        assert_ne!(seg1, seg3, "Short and long TTL should use different segments");
    }

    #[tokio::test]
    async fn test_ttl_based_eviction_order() {
        // Create a small cache that will force eviction
        // This test verifies that TTL buckets work correctly during eviction
        let cache = CacheBuilder::new()
            .heap_size(5 * 1024 * 1024) // 5MB = 5 segments
            .segment_size(1 * 1024 * 1024) // 1MB segments
            .build();

        let initial_evictions = cache.metrics().segment_evict.value();

        // Fill cache with items using same TTL to maximize segment usage in one bucket
        // Each 500KB item means ~2 items per segment
        for i in 0..12 {
            let key = format!("item_{}", i);
            cache.set(
                key.as_bytes(),
                &[0u8; 500 * 1024], // 500KB items
                b"",
                Some(Duration::from_secs(60)) // Same TTL to use same bucket
            ).await.unwrap();
        }

        // Verify eviction happened (we added ~6MB of data to a 5MB cache)
        let evictions = cache.metrics().segment_evict.value() - initial_evictions;
        assert!(
            evictions > 0,
            "Should have evicted segments: evict_count={}, segments_free={}, segments_live={}, segments_sealed={}",
            evictions,
            cache.metrics().segments_free.value(),
            cache.metrics().segments_live.value(),
            cache.metrics().segments_sealed.value()
        );

        // Verify that some items survived and cache is still functional
        let mut buffer = [0u8; 600 * 1024];
        let survivors = (0..12)
            .filter(|&i| {
                let key = format!("item_{}", i);
                cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok()
            })
            .count();

        println!("Eviction test: added 12 items, {} survived, {} evicted",
                 survivors, 12 - survivors);

        // At least some items should survive (we can't fit all 12 in a 5MB cache)
        assert!(
            survivors > 0 && survivors < 12,
            "Some but not all items should survive: survivors={}",
            survivors
        );
    }

    #[tokio::test]
    async fn test_mixed_ttl_segments() {
        let cache = Cache::new();

        // Add items with various TTLs to verify they can coexist
        let ttls = [
            10,    // 10 seconds
            60,    // 1 minute
            300,   // 5 minutes
            600,   // 10 minutes
            1800,  // 30 minutes
            3600,  // 1 hour
            7200,  // 2 hours
            86400, // 1 day
        ];

        for (i, &ttl) in ttls.iter().enumerate() {
            let key = format!("key_{}", i);
            let value = format!("value_with_ttl_{}", ttl);
            cache.set(
                key.as_bytes(),
                value.as_bytes(),
                b"",
                Some(Duration::from_secs(ttl))
            ).await.unwrap();
        }

        // Verify all items are accessible
        let mut buffer = [0u8; 256];
        for (i, &ttl) in ttls.iter().enumerate() {
            let key = format!("key_{}", i);
            let expected_value = format!("value_with_ttl_{}", ttl);

            let item = cache.get_with_buffer(key.as_bytes(), &mut buffer).unwrap();
            assert_eq!(
                item.value(),
                expected_value.as_bytes(),
                "Item with TTL {} should be accessible",
                ttl
            );
        }

        // Verify segments are allocated across different TTL buckets
        let segments_live = cache.metrics().segments_live.value();
        assert!(
            segments_live >= 1,
            "Should have at least 1 live segment, got {}",
            segments_live
        );
    }

    #[tokio::test]
    async fn test_no_ttl_uses_largest_bucket() {
        let cache = Cache::new();

        // Items without TTL should use the largest TTL bucket
        cache.set(b"no_ttl_1", b"value1", b"", None).await.unwrap();
        cache.set(b"no_ttl_2", b"value2", b"", None).await.unwrap();

        // Items with explicit long TTL
        cache.set(
            b"explicit_long",
            b"value3",
            b"",
            Some(Duration::from_secs(86400)) // 1 day
        ).await.unwrap();

        // Verify all are accessible
        let mut buffer = [0u8; 256];

        cache.get_with_buffer(b"no_ttl_1", &mut buffer).unwrap();
        cache.get_with_buffer(b"no_ttl_2", &mut buffer).unwrap();
        cache.get_with_buffer(b"explicit_long", &mut buffer).unwrap();

        // Items without TTL might share segments with each other
        let (seg1, _) = cache.hashtable().get(b"no_ttl_1", &cache.segments()).unwrap();
        let (seg2, _) = cache.hashtable().get(b"no_ttl_2", &cache.segments()).unwrap();

        // With fresh cache, both no-TTL items should be in same segment
        assert_eq!(
            seg1, seg2,
            "Items without TTL should use the same segment when added consecutively"
        );
    }

    #[tokio::test]
    async fn test_ttl_bucket_fills_and_seals() {
        let cache = CacheBuilder::new()
            .segment_size(256 * 1024) // Small segments to force sealing
            .build();

        // Fill a single TTL bucket with large items to force segment sealing
        let large_value = vec![0u8; 50 * 1024]; // 50KB items

        for i in 0..10 {
            let key = format!("item_{}", i);
            cache.set(
                key.as_bytes(),
                &large_value,
                b"",
                Some(Duration::from_secs(60)) // All same TTL
            ).await.unwrap();
        }

        // Should have sealed at least one segment
        let sealed = cache.metrics().segments_sealed.value();
        assert!(
            sealed > 0,
            "Should have sealed segments after filling: sealed={}, live={}",
            sealed,
            cache.metrics().segments_live.value()
        );

        // Verify all items are still accessible
        let mut buffer = [0u8; 128 * 1024];
        for i in 0..10 {
            let key = format!("item_{}", i);
            let result = cache.get_with_buffer(key.as_bytes(), &mut buffer);
            assert!(
                result.is_ok(),
                "Item {} should be accessible",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_hashtable_link_metric() {
        let cache = Cache::new();

        let initial_links = cache.metrics().hashtable_link.value();

        // Add several items
        cache.set(b"key1", b"value1", b"", Some(Duration::from_secs(60))).await.unwrap();
        cache.set(b"key2", b"value2", b"", Some(Duration::from_secs(60))).await.unwrap();
        cache.set(b"key3", b"value3", b"", Some(Duration::from_secs(60))).await.unwrap();

        // Each successful set should increment hashtable_link
        let links = cache.metrics().hashtable_link.value() - initial_links;
        assert_eq!(
            links, 3,
            "Should have incremented hashtable_link for each item"
        );
    }

    #[tokio::test]
    async fn test_hashtable_bucket_fills() {
        // Create a cache with minimal hashtable (16 buckets)
        let cache = CacheBuilder::new()
            .hashtable_power(4) // 2^4 = 16 buckets
            .build();

        let initial_links = cache.metrics().hashtable_link.value();

        // Try to add more items than hashtable capacity
        let mut successful = 0;
        let mut failed = 0;
        for i in 0..20 {
            let key = format!("bucket_{:03}", i);
            match cache.set(
                key.as_bytes(),
                b"value",
                b"",
                Some(Duration::from_secs(60))
            ) .await {
                Ok(_) => successful += 1,
                Err(_) => failed += 1,
            }
        }

        let links = cache.metrics().hashtable_link.value() - initial_links;

        // Some items should have been linked successfully
        assert!(links > 0, "Some items should have been linked");
        assert_eq!(links, successful as u64, "Links should match successful inserts");

        // Verify all successful items are accessible
        let mut buffer = [0u8; 128];
        let mut accessible_count = 0;
        for i in 0..20 {
            let key = format!("bucket_{:03}", i);
            if cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok() {
                accessible_count += 1;
            }
        }

        println!(
            "Bucket fill test: {} links, {} successful, {} failed, {} accessible",
            links, successful, failed, accessible_count
        );

        // All successfully inserted items should be accessible
        assert_eq!(
            accessible_count, successful,
            "All successfully inserted items should be accessible"
        );

        // No evictions should have occurred
        assert_eq!(cache.metrics().hashtable_evict.value(), 0, "No evictions should occur");
    }

    #[tokio::test]
    async fn test_hashtable_metrics_comprehensive() {
        // Test hashtable metrics with link success and failure
        let cache = CacheBuilder::new()
            .hashtable_power(5) // 2^5 = 32 buckets = 224 capacity
            .build();

        let initial_links = cache.metrics().hashtable_link.value();

        // Phase 1: Add items (should increment links)
        for i in 0..50 {
            let key = format!("item_{}", i);
            cache.set(key.as_bytes(), b"value", b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let phase1_links = cache.metrics().hashtable_link.value() - initial_links;
        assert_eq!(phase1_links, 50, "Should have 50 links after adding 50 items");

        // Phase 2: Overwrite some items (should increment links again)
        for i in 0..10 {
            let key = format!("item_{}", i);
            cache.set(key.as_bytes(), b"new_value", b"", Some(Duration::from_secs(60))).await.unwrap();
        }

        let phase2_links = cache.metrics().hashtable_link.value() - initial_links;
        assert_eq!(
            phase2_links, 60,
            "Should have 60 total links after overwrites"
        );

        // Phase 3: Try to add many more items - some will fail when hashtable fills
        let mut successful = 0;
        let mut failed = 0;
        for i in 50..300 {
            let key = format!("item_{}", i);
            match cache.set(key.as_bytes(), b"value", b"", Some(Duration::from_secs(60))) .await {
                Ok(_) => successful += 1,
                Err(_) => failed += 1,
            }
        }

        let final_links = cache.metrics().hashtable_link.value() - initial_links;

        println!(
            "Comprehensive hashtable test: {} total links, {} successful in phase 3, {} failed",
            final_links, successful, failed
        );

        // With 32 buckets * 7 slots = 224 total hashtable capacity
        // We attempted 310 total inserts (50 + 10 + 250)
        // So we should see some failures
        assert!(
            failed > 0,
            "Should have failed inserts when exceeding hashtable capacity"
        );

        // No evictions should have occurred
        assert_eq!(
            cache.metrics().hashtable_evict.value(), 0,
            "Should not have any hashtable evictions"
        );
    }

    #[tokio::test]
    async fn test_hashtable_full_rejects_inserts() {
        // This test verifies that when hashtable buckets fill up, inserts fail
        // instead of silently evicting items

        let cache = CacheBuilder::new()
            .hashtable_power(4) // 16 buckets = 112 total hashtable slots
            .build();

        let mut successful = 0;
        let mut failed = 0;

        // Try to add 200 items - some will fail when buckets fill
        for i in 0..200 {
            let key = format!("item_{}", i);
            match cache.set(key.as_bytes(), b"value_data", b"", Some(Duration::from_secs(60))) .await {
                Ok(_) => successful += 1,
                Err(_) => failed += 1,
            }
        }

        println!(
            "Hashtable capacity test:\n  \
             - Attempted: 200 inserts\n  \
             - Successful: {}\n  \
             - Failed: {}",
            successful, failed
        );

        // With 16 buckets * 7 slots = 112 capacity, we should fail some inserts
        assert!(
            failed > 0,
            "Should have failed inserts when hashtable buckets filled"
        );

        assert!(
            successful <= 112,
            "Should not successfully insert more than hashtable capacity"
        );

        // items_live should match successful inserts (no silent evictions)
        let items_live = cache.metrics().items_live.value();
        assert_eq!(
            items_live, successful as i64,
            "items_live should equal successful inserts (no evictions)"
        );

        // All successfully inserted items should be accessible
        let mut buffer = [0u8; 128];
        let mut accessible = 0;
        for i in 0..200 {
            let key = format!("item_{}", i);
            if cache.get_with_buffer(key.as_bytes(), &mut buffer).is_ok() {
                accessible += 1;
            }
        }

        assert_eq!(
            accessible, successful,
            "All successfully inserted items should be accessible"
        );

        // Verify no evictions happened
        assert_eq!(
            cache.metrics().hashtable_evict.value(), 0,
            "Should not have any hashtable evictions"
        );

        println!(
            "  - items_live: {}\n  \
             - Accessible: {}\n  \
             - No evictions occurred",
            items_live, accessible
        );
    }
}
