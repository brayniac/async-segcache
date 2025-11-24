use crate::*;
use crate::util::*;
use clocksource::coarse::AtomicInstant;
use std::alloc::{Layout, alloc_zeroed};
use std::ptr::NonNull;
use crate::sync::*;

const MB: usize = 1024 * 1024;

const DEFAULT_SEGMENT_SIZE: usize = 1 * MB;
const DEFAULT_HEAP_SIZE: usize = 64 * MB;

#[repr(C, align(64))]
pub struct Segments {
    pub segments: Vec<Segment<'static>>,
    heap_ptr: *mut u8,
    heap_layout: Layout,
    segment_size: usize,

    free_queue: crossbeam_deque::Injector<u32>,
}

impl Segments {
    /// Get a reference to a segment by ID
    /// Returns None if the ID is out of bounds
    pub fn get(&self, id: u32) -> Option<&Segment<'_>> {
        self.segments.get(id as usize)
    }

    /// Reserve a segment from the free queue.
    ///
    /// Returns the segment ID if one is available, or None if all segments are in use.
    /// The segment's statistics are reset upon reservation.
    ///
    /// # Loom Test Coverage
    /// - `single_segment_reserve_metrics` - Single-threaded reserve with metric validation
    /// - `concurrent_segment_reserve_and_release` - Two threads reserving different segments
    /// - `concurrent_ttl_bucket_append` - Reserve called as part of TTL bucket operations
    pub fn reserve(&self, metrics: &crate::metrics::CacheMetrics) -> Option<u32> {
        // Try to steal a segment ID from the free queue
        match self.free_queue.steal() {
            crossbeam_deque::Steal::Success(segment_id) => {
                let segment = &self.segments[segment_id as usize];

                // Load current state and transition from Free to Reserved atomically
                let current_packed = segment.packed_meta.load(Ordering::Acquire);
                let current_meta = PackedSegmentMeta::unpack(current_packed);

                // Prepare new state: Reserved with no links
                let new_meta = PackedSegmentMeta {
                    next: INVALID_SEGMENT_ID, // Clear links when reserving
                    prev: INVALID_SEGMENT_ID,
                    state: SegmentState::Reserved,
                };

                match segment.packed_meta.compare_exchange(
                    current_packed,
                    new_meta.pack(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Successfully reserved! Reset segment statistics
                        // All use Relaxed since state transition provides synchronization
                        segment.write_offset.store(0, Ordering::Relaxed);
                        segment.live_items.store(0, Ordering::Relaxed);
                        segment.live_bytes.store(0, Ordering::Relaxed);
                        segment
                            .expire_at
                            .store(clocksource::coarse::Instant::now(), Ordering::Relaxed);

                        metrics.segment_reserve.increment();
                        // Update state gauge: Free → Reserved (decrement free)
                        metrics.segments_free.decrement();
                        Some(segment_id)
                    }
                    Err(_) => {
                        // Segment state changed between load and CAS
                        // This indicates the segment was modified while in the free queue,
                        // which is a serious bug (segments in free queue should be untouched)
                        panic!(
                            "Segment {} state changed during reservation (loaded state: {:?}, expected Free)",
                            segment_id, current_meta.state
                        );
                    }
                }
            }
            crossbeam_deque::Steal::Empty => None, // No segments available
            crossbeam_deque::Steal::Retry => {
                // Transient contention, could retry but let's keep it simple
                None
            }
        }
    }

    /// Release a segment back to the free queue for reuse.
    ///
    /// # Panics
    /// Panics if the segment is not in Reserved or Linking state, or if the segment ID is invalid.
    ///
    /// # Note
    /// The caller should ensure the segment is no longer referenced before releasing.
    /// Attempting to release a segment in Free state (double-release) will panic.
    ///
    /// # Loom Test Coverage
    /// - `single_segment_reserve_metrics` - Single-threaded release with metric validation
    /// - `concurrent_segment_reserve_and_release` - Two threads releasing different segments
    pub fn release(&self, id: u32, metrics: &crate::metrics::CacheMetrics) {
        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            panic!("Invalid segment ID: {id}");
        }

        let segment = &self.segments[id_usize];

        // Atomically transition from Reserved/Linking to Free using CAS loop
        // This prevents double-release by ensuring only one thread can transition to Free
        loop {
            let current_packed = segment.packed_meta.load(Ordering::Acquire);
            let current_meta = PackedSegmentMeta::unpack(current_packed);

            // Check current state - only allow release from Reserved or Linking
            match current_meta.state {
                SegmentState::Reserved | SegmentState::Linking => {
                    // Valid states for release - proceed with CAS
                }
                SegmentState::Free => {
                    // Already Free - another thread released it (can happen with concurrent clear())
                    // This is idempotent, just return early
                    return;
                }
                _ => {
                    // Segment in unexpected state (Live, Sealed, Draining, Locked)
                    // This indicates a serious bug - releasing an active segment
                    panic!(
                        "Attempt to release segment {id} in invalid state {:?} - this indicates a serious bug",
                        current_meta.state
                    );
                }
            }

            // Transition to Free state with no links (will be in queue)
            let new_meta = PackedSegmentMeta {
                next: INVALID_SEGMENT_ID,
                prev: INVALID_SEGMENT_ID,
                state: SegmentState::Free,
            };

            // Use CAS to atomically transition state
            match segment.packed_meta.compare_exchange(
                current_packed,
                new_meta.pack(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully transitioned to Free state
                    metrics.segment_release.increment();
                    // Update state gauge: Reserved/Linking → Free (increment free)
                    metrics.segments_free.increment();
                    break;
                }
                Err(_actual) => {
                    // State changed between load and CAS - retry the loop
                    // This handles the race where another thread modified the state
                    continue;
                }
            }
        }

        // Add segment ID back to the free queue
        // Since crossbeam-deque is lock-free, this is safe to call concurrently
        self.free_queue.push(id);
    }

    /// Clear a segment and prepare it for reuse without adding to free queue.
    ///
    /// Used by the eviction path where the segment is already in Locked state
    /// and has been unlinked from chains.
    ///
    /// Unlike `clear()`, does NOT handle concurrent calls - the caller must ensure
    /// exclusive access by transitioning the segment to Locked state first.
    ///
    /// # Returns
    /// - `true` if segment was successfully cleared and is now in Reserved state
    /// - `false` if segment is not in Locked state
    ///
    /// # Panics
    /// - If the segment has chain links (must be unlinked from TTL bucket first)
    /// - If segment data is corrupted
    /// - If state transition from Locked to Reserved fails
    pub fn evict_and_clear(&self, id: u32, hashtable: &Hashtable, metrics: &crate::metrics::CacheMetrics) -> bool {
        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            return false;
        }

        let segment = &self.segments[id_usize];

        // Segment must be in Locked state (prepared by TTL bucket eviction)
        if segment.state() != SegmentState::Locked {
            return false;
        }

        // Verify segment has been unlinked from TTL bucket chains
        let next = segment.next();
        let prev = segment.prev();
        let next_cleared = next.is_none() || next == Some(INVALID_SEGMENT_ID);
        let prev_cleared = prev.is_none() || prev == Some(INVALID_SEGMENT_ID);
        if !next_cleared || !prev_cleared {
            panic!(
                "Segment {} still has chain links during evict_and_clear (next={:?}, prev={:?}) - must be unlinked first",
                id, next, prev
            );
        }

        // Clear all items in the segment, removing them from the hashtable
        let mut current_offset = 0u32;
        let write_offset = segment.write_offset.load(Ordering::Acquire);

        // Synchronize with append_item's Release fence
        fence(Ordering::Acquire);

        while current_offset < write_offset {
            if current_offset + ItemHeader::SIZE as u32 > segment.data_len {
                panic!(
                    "Segment {} corruption: item header at offset {} extends past segment",
                    id, current_offset
                );
            }

            let data_ptr = unsafe { segment.data.as_ptr().add(current_offset as usize) };
            let header = ItemHeader::from_bytes(unsafe {
                std::slice::from_raw_parts(data_ptr, ItemHeader::SIZE)
            });

            let item_size = header.padded_size() as u32;

            if current_offset + item_size > write_offset {
                panic!(
                    "Segment {} corruption: item at offset {} extends past write_offset",
                    id, current_offset
                );
            }

            if !header.is_deleted() {
                let raw_item = unsafe { std::slice::from_raw_parts(data_ptr, header.padded_size()) };
                let key_start = ItemHeader::SIZE + header.optional_len() as usize;
                let key_end = key_start + header.key_len() as usize;

                if key_end > raw_item.len() {
                    panic!(
                        "Segment {} corruption: key extends past item bounds",
                        id
                    );
                }

                let key = &raw_item[key_start..key_end];

                if hashtable.unlink_item(key, id, current_offset, metrics) {
                    metrics.item_unlink.increment();
                } else {
                    metrics.item_unlink_not_found.increment();
                }
            }

            current_offset += item_size;
        }

        // Reset segment statistics
        segment.write_offset.store(0, Ordering::Release);
        segment.live_items.store(0, Ordering::Release);
        segment.live_bytes.store(0, Ordering::Release);
        segment.expire_at.store(clocksource::coarse::Instant::now(), Ordering::Relaxed);
        segment.clear_bucket_id();

        // Transition from Locked to Reserved (ready for immediate reuse)
        assert!(
            segment.cas_metadata(SegmentState::Locked, SegmentState::Reserved, None, None, metrics),
            "Failed to transition segment {} from Locked to Reserved",
            id
        );

        metrics.segment_clear.increment();
        true
    }

    /// Clear all items from a segment and return it to the free pool.
    ///
    /// This function safely removes all items from the segment by:
    /// 1. Transitioning the segment to Draining state
    /// 2. Iterating through all items and unlinking them from the hashtable
    /// 3. Waiting for all readers to finish
    /// 4. Resetting the segment and returning it to the free pool
    ///
    /// # Safety
    ///
    /// This operation is safe because:
    /// - Items are marked as deleted before being unlinked from hashtable
    /// - Reference counting ensures no readers access the segment during cleanup
    /// - State transitions prevent new readers from entering
    ///
    /// Clear a segment by unlinking all items and resetting statistics.
    ///
    /// This is the core primitive for segment clearing that handles all concurrency safety:
    /// - Transitions segment to Locked state (via Draining if needed)
    /// - Unlinks all items from the hashtable
    /// - Resets segment statistics
    /// - Leaves segment in Locked state for caller to decide final disposition
    ///
    /// Callers should use:
    /// - `expire()` to clear and return segment to free pool
    /// - `evict()` to clear and reuse segment immediately (memory pressure)
    ///
    /// # Returns
    /// - `true` if segment was successfully cleared and is now in Locked state
    /// - `false` if segment couldn't be cleared (already being cleared by another thread)
    ///
    /// # Concurrent Safety
    /// Multiple threads can safely call this concurrently on the same segment.
    /// Only one will succeed in clearing; others will detect the race and return false.
    pub fn clear(&self, id: u32, cache: &impl CacheOps) -> bool {
        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            return false;
        }

        let segment = &self.segments[id_usize];

        // Step 1: Transition segment to Draining state
        // This prevents new readers from entering while allowing existing ones to finish
        let initial_state = segment.state();

        // Check if segment is already being cleared or is already clear
        let need_state_transition = match initial_state {
            SegmentState::Free => {
                // Segment is already cleared
                return false;
            }
            SegmentState::Draining => {
                // Another thread is already clearing - they're transitioning to Locked
                return false;
            }
            SegmentState::Locked => {
                // Segment is already in Locked state - another thread owns it
                // (either clearing it or in eviction path)
                return false;
            }
            SegmentState::Reserved => {
                // Check if segment is already empty (already cleared by another thread)
                // A cleared segment will have: write_offset=0, no links, and be in Reserved state
                if segment.write_offset.load(Ordering::Acquire) == 0
                    && segment.next() == Some(INVALID_SEGMENT_ID)
                    && segment.prev() == Some(INVALID_SEGMENT_ID) {
                    // Segment is already cleared, nothing to do
                    return false;
                }
                // Proceed with clear attempt
                true
            }
            _ => {
                // Need to transition to Locked state
                true
            }
        };

        // Transition to Draining state if needed
        if need_state_transition {
            // Use None for next/prev to preserve current values atomically
            // (avoids TOCTOU race where another thread modifies links between our loads)
            if !segment.cas_metadata(
                initial_state,
                SegmentState::Draining,
                None, // Preserve current next
                None, // Preserve current prev
                cache.metrics(),
            ) {
                // CAS failed - state changed concurrently
                // Check if segment is already empty (was cleared by another thread)
                if segment.write_offset.load(Ordering::Acquire) == 0 {
                    // Segment was cleared by another thread, don't proceed
                    return false;
                }
                // State changed to something else - another thread is handling it
                return false;
            }
        }

        // Step 2: Iterate through all items in the segment
        let mut current_offset = 0u32;
        let mut _unlinked_count = 0u32;
        let write_limit = segment.write_offset.load(Ordering::Acquire);

        while current_offset < write_limit {
            // Ensure we have space for at least the header
            if current_offset + ItemHeader::SIZE as u32 > segment.data_len {
                break;
            }

            let data_ptr = unsafe { segment.data.as_ptr().add(current_offset as usize) };
            let header = ItemHeader::from_bytes(unsafe {
                std::slice::from_raw_parts(data_ptr, ItemHeader::SIZE)
            });

            // Calculate the full item size including padding
            let item_size = header.padded_size() as u32;

            // Ensure the full item fits within the segment
            if current_offset + item_size > segment.data_len {
                break;
            }

            // Step 3: Process the item if it's not already deleted
            if !header.is_deleted() {
                // Extract the key for hashtable unlinking
                let raw_item =
                    unsafe { std::slice::from_raw_parts(data_ptr, header.padded_size()) };

                let key_start = ItemHeader::SIZE + header.optional_len() as usize;
                let key_end = key_start + header.key_len() as usize;
                let key = &raw_item[key_start..key_end];

                // First, mark the item as deleted in the segment
                // This is safe because we're in Draining state
                let flags_ptr = unsafe { data_ptr.add(4) };

                // Use the same loom-compatible approach as mark_deleted
                #[cfg(not(feature = "loom"))]
                {
                    let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
                    flags_atomic.fetch_or(0x40, Ordering::Release);
                }

                #[cfg(feature = "loom")]
                {
                    let old_val = unsafe { std::ptr::read_volatile(flags_ptr) };
                    if (old_val & 0x40) == 0 {
                        fence(Ordering::Release);
                        unsafe { std::ptr::write_volatile(flags_ptr, old_val | 0x40) };
                    }
                }

                // Then unlink from hashtable
                if cache.hashtable().unlink_item(key, id, current_offset, cache.metrics()) {
                    _unlinked_count += 1;
                }
            }

            // Move to the next item
            current_offset += item_size;
        }

        // Step 4: Wait for all readers to finish (only if we transitioned through Draining)
        if need_state_transition {
            // Aggressive bounded spinning - no yields for async compatibility
            #[cfg(not(feature = "loom"))]
            {
                const MAX_SPINS: u32 = 100_000;
                let mut spin_count = 0;

                while segment.ref_count.load(Ordering::Acquire) > 0 {
                    if spin_count >= MAX_SPINS {
                        // Readers taking too long - abort eviction
                        // Transition back to original state
                        segment.cas_metadata(
                            SegmentState::Draining,
                            SegmentState::Live, // or Sealed, but we'll use Live as safe fallback
                            None,
                            None,
                            cache.metrics(),
                        );
                        return false;
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
                // In loom, we can't spin like this - check once
                if segment.ref_count.load(Ordering::Acquire) > 0 {
                    panic!("Segment still has active readers in loom test");
                }
            }

            // Step 5: Transition to Locked state for final cleanup
            if !segment.cas_metadata(
                SegmentState::Draining,
                SegmentState::Locked,
                None, // Preserve current next
                None, // Preserve current prev
                cache.metrics(),
            ) {
                panic!("Failed to lock segment for cleanup");
            }
        }

        // Step 6: Reset segment statistics
        segment.write_offset.store(0, Ordering::Release);
        segment.live_items.store(0, Ordering::Release);
        segment.live_bytes.store(0, Ordering::Release);
        segment
            .expire_at
            .store(clocksource::coarse::Instant::now(), Ordering::Relaxed);
        segment.clear_bucket_id();

        // Step 7: Clear the segment's chain links if not already cleared
        // In eviction path, links are already cleared by TTL bucket code
        // Links are considered cleared if they're None or INVALID_SEGMENT_ID
        let next = segment.next();
        let prev = segment.prev();
        let next_cleared = next.is_none() || next == Some(INVALID_SEGMENT_ID);
        let prev_cleared = prev.is_none() || prev == Some(INVALID_SEGMENT_ID);

        if !next_cleared || !prev_cleared {
            // Try to clear links while staying in Locked state
            if !segment.cas_metadata(
                SegmentState::Locked,
                SegmentState::Locked,
                Some(INVALID_SEGMENT_ID),
                Some(INVALID_SEGMENT_ID),
                cache.metrics(),
            ) {
                // CAS failed - verify links are now cleared
                let next_now = segment.next();
                let prev_now = segment.prev();
                let next_cleared_now = next_now.is_none() || next_now == Some(INVALID_SEGMENT_ID);
                let prev_cleared_now = prev_now.is_none() || prev_now == Some(INVALID_SEGMENT_ID);

                if !next_cleared_now || !prev_cleared_now {
                    // Links still not cleared - this is corruption
                    panic!(
                        "Failed to clear segment {} links and they're still not cleared (next={:?}, prev={:?})",
                        id, next_now, prev_now
                    );
                }
                // Links were cleared by another thread, continue
            }
        }

        // Segment is now cleared and in Locked state
        // Caller can either release() it or transition to Live for reuse
        true
    }

    /// Expire a segment by clearing it and returning it to the free pool.
    ///
    /// Used for TTL expiration. Clears the segment and makes it available for
    /// future allocations.
    ///
    /// # Process
    /// 1. Calls `clear()` to unlink items and transition to Locked state
    /// 2. Transitions Locked → Reserved
    /// 3. Calls `release()` to add segment back to free pool
    ///
    /// # Returns
    /// - `true` if segment was successfully expired
    /// - `false` if segment couldn't be cleared (already being processed by another thread)
    ///
    /// # Note
    /// Superseded by `TtlBucket::try_expire_head_segment()` for production use.
    /// Kept for test compatibility.
    #[cfg(test)] // Test-only function
    pub(crate) fn expire(&self, id: u32, cache: &impl CacheOps) -> bool {
        // Clear the segment (leaves it in Locked state)
        if !self.clear(id, cache) {
            return false;
        }

        // Get the segment reference
        let segment = match self.get(id) {
            Some(seg) => seg,
            None => return false,
        };

        // Transition from Locked to Reserved
        if !segment.cas_metadata(
            SegmentState::Locked,
            SegmentState::Reserved,
            None, // Links already cleared by clear()
            None,
            cache.metrics(),
        ) {
            // This shouldn't happen - we own the segment in Locked state
            panic!("Failed to transition cleared segment {} from Locked to Reserved", id);
        }

        // Return segment to free pool
        self.release(id, cache.metrics());
        true
    }

    /// Evict a segment by clearing it for immediate reuse.
    ///
    /// Used during memory pressure to reclaim and reuse a segment immediately
    /// rather than returning it to the free pool.
    ///
    /// # Process
    /// 1. Calls `clear()` to unlink items and transition to Locked state
    /// 2. Leaves segment in Locked state for caller to provision
    ///
    /// # Returns
    /// - `true` if segment was successfully cleared and is ready for reuse
    /// - `false` if segment couldn't be cleared
    ///
    /// # Note
    /// Caller is responsible for transitioning the segment from Locked to Live
    /// when provisioning it for reuse.
    pub fn evict(&self, id: u32, cache: &impl CacheOps) -> bool {
        self.clear(id, cache)
    }

    /// Mark an item as deleted in a specific segment.
    ///
    /// Validates the segment ID and delegates to the segment's mark_deleted method.
    ///
    /// Empty sealed segments are removed from their TTL bucket chain and released
    /// back to the free pool.
    ///
    /// Returns:
    /// - Ok(true) if the item was successfully marked as deleted
    /// - Ok(false) if the item was already deleted or segment is being cleared
    /// - Err(()) if the key doesn't match (hash collision) or segment state is invalid
    #[cfg(test)] // Test-only function
    pub(crate) fn delete_item(
        &self,
        cache: &impl CacheOps,
        id: u32,
        offset: u32,
        key: &[u8],
    ) -> Result<bool, ()> {
        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            panic!("Invalid segment ID: {id}");
        }

        let segment = &self.segments[id_usize];

        // Segment::mark_deleted() handles all state validation
        let result = segment.mark_deleted(offset, key, cache.metrics())?;

        // If deletion succeeded, check if segment is now empty and sealed
        if result {
            let remaining_items = segment.live_items.load(Ordering::Acquire);
            let state = segment.state();

            // If segment is sealed and empty, try to release it early
            if remaining_items == 0 && state == SegmentState::Sealed {
                // Check if segment is in a TTL bucket
                if let Some(_bucket_id) = segment.bucket_id() {
                    // TODO: Early release optimization disabled for now
                    // Would need to spawn async task since remove_segment is now async
                    // Segments will be released through normal eviction path
                }
                // If bucket_id is None, segment is not in a TTL bucket chain
                // (used directly in tests) - nothing to do
            }
        }

        Ok(result)
    }

    /// High-level convenience method to get an item from a specific segment.
    ///
    /// # Parameters
    ///
    /// * `id` - The segment ID
    /// * `offset` - The offset within the segment
    /// * `key` - The expected key (for verification)
    /// * `buffer` - Buffer to copy the item data into
    ///
    /// # Returns
    ///
    /// - `Ok(Item)` if the item was successfully read
    /// - `Err(GetItemError::ItemDeleted)` if the item is marked as deleted
    /// - `Err(GetItemError::KeyMismatch)` if the key doesn't match (hash collision)
    /// - `Err(GetItemError::SegmentNotAccessible)` if segment is being cleared
    /// - `Err(GetItemError::InvalidOffset)` if offset is invalid or segment ID is out of bounds
    pub fn get_item<'a>(
        &self,
        id: u32,
        offset: u32,
        key: &[u8],
        buffer: &'a mut [u8],
    ) -> Result<Item<'a>, GetItemError> {
        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            return Err(GetItemError::InvalidOffset);
        }

        let segment = &self.segments[id_usize];

        // Segment::get_item() handles all validation and reference counting
        segment.get_item(offset, key, buffer)
    }

    /// Get a zero-copy guard that provides access to an item's data in the segment.
    ///
    /// This method returns an ItemGuard that holds references directly into the segment's
    /// memory, avoiding any allocations or copies. The segment's reference count is held
    /// while the guard exists, preventing eviction or clearing.
    ///
    /// # Parameters
    ///
    /// * `id` - The segment ID
    /// * `offset` - The offset within the segment
    /// * `key` - The expected key (for verification)
    ///
    /// # Returns
    ///
    /// - `Ok(ItemGuard)` - Guard providing zero-copy access to key, value, and optional data
    /// - `Err(GetItemError::ItemDeleted)` - Item is marked as deleted
    /// - `Err(GetItemError::KeyMismatch)` - Key doesn't match (hash collision)
    /// - `Err(GetItemError::SegmentNotAccessible)` - Segment is being cleared
    /// - `Err(GetItemError::InvalidOffset)` - Offset is invalid or segment ID out of bounds
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let guard = segments.get_item_guard(seg_id, offset, b"key")?;
    /// // Zero-copy access
    /// let value = guard.value();
    /// socket.write_all(value)?; // Serialize directly
    /// // Guard dropped here, ref_count decremented
    /// ```
    pub fn get_item_guard<'a>(
        &'a self,
        id: u32,
        offset: u32,
        key: &[u8],
    ) -> Result<crate::item::ItemGuard<'a>, GetItemError> {
        use std::sync::atomic::Ordering;
        use std::sync::atomic::fence;

        let id_usize = id as usize;

        // Bounds check
        if id_usize >= self.segments.len() {
            return Err(GetItemError::InvalidOffset);
        }

        let segment = &self.segments[id_usize];

        // Atomically increment reference count only if segment is accessible
        {
            let state = segment.state();
            if state == SegmentState::Draining || state == SegmentState::Locked {
                return Err(GetItemError::SegmentNotAccessible);
            }

            // Increment reference count
            segment.ref_count.fetch_add(1, Ordering::Acquire);

            // Double-check state after increment to handle race
            let state_after = segment.state();
            if state_after == SegmentState::Draining || state_after == SegmentState::Locked {
                segment.ref_count.fetch_sub(1, Ordering::Release);
                return Err(GetItemError::SegmentNotAccessible);
            }

            // Successfully acquired reference
            fence(Ordering::Acquire);
        }

        // Validate offset
        if offset.saturating_add(ItemHeader::MIN_ITEM_SIZE as u32) > segment.data_len {
            segment.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::InvalidOffset);
        }

        let data_ptr = unsafe { segment.data.as_ptr().add(offset as usize) };

        // Read and validate header
        let header = ItemHeader::from_bytes(unsafe {
            std::slice::from_raw_parts(data_ptr, ItemHeader::SIZE)
        });

        // Check if item is deleted
        if header.is_deleted() {
            segment.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::ItemDeleted);
        }

        // Validate that full item fits within segment
        let item_size = header.padded_size() as u32;
        if offset.saturating_add(item_size) > segment.data_len {
            segment.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::InvalidOffset);
        }

        // Get raw slice for the entire item
        let raw = unsafe { std::slice::from_raw_parts(data_ptr, header.padded_size()) };

        // Calculate ranges
        let optional_start = ItemHeader::SIZE;
        let optional_end = optional_start + header.optional_len() as usize;
        let key_start = optional_end;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_end = value_start + header.value_len() as usize;

        // Verify key matches
        if &raw[key_start..key_end] != key {
            segment.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::KeyMismatch);
        }

        // Create guard with slices into segment data
        // The guard will decrement ref_count on drop
        Ok(crate::item::ItemGuard::new(
            segment,
            &raw[key_start..key_end],
            &raw[value_start..value_end],
            &raw[optional_start..optional_end],
        ))
    }

}

// Safety: Segments can be sent between threads, since Segment can
unsafe impl Send for Segments {}
unsafe impl Sync for Segments {}

impl Drop for Segments {
    fn drop(&mut self) {
        self.segments.clear();

        // Deallocate heap memory
        unsafe {
            std::alloc::dealloc(self.heap_ptr, self.heap_layout);
        }
    }
}

pub struct SegmentsBuilder {
    segment_size: usize,
    heap_size: usize,
}

impl Default for SegmentsBuilder {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            heap_size: DEFAULT_HEAP_SIZE,
        }
    }
}

impl SegmentsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    pub fn heap_size(mut self, size: usize) -> Self {
        self.heap_size = size;
        self
    }

    pub fn build(self) -> Segments {
        assert!(
            self.heap_size >= self.segment_size,
            "Heap size must be at least as large as segment size"
        );
        assert!(
            self.heap_size % self.segment_size == 0,
            "Heap size must be a multiple of segment size"
        );

        let num_segments = self.heap_size / self.segment_size;

        // Allocate the entire heap as a single page-aligned block
        // Use 2MB alignment for potential huge page support on systems that support it
        // Falls back to regular pages if huge pages are not available
        const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024; // 2MB
        const REGULAR_PAGE_SIZE: usize = 4096;

        let alignment = if self.heap_size >= HUGE_PAGE_SIZE && self.heap_size % HUGE_PAGE_SIZE == 0
        {
            HUGE_PAGE_SIZE
        } else {
            REGULAR_PAGE_SIZE
        };

        let layout =
            Layout::from_size_align(self.heap_size, alignment).expect("Failed to create layout");

        // Use alloc_zeroed to get zero-initialized memory
        // This is important for security and consistency
        let heap_ptr = unsafe { alloc_zeroed(layout) };
        if heap_ptr.is_null() {
            panic!(
                "Failed to allocate {} bytes for segments heap",
                self.heap_size
            );
        }

        // Pre-fault all pages by touching them
        // This forces the OS to allocate physical pages now rather than on first access
        // which avoids page faults during critical write operations
        unsafe {
            // Touch only one location per page to minimize memory traffic
            // One write per page is sufficient to fault the entire page
            const PAGE_SIZE: usize = 4096;

            for i in (0..self.heap_size).step_by(PAGE_SIZE) {
                std::ptr::write_volatile(heap_ptr.add(i) as *mut u64, 0);
            }
        }

        // Create segments vector - align(64) on Segment ensures proper alignment
        let mut segments = Vec::with_capacity(num_segments);

        // Initialize each segment with its slice of the heap
        for id in 0..num_segments {
            let offset = id * self.segment_size;
            let segment_ptr = unsafe { heap_ptr.add(offset) };

            // Create lock-free segment
            let segment = unsafe { Segment::new(id as u32, segment_ptr, self.segment_size) };
            segments.push(segment);
        }

        // Create the free queue and populate it with all segment IDs
        let free_queue = crossbeam_deque::Injector::new();
        for id in 0..num_segments {
            free_queue.push(id as u32);
        }

        Segments {
            segments,
            heap_ptr,
            heap_layout: layout,
            segment_size: self.segment_size,
            free_queue,
        }
    }
}

/// State of a segment in its lifecycle
///
/// # State Semantics
///
/// - **Free**: In free queue, not in use
/// - **Reserved**: Allocated for use, being prepared for chain insertion
/// - **Linking**: Being added to TTL bucket chain (next/prev being set)
/// - **Live**: Active tail segment accepting writes and reads
/// - **Sealed**: No more writes accepted, but data readable and chain stable
/// - **Relinking**: Chain pointers being updated during neighbor removal.
///   Data remains readable, only next/prev pointers are being modified.
///   This state allows safe updates to the doubly-linked list structure
///   without blocking read access to segment data.
/// - **Draining**: Waiting for readers to finish before clearing. Reads are rejected.
/// - **Locked**: Being cleared, all access rejected
///
/// # Relinking State and Chain Update Protocol
///
/// When removing a middle segment (B) from a chain A <-> B <-> C:
/// 1. Lock target B: Sealed → Draining → Locked
/// 2. Lock prev segment A: Sealed → Relinking (A cannot be Live since B exists after it)
/// 3. Update A's next pointer to point to C
/// 4. Unlock A: Relinking → Sealed
/// 5. Lock next segment C: Sealed | Live → Relinking
/// 6. Update C's prev pointer to point to A
/// 7. Unlock C: Relinking → (original state)
/// 8. Update bucket head/tail if needed
/// 9. Clear B and release
///
/// The Relinking state provides mutual exclusion for chain pointer updates
/// while still allowing reads of segment data.
///
/// # Future: Migration to Bitflags
///
/// Consider migrating from enum to bitflags for more flexible combinations:
/// ```ignore
/// bitflags! {
///     struct SegmentFlags: u8 {
///         const SEALED = 1 << 0;       // No more writes accepted
///         const METADATA_LOCK = 1 << 1; // Chain pointers locked (Relinking)
///         const DATA_LOCK = 1 << 2;     // Data being cleared (Draining/Locked)
///         const LIVE = 1 << 3;          // Accepting writes (tail)
///     }
/// }
/// ```
/// This would allow more granular state combinations and easier reasoning
/// about which operations are permitted in each state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Free = 0,
    Reserved = 1,
    Linking = 2, // Intermediate state while linking into chain
    Live = 3,
    Sealed = 4,
    Relinking = 5, // Chain pointers being updated (data still readable)
    Draining = 6, // Intermediate state while readers finish (reads rejected)
    Locked = 7, // Being cleared (all access rejected)
}

/// Packed representation of segment metadata in a single AtomicU64
/// Layout: [8 bits unused] [8 bits state] [24 bits prev] [24 bits next]
pub struct PackedSegmentMeta {
    pub next: u32, // Only uses 24 bits
    pub prev: u32, // Only uses 24 bits
    pub state: SegmentState,
}

pub const INVALID_SEGMENT_ID: u32 = 0xFFFFFF; // 24-bit max value

impl PackedSegmentMeta {
    pub fn pack(self) -> u64 {
        // Mask to ensure we only use 24 bits for IDs
        let next_24 = (self.next & 0xFFFFFF) as u64;
        let prev_24 = (self.prev & 0xFFFFFF) as u64;
        let state_8 = self.state as u64;

        // Pack: [8 unused][8 state][24 prev][24 next]
        (state_8 << 48) | (prev_24 << 24) | next_24
    }

    pub fn unpack(packed: u64) -> Self {
        let state_val = ((packed >> 48) & 0xFF) as u8;
        let state = match state_val {
            0 => SegmentState::Free,
            1 => SegmentState::Reserved,
            2 => SegmentState::Linking,
            3 => SegmentState::Live,
            4 => SegmentState::Sealed,
            5 => SegmentState::Relinking,
            6 => SegmentState::Draining,
            7 => SegmentState::Locked,
            _ => {
                panic!("Corrupted segment metadata: invalid state {}", state_val);
            }
        };

        Self {
            next: (packed & 0xFFFFFF) as u32,
            prev: ((packed >> 24) & 0xFFFFFF) as u32,
            state,
        }
    }
}

#[repr(C, align(64))]
pub struct Segment<'a> {
    // Hot path: frequently accessed during append operations
    // Keep these in the first cache line for better performance
    write_offset: AtomicU32,

    // Packed metadata: next pointer, prev pointer, and state in single AtomicU64
    // This enables atomic updates of all three fields together
    packed_meta: AtomicU64,

    live_items: AtomicU32,
    live_bytes: AtomicU32,
    pub(crate) ref_count: AtomicU32, // Reference count for live readers

    // Less frequently accessed metadata
    // These can span into second cache line
    id: u32,
    data_len: u32,
    data: NonNull<u8>,

    // Cold path: expiration tracking
    // Rarely accessed except during TTL management
    expire_at: AtomicInstant,
    bucket_id: AtomicU16, // TTL bucket ID (0xFFFF = not in bucket)

    _lifetime: std::marker::PhantomData<&'a u8>,
}

/// Error types for get_item operations
#[derive(Debug, PartialEq)]
pub enum GetItemError {
    /// Item not found in hashtable
    NotFound,
    /// Item has been marked as deleted
    ItemDeleted,
    /// Key doesn't match (hash collision)
    KeyMismatch,
    /// Segment is being cleared/removed
    SegmentNotAccessible,
    /// Invalid offset or corrupted data
    InvalidOffset,
    /// Buffer provided is too small for the item
    BufferTooSmall,
}

// SAFETY: Segment synchronizes access via atomics
unsafe impl<'a> Send for Segment<'a> {}
unsafe impl<'a> Sync for Segment<'a> {}

impl<'a> Segment<'a> {
    /// Sentinel value indicating segment is not in a TTL bucket
    const INVALID_BUCKET_ID: u16 = 0xFFFF;

    /// Get the current count of live items in the segment
    pub(crate) fn live_items(&self) -> u32 {
        self.live_items.load(Ordering::Acquire)
    }

    /// Get the current write offset in the segment
    #[cfg(test)] // Test-only function
    pub(crate) fn write_offset(&self) -> u32 {
        self.write_offset.load(Ordering::Acquire)
    }

    /// Get the data length (capacity) of this segment in bytes
    pub fn data_len(&self) -> usize {
        self.data_len as usize
    }

    /// Get a pointer to the segment's data region
    ///
    /// # Safety
    /// The returned pointer is valid for the lifetime of the segment and
    /// points to `data_len` bytes of memory. Callers must ensure proper
    /// synchronization when reading/writing.
    pub unsafe fn data_ptr(&self) -> *mut u8 {
        self.data.as_ptr()
    }

    /// Get the current count of live bytes in the segment
    pub fn live_bytes(&self) -> u32 {
        self.live_bytes.load(Ordering::Acquire)
    }

    /// Get the current reference count for the segment
    pub fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Acquire)
    }

    /// Get the expiration time of the segment
    pub fn expire_at(&self) -> clocksource::coarse::Instant {
        self.expire_at.load(Ordering::Acquire)
    }

    /// Set the expiration time for the segment
    pub fn set_expire_at(&self, expire_at: clocksource::coarse::Instant) {
        self.expire_at.store(expire_at, Ordering::Relaxed);
    }

    /// Get the TTL bucket ID this segment belongs to
    pub fn bucket_id(&self) -> Option<u16> {
        let id = self.bucket_id.load(Ordering::Acquire);
        if id == Self::INVALID_BUCKET_ID {
            None
        } else {
            Some(id)
        }
    }

    /// Set the TTL bucket ID for this segment
    pub fn set_bucket_id(&self, bucket_id: u16) {
        self.bucket_id.store(bucket_id, Ordering::Release);
    }

    /// Clear the TTL bucket ID (mark as not in bucket)
    pub fn clear_bucket_id(&self) {
        self.bucket_id.store(Self::INVALID_BUCKET_ID, Ordering::Release);
    }

    /// Get the current state of the segment
    pub fn state(&self) -> SegmentState {
        let packed = self.packed_meta.load(Ordering::Acquire);
        PackedSegmentMeta::unpack(packed).state
    }

    /// Get the next segment ID in the linked list
    pub fn next(&self) -> Option<u32> {
        let packed = self.packed_meta.load(Ordering::Acquire);
        let meta = PackedSegmentMeta::unpack(packed);
        if meta.next == INVALID_SEGMENT_ID {
            None
        } else {
            Some(meta.next)
        }
    }

    /// Get the previous segment ID in the linked list
    pub fn prev(&self) -> Option<u32> {
        let packed = self.packed_meta.load(Ordering::Acquire);
        let meta = PackedSegmentMeta::unpack(packed);
        if meta.prev == INVALID_SEGMENT_ID {
            None
        } else {
            Some(meta.prev)
        }
    }

    /// Atomically update the segment metadata with CAS
    /// Returns true if successful, false if the current value doesn't match expected
    ///
    /// # Arguments
    /// * `new_next` - New next pointer, or None to preserve current value
    /// * `new_prev` - New prev pointer, or None to preserve current value
    ///
    /// # Loom Test Coverage
    /// - `concurrent_packed_metadata_cas` - Low-level packed metadata CAS without retry logic
    /// - Called extensively by `concurrent_ttl_bucket_append` for state transitions
    pub fn cas_metadata(
        &self,
        expected_state: SegmentState,
        new_state: SegmentState,
        new_next: Option<u32>,
        new_prev: Option<u32>,
        metrics: &crate::metrics::CacheMetrics,
    ) -> bool {
        let result = retry_cas_metadata(
            &self.packed_meta,
            expected_state,
            |current_meta| {
                // Create new metadata with updated fields
                // If None is passed, preserve the current value
                Some(PackedSegmentMeta {
                    next: new_next.unwrap_or(current_meta.next),
                    prev: new_prev.unwrap_or(current_meta.prev),
                    state: new_state,
                })
            },
            CasRetryConfig::default(),
            metrics,
        );

        let success = matches!(result, CasResult::Success(()));

        // Update state gauges on successful transition
        if success && expected_state != new_state {
            // Decrement old state gauge
            match expected_state {
                SegmentState::Live => { metrics.segments_live.decrement(); }
                SegmentState::Sealed => { metrics.segments_sealed.decrement(); }
                _ => {}
            }

            // Increment new state gauge
            match new_state {
                SegmentState::Live => { metrics.segments_live.increment(); }
                SegmentState::Sealed => { metrics.segments_sealed.increment(); }
                _ => {}
            }
        }

        success
    }

    /// Create a new segment from a data slice
    /// SAFETY: data must outlive the segment
    pub(crate) unsafe fn new(id: u32, data: *mut u8, len: usize) -> Self {
        // Initially, segment is Free with no links
        let initial_meta = PackedSegmentMeta {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: SegmentState::Free,
        };

        Self {
            // Hot path atomics
            write_offset: AtomicU32::new(0),
            packed_meta: AtomicU64::new(initial_meta.pack()),
            live_items: AtomicU32::new(0),
            live_bytes: AtomicU32::new(0),
            ref_count: AtomicU32::new(0),

            // Metadata
            id,
            data_len: len as u32,
            data: unsafe { NonNull::new_unchecked(data) },

            // Cold path
            expire_at: AtomicInstant::now(),
            bucket_id: AtomicU16::new(Self::INVALID_BUCKET_ID),

            _lifetime: std::marker::PhantomData,
        }
    }

    /// Appends an item to the segment atomically.
    ///
    /// # Safety and Synchronization
    ///
    /// This method uses lock-free CAS operations to reserve space and includes
    /// a release fence after writing data to ensure visibility. The synchronization
    /// protocol guarantees that:
    /// - Space reservation is atomic via compare_exchange on write_offset
    /// - All data writes complete before the release fence
    /// - Items are not accessible until linked into the hashtable (happens after this returns)
    /// - Concurrent readers cannot see partially written data
    ///
    /// Returns the offset where the item was written.
    /// Low-level segment append that bypasses most state validation.
    ///
    /// # Safety and State Validation
    ///
    /// This method intentionally does NOT validate segment state to allow:
    /// - Testing scenarios where segments are used directly
    /// - Emergency operations that need to bypass normal state machines
    ///
    /// For normal production usage, use `TtlBucket::append_item()` which
    /// includes proper state validation and only appends to Live segments.
    ///
    /// **Note**: This creates an intentional inconsistency with `mark_deleted()`
    /// which DOES validate state. This is by design to separate:
    /// - Low-level operations (append_item): permissive for testing/flexibility
    /// - Safety operations (mark_deleted): strict to prevent races with clear
    ///
    /// # Loom Test Coverage
    /// - `concurrent_write_offset_cas` - Low-level CAS on write_offset without retry logic
    /// - `single_item_append_metrics` - Single-threaded append with metric validation
    /// - `concurrent_item_append_to_segment` - Two threads appending to same segment
    /// - `segment_full_tracking` - Segment capacity limits and ITEM_APPEND_FULL metric
    /// - `cas_retry_tracking` (ignored) - Three threads for CAS retry validation
    pub fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8], metrics: &crate::metrics::CacheMetrics) -> Option<u32> {
        // Validate segment data structures are not corrupted
        assert!(self.data_len > 0,
            "CORRUPTION: segment {} has data_len=0", self.id);

        if key.len() == 0 || key.len() > ItemHeader::MAX_KEY_LEN {
            panic!(
                "key size is out of range: must be 1-{} bytes",
                ItemHeader::MAX_KEY_LEN
            );
        }

        if optional.len() > ItemHeader::MAX_OPTIONAL_LEN {
            panic!(
                "optional size is out of range: must be 0-{} bytes",
                ItemHeader::MAX_OPTIONAL_LEN
            );
        }

        if value.len() > ItemHeader::MAX_VALUE_LEN {
            panic!(
                "value size is out of range: must be 0-{} bytes",
                ItemHeader::MAX_VALUE_LEN
            );
        }

        let header = ItemHeader::new(
            key.len() as u8,
            optional.len() as u8,
            value.len() as u32,
            false, // is_deleted
            false, // is_numeric
        );

        if header.padded_size() as u32 > self.data_len {
            panic!("item size is out of range. increase segment size");
        }

        let item_size = header.padded_size() as u32;

        // Use the standard CAS retry pattern for reserving space
        let reserved_offset = match retry_cas_u32(
            &self.write_offset,
            |current_offset| {
                let new_offset = current_offset.saturating_add(item_size);

                // Check if there's enough space
                if new_offset > self.data_len {
                    return None; // Segment is full
                }

                Some((new_offset, current_offset))
            },
            CasRetryConfig {
                max_attempts: 16,
                early_spin_threshold: 4,
            },
            metrics,
        ) {
            CasResult::Success(offset) => offset,
            CasResult::Failed(_) | CasResult::Aborted => {
                // Segment is full, return None
                metrics.item_append_full.increment();
                return None;
            }
        };

        // Space successfully reserved, now write the data
        {
            // CRITICAL: Check reserved_offset BEFORE any pointer arithmetic
            // A corrupted offset could cause segfault in pointer addition itself
            if reserved_offset >= self.data_len {
                panic!(
                    "CORRUPTION: reserved_offset ({}) >= data_len ({}) in segment {}. \
                     write_offset was: {}. This should be impossible after CAS!",
                    reserved_offset, self.data_len, self.id,
                    self.write_offset.load(Ordering::Relaxed)
                );
            }

            // Check if data pointer looks valid (not null, not clearly corrupted)
            let base_ptr = self.data.as_ptr() as usize;
            if base_ptr == 0 {
                panic!("CORRUPTION: segment {} has null data pointer", self.id);
            }
            // Check if pointer is in a reasonable range (not 0xFFFF... or very low addresses)
            if base_ptr < 0x1000 || base_ptr == usize::MAX {
                panic!("CORRUPTION: segment {} has invalid data pointer: {:p}", self.id, self.data.as_ptr());
            }

            // Runtime assertions to catch corruption early (always enabled for safety)
            // These have minimal performance impact but will catch serious bugs
            assert!(reserved_offset < self.data_len,
                "CORRUPTION: reserved_offset ({}) >= data_len ({}) in segment {}",
                reserved_offset, self.data_len, self.id);
            assert!(reserved_offset.saturating_add(item_size) <= self.data_len,
                "CORRUPTION: reserved_offset ({}) + item_size ({}) > data_len ({}) in segment {}",
                reserved_offset, item_size, self.data_len, self.id);

            // Debug logging disabled for performance - re-enable if segfault occurs
            // use std::io::Write;
            // if let Ok(mut file) = std::fs::OpenOptions::new()
            //     .create(true)
            //     .append(true)
            //     .open("/tmp/segcache_debug.log")
            // {
            //     let _ = writeln!(file, "[BEFORE ADD seg={}] reserved_offset={}, item_size={}, data_len={}, data_ptr={:p}, write_offset={}",
            //         self.id, reserved_offset, item_size, self.data_len, self.data.as_ptr(),
            //         self.write_offset.load(Ordering::Relaxed));
            // }

            let mut data_ptr = unsafe { self.data.as_ptr().add(reserved_offset as usize) };

            // if let Ok(mut file) = std::fs::OpenOptions::new()
            //     .create(true)
            //     .append(true)
            //     .open("/tmp/segcache_debug.log")
            // {
            //     let _ = writeln!(file, "[AFTER ADD seg={}] data_ptr={:p}, segment_end_offset={}, will write {} bytes",
            //         self.id, data_ptr, self.data_len, item_size);
            // }

            // Validate pointer is within segment bounds before any writes
            let segment_end = unsafe { self.data.as_ptr().add(self.data_len as usize) };
            let write_end = unsafe { data_ptr.add(item_size as usize) };
            assert!(write_end <= segment_end,
                "CORRUPTION: write would extend past segment end. data_ptr offset={}, item_size={}, data_len={}, segment={}",
                reserved_offset, item_size, self.data_len, self.id);

            {
                let mut data =
                    unsafe { std::slice::from_raw_parts_mut(data_ptr, ItemHeader::SIZE) };
                header.to_bytes(&mut data);
            }

            unsafe {
                data_ptr = data_ptr.add(ItemHeader::SIZE);

                // Copy optional metadata (usually small)
                if optional.len() > 0 {
                    assert!(data_ptr.add(optional.len()) <= segment_end,
                        "CORRUPTION: optional write would exceed segment bounds in segment {}", self.id);
                    std::ptr::copy_nonoverlapping(optional.as_ptr(), data_ptr, optional.len());
                    data_ptr = data_ptr.add(optional.len());
                }

                // Copy key (usually small)
                assert!(data_ptr.add(key.len()) <= segment_end,
                    "CORRUPTION: key write would exceed segment bounds in segment {}", self.id);
                std::ptr::copy_nonoverlapping(key.as_ptr(), data_ptr, key.len());
                data_ptr = data_ptr.add(key.len());

                // Copy value
                if value.len() > 0 {
                    assert!(data_ptr.add(value.len()) <= segment_end,
                        "CORRUPTION: value write (len={}) would exceed segment bounds in segment {}", value.len(), self.id);
                    // Use regular copy for all values (non-temporal stores disabled due to alignment issues)
                    std::ptr::copy_nonoverlapping(value.as_ptr(), data_ptr, value.len());
                }
            }

            // Ensure all writes are visible before returning offset
            // The item won't be accessible until linked into hashtable
            fence(Ordering::Release);

            // Update segment statistics
            self.live_items.fetch_add(1, Ordering::Relaxed);
            self.live_bytes.fetch_add(item_size, Ordering::Relaxed);

            metrics.item_append.increment();
            // Update cache-wide item tracking
            metrics.items_live.increment();
            metrics.bytes_live.add(item_size as i64);
            return Some(reserved_offset);
        }
    }

    pub fn get_item(
        &self,
        offset: u32,
        key: &[u8],
        buffer: &'a mut [u8],
    ) -> Result<Item<'a>, GetItemError> {
        // Atomically increment reference count only if segment is accessible
        // using double-check pattern to handle TOCTOU race with state transitions
        {
            // Check state before incrementing ref count
            let state = self.state();
            if state == SegmentState::Draining || state == SegmentState::Locked {
                return Err(GetItemError::SegmentNotAccessible);
            }

            // Increment reference count
            self.ref_count.fetch_add(1, Ordering::Acquire);

            // Double-check state after increment to handle race where segment
            // transitions to Draining/Locked between the check and increment
            let state_after = self.state();
            if state_after == SegmentState::Draining || state_after == SegmentState::Locked {
                self.ref_count.fetch_sub(1, Ordering::Release);
                return Err(GetItemError::SegmentNotAccessible);
            }

            // Successfully acquired reference - synchronize with append_item's Release fence
            fence(Ordering::Acquire);
        }
        // Validate that we have at least enough space for the header
        if offset.saturating_add(ItemHeader::MIN_ITEM_SIZE as u32) > self.data_len {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::InvalidOffset);
        }

        // Additional runtime assertion to catch corruption
        assert!(offset < self.data_len,
            "CORRUPTION in get_item: offset ({}) >= data_len ({}) in segment {}",
            offset, self.data_len, self.id);

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Validate pointer is within bounds before reading header
        let segment_end = unsafe { self.data.as_ptr().add(self.data_len as usize) };
        let header_end = unsafe { data_ptr.add(ItemHeader::SIZE) };
        assert!(header_end <= segment_end,
            "CORRUPTION in get_item: reading header at offset {} would exceed segment bounds in segment {}",
            offset, self.id);

        let header = ItemHeader::from_bytes(unsafe {
            std::slice::from_raw_parts(data_ptr, ItemHeader::SIZE)
        });

        // Check if item is deleted - this is a normal case that can happen due to races
        if header.is_deleted() {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::ItemDeleted);
        }

        // Validate that the full item fits within the segment
        let item_size = header.padded_size() as u32;
        if offset.saturating_add(item_size) > self.data_len {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::InvalidOffset);
        }

        // Additional runtime assertion before reading full item
        let item_end = unsafe { data_ptr.add(header.padded_size()) };
        assert!(item_end <= segment_end,
            "CORRUPTION in get_item: reading item at offset {} with size {} would exceed segment bounds in segment {}",
            offset, header.padded_size(), self.id);

        let raw = unsafe { std::slice::from_raw_parts(data_ptr, header.padded_size()) };

        // Calculate key range relative to the start of this item (offset 0 within raw slice)
        let key_start = ItemHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;

        if &raw[key_start..key_end] != key {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::KeyMismatch);
        }

        // Copy only the actual item data, not the entire buffer
        let item_len = raw.len();

        // Check if buffer is large enough
        if buffer.len() < item_len {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(GetItemError::BufferTooSmall);
        }

        buffer[..item_len].copy_from_slice(raw);

        // Decrement reference count now that we're done reading
        self.ref_count.fetch_sub(1, Ordering::Release);

        Ok(Item::new(header, &mut buffer[..item_len]))
    }

    pub fn mark_deleted(&self, offset: u32, key: &[u8], metrics: &crate::metrics::CacheMetrics) -> Result<bool, ()> {
        // Check segment state first
        let current_state = self.state();
        match current_state {
            SegmentState::Free
            | SegmentState::Reserved
            | SegmentState::Linking
            | SegmentState::Live
            | SegmentState::Sealed
            | SegmentState::Relinking => {
                // OK to mark items deleted in these states
                // Free: segment not yet reserved but can have items (testing/direct use)
                // Reserved: segment has items but not yet in TTL bucket
                // Linking: segment being added to TTL bucket
                // Live: normal active segment
                // Sealed: full segment, no new appends but deletions still valid
                // Relinking: chain pointers locked but data still accessible
            }
            SegmentState::Draining | SegmentState::Locked => {
                // Segment is being cleared - don't interfere, treat as "already deleted"
                return Ok(false);
            }
        }

        // Validate that we have at least enough space for the header
        if offset.saturating_add(ItemHeader::MIN_ITEM_SIZE as u32) > self.data_len {
            panic!("offset too deep into segment: header would be out of bounds");
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Read header to validate item and get size
        let header = ItemHeader::from_bytes(unsafe {
            std::slice::from_raw_parts(data_ptr, ItemHeader::SIZE)
        });

        // Check if already deleted before doing more work
        if header.is_deleted() {
            return Ok(false);
        }

        // Validate that the full item fits within the segment
        let item_size = header.padded_size() as u32;
        if offset.saturating_add(item_size) > self.data_len {
            panic!("item overruns segment region");
        }

        // Read the item to verify key match
        let raw = unsafe { std::slice::from_raw_parts(data_ptr, header.padded_size()) };

        // Calculate key range relative to the start of this item (offset 0 within raw slice)
        let key_start = ItemHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;

        if &raw[key_start..key_end] != key {
            // Key mismatch - collision case that caller needs to handle
            return Err(());
        }

        // SAFETY: Atomically set the deleted bit in the flags byte.
        // The flags byte is at offset 4 in the packed header struct.
        let flags_ptr = unsafe { data_ptr.add(4) };

        #[cfg(not(feature = "loom"))]
        let old_flags = {
            // In production, we can use AtomicU8 directly (naturally aligned)
            let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
            flags_atomic.fetch_or(0x40, Ordering::Release)
        };

        #[cfg(feature = "loom")]
        let old_flags = {
            // In loom, AtomicU8 requires 8-byte alignment which we don't have at offset 4.
            // Use a CAS loop with volatile operations. Loom tracks data races even through
            // volatile operations, so this is safe for testing.
            let mut old_val = unsafe { std::ptr::read_volatile(flags_ptr) };
            loop {
                // Check if already deleted
                if (old_val & 0x40) != 0 {
                    break old_val;
                }

                let new_val = old_val | 0x40;

                // Atomic fence before the write
                fence(Ordering::Release);

                // Try to write the new value
                // In loom, this volatile write will be tracked for races
                unsafe {
                    let current = std::ptr::read_volatile(flags_ptr);
                    if current == old_val {
                        std::ptr::write_volatile(flags_ptr, new_val);
                        break old_val;
                    }
                    old_val = current;
                }
            }
        };

        // Check if the item was already deleted
        if (old_flags & 0x40) != 0 {
            // Another thread already deleted it
            return Ok(false);
        }

        // Update segment statistics to reflect the deletion
        // fetch_sub returns the OLD value before decrement
        self.live_items.fetch_sub(1, Ordering::Relaxed);
        self.live_bytes.fetch_sub(item_size, Ordering::Relaxed);

        // Update cache-wide item tracking
        metrics.items_live.decrement();
        metrics.bytes_live.sub(item_size as i64);

        Ok(true)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::Cache;

    #[test]
    fn test_get_item_success() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Append an item
        let offset = segment.append_item(b"testkey", b"testvalue", b"", cache.metrics()).unwrap();

        // Get the item back
        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(offset, b"testkey", &mut buffer);

        assert!(result.is_ok(), "Should successfully get item");
        let item = result.unwrap();
        assert_eq!(item.key(), b"testkey");
        assert_eq!(item.value(), b"testvalue");
        assert_eq!(item.optional(), b"");
    }

    #[test]
    fn test_get_item_wrong_key() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();

        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(offset, b"key2", &mut buffer);

        assert!(matches!(result, Err(GetItemError::KeyMismatch)));
    }

    #[test]
    fn test_get_item_invalid_offset() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Try to read from an offset beyond the segment
        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(segment.data_len + 100, b"key", &mut buffer);

        assert!(matches!(result, Err(GetItemError::InvalidOffset)));
    }

    #[test]
    fn test_get_item_buffer_too_small() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Provide a buffer that's too small
        let mut buffer = vec![0u8; 5];
        let result = segment.get_item(offset, b"key", &mut buffer);

        assert!(matches!(result, Err(GetItemError::BufferTooSmall)));
    }

    #[test]
    fn test_get_item_on_draining_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Draining state
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Draining,
            None,
            None,
            cache.metrics(),
        );

        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(offset, b"key", &mut buffer);

        assert!(matches!(result, Err(GetItemError::SegmentNotAccessible)));
    }

    #[test]
    fn test_get_item_on_locked_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Locked state
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Locked,
            None,
            None,
            cache.metrics(),
        );

        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(offset, b"key", &mut buffer);

        assert!(matches!(result, Err(GetItemError::SegmentNotAccessible)));
    }

    #[test]
    fn test_get_item_on_relinking_segment() {
        // Relinking state allows reads (only chain pointers are locked)
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Relinking state
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Relinking,
            None,
            None,
            cache.metrics(),
        );

        let mut buffer = vec![0u8; 1024];
        let result = segment.get_item(offset, b"key", &mut buffer);

        // Should succeed - Relinking doesn't block reads
        assert!(result.is_ok(), "Should be able to read from Relinking segment");
    }

    #[test]
    fn test_get_item_multiple_items() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Append multiple items
        let offset1 = segment.append_item(b"key1", b"value1", b"opt1", cache.metrics()).unwrap();
        let offset2 = segment.append_item(b"key2", b"value2", b"", cache.metrics()).unwrap();
        let offset3 = segment.append_item(b"key3", b"value3", b"opt3", cache.metrics()).unwrap();

        // Read them back
        let mut buffer = vec![0u8; 1024];

        let item1 = segment.get_item(offset1, b"key1", &mut buffer).unwrap();
        assert_eq!(item1.key(), b"key1");
        assert_eq!(item1.value(), b"value1");
        assert_eq!(item1.optional(), b"opt1");

        let item2 = segment.get_item(offset2, b"key2", &mut buffer).unwrap();
        assert_eq!(item2.key(), b"key2");
        assert_eq!(item2.value(), b"value2");
        assert_eq!(item2.optional(), b"");

        let item3 = segment.get_item(offset3, b"key3", &mut buffer).unwrap();
        assert_eq!(item3.key(), b"key3");
        assert_eq!(item3.value(), b"value3");
        assert_eq!(item3.optional(), b"opt3");
    }

    #[test]
    fn test_mark_deleted_success() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Append an item
        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();
        assert_eq!(segment.live_items(), 1);

        // Mark it deleted
        let result = segment.mark_deleted(offset, b"key", cache.metrics());
        assert_eq!(result, Ok(true), "Should successfully mark deleted");

        // Check statistics updated
        assert_eq!(segment.live_items(), 0);

        // Verify item is marked deleted
        let mut buffer = vec![0u8; 1024];
        let get_result = segment.get_item(offset, b"key", &mut buffer);
        assert!(matches!(get_result, Err(GetItemError::ItemDeleted)));
    }

    #[test]
    fn test_mark_deleted_twice() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // First delete succeeds
        assert_eq!(segment.mark_deleted(offset, b"key", cache.metrics()), Ok(true));

        // Second delete returns false (already deleted)
        assert_eq!(segment.mark_deleted(offset, b"key", cache.metrics()), Ok(false));

        // live_items should only be decremented once
        assert_eq!(segment.live_items(), 0);
    }

    #[test]
    fn test_mark_deleted_wrong_key() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();

        // Try to delete with wrong key
        let result = segment.mark_deleted(offset, b"key2", cache.metrics());
        assert_eq!(result, Err(()), "Should return error for key mismatch");

        // Item should still be live
        assert_eq!(segment.live_items(), 1);

        // Item should still be readable
        let mut buffer = vec![0u8; 1024];
        let get_result = segment.get_item(offset, b"key1", &mut buffer);
        assert!(get_result.is_ok());
    }

    #[test]
    #[should_panic(expected = "offset too deep into segment")]
    fn test_mark_deleted_invalid_offset() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Try to mark deleted at invalid offset
        segment.mark_deleted(segment.data_len + 100, b"key", cache.metrics()).unwrap();
    }

    #[test]
    fn test_mark_deleted_on_draining_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Draining
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Draining,
            None,
            None,
            cache.metrics(),
        );

        // mark_deleted should return Ok(false) - treat as already deleted
        assert_eq!(segment.mark_deleted(offset, b"key", cache.metrics()), Ok(false));
    }

    #[test]
    fn test_mark_deleted_on_locked_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Locked
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Locked,
            None,
            None,
            cache.metrics(),
        );

        // mark_deleted should return Ok(false)
        assert_eq!(segment.mark_deleted(offset, b"key", cache.metrics()), Ok(false));
    }

    #[test]
    fn test_mark_deleted_on_relinking_segment() {
        // Relinking allows data modifications (only chain pointers locked)
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Transition to Relinking
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Relinking,
            None,
            None,
            cache.metrics(),
        );

        // mark_deleted should succeed
        assert_eq!(segment.mark_deleted(offset, b"key", cache.metrics()), Ok(true));
        assert_eq!(segment.live_items(), 0);
    }

    #[test]
    fn test_mark_deleted_multiple_items() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Append multiple items
        let offset1 = segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();
        let offset2 = segment.append_item(b"key2", b"value2", b"", cache.metrics()).unwrap();
        let offset3 = segment.append_item(b"key3", b"value3", b"", cache.metrics()).unwrap();

        assert_eq!(segment.live_items(), 3);

        // Delete them in any order
        assert_eq!(segment.mark_deleted(offset2, b"key2", cache.metrics()), Ok(true));
        assert_eq!(segment.live_items(), 2);

        assert_eq!(segment.mark_deleted(offset1, b"key1", cache.metrics()), Ok(true));
        assert_eq!(segment.live_items(), 1);

        assert_eq!(segment.mark_deleted(offset3, b"key3", cache.metrics()), Ok(true));
        assert_eq!(segment.live_items(), 0);
    }

    #[test]
    fn test_clear_empty_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();

        // Clear an empty segment
        cache.segments().expire(seg_id, &cache);

        // Segment should be back in Free state
        let segment = cache.segments().get(seg_id).unwrap();
        assert_eq!(segment.state(), SegmentState::Free);
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.write_offset(), 0);
    }

    #[test]
    fn test_clear_segment_with_items() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Add some items
        segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();
        segment.append_item(b"key2", b"value2", b"", cache.metrics()).unwrap();
        segment.append_item(b"key3", b"value3", b"", cache.metrics()).unwrap();

        assert_eq!(segment.live_items(), 3);
        let write_offset_before = segment.write_offset();
        assert!(write_offset_before > 0);

        // Clear the segment
        cache.segments().expire(seg_id, &cache);

        // Segment should be cleared and back in Free state
        assert_eq!(segment.state(), SegmentState::Free);
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.write_offset(), 0);
        assert_eq!(segment.bucket_id(), None);
    }

    #[test]
    fn test_clear_already_free_segment() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();

        // Release it back to free pool
        cache.segments().release(seg_id, cache.metrics());

        // Clear should be a no-op and return early
        cache.segments().expire(seg_id, &cache);

        let segment = cache.segments().get(seg_id).unwrap();
        assert_eq!(segment.state(), SegmentState::Free);
    }

    #[test]
    fn test_clear_from_live_state() {
        // clear() should work from any state (except Free which returns early)
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Transition to Live state
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Live,
            None,
            None,
            cache.metrics(),
        );

        // Add an item
        segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Clear should work from Live state
        cache.segments().expire(seg_id, &cache);

        // Segment should be cleared
        assert_eq!(segment.state(), SegmentState::Free);
        assert_eq!(segment.live_items(), 0);
    }

    #[test]
    fn test_clear_with_bucket_id() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Set a bucket ID
        segment.set_bucket_id(42);
        assert_eq!(segment.bucket_id(), Some(42));

        // Add an item
        segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Clear the segment
        cache.segments().expire(seg_id, &cache);

        // Bucket ID should be cleared
        assert_eq!(segment.bucket_id(), None);
        assert_eq!(segment.state(), SegmentState::Free);
    }

    #[test]
    fn test_delete_item_basic() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();
        assert_eq!(segment.live_items(), 1);

        // Delete through high-level API
        let result = cache.segments().delete_item(&cache, seg_id, offset, b"key");
        assert_eq!(result, Ok(true));
        assert_eq!(segment.live_items(), 0);
    }

    #[test]
    fn test_delete_item_no_bucket_no_eager_removal() {
        // When segment has no bucket_id (test segment), deletion succeeds
        // but no eager removal is attempted
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        // Transition to Sealed state (would trigger eager removal if in bucket)
        segment.cas_metadata(
            SegmentState::Reserved,
            SegmentState::Sealed,
            None,
            None,
            cache.metrics(),
        );

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // Delete the item - segment becomes empty and sealed
        let result = cache.segments().delete_item(&cache, seg_id, offset, b"key");
        assert_eq!(result, Ok(true));
        assert_eq!(segment.live_items(), 0);

        // Segment should still be Sealed (no eager removal without bucket_id)
        assert_eq!(segment.state(), SegmentState::Sealed);
        assert_eq!(segment.bucket_id(), None);
    }

    #[tokio::test]
    async fn test_delete_item_with_bucket_eager_removal() {
        // When segment is in a TTL bucket and becomes empty, it should be removed
        let cache = Cache::new();

        // Insert item with TTL (this will link it in hashtable and place in TTL bucket)
        cache.set(
            b"testkey",
            b"testvalue",
            b"",
            Some(std::time::Duration::from_secs(60))
        ).await.unwrap();

        // Find the segment containing this item using the hashtable
        let (seg_id, _offset) = cache.hashtable()
            .get(b"testkey", cache.segments())
            .expect("Item should be in hashtable");
        let segment = cache.segments().get(seg_id).unwrap();

        assert_eq!(segment.live_items(), 1);
        assert_eq!(segment.state(), SegmentState::Live);
        assert!(segment.bucket_id().is_some());

        // Seal the segment (required for eager removal)
        segment.cas_metadata(
            SegmentState::Live,
            SegmentState::Sealed,
            None,
            None,
            cache.metrics(),
        );

        // Delete the item using the high-level API - this should trigger eager removal
        cache.delete(b"testkey").await.unwrap();

        // Segment should be removed and returned to free pool
        assert_eq!(segment.state(), SegmentState::Free);
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.bucket_id(), None);
    }

    #[tokio::test]
    async fn test_set_overwrite_with_bucket_eager_removal() {
        // When set() overwrites the last item in a sealed segment, it should be removed
        let cache = Cache::new();

        // Insert item with TTL (this will link it in hashtable and place in TTL bucket)
        cache.set(
            b"testkey",
            b"original_value",
            b"",
            Some(std::time::Duration::from_secs(60))
        ).await.unwrap();

        // Find the segment containing this item using the hashtable
        let (seg_id, _offset) = cache.hashtable()
            .get(b"testkey", cache.segments())
            .expect("Item should be in hashtable");
        let segment = cache.segments().get(seg_id).unwrap();

        assert_eq!(segment.live_items(), 1);
        assert_eq!(segment.state(), SegmentState::Live);
        assert!(segment.bucket_id().is_some());

        // Seal the segment (required for eager removal)
        segment.cas_metadata(
            SegmentState::Live,
            SegmentState::Sealed,
            None,
            None,
            cache.metrics(),
        );

        // Overwrite the item - this will create a new item in a different segment
        // and mark the old item as deleted, triggering eager removal
        cache.set(
            b"testkey",
            b"new_value",
            b"",
            Some(std::time::Duration::from_secs(60))
        ).await.unwrap();

        // Old segment should be removed and returned to free pool
        assert_eq!(segment.state(), SegmentState::Free);
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.bucket_id(), None);

        // Verify we can still get the new value
        let mut buffer = vec![0u8; 1024];
        let item = cache.get_with_buffer(b"testkey", &mut buffer).unwrap();
        assert_eq!(item.value(), b"new_value");
    }

    #[test]
    fn test_delete_item_already_deleted() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"", cache.metrics()).unwrap();

        // First deletion succeeds
        assert_eq!(cache.segments().delete_item(&cache, seg_id, offset, b"key"), Ok(true));

        // Second deletion returns false
        assert_eq!(cache.segments().delete_item(&cache, seg_id, offset, b"key"), Ok(false));
    }

    #[test]
    fn test_delete_item_wrong_key() {
        let cache = Cache::new();
        let seg_id = cache.segments().reserve(cache.metrics()).unwrap();
        let segment = cache.segments().get(seg_id).unwrap();

        let offset = segment.append_item(b"key1", b"value1", b"", cache.metrics()).unwrap();

        // Wrong key returns error
        let result = cache.segments().delete_item(&cache, seg_id, offset, b"key2");
        assert_eq!(result, Err(()));
        assert_eq!(segment.live_items(), 1);
    }
}
