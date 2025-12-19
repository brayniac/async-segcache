use std::hash::Hasher;
use std::hash::BuildHasher;
use std::hash::Hash;
use crate::util::*;
use ahash::RandomState;
use crate::sync::*;

pub struct Hashtable {
    pub(crate) hash_builder: Box<RandomState>,
    pub(crate) data: Vec<Hashbucket>,
    pub(crate) mask: u64,
}

impl Hashtable {
    pub fn new(power: u8) -> Self {
        if power < 4 {
            panic!("power too low");
        }

        // Use fixed seeds in tests for deterministic behavior, random seeds in production
        #[cfg(test)]
        let hash_builder = RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        );
        #[cfg(not(test))]
        let hash_builder = RandomState::new();

        let buckets = 1_u64 << power;
        let mask = buckets - 1;

        let mut data = Vec::with_capacity(buckets as usize);
        for _ in 0..(buckets as usize) {
            data.push(Hashbucket {
                info: AtomicU64::new(0),
                items: [
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                ],
            });
        }

        Self {
            hash_builder: Box::new(hash_builder),
            data,
            mask,
        }
    }

    /// Look up an item in the hashtable by key.
    ///
    /// Returns the (segment_id, offset) tuple if the item is found, None otherwise.
    ///
    /// # Process
    /// 1. Hash the key to get bucket index and tag
    /// 2. Search the 7 slots in the bucket for matching tags
    /// 3. For each matching tag, verify the actual key in the segment
    /// 4. Return the first verified match
    ///
    /// # Concurrent Safety
    /// - Safe to call concurrently with other get/insert/unlink operations
    /// - May return None if item is deleted during the lookup
    /// - May return stale location if item is moved/deleted after lookup
    ///
    /// # Parameters
    /// - `key`: The key to search for
    /// - `segments`: Reference to segments for key verification
    ///
    /// # Returns
    /// - `Some((segment_id, offset))` if found and key verified
    /// - `None` if not found or key verification failed
    pub fn get(&self, key: &[u8], segments: &crate::segments::Segments) -> Option<(u32, u32)> {
        // Hash the key
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Find primary bucket
        let bucket_index = (hash & self.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        // Search for the item in the bucket
        let bucket = &self.data[bucket_index];

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            // Skip empty slots
            if packed == 0 {
                continue;
            }

            // Check if tag matches
            if Hashbucket::item_tag(packed) == tag {
                let segment_id = Hashbucket::item_segment_id(packed);
                let offset = Hashbucket::item_offset(packed);

                // Verify the key actually matches by reading from segment
                if self.verify_key(key, segment_id, offset, segments) {
                    // Update frequency using ASFC algorithm for frequency-based eviction
                    // Skip update if frequency is already saturated (127) to avoid
                    // unnecessary RNG calls and CAS contention on hot items
                    let freq = Hashbucket::item_freq(packed);
                    if freq < 127 {
                        if let Some(new_packed) = Hashbucket::try_update_freq(packed, freq) {
                            // Best-effort update - if CAS fails, another thread updated it
                            let _ = slot.compare_exchange(
                                packed,
                                new_packed,
                                Ordering::Release,
                                Ordering::Relaxed,
                            );
                        }
                    }

                    return Some((segment_id, offset));
                }
                // Tag matched but key didn't - continue searching for another match
            }
        }

        None
    }

    /// Verify that a key matches the item at the given segment/offset, allowing deleted items.
    ///
    /// This is used during reserve_slot() to find existing entries even if they're deleted,
    /// since we can replace deleted items in the hashtable.
    fn verify_key_allow_deleted(&self, key: &[u8], segment_id: u32, offset: u32, segments: &crate::segments::Segments) -> bool {
        // Get the segment
        let segment = match segments.get(segment_id) {
            Some(seg) => seg,
            None => return false,
        };

        // Check if offset is within bounds
        if offset as usize + crate::item::ItemHeader::SIZE > segment.data_len() {
            return false;
        }

        // Read the item header
        let data_ptr = unsafe { segment.data_ptr().add(offset as usize) };
        let header = match crate::item::ItemHeader::try_from_bytes(unsafe {
            std::slice::from_raw_parts(data_ptr, crate::item::ItemHeader::SIZE)
        }) {
            Some(h) => h,
            None => {
                // Invalid header (bad magic bytes or checksum) - treat as not found
                return false;
            }
        };

        // DON'T check if deleted - we allow replacing deleted items

        // Check if the full item is within bounds
        let item_size = header.padded_size();
        if offset as usize + item_size > segment.data_len() {
            return false;
        }

        // Extract and compare the key
        let raw_item = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };
        let key_start = crate::item::ItemHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;

        if key_end > raw_item.len() {
            return false;
        }

        &raw_item[key_start..key_end] == key
    }

    /// Verify that a key matches the item at the given segment/offset.
    ///
    /// This is a helper for get() to check that a tag match is actually the right key
    /// (since tags are only 12 bits and can collide).
    ///
    /// # Optimizations
    /// - Early key length check before full comparison (avoids memcmp on length mismatch)
    /// - Caller should prefetch segment data before calling for best performance
    fn verify_key(&self, key: &[u8], segment_id: u32, offset: u32, segments: &crate::segments::Segments) -> bool {
        // Get the segment
        let segment = match segments.get(segment_id) {
            Some(seg) => seg,
            None => {
                #[cfg(feature = "loom")]
                {
                    // In loom, track when segment not found
                    eprintln!("verify_key: segment {} not found", segment_id);
                }
                return false;
            }
        };

        // Check if offset is within bounds
        if offset as usize + crate::item::ItemHeader::SIZE > segment.data_len() {
            #[cfg(feature = "loom")]
            {
                eprintln!("verify_key: offset {} out of bounds (header)", offset);
            }
            return false;
        }

        // Read the item header
        let data_ptr = unsafe { segment.data_ptr().add(offset as usize) };
        let header = match crate::item::ItemHeader::try_from_bytes(unsafe {
            std::slice::from_raw_parts(data_ptr, crate::item::ItemHeader::SIZE)
        }) {
            Some(h) => h,
            None => {
                // Invalid header (bad magic bytes or checksum) - treat as not found
                #[cfg(feature = "loom")]
                {
                    eprintln!("verify_key: invalid header at seg={}, off={}", segment_id, offset);
                }
                return false;
            }
        };

        // Check if item is deleted
        if header.is_deleted() {
            #[cfg(feature = "loom")]
            {
                use std::sync::atomic::Ordering;
                // Print which thread/context is hitting this
                eprintln!("verify_key: item at seg={}, off={} is deleted (checking key={:?})",
                    segment_id, offset, key);
            }
            return false;
        }

        // Early key length check - fast rejection before full memcmp
        // This is a significant optimization when tag collisions occur
        if header.key_len() as usize != key.len() {
            #[cfg(feature = "loom")]
            {
                eprintln!("verify_key: key length mismatch at seg={}, off={}: stored_len={}, search_len={}",
                    segment_id, offset, header.key_len(), key.len());
            }
            return false;
        }

        // Check if the full item is within bounds
        let item_size = header.padded_size();
        if offset as usize + item_size > segment.data_len() {
            #[cfg(feature = "loom")]
            {
                eprintln!("verify_key: offset {} + size {} out of bounds", offset, item_size);
            }
            return false;
        }

        // Extract and compare the key
        let raw_item = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };
        let key_start = crate::item::ItemHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;

        if key_end > raw_item.len() {
            #[cfg(feature = "loom")]
            {
                eprintln!("verify_key: key_end {} > item_len {}", key_end, raw_item.len());
            }
            return false;
        }

        let stored_key = &raw_item[key_start..key_end];
        let matches = stored_key == key;

        #[cfg(feature = "loom")]
        if !matches {
            eprintln!("verify_key: key mismatch at seg={}, off={}: stored={:?}, searching={:?}",
                segment_id, offset, stored_key, key);
        }

        matches
    }

    pub async fn reserve_and_link(&self, key: &[u8], value: &[u8], optional: &[u8], ttl: clocksource::coarse::Duration, cache: &impl crate::CacheOps) -> Result<(), ()> {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & self.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;
        let bucket = &self.data[bucket_index];

        // STEP 1: Scan for slot (no lock) - find existing entry or empty slot
        let mut target_slot_info: Option<(usize, u64)> = None; // (slot_idx, current_value)

        for (slot_idx, slot) in bucket.items.iter().enumerate() {
            let current = slot.load(Ordering::Acquire);

            if current == 0 {
                // Empty slot - remember first one
                if target_slot_info.is_none() {
                    target_slot_info = Some((slot_idx, 0));
                }
                continue;
            }

            // Check if tag matches
            if Hashbucket::item_tag(current) == tag {
                let seg_id = Hashbucket::item_segment_id(current);
                let offset = Hashbucket::item_offset(current);

                if self.verify_key_allow_deleted(key, seg_id, offset, cache.segments()) {
                    // Found existing entry with our key - will replace it
                    target_slot_info = Some((slot_idx, current));
                    break;
                }
            }
        }

        // Check if we have a slot available
        let (mut slot_idx, mut expected_value) = match target_slot_info {
            Some(info) => info,
            None => {
                cache.metrics().hashtable_full.increment();
                return Err(()); // Bucket full, no empty slots
            }
        };

        // Extract old location if we're replacing an existing entry
        let mut old_location = if expected_value != 0 {
            Some((
                Hashbucket::item_segment_id(expected_value),
                Hashbucket::item_offset(expected_value),
            ))
        } else {
            None
        };

        // STEP 2: Append item to segment ONCE (no lock held)
        let (segment_id, offset) = match cache.ttl_buckets().append_item(cache, key, value, optional, ttl).await {
            Some(result) => result,
            None => {
                cache.metrics().segment_alloc_fail.increment();
                return Err(()); // Out of memory
            }
        };

        // STEP 3: Try to CAS link into a slot - retry with different slots if we lose
        let new_packed = Hashbucket::pack_item(tag, 1, segment_id, offset);

        loop {
            match bucket.items[slot_idx].compare_exchange(
                expected_value,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // CAS succeeded! Item is now linked
                    cache.metrics().hashtable_link.increment();

                    // Mark old item deleted if this was an overwrite
                    if let Some((old_segment_id, old_offset)) = old_location {
                        if let Some(old_segment) = cache.segments().get(old_segment_id) {
                            let _ = old_segment.mark_deleted(old_offset, key, cache.metrics(), false);

                            // Check for eager removal - if old segment is now empty and sealed,
                            // remove it from the TTL bucket chain and return it to the free pool
                            let remaining_items = old_segment.live_items();
                            if remaining_items == 0 {
                                let state = old_segment.state();
                                if state == crate::segments::SegmentState::Sealed {
                                    // Old segment is empty and sealed - try to remove it from the bucket
                                    if let Some(bucket_id) = old_segment.bucket_id() {
                                        let bucket = cache.ttl_buckets().get_bucket_by_index(bucket_id);
                                        // Try to remove - don't fail the set if removal fails
                                        let _ = bucket.remove_segment(cache, old_segment_id, cache.metrics()).await;
                                    }
                                }
                            }
                        }
                    }

                    return Ok(());
                }
                Err(_actual) => {
                    // CAS failed - rescan for a new slot and retry with SAME appended item
                    let mut found_slot = false;

                    for (new_slot_idx, slot) in bucket.items.iter().enumerate() {
                        let current = slot.load(Ordering::Acquire);

                        if current == 0 {
                            // Empty slot - try this one
                            slot_idx = new_slot_idx;
                            expected_value = 0;
                            old_location = None;
                            found_slot = true;
                            break;
                        }

                        // Check if tag matches our key
                        if Hashbucket::item_tag(current) == tag {
                            let seg_id = Hashbucket::item_segment_id(current);
                            let offset_val = Hashbucket::item_offset(current);

                            if self.verify_key_allow_deleted(key, seg_id, offset_val, cache.segments()) {
                                // Found our key in another slot - replace it
                                slot_idx = new_slot_idx;
                                expected_value = current;
                                old_location = Some((seg_id, offset_val));
                                found_slot = true;
                                break;
                            }
                        }
                    }

                    if !found_slot {
                        // Bucket is full - mark our appended item deleted and fail
                        if let Some(our_segment) = cache.segments().get(segment_id) {
                            let _ = our_segment.mark_deleted(offset, key, cache.metrics(), false);
                        }
                        return Err(());
                    }
                    // Loop back to retry CAS with new slot
                }
            }
        }
    }

    /// Link an item into the hashtable.
    ///
    /// # Returns
    /// - `Ok(None)` if item was successfully linked with no replacement
    /// - `Ok(Some((old_seg_id, old_offset)))` if successfully linked and replaced existing item
    /// - `Err(())` if insertion failed (bucket full)
    pub(crate) fn link_item(&self, key: &[u8], segment_id: u32, offset: u32, segments: &crate::segments::Segments, metrics: &crate::metrics::CacheMetrics) -> Result<Option<(u32, u32)>, ()> {
        // Validate segment_id and offset fit in their bit fields
        // Offset is stored as offset/8 (20 bits), so max is 0xFFFFF * 8 = 8,388,600
        if segment_id > 0xFFFFFF {
            panic!("segment_id {} exceeds 24-bit limit", segment_id);
        }
        if offset > 0xFFFFF << 3 {
            panic!("offset {} exceeds limit (max {})", offset, 0xFFFFF << 3);
        }
        // Offset must be 8-byte aligned
        if offset & 0x7 != 0 {
            panic!("offset {} is not 8-byte aligned", offset);
        }

        // Hash the key
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Find primary bucket
        let bucket_index = (hash & self.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        // Pack the new item (initial frequency = 1)
        let new_packed = Hashbucket::pack_item(tag, 1, segment_id, offset);

        let bucket = &self.data[bucket_index];

        // First pass: check if this key already exists in the bucket
        // If so, atomically replace it to prevent duplicates
        for slot in &bucket.items {
            let current = slot.load(Ordering::Acquire);

            // Skip empty slots
            if current == 0 {
                continue;
            }

            // Check if tag matches
            if Hashbucket::item_tag(current) == tag {
                let old_segment_id = Hashbucket::item_segment_id(current);
                let old_offset = Hashbucket::item_offset(current);

                // Verify the key actually matches
                if self.verify_key(key, old_segment_id, old_offset, segments) {
                    // Found matching key - atomically replace it
                    match slot.compare_exchange(
                        current,
                        new_packed,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            metrics.hashtable_link.increment();
                            return Ok(Some((old_segment_id, old_offset)));
                        }
                        Err(_) => {
                            // CAS failed - slot was modified. Retry the whole operation
                            return self.link_item(key, segment_id, offset, segments, metrics);
                        }
                    }
                }
            }
        }

        // Key doesn't exist in bucket - try to find an empty slot
        for slot in &bucket.items {
            let current = slot.load(Ordering::Acquire);

            // Found empty slot - try to claim it
            if current == 0 {
                match slot.compare_exchange(
                    0,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        metrics.hashtable_link.increment();

                        // After successfully linking, scan other slots for duplicates
                        // This handles the race where another thread linked the same key
                        // into a different slot between our initial scan and our CAS
                        for other_slot in &bucket.items {
                            // Skip the slot we just filled
                            if std::ptr::eq(slot, other_slot) {
                                continue;
                            }

                            let other_packed = other_slot.load(Ordering::Acquire);
                            if other_packed == 0 {
                                continue;
                            }

                            // Check if this slot has the same key
                            if Hashbucket::item_tag(other_packed) == tag {
                                let other_seg_id = Hashbucket::item_segment_id(other_packed);
                                let other_offset = Hashbucket::item_offset(other_packed);

                                if self.verify_key(key, other_seg_id, other_offset, segments) {
                                    // Found duplicate! Keep trying to unlink it until we succeed
                                    // or the slot changes to a different key
                                    loop {
                                        let current = other_slot.load(Ordering::Acquire);
                                        if current == 0 {
                                            // Slot was already cleared by another thread
                                            // Don't return it as replaced since we didn't do the unlinking
                                            break;
                                        }

                                        // Check if it's still the same duplicate
                                        if Hashbucket::item_tag(current) != tag {
                                            // Slot changed to different tag - not our duplicate anymore
                                            break;
                                        }

                                        let curr_seg_id = Hashbucket::item_segment_id(current);
                                        let curr_offset = Hashbucket::item_offset(current);

                                        if curr_seg_id != other_seg_id || curr_offset != other_offset {
                                            // Slot changed to different item - check if it's still our key
                                            if !self.verify_key(key, curr_seg_id, curr_offset, segments) {
                                                // Not our key anymore
                                                break;
                                            }
                                            // Still our key but different location - update and retry
                                            // (another thread replaced it)
                                            // Just break - another thread will handle it
                                            break;
                                        }

                                        // Try to unlink it
                                        match other_slot.compare_exchange(
                                            current,
                                            0,
                                            Ordering::Release,
                                            Ordering::Acquire,
                                        ) {
                                            Ok(_) => {
                                                // Successfully unlinked the duplicate
                                                return Ok(Some((other_seg_id, other_offset)));
                                            }
                                            Err(_) => {
                                                // CAS failed - retry
                                                continue;
                                            }
                                        }
                                    }
                                    // If we broke out of the loop, continue scanning other slots
                                    continue;
                                }
                            }
                        }

                        return Ok(None);
                    }
                    Err(new_current) => {
                        // Slot was filled by another thread
                        // Check if it's our key (another thread inserted same key)
                        if new_current != 0 && Hashbucket::item_tag(new_current) == tag {
                            let new_seg_id = Hashbucket::item_segment_id(new_current);
                            let new_offset = Hashbucket::item_offset(new_current);
                            if self.verify_key(key, new_seg_id, new_offset, segments) {
                                // Another thread inserted our key - retry to replace it
                                return self.link_item(key, segment_id, offset, segments, metrics);
                            }
                        }
                        // Continue searching for another empty slot
                        continue;
                    }
                }
            }
        }

        // Second pass: check for empty slots again (may have been freed)
        for slot in &bucket.items {
            let current = slot.load(Ordering::Acquire);

            if current == 0 {
                match slot.compare_exchange(
                    0,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        metrics.hashtable_link.increment();

                        // After successfully linking, scan other slots for duplicates
                        // This handles the race where another thread linked the same key
                        // into a different slot between our initial scan and our CAS
                        for other_slot in &bucket.items {
                            // Skip the slot we just filled
                            if std::ptr::eq(slot, other_slot) {
                                continue;
                            }

                            let other_packed = other_slot.load(Ordering::Acquire);
                            if other_packed == 0 {
                                continue;
                            }

                            // Check if this slot has the same key
                            if Hashbucket::item_tag(other_packed) == tag {
                                let other_seg_id = Hashbucket::item_segment_id(other_packed);
                                let other_offset = Hashbucket::item_offset(other_packed);

                                if self.verify_key(key, other_seg_id, other_offset, segments) {
                                    // Found duplicate! Keep trying to unlink it until we succeed
                                    // or the slot changes to a different key
                                    loop {
                                        let current = other_slot.load(Ordering::Acquire);
                                        if current == 0 {
                                            // Slot was already cleared by another thread
                                            // Don't return it as replaced since we didn't do the unlinking
                                            break;
                                        }

                                        // Check if it's still the same duplicate
                                        if Hashbucket::item_tag(current) != tag {
                                            // Slot changed to different tag - not our duplicate anymore
                                            break;
                                        }

                                        let curr_seg_id = Hashbucket::item_segment_id(current);
                                        let curr_offset = Hashbucket::item_offset(current);

                                        if curr_seg_id != other_seg_id || curr_offset != other_offset {
                                            // Slot changed to different item - check if it's still our key
                                            if !self.verify_key(key, curr_seg_id, curr_offset, segments) {
                                                // Not our key anymore
                                                break;
                                            }
                                            // Still our key but different location - update and retry
                                            // (another thread replaced it)
                                            // Just break - another thread will handle it
                                            break;
                                        }

                                        // Try to unlink it
                                        match other_slot.compare_exchange(
                                            current,
                                            0,
                                            Ordering::Release,
                                            Ordering::Acquire,
                                        ) {
                                            Ok(_) => {
                                                // Successfully unlinked the duplicate
                                                return Ok(Some((other_seg_id, other_offset)));
                                            }
                                            Err(_) => {
                                                // CAS failed - retry
                                                continue;
                                            }
                                        }
                                    }
                                    // If we broke out of the loop, continue scanning other slots
                                    continue;
                                }
                            }
                        }

                        return Ok(None);
                    }
                    Err(new_current) => {
                        // Check if another thread inserted our key
                        if new_current != 0 && Hashbucket::item_tag(new_current) == tag {
                            let new_seg_id = Hashbucket::item_segment_id(new_current);
                            let new_offset = Hashbucket::item_offset(new_current);
                            if self.verify_key(key, new_seg_id, new_offset, segments) {
                                // Another thread inserted our key - retry to replace it
                                return self.link_item(key, segment_id, offset, segments, metrics);
                            }
                        }
                        continue;
                    }
                }
            }
        }

        // Bucket is full - fail the insertion
        Err(())
    }

    /// Unlink an item from the hash table.
    ///
    /// # Async Compatibility
    ///
    /// This function is designed to be async-friendly by using minimal backoff.
    /// However, for high-contention scenarios in async contexts, consider:
    /// 1. Using tokio::task::yield_now() between retries
    /// 2. Implementing an async variant that yields to the executor
    /// 3. Using a different strategy like message passing for deletions
    ///
    /// The current implementation uses bounded retries with minimal spin hints
    /// to avoid blocking the async executor thread.
    ///
    /// # Loom Test Coverage
    /// - `hashtable_unlink_concurrent` - Two threads unlinking (empty table)
    /// - Called by `clear` which is tested indirectly via TTL bucket eviction
    pub fn unlink_item(&self, key: &[u8], segment_id: u32, offset: u32, metrics: &crate::metrics::CacheMetrics) -> bool {
        // Hash the key
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Find primary bucket
        let bucket_index = (hash & self.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        // Search for the item
        let bucket = &self.data[bucket_index];

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            // Skip empty slots
            if packed == 0 {
                continue;
            }

            // Only retry if this is actually our item
            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_segment_id(packed) == segment_id
                && Hashbucket::item_offset(packed) == offset
            {
                // Use the standard CAS retry pattern
                let result = retry_cas_u64(
                    slot,
                    |current_packed| {
                        // Verify this is still our item
                        if current_packed == 0
                            || Hashbucket::item_tag(current_packed) != tag
                            || Hashbucket::item_segment_id(current_packed) != segment_id
                            || Hashbucket::item_offset(current_packed) != offset
                        {
                            return None; // Item changed or removed, abort
                        }

                        // Try to unlink by setting to 0
                        Some((0, true))
                    },
                    CasRetryConfig::default(),
                    metrics,
                );

                if let CasResult::Success(true) = result {
                    metrics.item_unlink.increment();
                    return true;
                }
            }
        }

        metrics.item_unlink_not_found.increment();
        false
    }

    /// Update the location of an item in the hashtable without changing its frequency.
    ///
    /// Used during segment compaction where an item is moved within the same segment.
    /// The tag and frequency are preserved, only the offset is updated.
    ///
    /// # Parameters
    /// - `key`: The key to update
    /// - `segment_id`: The segment ID (must match existing entry)
    /// - `old_offset`: The current offset (must match for CAS to succeed)
    /// - `new_offset`: The new offset within the segment (must fit in 20 bits)
    ///
    /// # Returns
    /// `true` if the update succeeded, `false` if item not found or changed
    pub fn update_item_location(&self, key: &[u8], segment_id: u32, old_offset: u32, new_offset: u32) -> bool {
        // Validate new offset fits (stored as offset/8 in 20 bits, so max is 0xFFFFF * 8)
        if new_offset > 0xFFFFF << 3 {
            return false;
        }
        // Offset must be 8-byte aligned
        if new_offset & 0x7 != 0 {
            return false;
        }
        // Hash the key
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Find primary bucket
        let bucket_index = (hash & self.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        let bucket = &self.data[bucket_index];

        // Search for existing entry with matching tag, segment_id, and old_offset
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_segment_id(packed) == segment_id
                && Hashbucket::item_offset(packed) == old_offset
            {
                // Found a matching entry - update the offset while preserving tag and frequency
                let freq = Hashbucket::item_freq(packed);
                let new_packed = Hashbucket::pack_item(tag, freq, segment_id, new_offset);

                // CAS to update - if it fails, the item was modified concurrently
                if slot
                    .compare_exchange(packed, new_packed, Ordering::Release, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
            }
        }

        false
    }
}

pub struct Hashbucket {
    pub(crate) info: AtomicU64,
    pub(crate) items: [AtomicU64; 7],
}

impl Hashbucket {
    #[allow(dead_code)]
    pub fn cas(&self) -> u32 {
        (self.info.load(Ordering::Acquire) >> 32) as u32
    }

    #[allow(dead_code)]
    pub fn timestamp(&self) -> u16 {
        self.info.load(Ordering::Acquire) as u16
    }

    /// Get a reference to the item slots
    #[allow(dead_code)]
    pub fn items(&self) -> &[AtomicU64; 7] {
        &self.items
    }

    /// Update the CAS value for this bucket
    #[allow(dead_code)]
    pub fn update_cas(&self, cas: u32, metrics: &crate::metrics::CacheMetrics) {
        let result = retry_cas_u64(
            &self.info,
            |current| Some((((cas as u64) << 32) | (current & 0xFFFFFFFF), ())),
            CasRetryConfig::default(),
            metrics,
        );

        assert!(
            matches!(result, CasResult::Success(())),
            "Failed to update CAS"
        );
    }

    /// Pack item information into a u64
    /// Layout: [12 bits tag][8 bits freq][24 bits segment_id][20 bits offset/8]
    ///
    /// Since items are 8-byte aligned, we store offset/8 to get 8x more addressable space.
    /// This allows offsets up to 8,388,600 bytes (~8MB) instead of ~1MB.
    pub fn pack_item(tag: u16, freq: u8, segment_id: u32, offset: u32) -> u64 {
        // Offset must be 8-byte aligned
        debug_assert!(offset & 0x7 == 0, "offset {} is not 8-byte aligned", offset);
        let tag_64 = (tag as u64 & 0xFFF) << 52;
        let freq_64 = (freq as u64 & 0xFF) << 44;
        let seg_64 = (segment_id as u64 & 0xFFFFFF) << 20;
        let off_64 = (offset >> 3) as u64 & 0xFFFFF; // Store offset/8
        tag_64 | freq_64 | seg_64 | off_64
    }

    /// Extract tag from packed item (12 bits)
    pub fn item_tag(packed: u64) -> u16 {
        (packed >> 52) as u16
    }

    /// Extract segment ID from packed item (24 bits)
    pub fn item_segment_id(packed: u64) -> u32 {
        ((packed >> 20) & 0xFFFFFF) as u32
    }

    /// Extract offset from packed item (20 bits stored, multiplied by 8)
    ///
    /// Since we store offset/8, we multiply by 8 to get the actual offset.
    pub fn item_offset(packed: u64) -> u32 {
        ((packed & 0xFFFFF) << 3) as u32 // Return offset * 8
    }

    /// Extract frequency from packed item (8 bits)
    pub(crate) fn item_freq(packed: u64) -> u8 {
        ((packed >> 44) & 0xFF) as u8
    }

    /// Update frequency in packed item using ASFC algorithm
    /// Returns new packed value with updated frequency
    #[cfg(test)]
    pub(crate) fn update_freq(packed: u64) -> u64 {
        let freq = Self::item_freq(packed);
        Self::try_update_freq(packed, freq).unwrap_or(packed)
    }

    /// Try to update frequency using ASFC algorithm.
    /// Returns Some(new_packed) if frequency should be incremented, None otherwise.
    ///
    /// This avoids RNG calls for hot items by using probabilistic increment:
    /// - Cold items (freq <= 16): Always increment
    /// - Hot items (freq > 16): Increment with probability 1/freq
    ///
    /// The caller should check freq < 127 before calling to avoid unnecessary work.
    pub(crate) fn try_update_freq(packed: u64, freq: u8) -> Option<u64> {
        // ASFC: Adaptive Software Frequency Counter
        // Caller should ensure freq < 127, but double-check
        if freq >= 127 {
            return None;
        }

        // Probabilistic increment:
        // - Always increment if freq <= 16 (cold items)
        // - For freq > 16, increment with probability 1/freq (hot items increment slower)
        let should_increment = if freq <= 16 {
            true
        } else {
            // Use thread-local RNG for better performance
            #[cfg(not(feature = "loom"))]
            let rand = {
                use rand::Rng;
                rand::thread_rng().r#gen::<u64>()
            };

            #[cfg(feature = "loom")]
            let rand = 0u64; // Deterministic for loom tests

            rand % (freq as u64) == 0
        };

        if should_increment {
            let new_freq = freq + 1;
            // Clear old freq bits and set new freq
            let freq_mask = 0xFF_u64 << 44;
            Some((packed & !freq_mask) | ((new_freq as u64) << 44))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segments::SegmentsBuilder;
    use crate::metrics::CacheMetrics;

    #[test]
    fn test_get_basic() {
        // Create hashtable and segments
        let hashtable = Hashtable::new(4); // 16 buckets
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Reserve a segment and append an item
        let seg_id = segments.reserve(&metrics).unwrap();
        let segment = segments.get(seg_id).unwrap();
        let offset = segment.append_item(b"test_key", b"test_value", b"", &metrics, false).unwrap();

        // Manually insert into hashtable for testing
        // Hash the key to get bucket and tag
        let mut hasher = hashtable.hash_builder.build_hasher();
        use std::hash::{Hash, Hasher};
        b"test_key".hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & hashtable.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        // Pack and store the item in first slot
        let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
        hashtable.data[bucket_index].items[0].store(packed, Ordering::Release);

        // Test get() - should find the item
        let result = hashtable.get(b"test_key", &segments);
        assert_eq!(result, Some((seg_id, offset)));
    }

    #[test]
    fn test_get_not_found() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();

        // Get non-existent key
        let result = hashtable.get(b"nonexistent", &segments);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_deleted_item() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Create and append item
        let seg_id = segments.reserve(&metrics).unwrap();
        let segment = segments.get(seg_id).unwrap();
        let offset = segment.append_item(b"deleted_key", b"value", b"", &metrics, false).unwrap();

        // Insert into hashtable
        let mut hasher = hashtable.hash_builder.build_hasher();
        use std::hash::{Hash, Hasher};
        b"deleted_key".hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & hashtable.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;
        let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
        hashtable.data[bucket_index].items[0].store(packed, Ordering::Release);

        // Mark item as deleted
        segment.mark_deleted(offset, b"deleted_key", &metrics, false).unwrap();

        // Get should return None for deleted item
        let result = hashtable.get(b"deleted_key", &segments);
        assert_eq!(result, None);
    }

    #[test]
    fn test_link_item_basic() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Reserve segment and append item
        let seg_id = segments.reserve(&metrics).unwrap();
        let segment = segments.get(seg_id).unwrap();
        let offset = segment.append_item(b"link_key", b"value", b"", &metrics, false).unwrap();

        // Link the item
        let result = hashtable.link_item(b"link_key", seg_id, offset, &segments, &metrics);
        assert!(result.is_ok(), "link_item should succeed");

        // Verify we can get it back
        let get_result = hashtable.get(b"link_key", &segments);
        assert_eq!(get_result, Some((seg_id, offset)));
    }

    #[test]
    fn test_link_item_bucket_full() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Reserve segment
        let seg_id = segments.reserve(&metrics).unwrap();

        // Find a bucket and fill all 7 slots
        // We'll use a fixed key to find its bucket
        let test_key = b"bucket_fill_key";

        // Hash to find the bucket
        let mut hasher = hashtable.hash_builder.build_hasher();
        use std::hash::{Hash, Hasher};
        test_key.hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & hashtable.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;

        // Fill all 7 slots with items (using 8-byte aligned offsets)
        for i in 0..7 {
            let packed = Hashbucket::pack_item(tag, (i + 1) as u8, seg_id, (i * 128) as u32);
            hashtable.data[bucket_index].items[i as usize].store(packed, Ordering::Release);
        }

        // Now try to link a new item - should fail because bucket is full
        let result = hashtable.link_item(test_key, seg_id, 1024, &segments, &metrics);
        assert!(result.is_err(), "link_item should fail when bucket is full");

        // Verify the new item is NOT in the bucket (all 7 slots should still have original items)
        let mut count = 0;
        for (i, slot) in hashtable.data[bucket_index].items.iter().enumerate() {
            let packed = slot.load(Ordering::Acquire);
            assert_ne!(packed, 0, "Slot {} should still be occupied", i);
            assert_ne!(Hashbucket::item_offset(packed), 1024, "New item should not be in slot {}", i);
            count += 1;
        }
        assert_eq!(count, 7, "All 7 slots should still be occupied");
    }

    #[test]
    #[should_panic(expected = "segment_id")]
    fn test_link_item_segment_id_overflow() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // segment_id is 24 bits, max is 0xFFFFFF
        let _ = hashtable.link_item(b"key", 0x1000000, 0, &segments, &metrics);
    }

    #[test]
    #[should_panic(expected = "offset")]
    fn test_link_item_offset_overflow() {
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // offset is stored as offset/8 in 20 bits, so max is 0xFFFFF * 8 = 8,388,600
        // Use an offset that exceeds this limit (and is 8-byte aligned)
        let _ = hashtable.link_item(b"key", 0, 0x800008, &segments, &metrics);
    }

    #[test]
    fn test_get_key_mismatch() {
        // Test that tag collision is handled correctly
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Create item with key1
        let seg_id = segments.reserve(&metrics).unwrap();
        let segment = segments.get(seg_id).unwrap();
        let offset = segment.append_item(b"key1", b"value1", b"", &metrics, false).unwrap();

        // Insert into hashtable with key1's hash
        let mut hasher = hashtable.hash_builder.build_hasher();
        use std::hash::{Hash, Hasher};
        b"key1".hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & hashtable.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;
        let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
        hashtable.data[bucket_index].items[0].store(packed, Ordering::Release);

        // Try to get with key2 (wrong key but might have same tag by chance)
        let result = hashtable.get(b"key2", &segments);
        // Should return None because key doesn't match
        assert_eq!(result, None);
    }

    #[test]
    fn test_asfc_frequency_tracking() {
        // Test that ASFC frequency tracking works on get operations
        let hashtable = Hashtable::new(4);
        let segments = SegmentsBuilder::new().build().unwrap();
        let metrics = CacheMetrics::new();

        // Create and insert an item
        let seg_id = segments.reserve(&metrics).unwrap();
        let segment = segments.get(seg_id).unwrap();
        let offset = segment.append_item(b"hot_key", b"value", b"", &metrics, false).unwrap();

        // Insert into hashtable with initial freq=0
        let mut hasher = hashtable.hash_builder.build_hasher();
        use std::hash::{Hash, Hasher};
        b"hot_key".hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_index = (hash & hashtable.mask) as usize;
        let tag = ((hash >> 32) & 0xFFF) as u16;
        let packed = Hashbucket::pack_item(tag, 0, seg_id, offset);
        hashtable.data[bucket_index].items[0].store(packed, Ordering::Release);

        // Initial frequency should be 0
        let initial_packed = hashtable.data[bucket_index].items[0].load(Ordering::Acquire);
        let initial_freq = Hashbucket::item_freq(initial_packed);
        assert_eq!(initial_freq, 0);

        // Perform multiple gets to increase frequency
        // With freq <= 16, ASFC always increments
        for _ in 0..20 {
            let result = hashtable.get(b"hot_key", &segments);
            assert_eq!(result, Some((seg_id, offset)));
        }

        // Check that frequency increased
        let final_packed = hashtable.data[bucket_index].items[0].load(Ordering::Acquire);
        let final_freq = Hashbucket::item_freq(final_packed);

        // Frequency should have increased (at least to some degree)
        // With deterministic loom behavior or actual random, freq should be > 0
        assert!(final_freq > initial_freq, "Frequency should increase with accesses: initial={}, final={}", initial_freq, final_freq);

        // Frequency should not exceed 127 (saturation limit)
        assert!(final_freq <= 127);
    }

    #[test]
    fn test_asfc_frequency_saturation() {
        // Test that frequency saturates at 127 (using 8-byte aligned offset)
        let packed = Hashbucket::pack_item(100, 127, 1, 1024);
        assert_eq!(Hashbucket::item_freq(packed), 127);

        // Updating a saturated frequency should not change it
        let updated = Hashbucket::update_freq(packed);
        assert_eq!(Hashbucket::item_freq(updated), 127);
    }

    #[test]
    fn test_asfc_update_freq() {
        // Test the update_freq function directly (using 8-byte aligned offset)
        let packed = Hashbucket::pack_item(100, 5, 1, 1024);
        assert_eq!(Hashbucket::item_freq(packed), 5);

        // For freq <= 16, ASFC always increments
        let updated = Hashbucket::update_freq(packed);
        let new_freq = Hashbucket::item_freq(updated);
        assert_eq!(new_freq, 6, "Frequency should always increment when <= 16");

        // Verify other fields unchanged
        assert_eq!(Hashbucket::item_tag(updated), 100);
        assert_eq!(Hashbucket::item_segment_id(updated), 1);
        assert_eq!(Hashbucket::item_offset(updated), 1024);
    }
}
