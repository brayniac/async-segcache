use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};

/// Monotonically increasing counter
pub struct Counter(AtomicU64);

impl Counter {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn value(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Gauge that can increase or decrease
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn new() -> Self {
        Self(AtomicI64::new(0))
    }

    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add(&self, value: i64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    pub fn sub(&self, value: i64) {
        self.0.fetch_sub(value, Ordering::Relaxed);
    }

    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    pub fn value(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Per-cache metrics for observability and testing
///
/// Each Cache instance has its own set of metrics, allowing:
/// - Isolated testing with loom (no global state contamination)
/// - Multiple cache instances with independent metrics
/// - Clean metric validation in tests
pub struct CacheMetrics {
    // Segment lifecycle metrics
    pub segment_reserve: Counter,
    pub segment_release: Counter,
    pub segment_evict: Counter,
    pub segment_clear: Counter,

    // Item operations
    pub item_append: Counter,
    pub item_append_full: Counter,
    pub item_unlink: Counter,
    pub item_unlink_not_found: Counter,
    pub item_expire: Counter,

    // Hashtable operations
    pub hashtable_link: Counter,
    pub hashtable_link_cas_failed: Counter,
    pub hashtable_evict: Counter,

    // TTL bucket operations
    pub ttl_append_segment: Counter,
    pub ttl_append_segment_error: Counter,
    pub ttl_evict_head: Counter,
    pub ttl_evict_head_retry: Counter,
    pub ttl_evict_head_give_up: Counter,

    // CAS contention metrics
    pub cas_retry: Counter,
    pub cas_abort: Counter,

    // Current state gauges
    pub segments_free: Gauge,
    pub segments_live: Gauge,
    pub segments_sealed: Gauge,

    // Cache-wide item tracking
    pub items_live: Gauge,
    pub bytes_live: Gauge,
}

impl CacheMetrics {
    /// Create a new set of metrics for a cache instance
    pub fn new() -> Self {
        Self {
            segment_reserve: Counter::new(),
            segment_release: Counter::new(),
            segment_evict: Counter::new(),
            segment_clear: Counter::new(),
            item_append: Counter::new(),
            item_append_full: Counter::new(),
            item_unlink: Counter::new(),
            item_unlink_not_found: Counter::new(),
            item_expire: Counter::new(),
            hashtable_link: Counter::new(),
            hashtable_link_cas_failed: Counter::new(),
            hashtable_evict: Counter::new(),
            ttl_append_segment: Counter::new(),
            ttl_append_segment_error: Counter::new(),
            ttl_evict_head: Counter::new(),
            ttl_evict_head_retry: Counter::new(),
            ttl_evict_head_give_up: Counter::new(),
            cas_retry: Counter::new(),
            cas_abort: Counter::new(),
            segments_free: Gauge::new(),
            segments_live: Gauge::new(),
            segments_sealed: Gauge::new(),
            items_live: Gauge::new(),
            bytes_live: Gauge::new(),
        }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}
