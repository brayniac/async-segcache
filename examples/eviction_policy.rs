use async_segcache::{Cache, CacheBuilder, EvictionPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Create cache with random eviction (default)
    let cache_random = Cache::new();
    println!("Default policy: {:?}", cache_random.eviction_policy());

    // Create cache with no automatic eviction
    let cache_manual = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024) // 8MB
        .segment_size(1024 * 1024)   // 1MB segments
        .eviction_policy(EvictionPolicy::None)
        .build().unwrap();
    println!("Manual policy: {:?}", cache_manual.eviction_policy());

    // Create cache with Random FIFO eviction
    let cache_fifo = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .eviction_policy(EvictionPolicy::RandomFifo)
        .build().unwrap();
    println!("Random FIFO policy: {:?}", cache_fifo.eviction_policy());

    // Create cache with CTE (Closest To Expiration) eviction
    let cache_cte = CacheBuilder::new()
        .heap_size(8 * 1024 * 1024)
        .segment_size(1024 * 1024)
        .eviction_policy(EvictionPolicy::Cte)
        .build().unwrap();
    println!("CTE policy: {:?}", cache_cte.eviction_policy());

    // Add some data to the FIFO cache with different TTLs
    println!("\nAdding items with different TTLs to Random FIFO cache...");
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let ttl = Duration::from_secs(60 + (i * 10)); // Varying TTLs
        let _ = cache_fifo.set(key.as_bytes(), value.as_bytes(), &[], Some(ttl));
    }

    // Add items to CTE cache with staggered expiration times
    println!("\nAdding items with staggered TTLs to CTE cache...");
    for i in 0..10 {
        let key = format!("cte_key_{}", i);
        let value = format!("cte_value_{}", i);
        // Items with increasing TTLs: 10s, 20s, 30s, ...
        let ttl = Duration::from_secs(10 + (i * 10));
        let _ = cache_cte.set(key.as_bytes(), value.as_bytes(), &[], Some(ttl));
    }

    // Try CTE eviction - should evict the segment with shortest remaining TTL
    match cache_cte.evict_cte_segment().await {
        Ok(segment_id) => println!("CTE evicted segment: {}", segment_id),
        Err(e) => println!("CTE eviction result: {}", e),
    }

    // Try Random FIFO eviction
    match cache_fifo.evict_random_fifo_segment().await {
        Ok(segment_id) => println!("Random FIFO evicted segment: {}", segment_id),
        Err(e) => println!("Random FIFO eviction result: {}", e),
    }

    // Try Random eviction on empty cache
    match cache_random.evict_random_segment().await {
        Ok(segment_id) => println!("Random evicted segment: {}", segment_id),
        Err(e) => println!("Random eviction result: {}", e),
    }

    println!("\nEviction policies configured successfully!");
}
