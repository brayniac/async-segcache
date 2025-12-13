use async_segcache::{Cache, Duration};
use std::thread;
use std::time::Duration as StdDuration;

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
    let item = cache.get(key).unwrap();
    assert_eq!(item.value(), value);

    // Wait for expiration
    thread::sleep(StdDuration::from_secs(2));

    // Should not be able to get it after expiration
    let result = cache.get(key);
    assert!(result.is_err(), "Item should have expired");

    // Verify expiration metric was incremented
    assert!(cache.metrics().item_expire.value() > 0, "item_expire metric should be incremented");
}

#[tokio::test]
async fn test_ttl_no_expiration_without_ttl() {
    let cache = Cache::new();

    let key = b"test_key";
    let value = b"test_value";

    // Set item without TTL (should use max TTL)
    cache.set(key, value, &[], None).await.unwrap();

    // Wait a bit
    thread::sleep(StdDuration::from_secs(1));

    // Should still be able to get it
    let mut buffer = vec![0u8; 1024];
    let item = cache.get(key).unwrap();
    assert_eq!(item.value(), value);
}

#[tokio::test]
async fn test_ttl_expiration_multiple_items() {
    let cache = Cache::new();

    let short_ttl = Duration::from_secs(1);
    let long_ttl = Duration::from_secs(10);

    // Set item with short TTL
    cache.set(b"short", b"value1", &[], Some(short_ttl)).await.unwrap();

    // Set item with long TTL
    cache.set(b"long", b"value2", &[], Some(long_ttl)).await.unwrap();

    // Wait for short TTL to expire
    thread::sleep(StdDuration::from_secs(2));

    let mut buffer = vec![0u8; 1024];

    // Short TTL item should be expired
    assert!(cache.get(b"short").is_err());

    // Long TTL item should still be available
    let item = cache.get(b"long").unwrap();
    assert_eq!(item.value(), b"value2");
}
