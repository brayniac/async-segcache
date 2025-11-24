use async_segcache::Cache;
use std::time::Duration;

#[tokio::test]
async fn test_proactive_expiration() {
    let cache = Cache::new();

    // Insert items with short TTL (1 second)
    cache.set(b"key1", b"value1", &[], Some(Duration::from_secs(1))).await.unwrap();
    cache.set(b"key2", b"value2", &[], Some(Duration::from_secs(1))).await.unwrap();
    cache.set(b"key3", b"value3", &[], Some(Duration::from_secs(1))).await.unwrap();

    // Verify items are accessible
    let mut buffer = vec![0u8; 1024];
    assert!(cache.get(b"key1", &mut buffer).is_ok());
    assert!(cache.get(b"key2", &mut buffer).is_ok());
    assert!(cache.get(b"key3", &mut buffer).is_ok());

    // Wait for expiration
    std::thread::sleep(Duration::from_secs(2));

    // Proactively expire segments
    let expired_count = cache.expire_segments();
    println!("Expired {} segments", expired_count);

    // Items should now be gone
    assert!(cache.get(b"key1", &mut buffer).is_err(), "key1 should be expired");
    assert!(cache.get(b"key2", &mut buffer).is_err(), "key2 should be expired");
    assert!(cache.get(b"key3", &mut buffer).is_err(), "key3 should be expired");

    // Verify expiration metric was incremented
    assert!(cache.metrics().item_expire.value() > 0 || expired_count > 0,
            "Either item_expire metric or expired_count should be positive");
}

#[tokio::test]
async fn test_proactive_expiration_preserves_live_items() {
    let cache = Cache::new();

    // Insert items with short TTL
    cache.set(b"short1", b"value1", &[], Some(Duration::from_secs(1))).await.unwrap();
    cache.set(b"short2", b"value2", &[], Some(Duration::from_secs(1))).await.unwrap();

    // Insert items with long TTL
    cache.set(b"long1", b"value3", &[], Some(Duration::from_secs(60))).await.unwrap();
    cache.set(b"long2", b"value4", &[], Some(Duration::from_secs(60))).await.unwrap();

    // Wait for short TTL items to expire
    std::thread::sleep(Duration::from_secs(2));

    // Proactively expire segments
    cache.expire_segments();

    let mut buffer = vec![0u8; 1024];

    // Short TTL items should be expired
    assert!(cache.get(b"short1", &mut buffer).is_err());
    assert!(cache.get(b"short2", &mut buffer).is_err());

    // Long TTL items should still be accessible
    assert!(cache.get(b"long1", &mut buffer).is_ok());
    assert!(cache.get(b"long2", &mut buffer).is_ok());
}
