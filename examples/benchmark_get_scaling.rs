/// Benchmark for measuring get operation throughput scaling across thread counts
///
/// This benchmark tests pure read workloads to measure:
/// - Throughput (operations per second) for each thread count
/// - How read performance scales as threads increase
/// - Read path concurrency efficiency
///
/// Run with: cargo run --release --example benchmark_get_scaling

use async_segcache::{Cache, CacheBuilder};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const BENCHMARK_DURATION_SECS: u64 = 5;
const KEY_SPACE: u64 = 100_000; // Number of unique keys to use
const VALUE_SIZE: usize = 128;
const TTL_SECS: u32 = 3600;

/// Generate a key based on ID
fn generate_key(key_id: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&key_id.to_le_bytes());
    key
}

/// Populate cache with keys
async fn populate_cache(cache: &Cache, num_keys: u64) {
    let value = vec![0xAB; VALUE_SIZE];
    let ttl = Duration::from_secs(TTL_SECS.into());

    for i in 0..num_keys {
        let key = generate_key(i);
        let _ = cache.set(&key, &value, &[], Some(ttl));
    }
}

/// Run benchmark for a specific number of threads
async fn run_benchmark(num_threads: usize) -> (u64, Duration) {
    let cache = Arc::new(CacheBuilder::new()
        .hashtable_power(18) // 256K buckets
        .build());

    // Populate cache first
    populate_cache(&cache, KEY_SPACE).await;

    let done = Arc::new(AtomicBool::new(false));
    let ops_counter = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Spawn worker threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let done = Arc::clone(&done);
        let ops_counter = Arc::clone(&ops_counter);

        let handle = tokio::task::spawn(async move {
            let mut local_ops = 0u64;
            let mut counter = 0u64;
            let mut buffer = vec![0u8; 4096]; // Buffer for get operations

            while !done.load(Ordering::Relaxed) {
                let key_id = (thread_id as u64 * 1_000_000 + counter) % KEY_SPACE;
                let key = generate_key(key_id);

                // Perform get operation
                let _ = cache.get(&key);

                local_ops += 1;
                counter += 1;

                // Periodically update global counter
                if local_ops % 1000 == 0 {
                    ops_counter.fetch_add(1000, Ordering::Relaxed);
                    local_ops = 0;
                }
            }

            // Final update
            ops_counter.fetch_add(local_ops, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    // Run for specified duration
    thread::sleep(Duration::from_secs(BENCHMARK_DURATION_SECS));
    done.store(true, Ordering::Relaxed);

    // Wait for all threads to finish
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = ops_counter.load(Ordering::Relaxed);

    (total_ops, elapsed)
}

/// Run latency benchmark for a specific number of threads
async fn run_latency_benchmark(num_threads: usize) -> Vec<Duration> {
    let cache = Arc::new(CacheBuilder::new()
        .hashtable_power(18)
        .build());

    // Populate cache first
    populate_cache(&cache, KEY_SPACE).await;

    let done = Arc::new(AtomicBool::new(false));

    // Collect latency samples
    let samples = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Spawn worker threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let done = Arc::clone(&done);
        let samples = Arc::clone(&samples);

        let handle = tokio::task::spawn(async move {
            let mut counter = 0u64;
            let mut local_samples = Vec::new();
            let mut buffer = vec![0u8; 4096]; // Buffer for get operations

            while !done.load(Ordering::Relaxed) {
                let key_id = (thread_id as u64 * 1_000_000 + counter) % KEY_SPACE;
                let key = generate_key(key_id);

                // Measure latency
                let start = Instant::now();
                let _ = cache.get(&key);
                let latency = start.elapsed();

                local_samples.push(latency);
                counter += 1;

                // Sample only every 100th operation to keep overhead low
                if counter % 100 != 0 {
                    local_samples.clear();
                }
            }

            // Merge local samples
            if !local_samples.is_empty() {
                samples.lock().unwrap().extend(local_samples);
            }
        });

        handles.push(handle);
    }

    // Run for shorter duration for latency test
    thread::sleep(Duration::from_secs(2));
    done.store(true, Ordering::Relaxed);

    // Wait for all threads to finish
    for handle in handles {
        handle.await.unwrap();
    }

    let mut all_samples = samples.lock().unwrap().clone();
    all_samples.sort();
    all_samples
}

/// Calculate percentile from sorted samples
fn percentile(samples: &[Duration], p: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((samples.len() as f64) * p / 100.0) as usize;
    let idx = idx.min(samples.len() - 1);
    samples[idx]
}

#[tokio::main]
async fn main() {
    println!("Segcache2 Get Operation Scaling Benchmark");
    println!("==========================================");
    println!("Benchmark duration: {} seconds per thread count", BENCHMARK_DURATION_SECS);
    println!("Key space: {} unique keys", KEY_SPACE);
    println!("Value size: {} bytes", VALUE_SIZE);
    println!();

    // Warmup
    println!("Warming up...");
    let _ = run_benchmark(1);
    println!();

    // Throughput benchmark
    println!("Throughput Benchmark");
    println!("--------------------");
    println!("{:>8} | {:>15} | {:>15} | {:>10}",
             "Threads", "Total Ops", "Ops/sec", "Speedup");
    println!("{:-<8}-+-{:-<15}-+-{:-<15}-+-{:-<10}", "", "", "", "");

    let mut baseline_ops_per_sec = 0.0;
    let mut results = Vec::new();

    for num_threads in 1..=8 {
        let (total_ops, elapsed) = run_benchmark(num_threads).await;
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

        if num_threads == 1 {
            baseline_ops_per_sec = ops_per_sec;
        }

        let speedup = ops_per_sec / baseline_ops_per_sec;

        results.push((num_threads, total_ops, ops_per_sec, speedup));

        println!("{:>8} | {:>15} | {:>15.0} | {:>10.2}x",
                 num_threads, total_ops, ops_per_sec, speedup);
    }

    println!();

    // Latency benchmark
    println!("Latency Benchmark (2 second sample)");
    println!("------------------------------------");
    println!("{:>8} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
             "Threads", "p50", "p90", "p95", "p99", "p999");
    println!("{:-<8}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<10}",
             "", "", "", "", "", "");

    for num_threads in [1, 2, 4, 8] {
        let samples = run_latency_benchmark(num_threads).await;

        if !samples.is_empty() {
            let p50 = percentile(&samples, 50.0);
            let p90 = percentile(&samples, 90.0);
            let p95 = percentile(&samples, 95.0);
            let p99 = percentile(&samples, 99.0);
            let p999 = percentile(&samples, 99.9);

            println!("{:>8} | {:>9.1}us | {:>9.1}us | {:>9.1}us | {:>9.1}us | {:>9.1}us",
                     num_threads,
                     p50.as_nanos() as f64 / 1000.0,
                     p90.as_nanos() as f64 / 1000.0,
                     p95.as_nanos() as f64 / 1000.0,
                     p99.as_nanos() as f64 / 1000.0,
                     p999.as_nanos() as f64 / 1000.0);
        }
    }

    println!();

    // Summary
    println!("Summary");
    println!("-------");
    let max_speedup = results.iter().map(|(_, _, _, s)| s).fold(0.0f64, |a, b| a.max(*b));
    let optimal_threads = results.iter().max_by(|(_, _, ops1, _), (_, _, ops2, _)| {
        ops1.partial_cmp(ops2).unwrap()
    }).map(|(t, _, _, _)| t).unwrap_or(&1);

    println!("Maximum speedup: {:.2}x (at {} threads)", max_speedup, optimal_threads);
    if results.len() >= 8 {
        println!("Scaling efficiency at 8 threads: {:.1}%",
                 (results[7].3 / 8.0) * 100.0);
    }
}
