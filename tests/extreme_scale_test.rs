use std::process::{Command, Stdio};
use std::time::Instant;
use serde_json::json;
use std::io::{Write, BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn daemon_binary() -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target/release/snerdmq");
    path
}

fn run_benchmark(shards: u32, count: usize) -> f64 {
    let bin = daemon_binary();
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    if shards > 1 {
        // Boot briefly to create membership.json
        let mut d = Command::new(&bin).arg(&storage).env("SNERD_MAX_SHARDS", "1").spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
        d.kill().unwrap();
        let _ = d.wait();
        
        // Add shards
        Command::new(&bin).arg("add-shards").arg((shards - 1).to_string()).arg(&storage).status().unwrap();
    }

    let mut daemon = Command::new(&bin)
        .arg(storage.as_os_str())
        .env("SNERD_MAX_SHARDS", shards.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = daemon.stdin.take().unwrap();
    
    let acks = Arc::new(AtomicUsize::new(0));
    let acks_clone = acks.clone();
    let stdout = daemon.stdout.take().unwrap();
    
    // Read stdout in background to count acks
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().flatten() {
            if line.contains("\"ack\"") {
                acks_clone.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let start = Instant::now();
    
    let mut batch = String::with_capacity(1024 * 1024);
    for i in 0..count {
        let msg = json!({
            "action": "enqueue",
            "task_id": format!("t-{}", i),
            "task_type": "bench",
            "task_data": "x",
            "max_retries": 0,
            "retry_after_hours": 0.0
        });
        batch.push_str(&msg.to_string());
        batch.push('\n');

        if i % 100 == 0 {
            stdin.write_all(batch.as_bytes()).unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        stdin.write_all(batch.as_bytes()).unwrap();
    }
    stdin.flush().unwrap();

    // Wait for all tasks to be enqueued and acked
    let timeout = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while acks.load(Ordering::Relaxed) < count {
        if std::time::Instant::now() > timeout {
            panic!("Timeout waiting for acks! Got {}/{}", acks.load(Ordering::Relaxed), count);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let duration = start.elapsed();
    daemon.kill().unwrap();
    
    (count as f64) / duration.as_secs_f64()
}

#[test]
fn extreme_scale_benchmark() {
    let bin = daemon_binary();
    if !bin.exists() {
        eprintln!("skipping: run `cargo build --release` first");
        return;
    }

    let count = 1000; 
    
    let rate_1 = run_benchmark(1, count);
    println!("=== 1 Shard Rate: {:.0} tasks/sec ===", rate_1);
    
    let rate_10 = run_benchmark(10, count);
    println!("=== 10 Shards Rate: {:.0} tasks/sec ===", rate_10);
}
