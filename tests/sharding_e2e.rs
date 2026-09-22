//! End-to-end daemon tests for Phase 2 (sharded layout & multi-engine).
//!
//! These spawn the real `snerdmq` binary and speak raw JSON IPC over
//! stdin/stdout, exactly like the language SDKs do. Run `cargo build` first
//! (or `cargo test` which builds the bin); tests skip gracefully if the
//! binary is missing.

use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(unix)]
extern crate libc;

fn daemon_binary() -> Option<PathBuf> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target/debug/snerdmq");
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

struct DaemonHandle {
    child: Child,
    /// Captured stderr lines, for diagnostics when a wait times out.
    stderr_lines: Arc<Mutex<Vec<String>>>,
    /// Stdout receiver for waiting on messages.
    stdout_rx: Option<mpsc::Receiver<String>>,
}

impl DaemonHandle {
    fn spawn(bin: &PathBuf, storage: &std::path::Path, max_shards: &str) -> Self {
        Self::spawn_with_offset(bin, storage, max_shards, 0)
    }

    fn spawn_with_offset(bin: &PathBuf, storage: &std::path::Path, max_shards: &str, offset_secs: i64) -> Self {
        Self::spawn_with_env(bin, storage, max_shards, offset_secs, None)
    }

    fn spawn_with_env(
        bin: &PathBuf,
        storage: &std::path::Path,
        max_shards: &str,
        offset_secs: i64,
        extra_env: Option<Vec<(&str, &str)>>,
    ) -> Self {
        let mut cmd = Command::new(bin);
        cmd.arg(storage.as_os_str())
            .env("SNERD_MAX_SHARDS", max_shards)
            .env("SNERD_TEST_CLOCK_OFFSET_SECS", offset_secs.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
            
        if let Some(env_vars) = extra_env {
            for (k, v) in env_vars {
                cmd.env(k, v);
            }
        }
            
        let mut child = cmd.spawn().expect("failed to spawn daemon");
        let stderr_lines = Arc::new(Mutex::new(Vec::new()));
        let stderr = stderr_lines.clone();
        if let Some(err) = child.stderr.take() {
            std::thread::spawn(move || {
                for line in BufReader::new(err).lines() {
                    match line {
                        Ok(l) => {
                            eprintln!("[daemon stderr] {}", l);
                            stderr.lock().unwrap().push(l);
                        }
                        Err(_) => return,
                    }
                }
            });
        }
        
        let (tx, rx) = mpsc::channel::<String>();
        if let Some(stdout) = child.stdout.take() {
            std::thread::spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines() {
                    match line {
                        Ok(l) => {
                            eprintln!("[daemon stdout] {}", l);
                            if tx.send(l).is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
            });
        }
        
        DaemonHandle { child, stderr_lines, stdout_rx: Some(rx) }
    }

    fn send(&mut self, msg: &Value) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{}", msg).unwrap();
        stdin.flush().unwrap();
    }

    /// Read stdout lines until one parses as JSON containing `needle`, or timeout.
    fn wait_for(&mut self, needle: &str, timeout: Duration) -> Option<Value> {
        let rx = self.stdout_rx.as_ref().unwrap();
        let deadline = Instant::now() + timeout;
        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            let line = match rx.recv_timeout(remaining) {
                Ok(l) => l,
                Err(_) => break,
            };
            if !line.contains(needle) {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                return Some(v);
            }
        }
        None
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        self.kill();
    }
}

#[test]
fn legacy_storage_auto_adopts_and_executes_task() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => {
            eprintln!("skipping: daemon binary not built");
            return;
        }
    };

    // Legacy layout: tasks/tasks.log with one pending task + root .lock.
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");
    fs::create_dir_all(storage.join("tasks")).unwrap();
    let task = json!({
        "taskId": "legacy-task-1",
        "taskType": "echo",
        "taskData": "hello",
        "maxRetries": 0,
        "retryAfterHours": 0.0,
        "retryCount": 0,
        "retryAfterTime": "2020-01-01T00:00:00Z",
        "executeAt": "2020-01-01T00:00:00Z"
    });
    fs::write(storage.join("tasks/tasks.log"), task.to_string() + "\n").unwrap();
    fs::write(storage.join(".lock"), "0").unwrap();

    let mut daemon = DaemonHandle::spawn(&bin, &storage, "4");

    // Register a handler; the task inherited from the legacy log should be
    // dispatched from shard-0 within the processor interval.
    daemon.send(&json!({"action": "register", "task_type": "echo"}));
    let exec = daemon.wait_for("\"execute\"", Duration::from_secs(20));
    assert!(
        exec.is_some(),
        "expected the legacy task to be dispatched after auto-adopt"
    );
    let exec = exec.unwrap();
    assert_eq!(exec["task_id"], "legacy-task-1");

    // Complete it so the daemon doesn't retry forever.
    daemon.send(&json!({"action": "result", "task_id": "legacy-task-1", "status": "success"}));

    // Layout on disk: adopted.
    daemon.kill();
    assert!(storage.join("membership.json").exists());
    assert!(storage.join("shard-0/tasks/tasks.log").exists());
    assert!(!storage.join("tasks/tasks.log").exists());
}

#[test]
fn enqueue_routes_and_acks_on_fresh_sharded_storage() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => {
            eprintln!("skipping: daemon binary not built");
            return;
        }
    };

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");
    let mut daemon = DaemonHandle::spawn(&bin, &storage, "2");

    daemon.send(&json!({"action": "register", "task_type": "echo"}));
    daemon.send(&json!({
        "action": "enqueue",
        "task_id": "route-me-1",
        "task_type": "echo",
        "task_data": "x",
        "max_retries": 0,
        "retry_after_hours": 0.0
    }));

    let ack = daemon.wait_for("route-me-1", Duration::from_secs(15));
    assert!(ack.is_some(), "expected enqueue ack");
    let ack = ack.unwrap();
    assert_eq!(ack["message"], "Enqueued successfully");

    // Exactly one shard dir received the task (routing is single-target).
    daemon.kill();
    let membership: Value =
        serde_json::from_str(&fs::read_to_string(storage.join("membership.json")).unwrap())
            .unwrap();
    assert_eq!(membership["shards"], 1); // SNERD_SHARDS unset → default 1

    let shard0_log = storage.join("shard-0/tasks/tasks.log");
    assert!(shard0_log.exists());
    let content = fs::read_to_string(&shard0_log).unwrap();
    assert!(content.contains("route-me-1"));
}

#[test]
fn second_daemon_becomes_standby_and_rejects_enqueues() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => {
            eprintln!("skipping: daemon binary not built");
            return;
        }
    };

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    // Daemon A claims the only shard.
    let mut daemon_a = DaemonHandle::spawn(&bin, &storage, "1");
    daemon_a.send(&json!({"action": "register", "task_type": "echo"}));
    let reg_ack = daemon_a.wait_for("Registered handler", Duration::from_secs(15));
    assert!(reg_ack.is_some(), "daemon A never acked registration");

    // Wait until A's claim is durable on disk so B deterministically boots
    // into a fully-claimed queue (membership claims happen before the IPC
    // loop, so a visible claim means A owns the shard).
    let claim_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let claimed = fs::read_to_string(storage.join("membership.json"))
            .map(|c| c.contains("lease_expiry"))
            .unwrap_or(false);
        if claimed {
            break;
        }
        assert!(
            Instant::now() < claim_deadline,
            "daemon A never persisted a claim; stderr: {:?}",
            daemon_a.stderr_lines.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Daemon B starts on the same storage — all shards taken → standby.
    let mut daemon_b = DaemonHandle::spawn(&bin, &storage, "1");
    daemon_b.send(&json!({"action": "register", "task_type": "echo"}));
    daemon_b.send(&json!({
        "action": "enqueue",
        "task_id": "rejected-1",
        "task_type": "echo",
        "task_data": "x",
        "max_retries": 0,
        "retry_after_hours": 0.0
    }));

    let resp = daemon_b.wait_for("rejected-1", Duration::from_secs(15));
    assert!(
        resp.is_some(),
        "expected zero-shard rejection; daemon B stderr: {:?}",
        daemon_b.stderr_lines.lock().unwrap()
    );
    let resp = resp.unwrap();
    assert_eq!(resp["action"], "error");
    assert_eq!(resp["message"], "[Snerd] No shards owned by this instance");
}

// ── Phase 3 tests ─────────────────────────────────────────────────────────────

#[test]
fn validate_reports_healthy_queue() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => { eprintln!("skipping: daemon binary not built"); return; }
    };

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    // Boot a daemon briefly to create the sharded layout, then kill it.
    let mut daemon = DaemonHandle::spawn(&bin, &storage, "1");
    daemon.send(&json!({"action": "register", "task_type": "echo"}));
    daemon.wait_for("Registered handler", Duration::from_secs(10));
    daemon.kill();

    // Run `snerdmq validate <dir>` — should exit 0.
    let status = Command::new(&bin)
        .arg("validate")
        .arg(&storage)
        .status()
        .expect("failed to run validate");
    assert!(status.success(), "validate should exit 0 on a healthy queue");
}

#[test]
fn add_shards_increases_shard_count() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => { eprintln!("skipping: daemon binary not built"); return; }
    };

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    // Boot a daemon briefly to create a 1-shard layout, then kill it.
    let mut daemon = DaemonHandle::spawn(&bin, &storage, "1");
    daemon.send(&json!({"action": "register", "task_type": "echo"}));
    daemon.wait_for("Registered handler", Duration::from_secs(10));
    daemon.kill();

    // Release any stale lock files before add-shards (daemon was killed).
    // The shard .lock inode stays; add-shards doesn't touch it.

    // Run `snerdmq add-shards 2 <dir>` — should exit 0.
    let status = Command::new(&bin)
        .arg("add-shards")
        .arg("2")
        .arg(&storage)
        .status()
        .expect("failed to run add-shards");
    assert!(status.success(), "add-shards should exit 0");

    // membership.json shards field should now be 3.
    let raw = fs::read_to_string(storage.join("membership.json")).unwrap();
    let m: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(m["shards"], 3, "shard count should be 1 + 2 = 3");

    // New shard directories should exist.
    assert!(storage.join("shard-1").exists());
    assert!(storage.join("shard-2").exists());
}

#[test]
fn graceful_drain_on_sigterm() {
    let bin = match daemon_binary() {
        Some(b) => b,
        None => { eprintln!("skipping: daemon binary not built"); return; }
    };

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    let mut daemon = DaemonHandle::spawn(&bin, &storage, "1");
    daemon.send(&json!({"action": "register", "task_type": "echo"}));

    // Wait for the registration ack so the engine is fully up.
    let ack = daemon.wait_for("Registered handler", Duration::from_secs(10));
    assert!(ack.is_some(), "daemon never acked registration");

    // Send SIGTERM to the daemon process.
    #[cfg(unix)]
    {
        let pid = daemon.child.id();
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM); }

        // Give the daemon up to 10s to drain and exit.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match daemon.child.try_wait() {
                Ok(Some(_)) => break, // exited
                Ok(None) => {
                    if Instant::now() > deadline {
                        panic!("daemon did not exit within 10s after SIGTERM");
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("try_wait error: {}", e),
            }
        }

        // After drain + exit, membership.json should have no active claims.
        let raw = fs::read_to_string(storage.join("membership.json"))
            .expect("membership.json missing after drain");
        let m: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let claims = m["claims"].as_object();
        let has_claims = claims.map(|c| !c.is_empty()).unwrap_or(false);
        assert!(!has_claims, "claims should be released after graceful shutdown; got: {}", m);
    }
    #[cfg(not(unix))]
    {
        eprintln!("skipping SIGTERM test on non-unix platform");
    }
}

// ── Phase 7 tests ─────────────────────────────────────────────────────────────

#[test]
fn clock_offset_simulation_prevents_split_brain() {
    let bin = match daemon_binary() { Some(b) => b, None => return };
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    let mut daemon_a = DaemonHandle::spawn(&bin, &storage, "1");
    daemon_a.send(&json!({"action": "register", "task_type": "echo"}));
    daemon_a.wait_for("Registered handler", Duration::from_secs(10)).unwrap();
    std::thread::sleep(Duration::from_millis(500));

    // Daemon B boots with a +3s clock offset. Margin is 5s, so it should NOT claim the lease.
    let mut daemon_b = DaemonHandle::spawn_with_offset(&bin, &storage, "1", 3);
    daemon_b.send(&json!({"action": "register", "task_type": "echo"}));
    daemon_b.send(&json!({
        "action": "enqueue", "task_id": "test-split-brain", "task_type": "echo",
        "task_data": "x", "max_retries": 0, "retry_after_hours": 0.0
    }));

    let resp = daemon_b.wait_for("test-split-brain", Duration::from_secs(10)).unwrap();
    assert_eq!(resp["action"], "error");
    assert_eq!(resp["message"], "[Snerd] No shards owned by this instance");
}

#[test]
fn stalled_but_alive_owner_is_downgraded_by_flock() {
    let bin = match daemon_binary() { Some(b) => b, None => return };
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    let mut daemon_a = DaemonHandle::spawn(&bin, &storage, "1");
    daemon_a.send(&json!({"action": "register", "task_type": "echo"}));
    // We can't use wait_for multiple times, so we wait for the last event.
    // Pause A's heartbeat.
    daemon_a.send(&json!({"action": "test_pause_heartbeat"}));
    daemon_a.wait_for("Heartbeat paused", Duration::from_secs(10)).unwrap();
    std::thread::sleep(Duration::from_millis(500));

    // B boots with +20s clock, thinks A's lease is lapsed.
    let mut daemon_b = DaemonHandle::spawn_with_offset(&bin, &storage, "1", 40);
    daemon_b.send(&json!({"action": "register", "task_type": "echo"}));
    daemon_b.send(&json!({
        "action": "enqueue", "task_id": "test-stall", "task_type": "echo",
        "task_data": "x", "max_retries": 0, "retry_after_hours": 0.0
    }));

    // B should revert the claim due to A's flock, and remain in standby.
    let resp = daemon_b.wait_for("test-stall", Duration::from_secs(10)).unwrap();
    assert_eq!(resp["action"], "error");
    assert_eq!(resp["message"], "[Snerd] No shards owned by this instance");
}

#[test]
#[cfg(unix)]
fn sigkill_mid_drain_guarantees_at_least_once_delivery() {
    let bin = match daemon_binary() { Some(b) => b, None => return };
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    let mut daemon_a = DaemonHandle::spawn(&bin, &storage, "1");
    daemon_a.send(&json!({"action": "register", "task_type": "long_task"}));
    daemon_a.send(&json!({
        "action": "enqueue", "task_id": "crash-task", "task_type": "long_task",
        "task_data": "x", "max_retries": 0, "retry_after_hours": 0.0
    }));
    
    let exec = daemon_a.wait_for("\"execute\"", Duration::from_secs(10)).unwrap();
    assert_eq!(exec["task_id"], "crash-task");

    // SIGKILL daemon A
    let pid = daemon_a.child.id();
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL); }
    let _ = daemon_a.child.wait(); 
    
    // Spawn daemon B with +20s offset so it immediately takes over A's lapsed lease.
    let mut daemon_b = DaemonHandle::spawn_with_offset(&bin, &storage, "1", 40);
    daemon_b.send(&json!({"action": "register", "task_type": "long_task"}));
    
    // Daemon B should adopt the shard, find the incomplete task, and redispatch it!
    let re_exec = daemon_b.wait_for("\"execute\"", Duration::from_secs(15)).unwrap();
    assert_eq!(re_exec["task_id"], "crash-task");
    
    daemon_b.kill();
}

#[test]
fn worker_pools_isolate_concurrency() {
    let bin = match daemon_binary() { Some(b) => b, None => return };
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join(".snerdata");

    // Spawn daemon with default pool = 1, urgent pool = 10
    let env_vars = vec![("SNERD_POOLS", "default=1,urgent=10")];
    let mut daemon = DaemonHandle::spawn_with_env(&bin, &storage, "1", 0, Some(env_vars));

    daemon.send(&json!({"action": "register", "task_type": "slow_task"}));
    daemon.send(&json!({"action": "register", "task_type": "urgent_task"}));

    // Enqueue 2 slow tasks. Since default pool has size 1, only 1 should execute.
    for i in 1..=2 {
        daemon.send(&json!({
            "action": "enqueue", "task_id": format!("slow-{}", i), "task_type": "slow_task",
            "task_data": "x", "max_retries": 0, "retry_after_hours": 0.0
        }));
    }

    // Wait for the first slow task to start executing
    let exec_slow = daemon.wait_for("\"execute\"", Duration::from_secs(10)).unwrap();
    assert!(exec_slow["task_id"].as_str().unwrap().starts_with("slow-"));
    
    // We intentionally DO NOT send a "result" for the slow task, simulating it blocking the single default worker forever.

    // Enqueue 5 urgent tasks, specifying the "urgent" pool
    for i in 1..=5 {
        daemon.send(&json!({
            "action": "enqueue", "task_id": format!("urgent-{}", i), "task_type": "urgent_task",
            "task_data": "x", "max_retries": 0, "retry_after_hours": 0.0,
            "pool": "urgent"
        }));
    }

    // Wait for all 5 urgent tasks to execute! They should NOT be blocked by the stalled slow task.
    let mut urgent_seen = 0;
    while urgent_seen < 5 {
        let exec = daemon.wait_for("\"execute\"", Duration::from_secs(5)).unwrap();
        // It could be slow-2 if it wasn't isolated, but slow-2 is blocked!
        assert!(exec["task_id"].as_str().unwrap().starts_with("urgent-"));
        urgent_seen += 1;
        
        // Complete the urgent task immediately
        daemon.send(&json!({
            "action": "result", "task_id": exec["task_id"], "status": "success"
        }));
    }

    assert_eq!(urgent_seen, 5);

    daemon.kill();
}
