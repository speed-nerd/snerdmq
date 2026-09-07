pub mod file_store;
pub mod membership;
pub mod protocol;
pub mod queue;
pub mod rate_limiter;
pub mod sharding;
pub mod task;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::fs;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tokio::sync::{RwLock, Semaphore, oneshot};

use crate::file_store::FileStore;
use crate::membership::{
    claimable_shards, owner_id, skew_margin, MembershipStore, RENEW_INTERVAL_SECS,
};
use crate::queue::{MaxRetryHandler, SnerdQueue, TaskHandler};
use crate::rate_limiter::RateLimiter;
use crate::sharding::{resolve_layout, route_shard, shard_dir, try_lock_shard, ShardLock};
use crate::task::RetryableTask;
type PendingExecutions = Arc<RwLock<HashMap<String, oneshot::Sender<Result<(), String>>>>>;
use crate::protocol::{IncomingMessage, OutgoingMessage};

/// Max shards one instance claims. Env `SNERD_MAX_SHARDS`, default 1.
const DEFAULT_MAX_SHARDS: usize = 1;
/// Standby retry interval when an instance owns no shards.
const STANDBY_RETRY_SECS: u64 = 10;
/// Engine scan interval — unchanged from pre-sharding behavior.
const PROCESSOR_INTERVAL: Duration = Duration::from_secs(2);
/// Default drain timeout on graceful shutdown.
const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 30;

/// Metrics counters for the daemon
struct Metrics {
    total_enqueued: AtomicU64,
    total_executed: AtomicU64,
    total_failed: AtomicU64,
    total_dlq: AtomicU64,
    start_time: Instant,
}

/// One claimed shard: its engine plus the flock handle proving ownership.
struct ShardEngine {
    key: String,
    queue: Arc<SnerdQueue>,
    /// Held (not read) for the engine's lifetime — dropping releases the lock.
    _lock: ShardLock,
}

/// A handler pair registered by an SDK, replayed onto every shard engine.
struct RegisteredHandler {
    task_type: String,
    handler: TaskHandler,
    max_retry: MaxRetryHandler,
}

/// The sharded runtime: membership client + owned shard engines.
struct Runtime {
    dir: PathBuf,
    store: MembershipStore,
    owner: String,
    max_shards: usize,
    skew: chrono::Duration,
    /// Shared worker budget across ALL shard engines (100 concurrent tasks).
    semaphore: Arc<Semaphore>,
    engines: RwLock<Vec<Arc<ShardEngine>>>,
    registered: RwLock<Vec<RegisteredHandler>>,
    /// Set to true when shutdown has been initiated.
    shutting_down: AtomicBool,
}

impl Runtime {
    fn new(dir: PathBuf, max_shards: usize, max_workers: usize) -> Self {
        let store = MembershipStore::new(&dir);
        Self {
            dir,
            store,
            owner: owner_id(),
            max_shards,
            skew: skew_margin(),
            semaphore: Arc::new(Semaphore::new(max_workers)),
            engines: RwLock::new(Vec::new()),
            registered: RwLock::new(Vec::new()),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Claim free/lapsed shards from membership, then take each shard's flock.
    /// A failed flock acquisition reverts the membership entry (zombie claim)
    /// before trying the next candidate.
    fn acquire_shards(&self) -> membership::Result<Vec<(String, ShardLock)>> {
        let now = chrono::Utc::now();
        let membership = self.store.load()?;
        let candidates = claimable_shards(&membership, now, self.skew, &self.owner);

        let mut claimed = Vec::new();
        for shard in candidates {
            if claimed.len() >= self.max_shards {
                break;
            }
            match self.store.claim(&shard, &self.owner, chrono::Utc::now(), self.skew)? {
                membership::ClaimOutcome::Claimed
                | membership::ClaimOutcome::Renewed
                | membership::ClaimOutcome::TakenOver { .. } => {}
            }
            match try_lock_shard(&self.dir, &shard)? {
                Some(lock) => claimed.push((shard, lock)),
                None => {
                    // Zombie claim: membership said free but the flock is held.
                    // Roll back so membership never disagrees with the locks.
                    eprintln!(
                        "[Snerd] Shard '{}' flock held by another process; reverting claim.",
                        shard
                    );
                    self.store.revert_claim(&shard, &self.owner)?;
                }
            }
        }
        Ok(claimed)
    }

    /// Build engines for freshly claimed shards, replay registered handlers
    /// onto them, and publish them for routing.
    async fn start_engines(&self, claimed: Vec<(String, ShardLock)>) -> std::io::Result<()> {
        let mut new_engines = Vec::new();
        for (shard, lock) in claimed {
            new_engines.push(self.build_engine(&shard, lock).await?);
        }
        let mut engines = self.engines.write().await;
        engines.extend(new_engines);

        // Emit Membership event to SDKs
        if let Ok(m) = self.store.load() {
            let owned_keys: Vec<String> = engines.iter().map(|e| e.key.clone()).collect();
            let msg = OutgoingMessage::Membership {
                queue: m.queue,
                shards: m.shards,
                owned: owned_keys,
                version: m.version,
            };
            println!("{}", serde_json::to_string(&msg).unwrap());
        }

        Ok(())
    }

    async fn build_engine(&self, shard: &str, lock: ShardLock) -> std::io::Result<Arc<ShardEngine>> {
        let sdir = shard_dir(&self.dir, shard);
        let tasks_log = sdir.join("tasks").join("tasks.log");
        let file_store = FileStore::new(&tasks_log)?;
        let rate_limiter = RateLimiter::new(&tasks_log);
        let queue = Arc::new(SnerdQueue::new_with_semaphore(
            &format!("snerdmq-{}", shard),
            file_store,
            rate_limiter,
            Arc::clone(&self.semaphore),
        ));

        // Replay every previously registered handler onto the new engine.
        let registered = self.registered.read().await;
        for entry in registered.iter() {
            queue
                .register_task_handler_arc(&entry.task_type, Arc::clone(&entry.handler))
                .await;
            queue
                .register_max_retry_handler_arc(&entry.task_type, Arc::clone(&entry.max_retry))
                .await;
        }
        drop(registered);

        queue.start_processor(PROCESSOR_INTERVAL).await;
        Ok(Arc::new(ShardEngine {
            key: shard.to_string(),
            queue,
            _lock: lock,
        }))
    }

    /// Store a handler pair and register it on all currently owned engines.
    async fn register_handler(
        &self,
        task_type: &str,
        handler: TaskHandler,
        max_retry: MaxRetryHandler,
    ) {
        self.registered.write().await.push(RegisteredHandler {
            task_type: task_type.to_string(),
            handler: Arc::clone(&handler),
            max_retry: Arc::clone(&max_retry),
        });
        for engine in self.engines.read().await.iter() {
            engine
                .queue
                .register_task_handler_arc(task_type, Arc::clone(&handler))
                .await;
            engine
                .queue
                .register_max_retry_handler_arc(task_type, Arc::clone(&max_retry))
                .await;
        }
    }

    /// Count the total number of in-flight tasks across all owned shard engines.
    async fn total_executing(&self) -> usize {
        let mut total = 0;
        for engine in self.engines.read().await.iter() {
            total += engine.queue.executing_tasks.lock().unwrap().len();
        }
        total
    }

    /// Graceful shutdown: pause all shards, drain in-flight tasks, release claims.
    async fn shutdown(&self, drain_timeout: Duration) {
        self.shutting_down.store(true, Ordering::Release);

        // Pause all engines: stops new task dispatch and enqueue acceptance.
        {
            let engines = self.engines.read().await;
            for engine in engines.iter() {
                engine.queue.paused.store(true, Ordering::Release);
                eprintln!("[Snerd] Shard '{}' paused for drain.", engine.key);
            }
        }

        // Wait for in-flight tasks to finish, up to drain_timeout.
        let deadline = Instant::now() + drain_timeout;
        loop {
            if self.total_executing().await == 0 {
                break;
            }
            if Instant::now() >= deadline {
                eprintln!("[Snerd] Drain timeout exceeded; releasing claims with in-flight tasks.");
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Release claims from membership.json.
        let engines = self.engines.read().await;
        for engine in engines.iter() {
            match self.store.release(&engine.key, &self.owner) {
                Ok(()) => eprintln!("[Snerd] Shard '{}' claim released.", engine.key),
                Err(e) => eprintln!("[Snerd] WARNING: failed to release shard '{}': {:?}", engine.key, e),
            }
        }
        eprintln!("[Snerd] Shutdown complete.");
    }
}

// ─── CLI subcommands ─────────────────────────────────────────────────────────

/// `snerdmq add-shards <n> <queue-dir>` — add N more shards to an existing queue.
fn cmd_add_shards(n: u32, queue_dir: &str) -> std::io::Result<()> {
    let dir = PathBuf::from(queue_dir);
    fs::create_dir_all(&dir)?;
    let store = MembershipStore::new(&dir);
    store.with_lock_held(|| -> std::io::Result<()> {
        let mut m = store.load_unlocked().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
        })?;
        let old_count = m.shards;
        let new_count = old_count + n;
        // Create new shard directories.
        for i in old_count..new_count {
            let sdir = shard_dir(&dir, &crate::membership::shard_key(i));
            fs::create_dir_all(sdir.join("tasks"))?;
        }
        m.shards = new_count;
        m.version += 1;
        // Write via temp-file + rename (crash-safe).
        store.write_membership_atomic(&m).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
        })?;
        eprintln!(
            "[Snerd] Added {} shard(s) to '{}'. Total shards: {} → {}.",
            n, queue_dir, old_count, new_count
        );
        Ok(())
    })
}

/// `snerdmq validate <queue-dir>` — health report, exits 1 if degraded.
fn cmd_validate(queue_dir: &str) -> bool {
    let dir = PathBuf::from(queue_dir);
    let store = MembershipStore::new(&dir);

    let m = match store.load() {
        Ok(m) => m,
        Err(crate::membership::MembershipError::NotInitialized) => {
            eprintln!("[Snerd] validate: '{}' is not a sharded queue (no membership.json).", queue_dir);
            return false;
        }
        Err(e) => {
            eprintln!("[Snerd] validate: failed to read membership.json: {:?}", e);
            return false;
        }
    };

    println!("[Snerd] validate: queue='{}' shards={} version={}", m.queue, m.shards, m.version);

    let now = chrono::Utc::now();
    let skew = skew_margin();
    let mut healthy = true;

    for i in 0..m.shards {
        let key = crate::membership::shard_key(i);
        let sdir = shard_dir(&dir, &key);

        // Check shard directory exists.
        if !sdir.exists() {
            eprintln!("[Snerd] validate: MISSING shard directory '{}'.", key);
            healthy = false;
        }

        // Check claim state.
        match m.claims.get(&key) {
            None => println!("[Snerd] validate: {} — free (unclaimed)", key),
            Some(claim) => {
                if crate::membership::is_lapsed(claim, now, skew) {
                    eprintln!(
                        "[Snerd] validate: {} — LAPSED claim (owner={}, expired={})",
                        key, claim.owner, claim.lease_expiry
                    );
                    healthy = false;
                } else {
                    println!(
                        "[Snerd] validate: {} — active (owner={}, expires={})",
                        key, claim.owner, claim.lease_expiry
                    );
                }
            }
        }
    }

    // Check for orphaned shard directories (dirs that exist but aren't in membership).
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("shard-") {
                let idx: Option<u32> = name.strip_prefix("shard-").and_then(|s| s.parse().ok());
                if let Some(i) = idx {
                    if i >= m.shards {
                        eprintln!("[Snerd] validate: ORPHANED shard directory '{}' (not in membership).", name);
                        healthy = false;
                    }
                }
            }
        }
    }

    if healthy {
        println!("[Snerd] validate: OK — queue is healthy.");
    } else {
        eprintln!("[Snerd] validate: DEGRADED — see warnings above.");
    }

    healthy
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // ── CLI subcommand dispatch ──────────────────────────────────────────────
    if args.len() >= 2 {
        match args[1].as_str() {
            "add-shards" => {
                if args.len() < 4 {
                    eprintln!("Usage: snerdmq add-shards <n> <queue-dir>");
                    std::process::exit(2);
                }
                let n: u32 = args[2].parse().unwrap_or_else(|_| {
                    eprintln!("Error: <n> must be a positive integer");
                    std::process::exit(2);
                });
                match cmd_add_shards(n, &args[3]) {
                    Ok(()) => std::process::exit(0),
                    Err(e) => { eprintln!("[Snerd] add-shards failed: {}", e); std::process::exit(1); }
                }
            }
            "validate" => {
                if args.len() < 3 {
                    eprintln!("Usage: snerdmq validate <queue-dir>");
                    std::process::exit(2);
                }
                let healthy = cmd_validate(&args[2]);
                std::process::exit(if healthy { 0 } else { 1 });
            }
            _ => {} // fall through to daemon mode
        }
    }

    // ── Daemon mode ──────────────────────────────────────────────────────────
    let storage_dir_str = args.get(1)
        .cloned()
        .unwrap_or_else(|| ".snerdata".to_string());

    // Create storage directory if it doesn't exist
    let storage_dir = std::path::PathBuf::from(&storage_dir_str);
    fs::create_dir_all(&storage_dir).expect("Failed to create storage directory");

    let requested_shards: u32 = std::env::var("SNERD_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let max_shards: usize = std::env::var("SNERD_MAX_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SHARDS);
    let max_workers: usize = std::env::var("SNERD_MAX_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let drain_timeout_secs: u64 = std::env::var("SNERD_DRAIN_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_DRAIN_TIMEOUT_SECS);

    // Boot-time layout detection: join / auto-adopt / fresh init.
    // membership.json is authoritative once present.
    let shard_count = match resolve_layout(&storage_dir, "snerdmq-daemon", requested_shards) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("[Snerd] ERROR: {}", e);
            std::process::exit(1);
        }
    };
    eprintln!(
        "[Snerd] Queue '{}' has {} shard(s); this instance will claim up to {}.",
        storage_dir_str, shard_count, max_shards
    );

    let pending_executions: PendingExecutions = Arc::new(RwLock::new(HashMap::new()));
    let metrics = Arc::new(Metrics {
        total_enqueued: AtomicU64::new(0),
        total_executed: AtomicU64::new(0),
        total_failed: AtomicU64::new(0),
        total_dlq: AtomicU64::new(0),
        start_time: Instant::now(),
    });

    let runtime = Arc::new(Runtime::new(storage_dir.clone(), max_shards, max_workers));

    // Claim shards and spin up their engines.
    match runtime.acquire_shards() {
        Ok(claimed) => {
            if claimed.is_empty() {
                eprintln!("[Snerd] All shards claimed by other daemons; starting as standby (retrying every {}s).", STANDBY_RETRY_SECS);
            } else {
                let keys: Vec<String> = claimed.iter().map(|(s, _)| s.clone()).collect();
                if let Err(e) = runtime.start_engines(claimed).await {
                    eprintln!("[Snerd] ERROR: failed to start shard engines: {}", e);
                    std::process::exit(1);
                }
                eprintln!("[Snerd] Owning shard(s): {}", keys.join(", "));
            }
        }
        Err(e) => {
            eprintln!("[Snerd] ERROR: failed to claim shards: {:?}", e);
            std::process::exit(1);
        }
    }

    // ── Heartbeat thread: renew leases + detect lease-loss ──────────────────
    {
        let rt = Arc::clone(&runtime);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(RENEW_INTERVAL_SECS));
            timer.tick().await; // first tick fires immediately
            loop {
                timer.tick().await;
                if rt.shutting_down.load(Ordering::Acquire) {
                    return;
                }
                let engines = rt.engines.read().await;
                for engine in engines.iter() {
                    // Skip already-paused engines.
                    if engine.queue.paused.load(Ordering::Acquire) {
                        continue;
                    }
                    match rt.store.renew(&engine.key, &rt.owner, chrono::Utc::now()) {
                        Ok(()) => {} // lease refreshed, engine stays Active
                        Err(crate::membership::MembershipError::LeaseLost { .. }) => {
                            eprintln!(
                                "[Snerd] Lease lost on shard '{}'; transitioning to Paused.",
                                engine.key
                            );
                            engine.queue.paused.store(true, Ordering::Release);
                        }
                        Err(e) => {
                            eprintln!("[Snerd] WARNING: heartbeat renew error on '{}': {:?}", engine.key, e);
                        }
                    }
                }
            }
        });
    }

    // ── Standby retry loop ───────────────────────────────────────────────────
    {
        let rt = Arc::clone(&runtime);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(STANDBY_RETRY_SECS));
            timer.tick().await; // first tick completes immediately
            loop {
                timer.tick().await;
                if rt.shutting_down.load(Ordering::Acquire) {
                    return;
                }
                if !rt.engines.read().await.is_empty() {
                    return; // no longer standby
                }
                match rt.acquire_shards() {
                    Ok(claimed) if !claimed.is_empty() => {
                        let keys: Vec<String> = claimed.iter().map(|(s, _)| s.clone()).collect();
                        if let Err(e) = rt.start_engines(claimed).await {
                            eprintln!("[Snerd] ERROR: standby claim failed to start engines: {}", e);
                            continue;
                        }
                        eprintln!("[Snerd] Standby promoted; now owning shard(s): {}", keys.join(", "));
                        return;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[Snerd] Standby claim retry failed: {:?}", e);
                    }
                }
            }
        });
    }

    // ── Graceful shutdown signal watcher ─────────────────────────────────────
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    {
        let rt = Arc::clone(&runtime);
        let flag = Arc::clone(&shutdown_flag);
        let drain = Duration::from_secs(drain_timeout_secs);
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).unwrap();
                let mut sigint = signal(SignalKind::interrupt()).unwrap();
                tokio::select! {
                    _ = sigterm.recv() => eprintln!("[Snerd] SIGTERM received; draining..."),
                    _ = sigint.recv()  => eprintln!("[Snerd] SIGINT received; draining..."),
                };
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await.ok();
                eprintln!("[Snerd] Ctrl-C received; draining...");
            }
            flag.store(true, Ordering::Release);
            rt.shutdown(drain).await;
            std::process::exit(0);
        });
    }

    // ── IPC loop (stdin/stdout) ───────────────────────────────────────────────
    let stdin_stream = stdin();
    let mut reader = BufReader::new(stdin_stream).lines();

    while let Ok(Some(line)) = reader.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        // Bail out of the IPC loop if shutdown has been requested.
        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }

        let msg_res: Result<IncomingMessage, _> = serde_json::from_str(&line);
        match msg_res {
            Ok(IncomingMessage::Register { task_type }) => {
                let pending_clone = pending_executions.clone();
                let metrics_clone = metrics.clone();
                let t_type = task_type.clone();

                let handler: TaskHandler = Arc::new(move |task: RetryableTask| {
                    let pending = pending_clone.clone();
                    let t_type = t_type.clone();
                    let met = metrics_clone.clone();
                    Box::pin(async move {
                        let (tx, rx) = oneshot::channel();
                        pending.write().await.insert(task.task_id.clone(), tx);
                        let out_msg = OutgoingMessage::Execute {
                            task_id: task.task_id.clone(),
                            task_type: t_type,
                            task_data: task.task_data.clone(),
                            max_execution_seconds: task.max_execution_seconds,
                        };
                        println!("{}", serde_json::to_string(&out_msg).unwrap());

                        let rx_result = if let Some(secs) = task.max_execution_seconds {
                            match tokio::time::timeout(std::time::Duration::from_secs(secs), rx).await {
                                Ok(res) => res,
                                Err(_) => {
                                    pending.write().await.remove(&task.task_id);
                                    return Err(format!("Task execution timed out after {} seconds", secs));
                                }
                            }
                        } else {
                            rx.await
                        };

                        match rx_result {
                            Ok(res) => {
                                match &res {
                                    Ok(_) => { met.total_executed.fetch_add(1, Ordering::Relaxed); }
                                    Err(_) => { met.total_failed.fetch_add(1, Ordering::Relaxed); }
                                }
                                res
                            }
                            Err(e) => {
                                met.total_failed.fetch_add(1, Ordering::Relaxed);
                                Err(e.to_string())
                            }
                        }
                    })
                });

                let t_type_dlq = task_type.clone();
                let metrics_dlq = metrics.clone();
                let max_retry: MaxRetryHandler = Arc::new(move |task: RetryableTask| {
                    let t_type = t_type_dlq.clone();
                    let met = metrics_dlq.clone();
                    Box::pin(async move {
                        met.total_dlq.fetch_add(1, Ordering::Relaxed);
                        let out_msg = OutgoingMessage::MaxRetriesReached {
                            task_id: task.task_id.clone(),
                            task_type: t_type,
                            task_data: task.task_data.clone(),
                        };
                        println!("{}", serde_json::to_string(&out_msg).unwrap());
                        Ok(())
                    })
                });

                runtime.register_handler(&task_type, handler, max_retry).await;

                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Ack {
                        task_id: None,
                        message: format!("Registered handler for {}", task_type),
                        shard: None,
                    })
                    .unwrap()
                );
            }

            Ok(IncomingMessage::Enqueue {
                task_id,
                task_type,
                task_data,
                max_retries,
                retry_after_hours,
                rate_limit_group,
                max_per_minute,
                auto_dedupe,
                urgency_score,
                execute_at,
                cron,
                webhook_url,
                max_execution_seconds,
            }) => {
                // Reject if shutting down.
                if runtime.shutting_down.load(Ordering::Acquire) {
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Error {
                            task_id: Some(task_id.clone()),
                            message: "[Snerd] Daemon is shutting down; enqueue rejected".to_string(),
                        })
                        .unwrap()
                    );
                    continue;
                }

                // Daemon-side routing: hash the task id across owned shards.
                let target = {
                    let engines = runtime.engines.read().await;
                    if engines.is_empty() {
                        None
                    } else {
                        let keys: Vec<String> = engines.iter().map(|e| e.key.clone()).collect();
                        let target_key = route_shard(&task_id, &keys).to_string();
                        engines
                            .iter()
                            .find(|e| e.key == target_key)
                            .map(|e| Arc::clone(e))
                    }
                };

                let engine = match target {
                    Some(e) => e,
                    None => {
                        println!(
                            "{}",
                            serde_json::to_string(&OutgoingMessage::Error {
                                task_id: Some(task_id.clone()),
                                message: "[Snerd] No shards owned by this instance".to_string()
                            })
                            .unwrap()
                        );
                        continue;
                    }
                };

                let t = RetryableTask::new(
                    task_id.clone(),
                    task_type.clone(),
                    task_data.clone(),
                    max_retries,
                    retry_after_hours,
                    rate_limit_group,
                    max_per_minute,
                    auto_dedupe,
                    urgency_score,
                    execute_at,
                    cron,
                    webhook_url,
                    max_execution_seconds,
                );
                if let Err(e) = engine.queue.enqueue(t) {
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Error {
                            task_id: Some(task_id.clone()), message: format!("Failed to enqueue: {}", e)
                        })
                        .unwrap()
                    );
                } else {
                    metrics.total_enqueued.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Ack {
                            task_id: Some(task_id.clone()),
                            message: "Enqueued successfully".to_string(),
                            shard: Some(engine.key.clone()),
                        })
                        .unwrap()
                    );
                }
            }

            Ok(IncomingMessage::Progress { task_id, data }) => {
                let out_msg = OutgoingMessage::Progress { task_id, data };
                println!("{}", serde_json::to_string(&out_msg).unwrap());
            }

            Ok(IncomingMessage::Result {
                task_id,
                status,
                error_msg,
            }) => {
                if let Some(tx) = pending_executions.write().await.remove(&task_id) {
                    let res = if status == "success" {
                        Ok(())
                    } else {
                        Err(error_msg.unwrap_or_else(|| "Unknown error".to_string()))
                    };
                    let _ = tx.send(res);
                } else {
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Error { task_id: None, message: format!(
                                "Received result for unknown/expired task_id {}",
                                task_id
                            )
                        })
                        .unwrap()
                    );
                }
            }

            Ok(IncomingMessage::Stats) => {
                let mut queue_depth = 0usize;
                let mut per_shard = Vec::new();
                for engine in runtime.engines.read().await.iter() {
                    let depth = engine
                        .queue
                        .file_store
                        .read_tasks()
                        .map(|tasks| tasks.iter().filter(|t| t.deleted_at.is_none()).count())
                        .unwrap_or(0);
                    queue_depth += depth;
                    per_shard.push(crate::protocol::ShardStat {
                        shard: engine.key.clone(),
                        depth,
                    });
                }
                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Stats {
                        total_enqueued: metrics.total_enqueued.load(Ordering::Relaxed),
                        total_executed: metrics.total_executed.load(Ordering::Relaxed),
                        total_failed: metrics.total_failed.load(Ordering::Relaxed),
                        total_dlq: metrics.total_dlq.load(Ordering::Relaxed),
                        queue_depth,
                        uptime_secs: metrics.start_time.elapsed().as_secs(),
                        per_shard: Some(per_shard),
                    })
                    .unwrap()
                );
            }

            Err(e) => {
                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Error { task_id: None, message: format!("Invalid JSON: {}", e)
                    })
                    .unwrap()
                );
            }
        }
    }
}
