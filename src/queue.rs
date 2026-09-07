use chrono::Utc;
use std::collections::{HashMap, HashSet, BinaryHeap};
use tokio::sync::Semaphore;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::file_store::FileStore;
use crate::task::{RetryableTask, PriorityTask};
use crate::rate_limiter::RateLimiter;
use serde_json::json;

use std::future::Future;
use std::pin::Pin;

pub type TaskHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;
pub type MaxRetryHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;

/// How long a completed task id stays in `completed_tasks` before eviction.
/// It is only a safety net against duplicate execution within a short window;
/// the tombstone in the task log is the durable source of truth.
const COMPLETED_TTL: Duration = Duration::from_secs(60);
/// How often the sweeper evicts expired completed entries.
const COMPLETED_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

struct ExecutingGuard {
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    task_id: String,
}

/// Drops completed entries whose completion time is older than `now - ttl`.
fn evict_expired_completed(completed: &mut HashMap<String, Instant>, now: Instant, ttl: Duration) {
    completed.retain(|_, completed_at| now.duration_since(*completed_at) < ttl);
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if let Ok(mut executing) = self.executing_tasks.lock() {
            executing.remove(&self.task_id);
        }
    }
}

#[derive(Clone)]
pub struct SnerdQueue {
    pub name: String,
    pub file_store: FileStore,
    pub rate_limiter: RateLimiter,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
    active_hashes: Arc<Mutex<HashSet<String>>>,
    pub executing_tasks: Arc<Mutex<HashSet<String>>>,
    /// Tasks that have been pushed to shared_pq but haven't started executing yet.
    /// Prevents process_due_tasks() from re-adding the same task to the queue.
    queued_tasks: Arc<Mutex<HashSet<String>>>,
    /// Tasks that have completed execution (successfully or max retries reached),
    /// mapped to their completion time. Final safety net to prevent duplicate
    /// execution; entries are evicted after COMPLETED_TTL to bound memory.
    completed_tasks: Arc<Mutex<HashMap<String, Instant>>>,
    worker_semaphore: Arc<Semaphore>,
    /// Shared priority queue — workers always pop the highest-priority task next.
    shared_pq: Arc<Mutex<BinaryHeap<PriorityTask>>>,
    /// Number of active dispatcher loops (prevents duplicates).
    dispatcher_count: Arc<std::sync::atomic::AtomicUsize>,
    /// When true: no new enqueues accepted and process_due_tasks is a no-op.
    /// Flipped by the heartbeat thread on lease-loss or graceful shutdown.
    pub paused: Arc<AtomicBool>,
}

impl SnerdQueue {
    pub fn new(name: &str, file_store: FileStore, rate_limiter: RateLimiter) -> Self {
        Self::new_with_semaphore(name, file_store, rate_limiter, Arc::new(Semaphore::new(100)))
    }

    /// Like `new`, but sharing a worker semaphore with other queues — used by
    /// the sharded daemon so all shard engines share one concurrency budget.
    pub fn new_with_semaphore(
        name: &str,
        file_store: FileStore,
        rate_limiter: RateLimiter,
        worker_semaphore: Arc<Semaphore>,
    ) -> Self {
        let mut initial_hashes = HashSet::new();
        if let Ok(tasks) = file_store.read_tasks() {
            for task in tasks {
                if task.deleted_at.is_none() {
                    if let Some(hash) = task.payload_hash {
                        initial_hashes.insert(hash);
                    }
                }
            }
        }
        
        Self {
            name: name.to_string(),
            file_store,
            rate_limiter,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
            active_hashes: Arc::new(Mutex::new(initial_hashes)),
            executing_tasks: Arc::new(Mutex::new(HashSet::new())),
            queued_tasks: Arc::new(Mutex::new(HashSet::new())),
            completed_tasks: Arc::new(Mutex::new(HashMap::new())),
            worker_semaphore,
            shared_pq: Arc::new(Mutex::new(BinaryHeap::new())),
            dispatcher_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            paused: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn register_task_handler<F, Fut>(&self, task_type: &str, handler: F)
    where
        F: Fn(RetryableTask) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        self.task_handlers.write().await.insert(
            task_type.to_string(),
            Arc::new(move |task| Box::pin(handler(task))),
        );
    }

    pub async fn register_max_retry_handler<F, Fut>(&self, task_type: &str, handler: F)
    where
        F: Fn(RetryableTask) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        self.max_retry_handlers.write().await.insert(
            task_type.to_string(),
            Arc::new(move |task| Box::pin(handler(task))),
        );
    }

    /// Register a pre-built handler Arc — used by the sharded daemon to
    /// replay the same handler onto every shard engine (existing and later).
    pub async fn register_task_handler_arc(&self, task_type: &str, handler: TaskHandler) {
        self.task_handlers
            .write()
            .await
            .insert(task_type.to_string(), handler);
    }

    /// Pre-built max-retry handler variant of `register_task_handler_arc`.
    pub async fn register_max_retry_handler_arc(&self, task_type: &str, handler: MaxRetryHandler) {
        self.max_retry_handlers
            .write()
            .await
            .insert(task_type.to_string(), handler);
    }

    pub fn enqueue(&self, mut task: RetryableTask) -> std::io::Result<()> {
        // Reject new enqueues when the shard is paused (lease-loss or shutdown).
        if self.paused.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "[Snerd] Shard is paused; enqueue rejected",
            ));
        }
        if let Some(ref hash) = task.payload_hash {
            if let Ok(mut hashes) = self.active_hashes.lock() {
                if hashes.contains(hash) {
                    // Duplicate found, drop silently
                    return Ok(());
                }
                hashes.insert(hash.clone());
            }
        }
        task.deleted_at = None;
        self.file_store.save_task(&task)?;

        // NOTE: We intentionally do NOT execute tasks immediately here.
        // All execution goes through the periodic processor (process_due_tasks)
        // which uses a BinaryHeap to respect priority ordering.
        // The fast path would bypass priority and cause low-priority tasks
        // enqueued first to always execute before high-priority tasks enqueued later.

        Ok(())
    }

    pub async fn start_processor(&self, interval: Duration) {
        let q = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                q.process_due_tasks().await;
            }
        });
        self.start_completed_sweeper();
    }

    /// Periodically evicts completed-task entries older than COMPLETED_TTL so
    /// the dedup set does not grow unboundedly on long-running daemons.
    fn start_completed_sweeper(&self) {
        let completed = Arc::clone(&self.completed_tasks);
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(COMPLETED_SWEEP_INTERVAL);
            loop {
                interval_timer.tick().await;
                evict_expired_completed(&mut completed.lock().unwrap(), Instant::now(), COMPLETED_TTL);
            }
        });
    }

    pub async fn process_due_tasks(&self) {
        // Skip processing when shard is paused (lease-lost or shutting down).
        if self.paused.load(Ordering::Acquire) {
            return;
        }

        let tasks = match self.file_store.read_tasks() {
            Ok(t) => t,
            Err(_) => return,
        };

        let now = Utc::now();

        // IMPORTANT: Check against LIVE executing_tasks and queued_tasks sets
        // (not snapshots) to prevent races where a task moves from queued → executing
        // between our snapshot and our check, making it invisible to both.
        {
            let mut pq = self.shared_pq.lock().unwrap();
            let mut queued = self.queued_tasks.lock().unwrap();
            let executing = self.executing_tasks.lock().unwrap();
            for task in tasks {
                if task.execute_at <= now
                    && task.retry_after_time <= now
                    && task.deleted_at.is_none()
                    && !executing.contains(&task.task_id)
                    && !queued.contains(&task.task_id)
                {
                    queued.insert(task.task_id.clone());
                    pq.push(PriorityTask(task));
                }
            }
        }

        // Start a priority dispatcher if there are tasks queued and not too many dispatchers
        let pq_len = self.shared_pq.lock().unwrap().len();
        if pq_len > 0 && self.dispatcher_count.load(std::sync::atomic::Ordering::Relaxed) < 2 {
            self.spawn_dispatcher();
        }
    }

    /// Spawns a persistent priority dispatcher that feeds tasks to workers
    /// in strict priority order. The dispatcher acquires a semaphore permit
    /// for each task, ensuring at most 100 concurrent executions. When a task
    /// completes and releases its permit, the dispatcher wakes up and spawns
    /// the next highest-priority task.
    fn spawn_dispatcher(&self) {
        self.dispatcher_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let q = self.clone();
        tokio::spawn(async move {
            loop {
                // Acquire a concurrency permit (blocks if all 100 are in use)
                let permit = match q.worker_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // Pop the highest-priority task from the shared queue
                let task = {
                    let mut pq = q.shared_pq.lock().unwrap();
                    pq.pop()
                };

                match task {
                    Some(PriorityTask(mut task)) => {
                        // Rate limit check
                        if let Some(ref group) = task.rate_limit_group {
                            if let Some(limit) = task.max_per_minute {
                                match q.rate_limiter.check_and_increment(group, limit) {
                                    Ok(true) => {}
                                    Ok(false) | Err(_) => {
                                        task.retry_after_time = Utc::now() + chrono::Duration::seconds(60);
                                        let _ = q.file_store.save_task(&task);
                                        // Remove from queued so it can be re-queued after rate limit window
                                        q.queued_tasks.lock().unwrap().remove(&task.task_id);
                                        drop(permit); // Release permit without executing
                                        continue;
                                    }
                                }
                            }
                        }

                        // Move from queued to executing
                        {
                            let mut queued = q.queued_tasks.lock().unwrap();
                            queued.remove(&task.task_id);
                            let mut executing = q.executing_tasks.lock().unwrap();
                            if executing.contains(&task.task_id) {
                                drop(permit);
                                continue;
                            }
                            executing.insert(task.task_id.clone());
                        }

                        let q2 = q.clone();
                        tokio::spawn(async move {
                            let _permit = permit; // Held until execution completes
                            q2.execute_task(task).await;
                            // Permit is released here when _permit drops
                        });
                    }
                    None => {
                        drop(permit);
                        break; // Queue empty, dispatcher exits
                    }
                }
            }
            q.dispatcher_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        // Final safety check: skip if already completed (prevents duplicate execution)
        {
            let completed = self.completed_tasks.lock().unwrap();
            if completed.contains_key(&task.task_id) {
                return; // Already completed, skip
            }
        }

        // Drop guard guarantees removal from executing_tasks
        let _guard = ExecutingGuard {
            executing_tasks: Arc::clone(&self.executing_tasks),
            task_id: task.task_id.clone(),
        };

        // If webhook_url is set, dispatch via HTTP instead of local handler
        let result: Result<(), String> = if let Some(ref url) = task.webhook_url.clone() {
            let payload = json!({
                "taskId": task.task_id,
                "taskType": task.task_type,
                "data": task.task_data,
            });
            let url = url.clone();
            match reqwest::Client::new()
                .post(&url)
                .header("Content-Type", "application/json")
                .header("X-SnerdMQ-Event", "Execute")
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => Ok(()),
                Ok(resp) => Err(format!("Webhook returned non-2xx status: {}", resp.status())),
                Err(e) => Err(format!("Webhook request failed: {}", e)),
            }
        } else {
            let handler = {
                let handlers = self.task_handlers.read().await;
                handlers.get(&task.task_type).cloned()
            };
            if let Some(h) = handler {
                h(task.clone()).await
            } else {
                return;
            }
        };

        match result {
            Ok(_) => {
                let mut rescheduled = false;
                if let Some(ref cron_expr) = task.cron_expression {
                    use cron::Schedule;
                    use std::str::FromStr;
                    if let Ok(schedule) = Schedule::from_str(cron_expr) {
                        if let Some(next) = schedule.upcoming(Utc).next() {
                            task.execute_at = next;
                            task.retry_count = 0;
                            task.last_error_obj = None;
                            task.last_job_error = None;
                            let _ = self.file_store.save_task(&task);
                            rescheduled = true;
                        }
                    }
                }

                if !rescheduled {
                    // Mark as completed to prevent duplicate execution
                    self.completed_tasks.lock().unwrap().insert(task.task_id.clone(), Instant::now());
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
            Err(e) => {
                // max_retries means total attempts (not retries after first).
                // retry_count starts at 0 and update_retry_config increments it AFTER this check.
                // So we allow retry while retry_count < max_retries - 1.
                if task.retry_count < task.max_retries - 1 {
                    task.update_retry_config(Some(e));
                    let _ = self.file_store.save_task(&task);
                } else {
                    // Max retries reached — fire DLQ webhook or local max retry handler
                    if let Some(ref url) = task.webhook_url.clone() {
                        let payload = json!({
                            "taskId": task.task_id,
                            "taskType": task.task_type,
                            "data": task.task_data,
                        });
                        let url = url.clone();
                        tokio::spawn(async move {
                            let _ = reqwest::Client::new()
                                .post(&url)
                                .header("Content-Type", "application/json")
                                .header("X-SnerdMQ-Event", "MaxRetriesReached")
                                .json(&payload)
                                .send()
                                .await;
                        });
                    } else {
                        let max_handler = {
                            let max_handlers = self.max_retry_handlers.read().await;
                            max_handlers.get(&task.task_type).cloned()
                        };
                        if let Some(mh) = max_handler {
                            let _ = mh(task.clone()).await;
                        }
                    }

                    // Mark as completed to prevent duplicate execution
                    self.completed_tasks.lock().unwrap().insert(task.task_id.clone(), Instant::now());
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod completed_eviction_tests {
    use super::*;

    #[test]
    fn evicts_entries_older_than_ttl() {
        let now = Instant::now();
        let mut completed = HashMap::new();
        completed.insert("stale".to_string(), now - Duration::from_secs(90));
        completed.insert("fresh".to_string(), now - Duration::from_secs(10));

        evict_expired_completed(&mut completed, now, COMPLETED_TTL);

        assert!(!completed.contains_key("stale"));
        assert!(completed.contains_key("fresh"));
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn keeps_entries_exactly_within_ttl() {
        let now = Instant::now();
        let mut completed = HashMap::new();
        completed.insert("edge".to_string(), now - (COMPLETED_TTL - Duration::from_secs(1)));

        evict_expired_completed(&mut completed, now, COMPLETED_TTL);

        assert!(completed.contains_key("edge"));
    }

    #[test]
    fn empty_map_is_noop() {
        let mut completed: HashMap<String, Instant> = HashMap::new();
        evict_expired_completed(&mut completed, Instant::now(), COMPLETED_TTL);
        assert!(completed.is_empty());
    }
}
