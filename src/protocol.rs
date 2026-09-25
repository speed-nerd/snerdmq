use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "action")]
pub enum IncomingMessage {
    #[serde(rename = "register")]
    Register { task_type: String },
    #[serde(rename = "enqueue")]
    Enqueue {
        task_id: String,
        task_type: String,
        task_data: String,
        max_retries: i32,
        retry_after_hours: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        rate_limit_group: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_per_minute: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_dedupe: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        urgency_score: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        execute_at: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cron: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        webhook_url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_execution_seconds: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pool: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        trigger_after_ids: Option<Vec<String>>,
    },
    #[serde(rename = "progress")]
    Progress {
        task_id: String,
        data: String,
    },
    #[serde(rename = "result")]
    Result {
        task_id: String,
        status: String,
        error_msg: Option<String>,
    },
    #[serde(rename = "stats")]
    Stats,
    #[cfg(debug_assertions)]
    #[serde(rename = "test_pause_heartbeat")]
    TestPauseHeartbeat,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "action")]
pub enum OutgoingMessage {
    #[serde(rename = "execute")]
    Execute {
        task_id: String,
        task_type: String,
        task_data: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_execution_seconds: Option<u64>,
    },
    #[serde(rename = "max_retries_reached")]
    MaxRetriesReached {
        task_id: String,
        task_type: String,
        task_data: String,
    },
    #[serde(rename = "ack")]
    Ack {
        #[serde(skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        message: String,
        /// Which shard an enqueue landed in (informational; SDKs ignore).
        #[serde(skip_serializing_if = "Option::is_none")]
        shard: Option<String>,
    },
    #[serde(rename = "progress")]
    Progress {
        task_id: String,
        data: String,
    },
    #[serde(rename = "error")]
    Error { #[serde(skip_serializing_if = "Option::is_none")] task_id: Option<String>, message: String },
    #[serde(rename = "stats")]
    Stats {
        total_enqueued: u64,
        total_executed: u64,
        total_failed: u64,
        total_dlq: u64,
        queue_depth: usize,
        uptime_secs: u64,
        /// Per-shard rollup breakdown (informational; SDKs may ignore).
        #[serde(skip_serializing_if = "Option::is_none")]
        per_shard: Option<Vec<ShardStat>>,
    },
    /// Informational membership snapshot, pushed on boot and on standby
    /// promotion. SDKs use it for display only — never for routing.
    #[serde(rename = "membership")]
    Membership {
        queue: String,
        shards: u32,
        owned: Vec<String>,
        version: u64,
    },
}

/// One shard's contribution to a `stats` rollup.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardStat {
    pub shard: String,
    pub depth: usize,
}
