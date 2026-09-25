use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobErrorReturn {
    #[serde(rename = "error")]
    pub error_string: String,
    pub retry_worthy: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RetryableTask {
    #[serde(rename = "taskId")]
    pub task_id: String,

    #[serde(rename = "retryCount")]
    pub retry_count: i32,

    #[serde(rename = "maxRetries")]
    pub max_retries: i32,

    #[serde(rename = "retryAfterHours")]
    pub retry_after_hours: f64,

    #[serde(rename = "retryAfterTime")]
    pub retry_after_time: DateTime<Utc>,

    #[serde(rename = "taskData")]
    pub task_data: String,

    #[serde(rename = "taskType")]
    pub task_type: String,

    #[serde(rename = "LastErrorObj", skip_serializing_if = "Option::is_none")]
    pub last_error_obj: Option<String>,

    #[serde(rename = "LastJobError", skip_serializing_if = "Option::is_none")]
    pub last_job_error: Option<JobErrorReturn>,

    #[serde(rename = "rateLimitGroup", skip_serializing_if = "Option::is_none")]
    pub rate_limit_group: Option<String>,

    #[serde(rename = "maxPerMinute", skip_serializing_if = "Option::is_none")]
    pub max_per_minute: Option<i32>,

    #[serde(rename = "autoDedupe", skip_serializing_if = "Option::is_none")]
    pub auto_dedupe: Option<bool>,

    #[serde(rename = "urgencyScore", skip_serializing_if = "Option::is_none")]
    pub urgency_score: Option<f64>,

    #[serde(rename = "payloadHash", skip_serializing_if = "Option::is_none")]
    pub payload_hash: Option<String>,

    #[serde(rename = "deletedAt", skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,

    #[serde(rename = "executeAt")]
    pub execute_at: DateTime<Utc>,

    #[serde(rename = "pool", skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,

    #[serde(rename = "triggerAfterIds", skip_serializing_if = "Option::is_none")]
    pub trigger_after_ids: Option<Vec<String>>,

    #[serde(rename = "cronExpression", skip_serializing_if = "Option::is_none")]
    pub cron_expression: Option<String>,

    #[serde(rename = "webhookUrl", skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,

    #[serde(rename = "maxExecutionSeconds", skip_serializing_if = "Option::is_none")]
    pub max_execution_seconds: Option<u64>,

    #[serde(skip, default = "Utc::now")]
    pub created_at: DateTime<Utc>,

    #[serde(skip, default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
}

impl RetryableTask {
    pub fn new(
        task_id: String,
        task_type: String,
        task_data: String,
        max_retries: i32,
        retry_after_hours: f64,
        rate_limit_group: Option<String>,
        max_per_minute: Option<i32>,
        auto_dedupe: Option<bool>,
        urgency_score: Option<f64>,
        execute_at_opt: Option<String>,
        cron_opt: Option<String>,
        webhook_url: Option<String>,
        max_execution_seconds: Option<u64>,
        pool: Option<String>,
        trigger_after_ids: Option<Vec<String>>,
    ) -> Self {
        let now = Utc::now();

        // Parse execute_at if provided, else use now
        let mut execute_at = now;
        let parsed_cron = cron_opt.map(|c| parse_cron_syntax(&c));
        
        if let Some(ref exec_str) = execute_at_opt {
            if let Ok(parsed_time) = chrono::DateTime::parse_from_rfc3339(exec_str) {
                execute_at = parsed_time.with_timezone(&Utc);
            }
        } else if let Some(ref cron_expr) = parsed_cron {
            // If no explicit execute_at is provided, but a cron is, default to the FIRST future cron tick!
            use cron::Schedule;
            use std::str::FromStr;
            if let Ok(schedule) = Schedule::from_str(cron_expr) {
                if let Some(next) = schedule.upcoming(Utc).next() {
                    execute_at = next;
                }
            }
        }

        let payload_hash = if auto_dedupe.unwrap_or(false) {
            use xxhash_rust::xxh64::xxh64;
            let combined = format!("{}{}", task_type, task_data);
            Some(format!("{:x}", xxh64(combined.as_bytes(), 0)))
        } else {
            None
        };
        
        Self {
            task_id,
            task_type,
            task_data,
            max_retries,
            retry_after_hours,
            retry_count: 0,
            retry_after_time: now,
            last_error_obj: None,
            last_job_error: None,
            rate_limit_group,
            max_per_minute,
            auto_dedupe,
            urgency_score,
            payload_hash,
            deleted_at: None,
            execute_at,
            cron_expression: parsed_cron,
            webhook_url,
            max_execution_seconds,
            pool,
            trigger_after_ids,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn mark_deleted(&mut self) {
        self.deleted_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn update_retry_config(&mut self, error_msg: Option<String>) {
        self.retry_count += 1;

        // Calculate next retry time
        let seconds = (self.retry_after_hours * 3600.0) as i64;
        self.retry_after_time = Utc::now() + chrono::Duration::seconds(seconds);

        self.last_error_obj = error_msg.clone();

        if let Some(msg) = error_msg {
            self.last_job_error = Some(JobErrorReturn {
                error_string: msg,
                retry_worthy: true,
            });
        } else {
            self.last_job_error = None;
        }

        self.updated_at = Utc::now();
    }
}

pub fn parse_cron_syntax(input: &str) -> String {
    let input = input.trim();
    // Shorthands
    if let Some(val) = input.strip_suffix("s") {
        if let Ok(num) = val.parse::<u32>() {
            return format!("*/{} * * * * *", num);
        }
    }
    if let Some(val) = input.strip_suffix("m") {
        if let Ok(num) = val.parse::<u32>() {
            return format!("0 */{} * * * *", num);
        }
    }
    if let Some(val) = input.strip_suffix("h") {
        if let Ok(num) = val.parse::<u32>() {
            return format!("0 0 */{} * * *", num);
        }
    }
    if let Some(val) = input.strip_suffix("d") {
        if let Ok(num) = val.parse::<u32>() {
            return format!("0 0 0 */{} * *", num);
        }
    }
    
    // Check if it's 5 fields (standard cron)
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.len() == 5 {
        return format!("0 {} *", input);
    }
    if parts.len() == 6 {
        return format!("{} *", input);
    }
    
    input.to_string()
}

use std::cmp::Ordering;

#[derive(Clone)]
pub struct PriorityTask(pub RetryableTask);

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.task_id == other.0.task_id
    }
}

impl Eq for PriorityTask {}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> Ordering {
        let score_a = self.0.urgency_score.unwrap_or(0.0);
        let score_b = other.0.urgency_score.unwrap_or(0.0);
        
        // Reverse order so the max score is popped first
        score_a.partial_cmp(&score_b).unwrap_or(Ordering::Equal)
    }
}
