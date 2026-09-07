//! Shard membership layer (Phase 1 of sharded queues).
//!
//! A queue directory containing `membership.json` is a logical queue of N
//! shards. This module decides which daemon owns which shard: daemons claim,
//! lease-renew, and take over lapsed claims. Every mutation happens under the
//! `.membership.lock` flock (short-hold pattern — never held across heartbeat
//! intervals) and is written via temp-file + atomic rename, so a torn
//! `membership.json` can never occur.
//!
//! Phase 1: primitives only — not yet wired into the daemon runtime.

use chrono::{DateTime, Duration, Utc};
use fs3::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// How long a claim stays valid without renewal.
pub const LEASE_DURATION_SECS: i64 = 30;
/// How often the owning daemon renews its leases (heartbeat, wired in Phase 3).
pub const RENEW_INTERVAL_SECS: u64 = 10;
/// Cross-server clock-skew tolerance before a challenger may take over.
pub const DEFAULT_SKEW_MARGIN_SECS: i64 = 5;

pub const MEMBERSHIP_FILE: &str = "membership.json";
pub const MEMBERSHIP_LOCK: &str = ".membership.lock";
const MEMBERSHIP_TMP: &str = "membership.json.tmp";

#[derive(Debug)]
pub enum MembershipError {
    Io(io::Error),
    /// Queue dir has no membership.json (legacy layout / not initialized).
    NotInitialized,
    /// Queue dir already has a membership.json (init is one-shot).
    AlreadyInitialized,
    /// Shard is actively claimed by another owner with a live lease.
    OwnedByOther { shard: String, owner: String },
    /// Caller no longer owns the shard (taken over or never claimed).
    LeaseLost { shard: String },
    /// membership.json exists but cannot be parsed.
    Corrupt(String),
}

impl From<io::Error> for MembershipError {
    fn from(e: io::Error) -> Self {
        MembershipError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, MembershipError>;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Claim {
    pub owner: String,
    pub lease_expiry: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Membership {
    pub queue: String,
    pub shards: u32,
    /// Bumped on every successful mutation; lets owners detect takeovers.
    pub version: u64,
    /// Absent key = shard is free.
    #[serde(default)]
    pub claims: HashMap<String, Claim>,
}

/// Result of a claim attempt.
#[derive(Debug, PartialEq)]
pub enum ClaimOutcome {
    /// Shard was free; now owned by the caller.
    Claimed,
    /// Caller already owned the shard; lease refreshed.
    Renewed,
    /// Previous lease had lapsed; shard adopted from the prior owner.
    TakenOver { previous_owner: String },
}

/// Filesystem name of shard `index` (also its subdirectory name).
pub fn shard_key(index: u32) -> String {
    format!("shard-{}", index)
}

/// Stable owner identity: `<host>@pid-<pid>`.
pub fn owner_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{}@pid-{}", host, std::process::id())
}

/// A lease is lapsed when `now > lease_expiry + skew_margin`.
pub fn is_lapsed(claim: &Claim, now: DateTime<Utc>, skew: Duration) -> bool {
    now > claim.lease_expiry + skew
}

/// Parse a skew margin from an optional string (seconds); clamps at 0.
pub fn skew_margin_from(value: Option<&str>) -> Duration {
    let secs = value
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_SKEW_MARGIN_SECS);
    Duration::seconds(secs.max(0))
}

/// Skew margin from `SNERD_CLOCK_SKEW_MARGIN`, defaulting to 5s.
pub fn skew_margin() -> Duration {
    skew_margin_from(std::env::var("SNERD_CLOCK_SKEW_MARGIN").ok().as_deref())
}

/// Shards the given owner may claim right now: free, already-owned-by-self,
/// or whose lease has lapsed (takeover candidates). Ordered by shard index.
pub fn claimable_shards(
    membership: &Membership,
    now: DateTime<Utc>,
    skew: Duration,
    owner: &str,
) -> Vec<String> {
    (0..membership.shards)
        .map(shard_key)
        .filter(|key| match membership.claims.get(key) {
            None => true,
            Some(claim) => claim.owner == owner || is_lapsed(claim, now, skew),
        })
        .collect()
}

/// Membership operations on one queue directory.
pub struct MembershipStore {
    dir: PathBuf,
}

impl MembershipStore {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub fn membership_path(&self) -> PathBuf {
        self.dir.join(MEMBERSHIP_FILE)
    }

    pub fn lock_path(&self) -> PathBuf {
        self.dir.join(MEMBERSHIP_LOCK)
    }

    /// True if this directory is already a sharded queue.
    pub fn exists(&self) -> bool {
        self.membership_path().exists()
    }

    /// Create membership for a new sharded queue. One-shot: fails if the
    /// file already exists. Runs under `.membership.lock`.
    pub fn init(&self, queue: &str, shards: u32) -> Result<()> {
        self.with_lock(|| self.init_unlocked(queue, shards))
    }

    /// Create membership assuming the caller already holds `.membership.lock`
    /// (used by auto-adopt, which migrates files and initializes atomically).
    pub fn init_unlocked(&self, queue: &str, shards: u32) -> Result<()> {
        if self.membership_path().exists() {
            return Err(MembershipError::AlreadyInitialized);
        }
        let membership = Membership {
            queue: queue.to_string(),
            shards,
            version: 0,
            claims: HashMap::new(),
        };
        self.write_unlocked(&membership)
    }

    /// Read the current membership state. Runs under `.membership.lock`.
    pub fn load(&self) -> Result<Membership> {
        self.with_lock(|| self.read_unlocked())
    }

    /// Read membership assuming the caller already holds `.membership.lock`.
    pub fn load_unlocked(&self) -> Result<Membership> {
        self.read_unlocked()
    }

    /// Hold `.membership.lock` for the duration of `f` — for compound
    /// operations (legacy auto-adopt) that must be atomic with membership
    /// initialization. Do not call locking methods (`load`, `claim`, ...)
    /// inside `f`; use the `_unlocked` variants instead.
    pub fn with_lock_held<T>(&self, f: impl FnOnce() -> T) -> T {
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.lock_path())
            .expect("failed to open .membership.lock");
        lock_file
            .lock_exclusive()
            .expect("failed to acquire .membership.lock");
        let out = f();
        let _ = lock_file.unlock();
        out
    }

    /// Claim a shard for `owner`. Free shards are claimed outright; a lapsed
    /// lease is taken over; a live lease held by someone else is rejected.
    /// Re-claiming a shard the caller already owns refreshes its lease.
    pub fn claim(
        &self,
        shard: &str,
        owner: &str,
        now: DateTime<Utc>,
        skew: Duration,
    ) -> Result<ClaimOutcome> {
        let expiry = now + Duration::seconds(LEASE_DURATION_SECS);
        self.mutate(|m| {
            match m.claims.get(shard) {
                None => {
                    m.claims.insert(
                        shard.to_string(),
                        Claim {
                            owner: owner.to_string(),
                            lease_expiry: expiry,
                        },
                    );
                    Ok(ClaimOutcome::Claimed)
                }
                Some(existing) if existing.owner == owner => {
                    m.claims.insert(
                        shard.to_string(),
                        Claim {
                            owner: owner.to_string(),
                            lease_expiry: expiry,
                        },
                    );
                    Ok(ClaimOutcome::Renewed)
                }
                Some(existing) if is_lapsed(existing, now, skew) => {
                    let previous_owner = existing.owner.clone();
                    m.claims.insert(
                        shard.to_string(),
                        Claim {
                            owner: owner.to_string(),
                            lease_expiry: expiry,
                        },
                    );
                    Ok(ClaimOutcome::TakenOver { previous_owner })
                }
                Some(existing) => Err(MembershipError::OwnedByOther {
                    shard: shard.to_string(),
                    owner: existing.owner.clone(),
                }),
            }
        })
    }

    /// Roll back a claim after its shard-level flock acquisition failed.
    /// Only removes the entry if the caller still owns it — if another daemon
    /// already took over, the new owner's claim must be left untouched.
    pub fn revert_claim(&self, shard: &str, owner: &str) -> Result<()> {
        self.mutate(|m| {
            if let Some(claim) = m.claims.get(shard) {
                if claim.owner == owner {
                    m.claims.remove(shard);
                }
            }
            Ok(())
        })
    }

    /// Refresh the lease on a shard the caller owns. Fails with `LeaseLost`
    /// if the entry is gone or owned by someone else (taken over).
    pub fn renew(&self, shard: &str, owner: &str, now: DateTime<Utc>) -> Result<()> {
        let expiry = now + Duration::seconds(LEASE_DURATION_SECS);
        self.mutate(|m| match m.claims.get_mut(shard) {
            Some(claim) if claim.owner == owner => {
                claim.lease_expiry = expiry;
                Ok(())
            }
            _ => Err(MembershipError::LeaseLost {
                shard: shard.to_string(),
            }),
        })
    }

    /// Drop a claim (graceful shutdown / drain). Fails with `LeaseLost` if
    /// the caller does not currently own the shard.
    pub fn release(&self, shard: &str, owner: &str) -> Result<()> {
        self.mutate(|m| match m.claims.get(shard) {
            Some(claim) if claim.owner == owner => {
                m.claims.remove(shard);
                Ok(())
            }
            _ => Err(MembershipError::LeaseLost {
                shard: shard.to_string(),
            }),
        })
    }

    // --- internals (all callers hold `.membership.lock`) ---

    fn with_lock<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.lock_path())?;
        lock_file.lock_exclusive()?;
        let out = f();
        let _ = lock_file.unlock();
        out
    }

    fn read_unlocked(&self) -> Result<Membership> {
        match std::fs::read_to_string(self.membership_path()) {
            Ok(raw) => serde_json::from_str(&raw)
                .map_err(|e| MembershipError::Corrupt(e.to_string())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Err(MembershipError::NotInitialized)
            }
            Err(e) => Err(MembershipError::Io(e)),
        }
    }

    /// Crash-safe write: temp file in the same directory, fsync, then an
    /// atomic rename over membership.json.
    fn write_unlocked(&self, membership: &Membership) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(membership)
            .map_err(|e| MembershipError::Corrupt(e.to_string()))?;
        let tmp_path = self.dir.join(MEMBERSHIP_TMP);
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(&bytes)?;
        tmp.sync_all()?;
        std::fs::rename(&tmp_path, self.membership_path())?;
        Ok(())
    }

    /// Read-modify-write under the flock. The version is bumped and the file
    /// rewritten only when the mutation succeeds.
    fn mutate<T>(&self, f: impl FnOnce(&mut Membership) -> Result<T>) -> Result<T> {
        self.with_lock(|| {
            let mut membership = self.read_unlocked()?;
            let out = f(&mut membership)?;
            membership.version += 1;
            self.write_unlocked(&membership)?;
            Ok(out)
        })
    }

    /// Public crash-safe write for callers that already hold the lock
    /// (e.g. the `add-shards` CLI subcommand). Callers must have acquired
    /// `.membership.lock` via `with_lock_held` before calling this.
    pub fn write_membership_atomic(&self, membership: &Membership) -> Result<()> {
        self.write_unlocked(membership)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn skew() -> Duration {
        Duration::seconds(DEFAULT_SKEW_MARGIN_SECS)
    }

    #[test]
    fn init_creates_membership_and_is_one_shot() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());

        assert!(!store.exists());
        store.init("test-queue", 3).unwrap();
        assert!(store.exists());

        let m = store.load().unwrap();
        assert_eq!(m.queue, "test-queue");
        assert_eq!(m.shards, 3);
        assert_eq!(m.version, 0);
        assert!(m.claims.is_empty());

        assert!(matches!(
            store.init("test-queue", 3),
            Err(MembershipError::AlreadyInitialized)
        ));
    }

    #[test]
    fn load_on_legacy_dir_is_not_initialized() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        assert!(matches!(store.load(), Err(MembershipError::NotInitialized)));
    }

    #[test]
    fn claim_free_shard_writes_owner_and_expiry() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 2).unwrap();

        let t0 = now();
        let outcome = store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();
        assert_eq!(outcome, ClaimOutcome::Claimed);

        let m = store.load().unwrap();
        assert_eq!(m.version, 1);
        let claim = m.claims.get("shard-0").unwrap();
        assert_eq!(claim.owner, "host-a@pid-1");
        assert_eq!(
            claim.lease_expiry,
            t0 + Duration::seconds(LEASE_DURATION_SECS)
        );
        // Atomic write leaves no temp file behind.
        assert!(!dir.path().join(MEMBERSHIP_TMP).exists());
    }

    #[test]
    fn claim_live_shard_held_by_other_is_rejected() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        match store.claim("shard-0", "host-b@pid-2", t0, skew()) {
            Err(MembershipError::OwnedByOther { shard, owner }) => {
                assert_eq!(shard, "shard-0");
                assert_eq!(owner, "host-a@pid-1");
            }
            other => panic!("expected OwnedByOther, got {:?}", other),
        }

        // Rejected claim must not bump the version or touch the entry.
        let m = store.load().unwrap();
        assert_eq!(m.version, 1);
        assert_eq!(m.claims.get("shard-0").unwrap().owner, "host-a@pid-1");
    }

    #[test]
    fn claim_own_shard_renews_lease() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        let t1 = t0 + Duration::seconds(10);
        let outcome = store.claim("shard-0", "host-a@pid-1", t1, skew()).unwrap();
        assert_eq!(outcome, ClaimOutcome::Renewed);

        let m = store.load().unwrap();
        assert_eq!(m.version, 2);
        assert_eq!(
            m.claims.get("shard-0").unwrap().lease_expiry,
            t1 + Duration::seconds(LEASE_DURATION_SECS)
        );
    }

    #[test]
    fn claim_lapsed_shard_takes_over() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        // Lease (30s) + skew (5s) fully elapsed.
        let t1 = t0 + Duration::seconds(36);
        match store.claim("shard-0", "host-b@pid-2", t1, skew()).unwrap() {
            ClaimOutcome::TakenOver { previous_owner } => {
                assert_eq!(previous_owner, "host-a@pid-1");
            }
            other => panic!("expected TakenOver, got {:?}", other),
        }

        let m = store.load().unwrap();
        assert_eq!(m.claims.get("shard-0").unwrap().owner, "host-b@pid-2");
    }

    #[test]
    fn claim_just_inside_lease_plus_skew_is_rejected() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        // 35s elapsed: lease lapsed (30s) but within skew margin (5s).
        let t1 = t0 + Duration::seconds(35);
        assert!(matches!(
            store.claim("shard-0", "host-b@pid-2", t1, skew()),
            Err(MembershipError::OwnedByOther { .. })
        ));
    }

    #[test]
    fn renew_updates_expiry_for_owner_only() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        let t1 = t0 + Duration::seconds(10);
        store.renew("shard-0", "host-a@pid-1", t1).unwrap();
        let m = store.load().unwrap();
        assert_eq!(m.version, 2);
        assert_eq!(
            m.claims.get("shard-0").unwrap().lease_expiry,
            t1 + Duration::seconds(LEASE_DURATION_SECS)
        );

        // Non-owner renewal fails and does not mutate.
        assert!(matches!(
            store.renew("shard-0", "host-b@pid-2", t1),
            Err(MembershipError::LeaseLost { .. })
        ));
        let m = store.load().unwrap();
        assert_eq!(m.version, 2);
        assert_eq!(m.claims.get("shard-0").unwrap().owner, "host-a@pid-1");
    }

    #[test]
    fn release_drops_claim_for_owner_only() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        assert!(matches!(
            store.release("shard-0", "host-b@pid-2"),
            Err(MembershipError::LeaseLost { .. })
        ));
        assert!(store.load().unwrap().claims.contains_key("shard-0"));

        store.release("shard-0", "host-a@pid-1").unwrap();
        let m = store.load().unwrap();
        assert!(!m.claims.contains_key("shard-0"));
        assert_eq!(m.version, 2);
    }

    #[test]
    fn revert_claim_only_removes_own_entry() {
        let dir = tempdir().unwrap();
        let store = MembershipStore::new(dir.path());
        store.init("q", 1).unwrap();

        let t0 = now();
        store.claim("shard-0", "host-a@pid-1", t0, skew()).unwrap();

        // Reverting someone else's claim leaves it untouched.
        store.revert_claim("shard-0", "host-b@pid-2").unwrap();
        assert!(store.load().unwrap().claims.contains_key("shard-0"));

        // Reverting your own claim clears it (shard-flock rollback path).
        store.revert_claim("shard-0", "host-a@pid-1").unwrap();
        assert!(!store.load().unwrap().claims.contains_key("shard-0"));
    }

    #[test]
    fn claimable_shards_mixes_free_lapsed_and_owned() {
        let t0 = now();
        let mut membership = Membership {
            queue: "q".to_string(),
            shards: 4,
            version: 0,
            claims: HashMap::new(),
        };
        // shard-0: free
        membership.claims.insert(
            "shard-1".to_string(),
            Claim {
                owner: "me".to_string(),
                lease_expiry: t0 + Duration::seconds(30),
            },
        );
        membership.claims.insert(
            "shard-2".to_string(),
            Claim {
                owner: "other".to_string(),
                lease_expiry: t0 + Duration::seconds(30),
            },
        );
        membership.claims.insert(
            "shard-3".to_string(),
            Claim {
                owner: "dead-owner".to_string(),
                lease_expiry: t0 - Duration::seconds(6),
            },
        );

        let claimable = claimable_shards(&membership, t0, skew(), "me");
        // shard-0 (free), shard-1 (own), shard-3 (lapsed) — not shard-2.
        assert_eq!(claimable, vec!["shard-0", "shard-1", "shard-3"]);
    }

    #[test]
    fn concurrent_claim_same_shard_has_exactly_one_winner() {
        let dir = tempdir().unwrap();
        let store = Arc::new(MembershipStore::new(dir.path()));
        store.init("q", 1).unwrap();

        let t0 = now();
        let mut handles = Vec::new();
        for i in 0..4 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                store.claim("shard-0", &format!("host-x@pid-{}", i), t0, skew())
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results
            .iter()
            .filter(|r| matches!(r, Ok(ClaimOutcome::Claimed)))
            .count();
        let rejects = results
            .iter()
            .filter(|r| matches!(r, Err(MembershipError::OwnedByOther { .. })))
            .count();
        assert_eq!(wins, 1);
        assert_eq!(rejects, 3);

        // File still parses and holds exactly one owner.
        let m = store.load().unwrap();
        assert_eq!(m.claims.len(), 1);
    }

    #[test]
    fn concurrent_claims_on_distinct_shards_all_succeed() {
        let dir = tempdir().unwrap();
        let store = Arc::new(MembershipStore::new(dir.path()));
        store.init("q", 4).unwrap();

        let t0 = now();
        let mut handles = Vec::new();
        for i in 0..4u32 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                store.claim(&shard_key(i), &format!("host-x@pid-{}", i), t0, skew())
            }));
        }

        for h in handles {
            assert_eq!(h.join().unwrap().unwrap(), ClaimOutcome::Claimed);
        }
        let m = store.load().unwrap();
        assert_eq!(m.claims.len(), 4);
        assert_eq!(m.version, 4);
    }

    #[test]
    fn skew_margin_parses_and_clamps() {
        assert_eq!(skew_margin_from(None), Duration::seconds(5));
        assert_eq!(skew_margin_from(Some("12")), Duration::seconds(12));
        assert_eq!(skew_margin_from(Some("garbage")), Duration::seconds(5));
        assert_eq!(skew_margin_from(Some("-3")), Duration::seconds(0));
    }

    #[test]
    fn owner_id_contains_pid_marker() {
        let id = owner_id();
        assert!(id.contains("@pid-"), "owner id was {}", id);
    }

    #[test]
    fn shard_key_format() {
        assert_eq!(shard_key(0), "shard-0");
        assert_eq!(shard_key(12), "shard-12");
    }
}
