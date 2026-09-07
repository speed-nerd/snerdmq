//! Sharded storage layout: boot detection, legacy auto-adopt, routing (Phase 2).
//!
//! A queue directory is one of three things at boot:
//!   1. A sharded queue (has `membership.json`) → join mode; the file is
//!      authoritative and any requested shard count is ignored.
//!   2. A legacy single-queue storage (has `tasks/tasks.log`) → auto-adopted
//!      into a 1-shard queue: files move into `shard-0/`, membership created.
//!   3. Empty/new → initialized as an N-shard queue.
//!
//! Auto-adopt runs under `.membership.lock` with a re-check after acquiring,
//! so two daemons booting on the same legacy dir cannot race.

use crate::membership::{shard_key, MembershipStore};
use fs3::FileExt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Legacy storage layout (pre-sharding).
pub const LEGACY_TASKS_DIR: &str = "tasks";
pub const LEGACY_TASKS_LOG: &str = "tasks.log";
pub const LEGACY_LOCK: &str = ".lock";
/// Per-shard exclusive lock file inside each shard directory.
pub const SHARD_LOCK: &str = ".lock";

/// What the storage directory turned out to be.
#[derive(Debug, PartialEq)]
pub enum StorageLayout {
    Sharded,
    Legacy,
    Fresh,
}

/// Inspect a queue directory without mutating it.
pub fn detect_layout(dir: &Path) -> StorageLayout {
    if dir.join(crate::membership::MEMBERSHIP_FILE).exists() {
        StorageLayout::Sharded
    } else if dir.join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG).exists() {
        StorageLayout::Legacy
    } else {
        StorageLayout::Fresh
    }
}

/// FNV-1a 64-bit hash of a task id — the routing function. Kept local (not
/// xxhash) so the mapping is trivially reproducible by any reader.
pub fn fnv1a64(data: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in data.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Route a task id to one of the given shard keys. Panics on empty input —
/// callers must reject zero-owned-shard enqueues before routing.
pub fn route_shard<'a>(task_id: &str, owned: &'a [String]) -> &'a str {
    assert!(!owned.is_empty(), "cannot route with zero owned shards");
    let idx = fnv1a64(task_id) % owned.len() as u64;
    &owned[idx as usize]
}

/// Directory of a shard inside the queue directory.
pub fn shard_dir(queue_dir: &Path, shard: &str) -> PathBuf {
    queue_dir.join(shard)
}

/// Convenience alias so main.rs has one import point for shard naming.
pub fn shard_key_from_index(index: u32) -> String {
    crate::membership::shard_key(index)
}

/// Move a legacy storage dir's files into `shard-0/`. The root `.lock` (if
/// any) moves with it and becomes the shard's lock file. `tasks.log` must
/// exist; it is moved atomically (rename) so no data can be lost mid-boot.
fn migrate_legacy_unlocked(dir: &Path) -> std::io::Result<()> {
    // Fail fast if another LEGACY daemon still runs on this storage — same
    // doctrine as pre-sharding. Its flock follows the .lock inode, so if we
    // can take it here, no legacy daemon is active.
    let legacy_lock_path = dir.join(LEGACY_LOCK);
    if legacy_lock_path.exists() {
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&legacy_lock_path)?;
        if probe.try_lock_exclusive().is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Another daemon is already running on storage '{}'.",
                    dir.display()
                ),
            ));
        }
        probe.unlock().ok();
    }

    let shard0 = shard_dir(dir, &shard_key(0));
    fs::create_dir_all(shard0.join(LEGACY_TASKS_DIR))?;

    let legacy_log = dir.join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG);
    fs::rename(
        &legacy_log,
        shard0.join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG),
    )?;

    let legacy_lock = dir.join(LEGACY_LOCK);
    if legacy_lock.exists() {
        fs::rename(&legacy_lock, shard0.join(SHARD_LOCK))?;
    }

    // Drop the now-empty legacy tasks dir (best effort).
    let _ = fs::remove_dir(dir.join(LEGACY_TASKS_DIR));
    Ok(())
}

/// Resolve the boot layout of `dir`, migrating or initializing as needed.
/// Returns the shard count from membership.json (authoritative).
pub fn resolve_layout(dir: &Path, queue_name: &str, requested_shards: u32) -> std::io::Result<u32> {
    fn io_err(e: crate::membership::MembershipError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
    }

    let store = MembershipStore::new(dir);
    match detect_layout(dir) {
        StorageLayout::Sharded => {
            let membership = store.load().map_err(io_err)?;
            if membership.shards != requested_shards {
                eprintln!(
                    "[Snerd] membership.json declares {} shards; requested {} — membership is authoritative, ignoring request.",
                    membership.shards, requested_shards
                );
            }
            Ok(membership.shards)
        }
        StorageLayout::Legacy => store.with_lock_held(|| {
            // Re-check: another daemon may have adopted between detect and lock.
            if store.exists() {
                return store.load_unlocked().map(|m| m.shards).map_err(io_err);
            }
            migrate_legacy_unlocked(dir)?;
            store.init_unlocked(queue_name, 1).map_err(io_err)?;
            eprintln!(
                "[Snerd] Auto-adopted legacy storage into shard-0 (queue '{}').",
                queue_name
            );
            Ok(1)
        }),
        StorageLayout::Fresh => store.with_lock_held(|| {
            if !store.exists() {
                store.init_unlocked(queue_name, requested_shards).map_err(io_err)?;
            }
            store.load_unlocked().map(|m| m.shards).map_err(io_err)
        }),
    }
}

/// Exclusive OS lock on one shard directory, held for the engine's lifetime.
/// Keeping the handle alive is what keeps the lock held.
pub struct ShardLock {
    #[allow(dead_code)]
    file: File,
}

/// Try to take a shard's `.lock`. Returns None if another engine holds it
/// (zombie claim or live owner) — the caller must revert the membership claim.
pub fn try_lock_shard(dir: &Path, shard: &str) -> std::io::Result<Option<ShardLock>> {
    let shard_path = shard_dir(dir, shard);
    fs::create_dir_all(shard_path.join(LEGACY_TASKS_DIR))?;
    let lock_path = shard_path.join(SHARD_LOCK);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            // Write PID for debugging, mirroring the old daemon lock behavior.
            let mut f = &file;
            f.set_len(0).ok();
            f.write_all(format!("{}", std::process::id()).as_bytes()).ok();
            f.flush().ok();
            Ok(Some(ShardLock { file }))
        }
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_legacy(dir: &Path) {
        fs::create_dir_all(dir.join(LEGACY_TASKS_DIR)).unwrap();
        fs::write(
            dir.join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG),
            "{\"taskId\":\"t1\"}\n",
        )
        .unwrap();
        fs::write(dir.join(LEGACY_LOCK), "1234").unwrap();
    }

    #[test]
    fn detect_layout_variants() {
        let dir = tempdir().unwrap();
        assert_eq!(detect_layout(dir.path()), StorageLayout::Fresh);

        make_legacy(dir.path());
        assert_eq!(detect_layout(dir.path()), StorageLayout::Legacy);
    }

    #[test]
    fn detect_sharded_after_init() {
        let dir = tempdir().unwrap();
        MembershipStore::new(dir.path()).init("q", 2).unwrap();
        assert_eq!(detect_layout(dir.path()), StorageLayout::Sharded);
    }

    #[test]
    fn resolve_fresh_creates_membership_with_requested_shards() {
        let dir = tempdir().unwrap();
        let n = resolve_layout(dir.path(), "q", 4).unwrap();
        assert_eq!(n, 4);
        assert_eq!(detect_layout(dir.path()), StorageLayout::Sharded);
    }

    #[test]
    fn resolve_sharded_membership_is_authoritative() {
        let dir = tempdir().unwrap();
        MembershipStore::new(dir.path()).init("q", 3).unwrap();
        // Request a different count — ignored.
        let n = resolve_layout(dir.path(), "q", 8).unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn auto_adopt_moves_legacy_files_into_shard0() {
        let dir = tempdir().unwrap();
        make_legacy(dir.path());

        let n = resolve_layout(dir.path(), "adopted", 1).unwrap();
        assert_eq!(n, 1);

        // Legacy root files are gone.
        assert!(!dir.path().join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG).exists());
        assert!(!dir.path().join(LEGACY_LOCK).exists());

        // They live in shard-0 now, contents intact.
        let shard0_log = dir
            .path()
            .join("shard-0")
            .join(LEGACY_TASKS_DIR)
            .join(LEGACY_TASKS_LOG);
        assert_eq!(fs::read_to_string(&shard0_log).unwrap(), "{\"taskId\":\"t1\"}\n");
        assert!(dir.path().join("shard-0").join(SHARD_LOCK).exists());

        // Membership exists with 1 shard.
        let m = MembershipStore::new(dir.path()).load().unwrap();
        assert_eq!(m.shards, 1);
        assert_eq!(m.queue, "adopted");

        // Second boot sees sharded layout, no re-adoption.
        assert_eq!(detect_layout(dir.path()), StorageLayout::Sharded);
        assert_eq!(resolve_layout(dir.path(), "adopted", 1).unwrap(), 1);
    }

    #[test]
    fn auto_adopt_without_root_lock_still_works() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join(LEGACY_TASKS_DIR)).unwrap();
        fs::write(
            dir.path().join(LEGACY_TASKS_DIR).join(LEGACY_TASKS_LOG),
            "",
        )
        .unwrap();

        assert_eq!(resolve_layout(dir.path(), "q", 1).unwrap(), 1);
        // Nothing to move: shard lock file was never created.
        assert!(!dir.path().join("shard-0").join(SHARD_LOCK).exists());
    }

    #[test]
    fn auto_adopt_refuses_when_legacy_daemon_holds_lock() {
        let dir = tempdir().unwrap();
        make_legacy(dir.path());

        // Simulate a running legacy daemon holding the root .lock.
        let held = File::open(dir.path().join(LEGACY_LOCK)).unwrap();
        held.lock_exclusive().unwrap();

        let err = resolve_layout(dir.path(), "q", 1).unwrap_err();
        assert!(err.to_string().contains("Another daemon"));

        // Nothing was migrated.
        assert!(dir
            .path()
            .join(LEGACY_TASKS_DIR)
            .join(LEGACY_TASKS_LOG)
            .exists());
        assert_eq!(detect_layout(dir.path()), StorageLayout::Legacy);
    }

    #[test]
    fn fnv1a_known_vectors() {
        // FNV-1a 64 reference values.
        assert_eq!(fnv1a64(""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64("a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a64("foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn route_shard_is_deterministic_and_in_range() {
        let owned: Vec<String> = (0..4).map(|i| format!("shard-{}", i)).collect();
        let first = route_shard("task-123", &owned).to_string();
        for _ in 0..50 {
            assert_eq!(route_shard("task-123", &owned), first);
        }
        for i in 0..200 {
            let target = route_shard(&format!("task-{}", i), &owned);
            assert!(owned.iter().any(|s| s == target));
        }
    }

    #[test]
    fn route_shard_single_shard_always_routes_there() {
        let owned = vec!["shard-0".to_string()];
        assert_eq!(route_shard("anything", &owned), "shard-0");
    }

    #[test]
    #[should_panic(expected = "cannot route with zero owned shards")]
    fn route_shard_panics_on_empty() {
        let owned: Vec<String> = Vec::new();
        route_shard("x", &owned);
    }

    #[test]
    fn try_lock_shard_second_attempt_fails() {
        let dir = tempdir().unwrap();
        let first = try_lock_shard(dir.path(), "shard-0").unwrap();
        assert!(first.is_some());

        let second = try_lock_shard(dir.path(), "shard-0").unwrap();
        assert!(second.is_none());

        drop(first);
        let third = try_lock_shard(dir.path(), "shard-0").unwrap();
        assert!(third.is_some());
    }
}
