//! Durable, fail-closed attempt budget used only by the `native-image-live-test` build.
//!
//! The reservation is committed before the network send. A process crash between
//! reservation and send can consume a slot, but can never cause the configured cap
//! to be exceeded after restart.

use rusqlite::{Connection, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const MAX_IMAGE_GENERATION_UPSTREAM_ATTEMPTS: u64 = 5;
#[cfg(feature = "tinix-m0-hardened")]
const DB_FILE: &str = "tinix_m0_image_attempts.sqlite3";
#[cfg(not(feature = "tinix-m0-hardened"))]
const DB_FILE: &str = "native_image_attempt_budget.sqlite3";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveAttemptError {
    Exhausted { limit: u64 },
    DuplicateRequestAttempt { correlation_id: String },
    Storage(String),
}

impl std::fmt::Display for ReserveAttemptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exhausted { limit } => write!(
                f,
                "Native image test upstream attempt budget exhausted (limit: {limit})"
            ),
            Self::DuplicateRequestAttempt { correlation_id } => write!(
                f,
                "Tinix M0 single-attempt policy violation: correlation_id {correlation_id} already reserved an upstream attempt"
            ),
            Self::Storage(message) => {
                write!(f, "Image attempt counter unavailable: {message}")
            }
        }
    }
}

impl std::error::Error for ReserveAttemptError {}

/// Reserve one upstream image-generation attempt in the current isolated profile.
/// The durable SQLite row is shared across requests and process restarts.
pub async fn reserve_current_profile_attempt() -> Result<u64, ReserveAttemptError> {
    let path = current_profile_budget_path()?;
    tokio::task::spawn_blocking(move || reserve_attempt_at(&path))
        .await
        .map_err(|error| ReserveAttemptError::Storage(format!("worker failed: {error}")))?
}

pub async fn reserve_current_profile_attempt_for_request(
    correlation_id: String,
) -> Result<u64, ReserveAttemptError> {
    let path = current_profile_budget_path()?;
    tokio::task::spawn_blocking(move || {
        reserve_attempt_for_request_at(&path, Some(correlation_id.as_str()))
    })
    .await
    .map_err(|error| ReserveAttemptError::Storage(format!("worker failed: {error}")))?
}

pub fn current_profile_budget_path() -> Result<PathBuf, ReserveAttemptError> {
    let configured_profile = std::env::var("ABV_DATA_DIR").ok();
    if configured_profile
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .is_none()
    {
        return Err(ReserveAttemptError::Storage(
            "ABV_DATA_DIR must explicitly select the isolated profile".to_string(),
        ));
    }
    Ok(crate::modules::account::get_data_dir()
        .map_err(ReserveAttemptError::Storage)?
        .join(DB_FILE))
}

/// Store only allowlisted fields from the final upstream body, never auth headers,
/// account/project identifiers, cookies, OAuth data, prompts or inline image data.
pub fn record_attempt_evidence_at(
    path: &Path,
    attempt: u64,
    endpoint: &str,
    body: &serde_json::Value,
) -> Result<(), ReserveAttemptError> {
    use std::io::Write;
    let evidence = serde_json::json!({
        "reserved_attempt": attempt,
        "timestamp_utc": chrono::Utc::now().to_rfc3339(),
        "endpoint": endpoint,
        "method": "generateContent",
        "payload_allowlist": {
            "model": body.get("model"),
            "requestType": body.get("requestType"),
            "request": {"generationConfig": {
                "candidateCount": body.pointer("/request/generationConfig/candidateCount"),
                "imageConfig": {
                    "imageSize": body.pointer("/request/generationConfig/imageConfig/imageSize"),
                    "aspectRatio": body.pointer("/request/generationConfig/imageConfig/aspectRatio")
                }
            }}
        },
        "omitted": "All non-allowlisted body fields and all HTTP headers",
        "stage": "reserved_before_send; does not prove Google accepted the request"
    });
    let bytes = serde_json::to_vec_pretty(&evidence)
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.with_extension(format!("attempt-{attempt}.json")))
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))
}

/// Atomically reserve a slot at a specified database path.
/// Exposed for tests so they can use disposable mock-only storage.
pub fn reserve_attempt_at(path: &Path) -> Result<u64, ReserveAttemptError> {
    reserve_attempt_for_request_at(path, None)
}

pub fn reserve_attempt_for_request_at(
    path: &Path,
    correlation_id: Option<&str>,
) -> Result<u64, ReserveAttemptError> {
    let parent = path.parent().ok_or_else(|| {
        ReserveAttemptError::Storage("database path has no parent directory".to_string())
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;

    let mut connection =
        Connection::open(path).map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
    connection
        .busy_timeout(Duration::from_secs(15))
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
    connection
        .execute_batch("PRAGMA synchronous=FULL;")
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;

    #[cfg(feature = "tinix-m0-hardened")]
    {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS tinix_m0_attempt_counter (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    used INTEGER NOT NULL CHECK (used >= 0)
                );
                INSERT OR IGNORE INTO tinix_m0_attempt_counter(singleton, used)
                    VALUES (1, 0);
                CREATE TABLE IF NOT EXISTS tinix_m0_request_attempts (
                    correlation_id TEXT PRIMARY KEY,
                    attempt_number INTEGER NOT NULL,
                    reserved_at_ms INTEGER NOT NULL
                );",
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;

        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;

        if let Some(cid) = correlation_id.map(str::trim).filter(|s| !s.is_empty()) {
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT attempt_number FROM tinix_m0_request_attempts WHERE correlation_id = ?1",
                    [cid],
                    |row| row.get(0),
                )
                .ok();
            if existing.is_some() {
                return Err(ReserveAttemptError::DuplicateRequestAttempt {
                    correlation_id: cid.to_string(),
                });
            }
        }

        let used: i64 = tx
            .query_row(
                "SELECT used FROM tinix_m0_attempt_counter WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        if used < 0 {
            return Err(ReserveAttemptError::Storage(
                "attempt counter state is invalid; refusing upstream request".to_string(),
            ));
        }

        let next = used + 1;
        let changed = tx
            .execute(
                "UPDATE tinix_m0_attempt_counter SET used = ?1 WHERE singleton = 1 AND used = ?2",
                (next, used),
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(ReserveAttemptError::Storage(
                "atomic reservation failed; refusing upstream request".to_string(),
            ));
        }

        if let Some(cid) = correlation_id.map(str::trim).filter(|s| !s.is_empty()) {
            let now_ms = chrono::Utc::now().timestamp_millis();
            tx.execute(
                "INSERT INTO tinix_m0_request_attempts(correlation_id, attempt_number, reserved_at_ms) VALUES (?1, ?2, ?3)",
                (cid, next, now_ms),
            )
            .map_err(|_| ReserveAttemptError::DuplicateRequestAttempt {
                correlation_id: cid.to_string(),
            })?;
        }

        tx.commit()
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        return Ok(next as u64);
    }

    #[cfg(not(feature = "tinix-m0-hardened"))]
    {
        let _ = correlation_id;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS native_image_attempt_budget (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    used INTEGER NOT NULL CHECK (used >= 0 AND used <= 5),
                    limit_count INTEGER NOT NULL CHECK (limit_count = 5)
                );
                INSERT OR IGNORE INTO native_image_attempt_budget(singleton, used, limit_count)
                    VALUES (1, 0, 5);",
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;

        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        let (used, limit_count): (i64, i64) = tx
            .query_row(
                "SELECT used, limit_count FROM native_image_attempt_budget WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        if limit_count != MAX_IMAGE_GENERATION_UPSTREAM_ATTEMPTS as i64
            || used < 0
            || used > limit_count
        {
            return Err(ReserveAttemptError::Storage(
                "budget state is invalid; refusing upstream request".to_string(),
            ));
        }
        if used >= limit_count {
            return Err(ReserveAttemptError::Exhausted {
                limit: MAX_IMAGE_GENERATION_UPSTREAM_ATTEMPTS,
            });
        }

        let next = used + 1;
        let changed = tx
            .execute(
                "UPDATE native_image_attempt_budget SET used = ?1 WHERE singleton = 1 AND used = ?2 AND used < limit_count",
                (next, used),
            )
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(ReserveAttemptError::Storage(
                "atomic reservation failed; refusing upstream request".to_string(),
            ));
        }
        tx.commit()
            .map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
        Ok(next as u64)
    }
}

/// Return the on-disk counter for test/report code without exposing unrelated profile data.
pub fn current_usage_at(path: &Path) -> Result<u64, ReserveAttemptError> {
    if !path.exists() {
        return Ok(0);
    }
    let connection =
        Connection::open(path).map_err(|error| ReserveAttemptError::Storage(error.to_string()))?;
    #[cfg(feature = "tinix-m0-hardened")]
    let query = "SELECT used FROM tinix_m0_attempt_counter WHERE singleton = 1";
    #[cfg(not(feature = "tinix-m0-hardened"))]
    let query = "SELECT used FROM native_image_attempt_budget WHERE singleton = 1";

    connection
        .query_row(query, [], |row| row.get::<_, i64>(0))
        .map(|used| used as u64)
        .map_err(|error| ReserveAttemptError::Storage(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn fresh_db_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "native-image-budget-{label}-{}.sqlite3",
            uuid::Uuid::new_v4()
        ))
    }

    #[cfg(not(feature = "tinix-m0-hardened"))]
    #[test]
    fn budget_survives_reopen_and_never_resets_on_restart() {
        let path = fresh_db_path("restart");
        for expected in 1..=MAX_IMAGE_GENERATION_UPSTREAM_ATTEMPTS {
            assert_eq!(reserve_attempt_at(&path).unwrap(), expected);
        }

        // A new object/process opening the same profile sees the consumed budget.
        assert_eq!(
            reserve_attempt_at(&path),
            Err(ReserveAttemptError::Exhausted { limit: 5 })
        );
        assert_eq!(current_usage_at(&path).unwrap(), 5);
        let _ = std::fs::remove_file(path);
    }

    #[cfg(feature = "tinix-m0-hardened")]
    #[test]
    fn tinix_m0_counter_is_not_capped_at_five_and_rejects_duplicate_correlation_id() {
        let path = fresh_db_path("tinix-m0-counter");
        for expected in 1..=12 {
            let cid = format!("req-{expected}");
            assert_eq!(
                reserve_attempt_for_request_at(&path, Some(&cid)).unwrap(),
                expected
            );
        }
        assert_eq!(current_usage_at(&path).unwrap(), 12);

        // Attempting a second upstream reservation for an existing correlation_id must fail-closed.
        assert_eq!(
            reserve_attempt_for_request_at(&path, Some("req-1")),
            Err(ReserveAttemptError::DuplicateRequestAttempt {
                correlation_id: "req-1".to_string()
            })
        );
        assert_eq!(current_usage_at(&path).unwrap(), 12);
        let _ = std::fs::remove_file(path);
    }

    #[cfg(not(feature = "tinix-m0-hardened"))]
    #[test]
    fn actual_child_processes_share_the_same_budget() {
        const CHILD_PATH: &str = "NATIVE_IMAGE_MOCK_CHILD_BUDGET";
        if let Ok(path) = std::env::var(CHILD_PATH) {
            let expected: u64 = std::env::var("NATIVE_IMAGE_MOCK_CHILD_ATTEMPT")
                .unwrap()
                .parse()
                .unwrap();
            let result = reserve_attempt_at(Path::new(&path));
            if expected <= 5 {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert_eq!(result, Err(ReserveAttemptError::Exhausted { limit: 5 }));
            }
            return;
        }
        let path = fresh_db_path("child-process");
        for attempt in 1..=6 {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "proxy::image_attempt_budget::tests::actual_child_processes_share_the_same_budget"])
                .env(CHILD_PATH, &path)
                .env("NATIVE_IMAGE_MOCK_CHILD_ATTEMPT", attempt.to_string())
                .status().unwrap();
            assert!(status.success());
        }
        assert_eq!(current_usage_at(&path).unwrap(), 5);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn evidence_excludes_secrets_and_refuses_overwrite() {
        let path = fresh_db_path("evidence");
        let body = serde_json::json!({
            "project": "sensitive-project", "model": "mock-image", "requestType": "image_gen",
            "request": {"contents": [{"text": "private-prompt"}], "generationConfig": {
                "candidateCount": 1, "api_key": "never-write-me",
                "imageConfig": {"imageSize": "4K", "aspectRatio": "1:1", "token": "never-write-me"}
            }}
        });
        record_attempt_evidence_at(&path, 1, "http://127.0.0.1/mock", &body).unwrap();
        let evidence_path = path.with_extension("attempt-1.json");
        let text = std::fs::read_to_string(&evidence_path).unwrap();
        assert!(text.contains("4K"));
        for secret in ["sensitive-project", "private-prompt", "never-write-me"] {
            assert!(!text.contains(secret));
        }
        assert!(record_attempt_evidence_at(&path, 1, "http://127.0.0.1/mock", &body).is_err());
        let _ = std::fs::remove_file(evidence_path);
    }

    #[cfg(not(feature = "tinix-m0-hardened"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_reservations_cannot_exceed_five() {
        let path = Arc::new(fresh_db_path("concurrent"));
        let mut jobs = Vec::new();
        for _ in 0..32 {
            let path = path.clone();
            jobs.push(tokio::task::spawn_blocking(move || {
                reserve_attempt_at(&path)
            }));
        }

        let mut reserved = 0;
        let mut exhausted = 0;
        for job in jobs {
            match job.await.unwrap() {
                Ok(_) => reserved += 1,
                Err(ReserveAttemptError::Exhausted { limit: 5 }) => exhausted += 1,
                other => panic!("unexpected concurrent reservation result: {other:?}"),
            }
        }
        assert_eq!(reserved, 5);
        assert_eq!(exhausted, 27);
        assert_eq!(current_usage_at(&path).unwrap(), 5);
        let _ = std::fs::remove_file(path.as_path());
    }

    #[cfg(feature = "tinix-m0-hardened")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_reservations_for_same_correlation_id_allow_only_one() {
        let path = Arc::new(fresh_db_path("concurrent-same-cid"));
        let mut jobs = Vec::new();
        for _ in 0..24 {
            let path = path.clone();
            jobs.push(tokio::task::spawn_blocking(move || {
                reserve_attempt_for_request_at(&path, Some("same-correlation-id"))
            }));
        }

        let mut reserved = 0;
        let mut duplicates = 0;
        for job in jobs {
            match job.await.unwrap() {
                Ok(1) => reserved += 1,
                Err(ReserveAttemptError::DuplicateRequestAttempt { .. }) => duplicates += 1,
                other => panic!("unexpected concurrent reservation result: {other:?}"),
            }
        }
        assert_eq!(reserved, 1);
        assert_eq!(duplicates, 23);
        assert_eq!(current_usage_at(&path).unwrap(), 1);
        let _ = std::fs::remove_file(path.as_path());
    }

    #[test]
    fn corrupted_or_nonexistent_state_never_falls_back_to_an_unlimited_budget() {
        let path = fresh_db_path("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not a sqlite database").unwrap();
        assert!(matches!(
            reserve_attempt_at(&path),
            Err(ReserveAttemptError::Storage(_))
        ));
        let _ = std::fs::remove_file(path);
    }
}
