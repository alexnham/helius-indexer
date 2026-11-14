//! Durable live intake: raw wire input, retry cursor and run records.
use crate::{Error, Result};
use rusqlite::{Connection, OpenFlags, params};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) struct LiveLog(Connection);

/// One stored run, as recorded; the caller derives ages and coverage claims.
pub(crate) struct Run {
    pub id: i64,
    pub from_slot: i64,
    pub state: String,
    pub reference_slot: Option<i64>,
    pub reference_at: Option<i64>,
    pub replay_first: Option<i64>,
    pub replay_at: Option<i64>,
    pub mint: String,
    pub decoder: String,
}

pub(crate) struct Snapshot {
    pub runs: Vec<Run>,
    pub retry_cursor: Option<i64>,
    pub pending: i64,
    pub oldest_pending_at: Option<i64>,
    pub last_received_slot: Option<i64>,
    /// Raw inputs received at or after `since`: (received, decoded).
    pub recent: (i64, i64),
}

impl LiveLog {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error("storage_configuration_failed"))?;
        db.execute_batch("PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS live_raw(id INTEGER PRIMARY KEY, slot INTEGER NOT NULL, protobuf BLOB NOT NULL, proto_schema TEXT NOT NULL DEFAULT 'laserstream-core-proto/14.0.0', status TEXT NOT NULL, received_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS live_retry_cursor(singleton INTEGER PRIMARY KEY CHECK(singleton=1),last_id INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS live_runs(id INTEGER PRIMARY KEY, from_slot INTEGER NOT NULL, mint TEXT NOT NULL, decoder TEXT NOT NULL, status TEXT NOT NULL, reference_slot INTEGER, reference_at INTEGER, replay_first INTEGER, replay_at INTEGER);")
            .map_err(|_|Error("storage_configuration_failed"))?;
        Ok(Self(db))
    }

    pub(crate) fn retain(&self, slot: i64, protobuf: &[u8]) -> Result<i64> {
        self.0
            .execute(
                "INSERT INTO live_raw(slot,protobuf,status,received_at) VALUES(?1,?2,'pending',?3)",
                params![slot, protobuf, now()],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(self.0.last_insert_rowid())
    }

    pub(crate) fn set_status(&self, id: i64, status: &str) -> Result<()> {
        self.0
            .execute(
                "UPDATE live_raw SET status=?1 WHERE id=?2",
                params![status, id],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(())
    }

    pub(crate) fn retry_cursor(&self) -> Result<i64> {
        self.0
            .query_row(
                "SELECT COALESCE(MAX(last_id),0) FROM live_retry_cursor",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))
    }

    /// Unresolved inputs after the cursor first, then wrapping to earlier ones.
    pub(crate) fn pending_after(&self, cursor: i64, limit: usize) -> Result<Vec<i64>> {
        let mut s = self
            .0
            .prepare(
                "SELECT id FROM live_raw WHERE status!='decoded' ORDER BY (id<=?1),id LIMIT ?2",
            )
            .map_err(|_| Error("storage_read_failed"))?;
        s.query_map(params![cursor, limit as i64], |r| r.get(0))
            .map_err(|_| Error("storage_read_failed"))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| Error("storage_read_failed"))
    }

    pub(crate) fn advance_retry_cursor(&self, id: i64) -> Result<()> {
        self.0.execute("INSERT INTO live_retry_cursor VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET last_id=excluded.last_id", [id])
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(())
    }

    /// Returns (protobuf, proto schema); inputs over 16 MiB are refused before loading.
    pub(crate) fn raw(&self, id: i64) -> Result<(Vec<u8>, String)> {
        self.0
            .query_row(
                "SELECT protobuf,proto_schema FROM live_raw WHERE id=?1 AND length(protobuf)<=16777216",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| Error("storage_read_failed"))
    }

    pub(crate) fn start_run(&self, from: i64, mint: &str, decoder: &str) -> Result<i64> {
        self.0
            .execute(
                "INSERT INTO live_runs(from_slot,mint,decoder,status) VALUES(?1,?2,?3,'running')",
                params![from, mint, decoder],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(self.0.last_insert_rowid())
    }

    pub(crate) fn record_reference(&self, run: i64, reference: i64, replay: i64) -> Result<()> {
        self.0.execute("UPDATE live_runs SET reference_slot=?1,reference_at=?2,replay_first=?3,replay_at=?2 WHERE id=?4",params![reference,now(),replay,run])
            .map_err(|_|Error("storage_write_failed"))?;
        Ok(())
    }

    pub(crate) fn finish_run(&self, run: i64, status: &str) -> Result<()> {
        self.0
            .execute(
                "UPDATE live_runs SET status=?1 WHERE id=?2",
                params![status, run],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(())
    }

    /// Read-only; `None` when live intake never initialized this database.
    pub(crate) fn snapshot(path: &Path, since: i64) -> Result<Option<Snapshot>> {
        if !path.exists() {
            return Err(Error("storage_open_failed"));
        }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let exists = |table: &str| -> Result<bool> {
            db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                [table],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))
        };
        if !exists("live_runs")? {
            return Ok(None);
        }
        let mut stmt=db.prepare("SELECT id,from_slot,status,reference_slot,reference_at,replay_first,replay_at,mint,decoder FROM live_runs ORDER BY id DESC LIMIT 100").map_err(|_|Error("storage_read_failed"))?;
        let runs = stmt
            .query_map([], |r| {
                Ok(Run {
                    id: r.get(0)?,
                    from_slot: r.get(1)?,
                    state: r.get(2)?,
                    reference_slot: r.get(3)?,
                    reference_at: r.get(4)?,
                    replay_first: r.get(5)?,
                    replay_at: r.get(6)?,
                    mint: r.get(7)?,
                    decoder: r.get(8)?,
                })
            })
            .map_err(|_| Error("storage_read_failed"))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error("storage_read_failed"))?;
        let retry_cursor: Option<i64> = if exists("live_retry_cursor")? {
            db.query_row("SELECT MAX(last_id) FROM live_retry_cursor", [], |r| {
                r.get(0)
            })
            .map_err(|_| Error("storage_read_failed"))?
        } else {
            None
        };
        let (pending, oldest_pending_at) = db
            .query_row(
                "SELECT COUNT(*),MIN(received_at) FROM live_raw WHERE status!='decoded'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        let last_received_slot: Option<i64> = db
            .query_row("SELECT MAX(slot) FROM live_raw", [], |r| r.get(0))
            .map_err(|_| Error("storage_read_failed"))?;
        let recent = db
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(status='decoded'),0) FROM live_raw WHERE received_at>=?1",
                [since],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        Ok(Some(Snapshot {
            runs,
            retry_cursor,
            pending,
            oldest_pending_at,
            last_received_slot,
            recent,
        }))
    }

    #[cfg(test)]
    pub(crate) fn sql(&self) -> &Connection {
        &self.0
    }
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}
