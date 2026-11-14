//! Durable live intake: raw wire input, retry cursor and run records.
use super::{Database, read_failed, write_failed};
use crate::Result;
use postgres::Client;
use std::{
    cell::RefCell,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) struct LiveLog(RefCell<Client>);

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
    pub(crate) fn open(db: &Database) -> Result<Self> {
        Ok(Self(RefCell::new(db.connect()?)))
    }

    /// Retains many wire payloads with one statement; ids return in input order.
    pub(crate) fn retain_batch(&self, rows: &[(i64, Vec<u8>)]) -> Result<Vec<i64>> {
        let slots: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let payloads: Vec<&[u8]> = rows.iter().map(|r| r.1.as_slice()).collect();
        let mut ids: Vec<i64> = self
            .0
            .borrow_mut()
            .query(
                "INSERT INTO live_raw(slot,protobuf,status,received_at)
                 SELECT s,p,'pending',$3 FROM unnest($1::bigint[],$2::bytea[]) WITH ORDINALITY AS u(s,p,n) ORDER BY n
                 RETURNING id",
                &[&slots, &payloads, &now()],
            )
            .map_err(write_failed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        // Identity values follow insertion order, which follows the ordinality.
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn retry_cursor(&self) -> Result<i64> {
        Ok(self
            .0
            .borrow_mut()
            .query_one(
                "SELECT COALESCE(MAX(last_id),0) FROM live_retry_cursor",
                &[],
            )
            .map_err(read_failed)?
            .get(0))
    }

    /// Unresolved inputs after the cursor first, then wrapping to earlier ones.
    pub(crate) fn pending_after(&self, cursor: i64, limit: usize) -> Result<Vec<i64>> {
        Ok(self
            .0
            .borrow_mut()
            .query(
                "SELECT id FROM live_raw WHERE status<>'decoded' ORDER BY (id<=$1),id LIMIT $2",
                &[&cursor, &(limit as i64)],
            )
            .map_err(read_failed)?
            .iter()
            .map(|r| r.get(0))
            .collect())
    }

    pub(crate) fn advance_retry_cursor(&self, id: i64) -> Result<()> {
        self.0.borrow_mut().execute("INSERT INTO live_retry_cursor VALUES(1,$1) ON CONFLICT(singleton) DO UPDATE SET last_id=excluded.last_id", &[&id])
            .map_err(write_failed)?;
        Ok(())
    }

    /// Returns (protobuf, proto schema); inputs over 16 MiB are refused before loading.
    pub(crate) fn raw(&self, id: i64) -> Result<(Vec<u8>, String)> {
        let row = self
            .0
            .borrow_mut()
            .query_one(
                "SELECT protobuf,proto_schema FROM live_raw WHERE id=$1 AND octet_length(protobuf)<=16777216",
                &[&id],
            )
            .map_err(read_failed)?;
        Ok((row.get(0), row.get(1)))
    }

    pub(crate) fn start_run(&self, from: i64, mint: &str, decoder: &str) -> Result<i64> {
        Ok(self
            .0
            .borrow_mut()
            .query_one(
                "INSERT INTO live_runs(from_slot,mint,decoder,status) VALUES($1,$2,$3,'running') RETURNING id",
                &[&from, &mint, &decoder],
            )
            .map_err(write_failed)?
            .get(0))
    }

    pub(crate) fn record_reference(&self, run: i64, reference: i64, replay: i64) -> Result<()> {
        self.0.borrow_mut().execute("UPDATE live_runs SET reference_slot=$1,reference_at=$2,replay_first=$3,replay_at=$2 WHERE id=$4",&[&reference,&now(),&replay,&run])
            .map_err(write_failed)?;
        Ok(())
    }

    pub(crate) fn finish_run(&self, run: i64, status: &str) -> Result<()> {
        self.0
            .borrow_mut()
            .execute(
                "UPDATE live_runs SET status=$1 WHERE id=$2",
                &[&status, &run],
            )
            .map_err(write_failed)?;
        Ok(())
    }

    /// Read-only and consistent. The schema always exists, so a session without runs reports empty state.
    pub(crate) fn snapshot(db: &Database, since: i64) -> Result<Snapshot> {
        let mut client = db.connect()?;
        let mut tx = client
            .build_transaction()
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(read_failed)?;
        let runs: Vec<Run> = tx.query("SELECT id,from_slot,status,reference_slot,reference_at,replay_first,replay_at,mint,decoder FROM live_runs ORDER BY id DESC LIMIT 100", &[])
            .map_err(read_failed)?
            .iter()
            .map(|r| Run {
                id: r.get(0),
                from_slot: r.get(1),
                state: r.get(2),
                reference_slot: r.get(3),
                reference_at: r.get(4),
                replay_first: r.get(5),
                replay_at: r.get(6),
                mint: r.get(7),
                decoder: r.get(8),
            })
            .collect();
        let retry_cursor: Option<i64> = tx
            .query_one("SELECT MAX(last_id) FROM live_retry_cursor", &[])
            .map_err(read_failed)?
            .get(0);
        let row = tx
            .query_one(
                "SELECT COUNT(*),MIN(received_at) FROM live_raw WHERE status<>'decoded'",
                &[],
            )
            .map_err(read_failed)?;
        let (pending, oldest_pending_at) = (row.get(0), row.get(1));
        let last_received_slot: Option<i64> = tx
            .query_one("SELECT MAX(slot) FROM live_raw", &[])
            .map_err(read_failed)?
            .get(0);
        let row = tx
            .query_one(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE status='decoded') FROM live_raw WHERE received_at>=$1",
                &[&since],
            )
            .map_err(read_failed)?;
        let recent = (row.get(0), row.get(1));
        tx.commit().map_err(read_failed)?;
        Ok(Snapshot {
            runs,
            retry_cursor,
            pending,
            oldest_pending_at,
            last_received_slot,
            recent,
        })
    }
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}
