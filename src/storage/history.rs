//! Durable historical progress: per-slot status, retained source responses and
//! per-source completion. Scoped to the current decoder revision.
use crate::{Error, Result, decoder::DECODER_VERSION};
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, time::Duration};

pub(crate) struct HistoryLog(Connection);

/// (status, error class, attempts, block time, first pending unix seconds)
pub(crate) type SlotRecord = (String, Option<String>, u64, Option<i64>, u64);

fn write_error(_: rusqlite::Error) -> Error {
    Error("history_write_failed")
}

fn read_error(_: rusqlite::Error) -> Error {
    Error("history_read_failed")
}

impl HistoryLog {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path).map_err(|_| Error("history_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(write_error)?;
        db.execute_batch(
            "PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS historical_slots (
                mint TEXT NOT NULL, decoder TEXT NOT NULL, slot INTEGER NOT NULL,
                status TEXT NOT NULL, error_class TEXT, first_pending INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, block_time INTEGER,
                PRIMARY KEY(mint,decoder,slot));
            CREATE TABLE IF NOT EXISTS historical_inputs (
                mint TEXT NOT NULL, decoder TEXT NOT NULL, slot INTEGER NOT NULL,
                attempt INTEGER NOT NULL, source_kind TEXT NOT NULL, received_at INTEGER NOT NULL,
                response BLOB NOT NULL, PRIMARY KEY(mint,decoder,slot,attempt));
            CREATE TABLE IF NOT EXISTS historical_source_slots (
                mint TEXT NOT NULL,decoder TEXT NOT NULL,slot INTEGER NOT NULL,source_kind TEXT NOT NULL,status TEXT NOT NULL,
                PRIMARY KEY(mint,decoder,slot,source_kind));
            CREATE TABLE IF NOT EXISTS historical_block_identity (
                slot INTEGER PRIMARY KEY,blockhash TEXT NOT NULL,parent_slot INTEGER NOT NULL,previous_blockhash TEXT NOT NULL,
                transaction_ids_hash BLOB NOT NULL,block_time INTEGER);
            CREATE TABLE IF NOT EXISTS bridge_overlap_proofs(mint TEXT NOT NULL,decoder TEXT NOT NULL,slot INTEGER NOT NULL,verified_attempt INTEGER NOT NULL,PRIMARY KEY(mint,decoder,slot));",
        )
        .map_err(write_error)?;
        Ok(Self(db))
    }

    /// Creates unknown records for unscheduled slots; existing progress is kept.
    pub(crate) fn schedule(&self, mint: &str, slot: u64, now: u64) -> Result<()> {
        self.0.execute("INSERT OR IGNORE INTO historical_slots(mint,decoder,slot,status,first_pending) VALUES (?1,?2,?3,'unknown',?4)",params![mint,DECODER_VERSION,slot as i64,now as i64]).map_err(write_error)?;
        Ok(())
    }

    pub(crate) fn slot(&self, mint: &str, slot: u64) -> Result<Option<SlotRecord>> {
        self.0.query_row("SELECT status,error_class,attempts,block_time,first_pending FROM historical_slots WHERE mint=?1 AND decoder=?2 AND slot=?3",params![mint,DECODER_VERSION,slot as i64],|r| Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,u64::try_from(r.get::<_,i64>(2)?).map_err(|_|rusqlite::Error::InvalidQuery)?,r.get::<_,Option<i64>>(3)?,u64::try_from(r.get::<_,i64>(4)?).map_err(|_|rusqlite::Error::InvalidQuery)?))).optional().map_err(read_error)
    }

    /// Slot status and the active source's completion change together.
    pub(crate) fn set_status(
        &self,
        mint: &str,
        slot: u64,
        source: &str,
        status: &str,
        error: Option<&str>,
        time: Option<i64>,
    ) -> Result<()> {
        let transaction = self.0.unchecked_transaction().map_err(write_error)?;
        transaction.execute("UPDATE historical_slots SET status=?1,error_class=?2,block_time=?3 WHERE mint=?4 AND decoder=?5 AND slot=?6",params![status,error,time,mint,DECODER_VERSION,slot as i64]).map_err(write_error)?;
        transaction.execute("INSERT INTO historical_source_slots VALUES(?1,?2,?3,?4,?5) ON CONFLICT(mint,decoder,slot,source_kind) DO UPDATE SET status=excluded.status",params![mint,DECODER_VERSION,slot as i64,source,status]).map_err(write_error)?;
        transaction.commit().map_err(write_error)?;
        Ok(())
    }

    pub(crate) fn source_done(&self, mint: &str, slot: u64, source: &str) -> Result<bool> {
        self.0.query_row("SELECT EXISTS(SELECT 1 FROM historical_source_slots WHERE mint=?1 AND decoder=?2 AND slot=?3 AND source_kind=?4 AND status IN ('complete','skipped'))",params![mint,DECODER_VERSION,slot as i64,source],|r|r.get(0)).map_err(read_error)
    }

    pub(crate) fn add_attempt(&self, mint: &str, slot: u64) -> Result<()> {
        self.0.execute("UPDATE historical_slots SET attempts=attempts+1 WHERE mint=?1 AND decoder=?2 AND slot=?3",params![mint,DECODER_VERSION,slot as i64]).map_err(write_error)?;
        Ok(())
    }

    /// Raw input acknowledgment; every attempt is retained for reproduction.
    pub(crate) fn retain_input(
        &self,
        mint: &str,
        slot: u64,
        attempt: u64,
        source: &str,
        received_at: u64,
        raw: &[u8],
    ) -> Result<()> {
        self.0
            .execute(
                "INSERT INTO historical_inputs VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    mint,
                    DECODER_VERSION,
                    slot as i64,
                    attempt as i64,
                    source,
                    received_at as i64,
                    raw
                ],
            )
            .map_err(write_error)?;
        Ok(())
    }

    /// Newest retained response for this mint, slot and source, from any decoder revision's attempts.
    pub(crate) fn latest_input(
        &self,
        mint: &str,
        slot: u64,
        source: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.0.query_row("SELECT response FROM historical_inputs WHERE mint=?1 AND slot=?2 AND source_kind=?3 ORDER BY received_at DESC,attempt DESC LIMIT 1",params![mint,slot as i64,source],|r|r.get(0)).optional().map_err(read_error)
    }

    /// Like `latest_input` but refuses to load responses over 64 MiB: `Some(None)`.
    #[cfg(feature = "archive")]
    pub(crate) fn bounded_input(
        &self,
        mint: &str,
        slot: u64,
        source: &str,
    ) -> Result<Option<Option<Vec<u8>>>> {
        self.0.query_row("SELECT CASE WHEN length(response)<=67108864 THEN response END FROM historical_inputs WHERE mint=?1 AND slot=?2 AND source_kind=?3 ORDER BY received_at DESC,attempt DESC,decoder DESC LIMIT 1",params![mint,slot as i64,source],|r|r.get(0)).optional().map_err(read_error)
    }

    /// Any source returned a produced block for this slot, so it must never be marked skipped.
    pub(crate) fn produced_block_observed(&self, slot: u64) -> Result<bool> {
        self.0.query_row("SELECT EXISTS(SELECT 1 FROM historical_inputs WHERE slot=?1 AND CASE WHEN json_valid(CAST(response AS TEXT)) THEN json_type(CAST(response AS TEXT),'$.result')='object' ELSE 0 END)",[slot as i64],|r|r.get(0)).map_err(read_error)
    }

    /// A verified later block whose parent precedes `slot` proves no block was produced there.
    #[cfg(feature = "archive")]
    pub(crate) fn parent_link_spans(&self, mint: &str, slot: u64) -> Result<bool> {
        self.0.query_row("SELECT EXISTS(SELECT 1 FROM historical_slots h JOIN historical_block_identity b USING(slot) WHERE h.mint=?1 AND h.decoder=?2 AND h.status='complete' AND h.slot>?3 AND b.parent_slot<?3)",params![mint,DECODER_VERSION,slot as i64],|r|r.get(0)).map_err(read_error)
    }

    #[cfg(feature = "archive")]
    pub(crate) fn require_reverification(&self, mint: &str, start: u64, end: u64) -> Result<()> {
        self.0.execute("UPDATE historical_slots SET status='unknown',error_class='source_overlap_reverification' WHERE mint=?1 AND decoder=?2 AND slot BETWEEN ?3 AND ?4",params![mint,DECODER_VERSION,start as i64,end as i64]).map_err(write_error)?;
        Ok(())
    }

    #[cfg(feature = "archive")]
    pub(crate) fn acknowledge_bridge_overlap(&self, mint: &str, slot: u64) -> Result<()> {
        self.0.execute("INSERT INTO bridge_overlap_proofs SELECT mint,decoder,slot,attempts FROM historical_slots WHERE mint=?1 AND decoder=?2 AND slot=?3 AND status='complete' ON CONFLICT(mint,decoder,slot) DO UPDATE SET verified_attempt=excluded.verified_attempt",params![mint,DECODER_VERSION,slot as i64]).map_err(write_error)?;
        Ok(())
    }
}
