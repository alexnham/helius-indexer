//! Frozen archive-to-RPC bridge plans and the evidence their progress is derived from.
use super::{Store, history::HistoryLog};
use crate::{Error, Result, decoder::DECODER_VERSION};
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, time::Duration};

pub(crate) struct BridgeLog(Connection);

fn read_error(_: rusqlite::Error) -> Error {
    Error("storage_read_failed")
}

impl BridgeLog {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        Store::open(path)?;
        HistoryLog::open(path)?;
        let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error("storage_configuration_failed"))?;
        db.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS bridge_plans(mint TEXT NOT NULL,decoder TEXT NOT NULL,start INTEGER NOT NULL,archive_tip INTEGER NOT NULL,reference_slot INTEGER NOT NULL,snapshot TEXT NOT NULL,next_slot INTEGER NOT NULL,PRIMARY KEY(mint,decoder,start)); CREATE TABLE IF NOT EXISTS bridge_overlap_proofs(mint TEXT NOT NULL,decoder TEXT NOT NULL,slot INTEGER NOT NULL,verified_attempt INTEGER NOT NULL,PRIMARY KEY(mint,decoder,slot));").map_err(|_|Error("storage_write_failed"))?;
        Ok(Self(db))
    }

    /// (archive tip, reference slot, snapshot); the snapshot is truncated past 16 KiB
    /// so an oversized stored value is detectable without loading it.
    pub(crate) fn plan(&self, mint: &str, start: u64) -> Result<Option<(i64, i64, String)>> {
        self.0.query_row("SELECT archive_tip,reference_slot,substr(snapshot,1,16385) FROM bridge_plans WHERE mint=?1 AND decoder=?2 AND start=?3",params![mint,DECODER_VERSION,start as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(read_error)
    }

    /// First plan for a scope wins; its cursor starts at `start`.
    pub(crate) fn freeze_plan(
        &self,
        mint: &str,
        start: u64,
        tip: u64,
        end: u64,
        snapshot: &str,
    ) -> Result<()> {
        self.0
            .execute(
                "INSERT OR IGNORE INTO bridge_plans VALUES(?1,?2,?3,?4,?5,?6,?3)",
                params![
                    mint,
                    DECODER_VERSION,
                    start as i64,
                    tip as i64,
                    end as i64,
                    snapshot
                ],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(())
    }

    pub(crate) fn next_slot(&self, mint: &str, start: u64) -> Result<i64> {
        self.0
            .query_row(
                "SELECT next_slot FROM bridge_plans WHERE mint=?1 AND decoder=?2 AND start=?3",
                params![mint, DECODER_VERSION, start as i64],
                |r| r.get(0),
            )
            .map_err(read_error)
    }

    pub(crate) fn advance(&self, mint: &str, start: u64, next: u64) -> Result<()> {
        self.0
            .execute(
                "UPDATE bridge_plans SET next_slot=?1 WHERE mint=?2 AND decoder=?3 AND start=?4",
                params![next as i64, mint, DECODER_VERSION, start as i64],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(())
    }

    /// The overlap proof matches the slot's current attempt generation; retries invalidate it.
    pub(crate) fn overlap_proven(&self, mint: &str, slot: u64) -> Result<bool> {
        self.0.query_row("SELECT EXISTS(SELECT 1 FROM bridge_overlap_proofs p JOIN historical_slots h USING(mint,decoder,slot) WHERE p.mint=?1 AND p.decoder=?2 AND p.slot=?3 AND p.verified_attempt=h.attempts AND h.status='complete')",params![mint,DECODER_VERSION,slot as i64],|r|r.get(0)).map_err(read_error)
    }

    pub(crate) fn source_complete(&self, mint: &str, slot: u64, source: &str) -> Result<bool> {
        self.0.query_row("SELECT EXISTS(SELECT 1 FROM historical_source_slots WHERE mint=?1 AND decoder=?2 AND slot=?3 AND source_kind=?4 AND status='complete')",params![mint,DECODER_VERSION,slot as i64,source],|r|r.get(0)).map_err(read_error)
    }

    /// Slots in `start..=end` verified by the source their side of `tip` requires, ascending.
    pub(crate) fn verified_slots(
        &self,
        mint: &str,
        start: u64,
        end: u64,
        tip: u64,
    ) -> Result<Vec<i64>> {
        let mut query=self.0.prepare("SELECT h.slot FROM historical_slots h WHERE h.mint=?1 AND h.decoder=?2 AND h.slot BETWEEN ?3 AND ?4 AND (h.status='skipped' OR (h.status='complete' AND EXISTS(SELECT 1 FROM historical_source_slots s WHERE s.mint=h.mint AND s.decoder=h.decoder AND s.slot=h.slot AND s.status='complete' AND (s.source_kind=CASE WHEN h.slot<=?5 THEN 'old-faithful-jetstreamer' ELSE 'finalized-mainnet-rpc' END OR (h.slot>?5 AND s.source_kind='archive-rpc-corroborated' AND EXISTS(SELECT 1 FROM bridge_overlap_proofs p WHERE p.mint=h.mint AND p.decoder=h.decoder AND p.slot=h.slot AND p.verified_attempt=h.attempts)))))) ORDER BY h.slot").map_err(read_error)?;
        query
            .query_map(
                params![mint, DECODER_VERSION, start as i64, end as i64, tip as i64],
                |r| r.get::<_, i64>(0),
            )
            .map_err(read_error)?
            .collect::<std::result::Result<_, _>>()
            .map_err(read_error)
    }

    #[cfg(test)]
    pub(crate) fn sql(&self) -> &Connection {
        &self.0
    }
}
