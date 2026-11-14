//! Durable historical progress: per-slot status, retained source responses and
//! per-source completion. Scoped to the current decoder revision.
use super::Database;
use crate::{Error, Result, decoder::DECODER_VERSION};
use postgres::Client;
use std::cell::RefCell;

pub(crate) struct HistoryLog(RefCell<Client>);

/// (status, error class, attempts, block time, first pending unix seconds)
pub(crate) type SlotRecord = (String, Option<String>, u64, Option<i64>, u64);

fn write_error(_: postgres::Error) -> Error {
    Error("history_write_failed")
}

fn read_error(_: postgres::Error) -> Error {
    Error("history_read_failed")
}

impl HistoryLog {
    pub(crate) fn open(db: &Database) -> Result<Self> {
        Ok(Self(RefCell::new(
            db.connect().map_err(|_| Error("history_open_failed"))?,
        )))
    }

    /// Creates unknown records for unscheduled slots; existing progress is kept.
    pub(crate) fn schedule(&self, mint: &str, slot: u64, now: u64) -> Result<()> {
        self.0.borrow_mut().execute("INSERT INTO historical_slots(mint,decoder,slot,status,first_pending) VALUES ($1,$2,$3,'unknown',$4) ON CONFLICT DO NOTHING",&[&mint,&DECODER_VERSION,&(slot as i64),&(now as i64)]).map_err(write_error)?;
        Ok(())
    }

    pub(crate) fn slot(&self, mint: &str, slot: u64) -> Result<Option<SlotRecord>> {
        let row = self.0.borrow_mut().query_opt("SELECT status,error_class,attempts,block_time,first_pending FROM historical_slots WHERE mint=$1 AND decoder=$2 AND slot=$3",&[&mint,&DECODER_VERSION,&(slot as i64)]).map_err(read_error)?;
        row.map(|r| {
            Ok((
                r.get(0),
                r.get(1),
                u64::try_from(r.get::<_, i64>(2)).map_err(|_| Error("history_read_failed"))?,
                r.get(3),
                u64::try_from(r.get::<_, i64>(4)).map_err(|_| Error("history_read_failed"))?,
            ))
        })
        .transpose()
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
        let mut db = self.0.borrow_mut();
        let mut transaction = db.transaction().map_err(write_error)?;
        transaction.execute("UPDATE historical_slots SET status=$1,error_class=$2,block_time=$3 WHERE mint=$4 AND decoder=$5 AND slot=$6",&[&status,&error,&time,&mint,&DECODER_VERSION,&(slot as i64)]).map_err(write_error)?;
        transaction.execute("INSERT INTO historical_source_slots VALUES($1,$2,$3,$4,$5) ON CONFLICT(mint,decoder,slot,source_kind) DO UPDATE SET status=excluded.status",&[&mint,&DECODER_VERSION,&(slot as i64),&source,&status]).map_err(write_error)?;
        transaction.commit().map_err(write_error)?;
        Ok(())
    }

    pub(crate) fn source_done(&self, mint: &str, slot: u64, source: &str) -> Result<bool> {
        Ok(self.0.borrow_mut().query_one("SELECT EXISTS(SELECT 1 FROM historical_source_slots WHERE mint=$1 AND decoder=$2 AND slot=$3 AND source_kind=$4 AND status IN ('complete','skipped'))",&[&mint,&DECODER_VERSION,&(slot as i64),&source]).map_err(read_error)?.get(0))
    }

    pub(crate) fn add_attempt(&self, mint: &str, slot: u64) -> Result<()> {
        self.0.borrow_mut().execute("UPDATE historical_slots SET attempts=attempts+1 WHERE mint=$1 AND decoder=$2 AND slot=$3",&[&mint,&DECODER_VERSION,&(slot as i64)]).map_err(write_error)?;
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
        // Recorded once so skip evidence never parses stored bytes again.
        let produced_block =
            serde_json::from_slice::<serde_json::Value>(raw).is_ok_and(|v| v["result"].is_object());
        self.0
            .borrow_mut()
            .execute(
                "INSERT INTO historical_inputs VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &mint,
                    &DECODER_VERSION,
                    &(slot as i64),
                    &(attempt as i64),
                    &source,
                    &(received_at as i64),
                    &produced_block,
                    &raw,
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
        Ok(self.0.borrow_mut().query_opt("SELECT response FROM historical_inputs WHERE mint=$1 AND slot=$2 AND source_kind=$3 ORDER BY received_at DESC,attempt DESC LIMIT 1",&[&mint,&(slot as i64),&source]).map_err(read_error)?.map(|r| r.get(0)))
    }

    /// Like `latest_input` but refuses to load responses over 64 MiB: `Some(None)`.
    #[cfg(feature = "archive")]
    pub(crate) fn bounded_input(
        &self,
        mint: &str,
        slot: u64,
        source: &str,
    ) -> Result<Option<Option<Vec<u8>>>> {
        Ok(self.0.borrow_mut().query_opt("SELECT CASE WHEN octet_length(response)<=67108864 THEN response END FROM historical_inputs WHERE mint=$1 AND slot=$2 AND source_kind=$3 ORDER BY received_at DESC,attempt DESC,decoder DESC LIMIT 1",&[&mint,&(slot as i64),&source]).map_err(read_error)?.map(|r| r.get(0)))
    }

    /// Any source returned a produced block for this slot, so it must never be marked skipped.
    pub(crate) fn produced_block_observed(&self, slot: u64) -> Result<bool> {
        Ok(self
            .0
            .borrow_mut()
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM historical_inputs WHERE slot=$1 AND produced_block)",
                &[&(slot as i64)],
            )
            .map_err(read_error)?
            .get(0))
    }

    /// A verified later block whose parent precedes `slot` proves no block was produced there.
    #[cfg(feature = "archive")]
    pub(crate) fn parent_link_spans(&self, mint: &str, slot: u64) -> Result<bool> {
        Ok(self.0.borrow_mut().query_one("SELECT EXISTS(SELECT 1 FROM historical_slots h JOIN historical_block_identity b USING(slot) WHERE h.mint=$1 AND h.decoder=$2 AND h.status='complete' AND h.slot>$3 AND b.parent_slot<$3)",&[&mint,&DECODER_VERSION,&(slot as i64)]).map_err(read_error)?.get(0))
    }

    #[cfg(feature = "archive")]
    pub(crate) fn require_reverification(&self, mint: &str, start: u64, end: u64) -> Result<()> {
        self.0.borrow_mut().execute("UPDATE historical_slots SET status='unknown',error_class='source_overlap_reverification' WHERE mint=$1 AND decoder=$2 AND slot BETWEEN $3 AND $4",&[&mint,&DECODER_VERSION,&(start as i64),&(end as i64)]).map_err(write_error)?;
        Ok(())
    }

    #[cfg(feature = "archive")]
    pub(crate) fn acknowledge_bridge_overlap(&self, mint: &str, slot: u64) -> Result<()> {
        self.0.borrow_mut().execute("INSERT INTO bridge_overlap_proofs SELECT mint,decoder,slot,attempts FROM historical_slots WHERE mint=$1 AND decoder=$2 AND slot=$3 AND status='complete' ON CONFLICT(mint,decoder,slot) DO UPDATE SET verified_attempt=excluded.verified_attempt",&[&mint,&DECODER_VERSION,&(slot as i64)]).map_err(write_error)?;
        Ok(())
    }
}
