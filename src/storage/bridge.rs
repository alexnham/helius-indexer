//! Frozen archive-to-RPC bridge plans and the evidence their progress is derived from.
use super::{Database, read_failed, write_failed};
use crate::{Result, decoder::DECODER_VERSION};
use postgres::Client;
use std::cell::RefCell;

pub(crate) struct BridgeLog(RefCell<Client>);

impl BridgeLog {
    pub(crate) fn open(db: &Database) -> Result<Self> {
        Ok(Self(RefCell::new(db.connect()?)))
    }

    /// (archive tip, reference slot, snapshot); the snapshot is truncated past 16 KiB
    /// so an oversized stored value is detectable without loading it.
    pub(crate) fn plan(&self, mint: &str, start: u64) -> Result<Option<(i64, i64, String)>> {
        Ok(self.0.borrow_mut().query_opt("SELECT archive_tip,reference_slot,substr(snapshot,1,16385) FROM bridge_plans WHERE mint=$1 AND decoder=$2 AND start=$3",&[&mint,&DECODER_VERSION,&(start as i64)])
            .map_err(read_failed)?
            .map(|r| (r.get(0), r.get(1), r.get(2))))
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
            .borrow_mut()
            .execute(
                "INSERT INTO bridge_plans VALUES($1,$2,$3,$4,$5,$6,$3) ON CONFLICT DO NOTHING",
                &[
                    &mint,
                    &DECODER_VERSION,
                    &(start as i64),
                    &(tip as i64),
                    &(end as i64),
                    &snapshot,
                ],
            )
            .map_err(write_failed)?;
        Ok(())
    }

    pub(crate) fn next_slot(&self, mint: &str, start: u64) -> Result<i64> {
        Ok(self
            .0
            .borrow_mut()
            .query_one(
                "SELECT next_slot FROM bridge_plans WHERE mint=$1 AND decoder=$2 AND start=$3",
                &[&mint, &DECODER_VERSION, &(start as i64)],
            )
            .map_err(read_failed)?
            .get(0))
    }

    pub(crate) fn advance(&self, mint: &str, start: u64, next: u64) -> Result<()> {
        self.0
            .borrow_mut()
            .execute(
                "UPDATE bridge_plans SET next_slot=$1 WHERE mint=$2 AND decoder=$3 AND start=$4",
                &[&(next as i64), &mint, &DECODER_VERSION, &(start as i64)],
            )
            .map_err(write_failed)?;
        Ok(())
    }

    /// The overlap proof matches the slot's current attempt generation; retries invalidate it.
    pub(crate) fn overlap_proven(&self, mint: &str, slot: u64) -> Result<bool> {
        Ok(self.0.borrow_mut().query_one("SELECT EXISTS(SELECT 1 FROM bridge_overlap_proofs p JOIN historical_slots h USING(mint,decoder,slot) WHERE p.mint=$1 AND p.decoder=$2 AND p.slot=$3 AND p.verified_attempt=h.attempts AND h.status='complete')",&[&mint,&DECODER_VERSION,&(slot as i64)]).map_err(read_failed)?.get(0))
    }

    pub(crate) fn source_complete(&self, mint: &str, slot: u64, source: &str) -> Result<bool> {
        Ok(self.0.borrow_mut().query_one("SELECT EXISTS(SELECT 1 FROM historical_source_slots WHERE mint=$1 AND decoder=$2 AND slot=$3 AND source_kind=$4 AND status='complete')",&[&mint,&DECODER_VERSION,&(slot as i64),&source]).map_err(read_failed)?.get(0))
    }

    /// Slots in `start..=end` verified by the source their side of `tip` requires, ascending.
    pub(crate) fn verified_slots(
        &self,
        mint: &str,
        start: u64,
        end: u64,
        tip: u64,
    ) -> Result<Vec<i64>> {
        Ok(self.0.borrow_mut().query("SELECT h.slot FROM historical_slots h WHERE h.mint=$1 AND h.decoder=$2 AND h.slot BETWEEN $3 AND $4 AND (h.status='skipped' OR (h.status='complete' AND EXISTS(SELECT 1 FROM historical_source_slots s WHERE s.mint=h.mint AND s.decoder=h.decoder AND s.slot=h.slot AND s.status='complete' AND (s.source_kind=CASE WHEN h.slot<=$5 THEN 'old-faithful-jetstreamer' ELSE 'finalized-mainnet-rpc' END OR (h.slot>$5 AND s.source_kind='archive-rpc-corroborated' AND EXISTS(SELECT 1 FROM bridge_overlap_proofs p WHERE p.mint=h.mint AND p.decoder=h.decoder AND p.slot=h.slot AND p.verified_attempt=h.attempts)))))) ORDER BY h.slot",
                &[&mint, &DECODER_VERSION, &(start as i64), &(end as i64), &(tip as i64)])
            .map_err(read_failed)?
            .iter()
            .map(|r| r.get(0))
            .collect())
    }
}
