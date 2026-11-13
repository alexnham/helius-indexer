//! Corroborate explicit RPC log truncation; never label the native RPC input complete.
use crate::archive::{Observation, compare_blocks};
use crate::{
    Error, Result,
    backfill::{BlockSource, History, Metrics},
    decoder,
    model::Transfer,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(crate) const SOURCE: &str = "archive-rpc-corroborated";
const MAX_REPAIRED_TRANSACTIONS: usize = 16;
#[derive(Serialize)]
pub(crate) struct Outcome {
    pub archive_complete: bool,
    pub rpc_complete: bool,
    pub recovered: bool,
    pub recovery_attempted: bool,
    pub repaired_transactions: usize,
    pub recovery_error: Option<&'static str>,
    pub metrics: Vec<Metrics>,
}
impl Outcome {
    pub fn ready(&self) -> bool {
        self.archive_complete && (self.rpc_complete || self.recovered)
    }
}
struct Corroborated {
    slot: u64,
    raw: Vec<u8>,
}
impl BlockSource for Corroborated {
    fn provenance(&self) -> &'static str {
        SOURCE
    }
    fn capture_source(&self) -> &'static str {
        SOURCE
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if slot != self.slot {
            return Err(Error("bridge_block_missing"));
        }
        Ok(self.raw.clone())
    }
}
pub(crate) fn ingest(
    history: &mut History,
    mint: &str,
    slot: u64,
    a: Vec<u8>,
    b: Vec<u8>,
    cancelled: &impl Fn() -> bool,
) -> Result<Outcome> {
    history.reverify(mint, slot, slot)?;
    compare_blocks(&a, &b, slot)?;
    let mut archive = Observation {
        slot,
        raw: a,
        archive: true,
    };
    let mut rpc = Observation {
        slot,
        raw: b,
        archive: false,
    };
    let mut outcome = Outcome {
        archive_complete: false,
        rpc_complete: false,
        recovered: false,
        recovery_attempted: false,
        repaired_transactions: 0,
        recovery_error: None,
        metrics: vec![],
    };
    for observation in [&mut archive, &mut rpc] {
        history.reverify(mint, slot, slot)?;
        let (coverage, metrics) = history.run(mint, slot, slot, observation, cancelled)?;
        if observation.archive {
            outcome.archive_complete = coverage.complete;
        } else {
            outcome.rpc_complete = coverage.complete;
        }
        outcome.metrics.push(metrics);
    }
    if outcome.archive_complete && !outcome.rpc_complete && !cancelled() {
        outcome.recovery_attempted = true;
        match corroborate(&archive.raw, &rpc.raw, slot, cancelled) {
            Ok((raw, count)) => {
                history.reverify(mint, slot, slot)?;
                let (coverage, metrics) =
                    history.run(mint, slot, slot, &mut Corroborated { slot, raw }, cancelled)?;
                outcome.recovered = coverage.complete;
                outcome.repaired_transactions = if coverage.complete { count } else { 0 };
                outcome.metrics.push(metrics);
            }
            Err(error) => outcome.recovery_error = Some(error.0),
        }
    }
    if outcome.ready() {
        history.acknowledge_bridge_overlap(mint, slot)?;
    }
    Ok(outcome)
}
fn capture(tx: &Value, slot: u64, time: &Value, source: &str) -> Result<crate::model::Capture> {
    let mut result = tx.clone();
    result["slot"] = json!(slot);
    result["blockTime"] = time.clone();
    let raw=serde_json::to_vec(&json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":source,"result":result})).map_err(|_|Error("serialization_failed"))?;
    decoder::parse_capture(&raw)
}
fn compare_rows(a: &[Transfer], b: &[Transfer]) -> Result<()> {
    if a.len() != b.len() {
        return Err(Error("capture_conflict"));
    }
    for (a, b) in a.iter().zip(b) {
        crate::reconcile::transfer(a, b)?;
    }
    Ok(())
}
fn corroborate_logs(full: &[Value], partial: &[Value]) -> Result<()> {
    let markers: Vec<_> = partial
        .iter()
        .enumerate()
        .filter(|(_, l)| *l == "Log truncated")
        .map(|(i, _)| i)
        .collect();
    if markers.len() != 1
        || markers[0] == 0
        || full.iter().any(|l| l == "Log truncated")
        || full.len() <= partial.len()
    {
        return Err(Error("rpc_log_truncation_unproven"));
    }
    let prefix = markers[0];
    if full.get(..prefix) != Some(&partial[..prefix]) {
        return Err(Error("rpc_log_evidence_conflict"));
    }
    // Some collectors retain later small log lines after dropping an oversized line.
    let mut next = prefix;
    for observed in &partial[prefix + 1..] {
        let offset = full[next..]
            .iter()
            .position(|l| l == observed)
            .ok_or(Error("rpc_log_evidence_conflict"))?;
        next += offset + 1;
    }
    Ok(())
}
/// Only log arrays change; every original input is retained separately before this is used.
pub(crate) fn corroborate(
    archive: &[u8],
    rpc: &[u8],
    slot: u64,
    cancelled: &impl Fn() -> bool,
) -> Result<(Vec<u8>, usize)> {
    compare_blocks(archive, rpc, slot)?;
    let a: Value = serde_json::from_slice(archive).map_err(|_| Error("invalid_block_json"))?;
    let mut b: Value = serde_json::from_slice(rpc).map_err(|_| Error("invalid_block_json"))?;
    let time = b["result"]["blockTime"].clone();
    let mut size = rpc.len();
    let mut repaired = 0;
    let txs = b["result"]["transactions"]
        .as_array_mut()
        .ok_or(Error("missing_block_transactions"))?;
    for (full, partial) in a["result"]["transactions"]
        .as_array()
        .ok_or(Error("missing_block_transactions"))?
        .iter()
        .zip(txs)
    {
        if cancelled() {
            return Err(Error("cancelled"));
        }
        crate::reconcile::reference(full, partial)?;
        let expected = decoder::decode(&capture(full, slot, &time, crate::archive::SOURCE)?)?;
        let original = decoder::decode(&capture(partial, slot, &time, "solana-mainnet-rpc")?);
        match original {
            Ok(rows) => compare_rows(&expected, &rows)?,
            Err(error) if error.0 == "incomplete_execution_evidence" => {
                let logs = full["meta"]["logMessages"]
                    .as_array()
                    .ok_or(Error("rpc_log_truncation_unproven"))?;
                let observed = partial["meta"]["logMessages"]
                    .as_array()
                    .ok_or(Error("rpc_log_truncation_unproven"))?;
                corroborate_logs(logs, observed)?;
                if repaired == MAX_REPAIRED_TRANSACTIONS {
                    return Err(Error("rpc_evidence_repair_limit"));
                }
                let before = serde_json::to_vec(observed)
                    .map_err(|_| Error("serialization_failed"))?
                    .len();
                let after = serde_json::to_vec(logs)
                    .map_err(|_| Error("serialization_failed"))?
                    .len();
                size = size
                    .checked_sub(before)
                    .and_then(|s| s.checked_add(after))
                    .ok_or(Error("block_too_large"))?;
                if size > crate::backfill::MAX_BLOCK_BYTES {
                    return Err(Error("block_too_large"));
                }
                partial["meta"]["logMessages"] = Value::Array(logs.clone());
                compare_rows(
                    &expected,
                    &decoder::decode(&capture(partial, slot, &time, SOURCE)?)?,
                )?;
                repaired += 1;
            }
            Err(error) => return Err(error),
        }
    }
    if repaired == 0 {
        return Err(Error("rpc_log_truncation_unproven"));
    }
    b["evidence_provenance"] = json!({"method":"archive_log_corroboration_v1","slot":slot,"archive_input_sha256":format!("{:x}",Sha256::digest(archive)),"rpc_input_sha256":format!("{:x}",Sha256::digest(rpc)),"archive_receipt":a.get("archive_provenance"),"repaired_transactions":repaired});
    let raw = serde_json::to_vec(&b).map_err(|_| Error("serialization_failed"))?;
    if raw.len() > crate::backfill::MAX_BLOCK_BYTES {
        return Err(Error("block_too_large"));
    }
    Ok((raw, repaired))
}

#[cfg(test)]
mod tests;
