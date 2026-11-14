//! Bounded Old Faithful source. Public-network use is opt-in; caches replay offline.
use crate::storage::Database;
mod decode;
use crate::coordinator::evidence;
mod transport;
pub use decode::{decode_slot, normalize_transaction};
pub use transport::{ArchiveSource, Limits};

pub const JETSTREAMER_REVISION: &str = "8f91a4a7c918674f8799a9c887ace0f7f39fb844";
pub const SOURCE: &str = "old-faithful-jetstreamer";
pub const MAX_ARCHIVE_SLOTS: u64 = 8;

use crate::{
    Error, Result,
    backfill::{BlockSource, History, RpcSource},
    telemetry::Telemetry,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};

fn range(mint: &str, start: u64, end: u64) -> Result<()> {
    crate::backfill::scope(mint, start, end)?;
    if end - start >= MAX_ARCHIVE_SLOTS {
        return Err(Error("invalid_archive_range"));
    }
    Ok(())
}
pub fn run(
    database: &Database,
    cache: &Path,
    mint: &str,
    start: u64,
    end: u64,
    network: bool,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    range(mint, start, end)?;
    let began = Instant::now();
    let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
    let mut source = ArchiveSource::new(cache, network, Limits::default())?;
    let mut history = History::open(database)?;
    let run_id = history.run_id().to_owned();
    telemetry.set_context("backfill", "archive", "historical");
    telemetry.run_id = run_id.clone();
    let (coverage, metrics) = history.run(mint, start, end, &mut source, || {
        began.elapsed() >= Duration::from_secs(120)
            || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
    })?;
    Ok(
        json!({"run_id":run_id,"jetstreamer_revision":JETSTREAMER_REVISION,"source":"old-faithful-jetstreamer","network_allowed":network,"archive":source.stats,"coverage":coverage,"metrics":metrics,"live_coverage":"unknown"}),
    )
}

/// Verifies block anchors and ordered transaction identities before reconciling both observations.
pub fn overlap(
    database: &Database,
    cache: &Path,
    mint: &str,
    start: u64,
    end: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    range(mint, start, end)?;
    telemetry.set_context("archive_bridge", "archive_rpc", "historical");
    let began = Instant::now();
    let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
    let mut archive = ArchiveSource::new(cache, true, Limits::default())?;
    let endpoint = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let mut rpc = RpcSource::bounded(endpoint, began + Duration::from_secs(120))?;
    let mut history = History::open(database)?;
    let mut verified = 0;
    let mut recovered = 0;
    let mut observations = Vec::new();
    let mut rpc_bytes = 0usize;
    for slot in start..=end {
        if began.elapsed() >= Duration::from_secs(120)
            || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
        {
            break;
        }
        let archive_raw = archive.block(slot)?;
        let rpc_raw = rpc.block(slot)?;
        rpc_bytes = rpc_bytes
            .checked_add(rpc_raw.len())
            .ok_or(Error("archive_bridge_byte_limit"))?;
        if rpc_bytes > 8 * crate::backfill::MAX_BLOCK_BYTES {
            return Err(Error("archive_bridge_byte_limit"));
        }
        let outcome = evidence::ingest(&mut history, mint, slot, archive_raw, rpc_raw, &|| {
            began.elapsed() >= Duration::from_secs(120)
                || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
        })?;
        verified += u64::from(outcome.archive_complete && outcome.rpc_complete);
        recovered += u64::from(outcome.recovered);
        observations.push(json!({"slot":slot,"outcome":outcome}));
    }
    Ok(
        json!({"run_id":telemetry.run_id,"overlap_verified":verified==end-start+1,"boundary_ready":verified+recovered==end-start+1,"verified_slots":verified,"corroborated_slots":recovered,"observations":observations,"start":start,"end":end,"jetstreamer_revision":JETSTREAMER_REVISION,"archive":archive.stats,"rpc_response_body_bytes":rpc_bytes,"rpc_transport":rpc.diagnostics(),"coverage":history.coverage(mint,start,end)?,"summary":crate::storage::Store::summary(database,mint)?,"live_coverage":"unknown"}),
    )
}

/// Bounded recovery from both original normalized observations, without network access.
pub fn replay_overlap(
    database: &Database,
    mint: &str,
    start: u64,
    end: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    range(mint, start, end)?;
    telemetry.set_context("archive_bridge", "retained_pair", "recovery");
    let mut history = History::open(database)?;
    let began = Instant::now();
    let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
    let cancelled = || {
        began.elapsed() >= Duration::from_secs(120)
            || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
    };
    let mut observations = Vec::new();
    let mut verified = 0;
    let mut recovered = 0;
    for slot in start..=end {
        if cancelled() {
            break;
        }
        let a = history.retained_input(mint, slot, SOURCE)?;
        let b = history.retained_input(mint, slot, "finalized-mainnet-rpc")?;
        let outcome = evidence::ingest(&mut history, mint, slot, a, b, &cancelled)?;
        verified += u64::from(outcome.archive_complete && outcome.rpc_complete);
        recovered += u64::from(outcome.recovered);
        observations.push(json!({"slot":slot,"outcome":outcome}));
    }
    let ready = verified + recovered == end - start + 1;
    telemetry.emit(
        "overlap_recovery_finished",
        "retained_overlap",
        if ready { "success" } else { "incomplete" },
        None,
    );
    Ok(
        json!({"run_id":telemetry.run_id,"overlap_verified":verified==end-start+1,"boundary_ready":ready,"verified_slots":verified,"corroborated_slots":recovered,"observations":observations,"coverage":history.coverage(mint,start,end)?,"summary":crate::storage::Store::summary(database,mint)?,"network_requests":0,"live_coverage":"unknown"}),
    )
}

pub fn live(
    database: &Database,
    cache: &Path,
    start: u64,
    end: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    range(crate::live::USDC, start, end)?;
    crate::live::validate_options(start, seconds)?;
    let source = ArchiveSource::new(
        cache,
        true,
        Limits {
            seconds: seconds.min(120),
            ..Limits::default()
        },
    )?;
    crate::coordinator::run_with_source(database, start, end, seconds, telemetry, source)
}

pub(crate) struct Observation {
    pub(crate) slot: u64,
    pub(crate) raw: Vec<u8>,
    pub(crate) archive: bool,
}
impl BlockSource for Observation {
    fn provenance(&self) -> &'static str {
        if self.archive {
            SOURCE
        } else {
            "finalized-mainnet-rpc"
        }
    }
    fn capture_source(&self) -> &'static str {
        if self.archive {
            SOURCE
        } else {
            "solana-mainnet-rpc"
        }
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if slot != self.slot {
            return Err(Error("bridge_block_missing"));
        }
        Ok(self.raw.clone())
    }
}
pub fn compare_blocks(archive: &[u8], rpc: &[u8], slot: u64) -> Result<()> {
    let parse = |raw: &[u8]| -> Result<Value> {
        if raw.len() > crate::backfill::MAX_BLOCK_BYTES {
            return Err(Error("block_too_large"));
        }
        let value: Value = serde_json::from_slice(raw).map_err(|_| Error("invalid_block_json"))?;
        if value["jsonrpc"] != "2.0"
            || value["id"] != 1
            || value.get("error").is_some()
            || !value["result"].is_object()
        {
            return Err(Error("archive_bridge_block_missing"));
        }
        Ok(value)
    };
    let a = parse(archive)?;
    let b = parse(rpc)?;
    for field in ["blockhash", "previousBlockhash", "parentSlot", "blockTime"] {
        if a["result"].get(field).is_none() || a["result"][field] != b["result"][field] {
            return Err(Error("archive_bridge_block_conflict"));
        }
    }
    let a = a["result"]["transactions"]
        .as_array()
        .ok_or(Error("missing_block_transactions"))?;
    let b = b["result"]["transactions"]
        .as_array()
        .ok_or(Error("missing_block_transactions"))?;
    if a.len() != b.len() || a.len() > 100_000 {
        return Err(Error("archive_bridge_transaction_count"));
    }
    for (a, b) in a.iter().zip(b) {
        let mut a = a.clone();
        let mut b = b.clone();
        a["slot"] = json!(slot);
        b["slot"] = json!(slot);
        crate::reconcile::compatible_results(&a, &b)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Observe a bounded archive tip relative to a freshly checked finalized RPC reference.
pub fn tip(cache: &Path, telemetry: &mut Telemetry) -> Result<Value> {
    telemetry.set_context("archive_tip", "archive_rpc", "historical");
    let mut archive = ArchiveSource::new(cache, true, Limits::default())?;
    let endpoint = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let mut rpc = RpcSource::new(endpoint)?;
    let (reference, time) = rpc.finalized_reference()?;
    let epoch = reference / 432000;
    let mut probes = Vec::new();
    let mut uncertain = false;
    for distance in 0..4 {
        let Some(candidate) = epoch.checked_sub(distance) else {
            break;
        };
        match archive.probe_epoch(candidate) {
            Ok(true) => {
                probes.push(json!({"epoch":candidate,"state":"present"}));
                let tip = archive.tip_candidate(candidate)?;
                if tip > reference {
                    return Err(Error("archive_tip_ahead_of_reference"));
                }
                let tip_time = archive.tip_time(tip)?;
                let lag = time
                    .and_then(|now| now.checked_sub(tip_time))
                    .filter(|lag| *lag >= 0);
                return Ok(
                    json!({"run_id":telemetry.run_id,"rpc_transport":rpc.diagnostics(),"reference_slot":reference,"reference_block_time":time,"available_epoch":candidate,"last_observed_archive_slot":tip,"archive_block_time":tip_time,"slot_gap":reference-tip,"observed_freshness_gap_seconds":lag,"tip_exact_within_probed_epochs":!uncertain,"publication_delay_after_epoch_end":"not_measured","probes":probes,"archive":archive.stats,"jetstreamer_revision":JETSTREAMER_REVISION}),
                );
            }
            Ok(false) => probes.push(json!({"epoch":candidate,"state":"not_found"})),
            Err(error) => {
                uncertain = true;
                probes.push(json!({"epoch":candidate,"state":"unknown","error_class":error.0}));
            }
        }
    }
    Ok(
        json!({"run_id":telemetry.run_id,"rpc_transport":rpc.diagnostics(),"reference_slot":reference,"last_observed_archive_slot":null,"archive_tip":"unknown","probes":probes,"archive":archive.stats}),
    )
}
