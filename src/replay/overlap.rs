//! Reconcile archive and replay evidence for routed overlap slots, and derive the
//! job's verified slots. Filtered replay alone never verifies a slot.
use crate::{
    Error, Result, decoder,
    model::Transfer,
    reconcile,
    storage::{
        Database,
        replay::{ARCHIVE_SOURCE, JobLog, Overlap, REPLAY_SOURCE, valid_id},
    },
    telemetry::Telemetry,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// The replay filter's predicate (`account_include` with `match_mints`): the mint is a
/// static or loaded account key, or the mint of a pre/post token balance.
pub(crate) fn touches_mint(result: &Value, mint: &str) -> bool {
    let keys = [
        "/transaction/message/accountKeys",
        "/meta/loadedAddresses/writable",
        "/meta/loadedAddresses/readonly",
    ];
    let key_match = keys.iter().any(|p| {
        result
            .pointer(p)
            .and_then(Value::as_array)
            .is_some_and(|keys| {
                keys.iter()
                    .any(|k| k.as_str().or_else(|| k["pubkey"].as_str()) == Some(mint))
            })
    });
    key_match
        || ["/meta/preTokenBalances", "/meta/postTokenBalances"]
            .iter()
            .any(|p| {
                result
                    .pointer(p)
                    .and_then(Value::as_array)
                    .is_some_and(|b| b.iter().any(|b| b["mint"] == mint))
            })
}

fn decoded(capture: &[u8]) -> Result<(Value, Vec<Transfer>)> {
    let parsed = decoder::parse_capture(capture)?;
    let value: Value =
        serde_json::from_slice(capture).map_err(|_| Error("stored_record_invalid"))?;
    Ok((value["result"].clone(), decoder::decode(&parsed)?))
}

/// Replay captures carry no block time; compare it only when both sides have one.
fn same_effects(mut a: Vec<Transfer>, mut b: Vec<Transfer>) -> bool {
    for (x, y) in a.iter_mut().zip(b.iter_mut()) {
        if x.block_time.is_none() || y.block_time.is_none() {
            x.block_time = None;
            y.block_time = None;
        }
    }
    a == b
}

fn outcome(
    outcome: &'static str,
    reason: Option<&'static str>,
    replay: usize,
    archive: usize,
) -> Overlap {
    Overlap {
        outcome,
        reason,
        replay_inputs: replay,
        archive_mint_inputs: archive,
    }
}

/// Compare one overlap slot through the shared reconciler and decoder.
fn slot_outcome(log: &mut JobLog, id: &str, mint: &str, slot: u64) -> Result<Overlap> {
    if !log.archive_slot_verified(mint, slot)? {
        return Ok(outcome("pending", Some("archive_slot_incomplete"), 0, 0));
    }
    let receipts = log.slot_receipts(id, slot)?;
    let archive = log.slot_captures(slot, ARCHIVE_SOURCE)?;
    let replay = log.slot_captures(slot, REPLAY_SOURCE)?;
    let mut archive_mint = Vec::new();
    for (signature, (capture, _)) in &archive {
        if touches_mint(&decoded(capture).map(|d| d.0).unwrap_or_default(), mint) {
            archive_mint.push(signature.clone());
        }
    }
    let counts = (receipts.len(), archive_mint.len());
    let pending = |reason| Ok(outcome("pending", Some(reason), counts.0, counts.1));
    if receipts.iter().any(|(_, status)| status != "decoded") {
        return pending("replay_input_unresolved");
    }
    let signatures: Vec<String> = receipts.iter().map(|r| r.0.clone()).collect();
    if log
        .output_decoders(&signatures)?
        .iter()
        .any(|d| d != decoder::DECODER_VERSION)
    {
        return pending("decoder_revision_mismatch");
    }
    for signature in &signatures {
        let Some((replay_capture, _)) = replay.get(signature) else {
            return pending("replay_evidence_not_retained");
        };
        let Some((archive_capture, archive_status)) = archive.get(signature) else {
            return Ok(outcome(
                "conflict",
                Some("replay_not_in_archive"),
                counts.0,
                counts.1,
            ));
        };
        let (a, b) = (decoded(archive_capture)?, decoded(replay_capture)?);
        if archive_status == "capture_conflict"
            || reconcile::compatible_results(&a.0, &b.0).is_err()
            || !same_effects(a.1, b.1)
        {
            return Ok(outcome(
                "conflict",
                Some("effects_differ"),
                counts.0,
                counts.1,
            ));
        }
    }
    Ok(if archive_mint.iter().any(|s| !signatures.contains(s)) {
        outcome(
            "replay_omission",
            Some("archive_has_undelivered_mint_input"),
            counts.0,
            counts.1,
        )
    } else {
        outcome("consistent", None, counts.0, counts.1)
    })
}

/// Reconcile every slot of the latest route's overlap segments; no provider calls.
pub fn reconcile(database: &Database, id: &str, telemetry: &mut Telemetry) -> Result<Value> {
    valid_id(id)?;
    let mut log = JobLog::open(database)?;
    let mint = log.scope(id)?.mint;
    let (revision, segments) = log.latest_route(id)?.ok_or(Error("replay_route_missing"))?;
    telemetry.set_context("replay_overlap", "archive_replay", "historical");
    let mut by_outcome: BTreeMap<&str, u64> = BTreeMap::new();
    for (start, end, _) in segments.iter().filter(|s| s.2 == "overlap") {
        for slot in *start..=*end {
            let result = slot_outcome(&mut log, id, &mint, slot)?;
            log.record_overlap(id, revision, slot, &result)?;
            *by_outcome.entry(result.outcome).or_default() += 1;
        }
    }
    let conflicts = by_outcome.get("conflict").copied().unwrap_or(0);
    let mut event = telemetry.event(
        "overlap_reconciled",
        "overlap",
        if conflicts > 0 { "conflict" } else { "success" },
        (conflicts > 0).then_some("overlap_conflict"),
    );
    event["route_revision"] = json!(revision);
    event["outcomes"] = json!(by_outcome);
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{event}");
    Ok(json!({"route_revision":revision,"outcomes":by_outcome,"job":super::status(database, id)?}))
}

/// Verified slots under the latest route: archive slots with retained full-block
/// evidence, and overlap slots whose latest outcome is consistent or a replay omission.
/// Replay and uncovered slots stay unknown. The boundary never crosses an unverified slot.
pub(crate) fn verified(database: &Database, id: &str) -> Result<Value> {
    let mut log = JobLog::open(database)?;
    let scope = log.scope(id)?;
    let Some((revision, segments)) = log.latest_route(id)? else {
        return Ok(
            json!({"route_revision":null,"verified_through":null,"archive_slots":0,"overlap_slots":0,"unknown_slots":scope.end-scope.start+1}),
        );
    };
    let outcomes = log.latest_overlaps(id, revision)?;
    let (mut archive, mut overlap, mut through, mut contiguous) = (0u64, 0u64, None, true);
    let mut conflicts = Vec::new();
    for (start, end, kind) in &segments {
        for slot in *start..=*end {
            let ok = match kind.as_str() {
                "archive" => log.archive_slot_verified(&scope.mint, slot)?,
                "overlap" => {
                    let o = outcomes.get(&slot);
                    if o.is_some_and(|o| o.0 == "conflict") && conflicts.len() < 32 {
                        conflicts.push(json!({"slot":slot,"reason":o.and_then(|o| o.1.clone())}));
                    }
                    o.is_some_and(|o| o.0 == "consistent" || o.0 == "replay_omission")
                        && log.archive_slot_verified(&scope.mint, slot)?
                }
                _ => false,
            };
            match (ok, kind.as_str()) {
                (true, "archive") => archive += 1,
                (true, _) => overlap += 1,
                _ => {}
            }
            contiguous &= ok;
            if contiguous {
                through = Some(slot);
            }
        }
    }
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    for (o, _) in outcomes.values() {
        *counts.entry(o.as_str()).or_default() += 1;
    }
    Ok(
        json!({"route_revision":revision,"decoder_version":decoder::DECODER_VERSION,"verified_through":through,
        "archive_slots":archive,"overlap_slots":overlap,"unknown_slots":scope.end-scope.start+1-archive-overlap,
        "overlap_outcomes":counts,"conflicts":conflicts}),
    )
}

#[cfg(test)]
mod tests;
