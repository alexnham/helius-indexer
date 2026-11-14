//! Finite mint-filtered recovery. Delivery and processing never prove full-slot coverage.
use crate::{
    Error, Result,
    coordinator::WriteGate,
    live::{self, Intake},
    storage::{
        Database,
        replay::{JobLog, Reservation, Retained, Scope, valid_id},
    },
    telemetry::Telemetry,
};
use laserstream_core_proto::geyser::SubscribeUpdateTransaction;
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn create(database: &Database, id: &str, mint: &str, start: u64, end: u64) -> Result<Value> {
    valid_id(id)?;
    crate::backfill::scope(mint, start, end)?;
    let mut log = JobLog::open(database)?;
    log.create(id, mint, start, end)?;
    log.status(id)
}
pub fn status(database: &Database, id: &str) -> Result<Value> {
    valid_id(id)?;
    JobLog::open(database)?.status(id)
}
pub fn grant(database: &Database, id: &str, telemetry: &mut Telemetry) -> Result<Value> {
    valid_id(id)?;
    let mut log = JobLog::open(database)?;
    log.grant(id)?;
    telemetry.set_context("replay", "laserstream", "historical");
    telemetry.emit("replay_budget_granted", "budget", "success", None);
    log.status(id)
}

pub(crate) struct ReplayRun {
    pub scope: Scope,
    pub reservation: Reservation,
}
impl ReplayRun {
    pub fn input(&mut self, update: &SubscribeUpdateTransaction) -> Result<Retained> {
        let signature = &update
            .transaction
            .as_ref()
            .ok_or(Error("missing_transaction"))?
            .signature;
        self.reservation
            .retain(update.slot, signature, &update.encode_to_vec())
    }
}

pub fn run(
    database: &Database,
    id: &str,
    seconds: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    run_admitted(database, id, seconds, telemetry, &WriteGate::default())
}
pub fn run_admitted(
    database: &Database,
    id: &str,
    seconds: u64,
    telemetry: &mut Telemetry,
    gate: &WriteGate,
) -> Result<Value> {
    valid_id(id)?;
    if !(1..=60).contains(&seconds) {
        return Err(Error("invalid_replay_bounds"));
    }
    let scope = JobLog::open(database)?.scope(id)?;
    live::validate_options(scope.start, seconds)?;
    telemetry.set_context("replay", "laserstream", "historical");
    let mut replay = {
        let _permit = gate.historical()?;
        ReplayRun {
            scope,
            reservation: JobLog::reserve(database, id, seconds)?,
        }
    };
    let result = live::run_replay(database, seconds, telemetry, gate, &mut replay);
    let terminal = match &result {
        Err(e) => e.0,
        Ok(v) => match v["status"].as_str() {
            Some("end_observed") => "end_observed",
            Some("time_limit") => "time_limit",
            Some("message_limit") => "message_limit",
            Some("cancelled") => "cancelled",
            _ => "stopped",
        },
    };
    {
        let _permit = gate.historical()?;
        replay.reservation.finish(terminal)?;
    }
    telemetry.emit(
        "replay_attempt_finished",
        "replay",
        if result.is_ok() { "success" } else { "failed" },
        result.as_ref().err().map(|e| e.0),
    );
    let report = JobLog::open(database)?.status(id)?;
    result.map(|attempt| json!({"attempt":attempt,"job":report}))
}

/// Retry at most 100 retained unresolved inputs without a provider connection.
pub fn retry(database: &Database, id: &str, telemetry: &mut Telemetry) -> Result<Value> {
    valid_id(id)?;
    let mut log = JobLog::open(database)?;
    let scope = log.scope(id)?;
    let pending = log.pending(id)?;
    let mut intake = Intake::open(database)?;
    telemetry.set_context("replay", "laserstream", "recovery");
    telemetry.emit("replay_retry_started", "retry", "startup", None);
    let mut decoded = 0;
    for item in &pending {
        log.retry_cursor(id, &item.signature)?;
        let retained = log.raw(item.raw_id)?;
        let result = match retained
            .as_ref()
            .map(|(raw, schema)| (raw, schema.as_str()))
        {
            Some((raw, "laserstream-core-proto/14.0.0"))
                if Sha256::digest(raw).to_vec() == item.hash =>
            {
                SubscribeUpdateTransaction::decode(raw.as_slice())
                    .map_err(|_| Error("stored_record_invalid"))
                    .and_then(|tx| {
                        if tx.slot < scope.start
                            || tx.slot > scope.end
                            || tx.transaction.as_ref().map(|t| t.signature.as_slice())
                                != Some(item.signature.as_slice())
                        {
                            return Err(Error("stored_record_invalid"));
                        }
                        intake.process(item.raw_id, &tx)
                    })
            }
            None => Err(Error("replay_input_needs_refetch")),
            Some((_, "laserstream-core-proto/14.0.0")) => Err(Error("stored_record_invalid")),
            _ => Err(Error("unsupported_stored_proto_schema")),
        };
        decoded += usize::from(result.is_ok());
        log.retry_acknowledge(id, item.raw_id, &result)?;
    }
    telemetry.emit(
        "replay_retry_finished",
        "retry",
        if decoded == pending.len() {
            "success"
        } else {
            "partial"
        },
        (decoded != pending.len()).then_some("unresolved_decode_items"),
    );
    Ok(
        json!({"attempted":pending.len(),"decoded":decoded,"failed":pending.len()-decoded,"job":log.status(id)?}),
    )
}

#[cfg(test)]
mod tests;
