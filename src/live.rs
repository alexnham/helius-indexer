//! Bounded LaserStream intake. Filtered transaction delivery never proves slot coverage.
use crate::storage::Database;
use crate::{
    Error, Result, decoder,
    storage::{
        Acknowledge, BatchItem, Store,
        live::{LiveLog, now},
    },
};
use futures_util::StreamExt;
use laserstream_core_proto::geyser::*;
use prost::Message;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tonic::{
    Request,
    transport::{ClientTlsConfig, Endpoint},
};

pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MAX_MESSAGES: usize = 10_000;
/// `SECONDS` value for a live run that keeps its subscription until it fails or is stopped.
pub const CONTINUOUS: u64 = 0;
// shortcut: a continuous run is a deadline this far away; switch to an explicit stop signal if runs ever need to outlive it.
const CONTINUOUS_DEADLINE: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// A live batch flushes at this many inputs, bytes or age, whichever comes first.
pub(crate) const BATCH_INPUTS: usize = 100;
pub(crate) const BATCH_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const BATCH_WINDOW: Duration = Duration::from_millis(200);
/// HTTP/2 receive window per stream and per connection. It bounds unacknowledged input
/// in flight; 64 KiB capped throughput near window/RTT (~140 tx/s at a 30–60 ms RTT).
const RECEIVE_WINDOW: u32 = 8 * 1024 * 1024;
pub(crate) const PROTO_SCHEMA: &str = "laserstream-core-proto/14.0.0";
const RECENT_SECONDS: i64 = 60;

pub fn request(from: u64) -> SubscribeRequest {
    request_mint(from, USDC)
}

pub fn request_mint(from: u64, mint: &str) -> SubscribeRequest {
    SubscribeRequest {
        commitment: Some(CommitmentLevel::Finalized as i32),
        from_slot: Some(from),
        transactions: [(
            "usdc".into(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![mint.into()],
                match_mints: true,
                ..Default::default()
            },
        )]
        .into(),
        ..Default::default()
    }
}

fn b58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

/// Preserve missing metadata as unknown and distinguish v1 inline budget config.
pub fn normalize(update: &SubscribeUpdateTransaction) -> Result<Vec<u8>> {
    let info = update
        .transaction
        .as_ref()
        .ok_or(Error("missing_transaction"))?;
    let tx = info
        .transaction
        .as_ref()
        .ok_or(Error("missing_transaction"))?;
    if tx.signatures.first() != Some(&info.signature) {
        return Err(Error("signature_mismatch"));
    }
    let msg = tx.message.as_ref().ok_or(Error("missing_message"))?;
    let meta = info
        .meta
        .as_ref()
        .ok_or(Error("missing_execution_metadata"))?;
    let instruction = |program: u32, accounts: &[u8], data: &[u8]| {
        json!({
            "programIdIndex":program,"accounts":accounts,"data":b58(data)
        })
    };
    let balances = |values: &[laserstream_core_proto::solana::storage::confirmed_block::TokenBalance]| -> Result<Vec<Value>> {
        values.iter().map(|b| Ok(json!({"accountIndex":b.account_index,"mint":b.mint,
            "owner":if b.owner.is_empty(){None}else{Some(&b.owner)},
            "programId":if b.program_id.is_empty(){None}else{Some(&b.program_id)},
            "uiTokenAmount":{"decimals":b.ui_token_amount.as_ref().ok_or(Error("missing_token_amount"))?.decimals}
        }))).collect()
    };
    let inner: Option<Vec<Value>> = if meta.inner_instructions_none {
        None
    } else {
        Some(meta.inner_instructions.iter().map(|g| json!({
        "index":g.index,"instructions":g.instructions.iter().map(|i|instruction(i.program_id_index,&i.accounts,&i.data)).collect::<Vec<_>>()
    })).collect())
    };
    let capture = json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":"helius-laserstream",
        "result":{"slot":update.slot,"blockTime":null,
            "version":if msg.config.is_some(){json!(1)}else if msg.versioned{json!(0)}else{json!("legacy")},
            "transaction":{"signatures":tx.signatures.iter().map(|s|b58(s)).collect::<Vec<_>>(),
                "message":{"accountKeys":msg.account_keys.iter().map(|k|b58(k)).collect::<Vec<_>>(),
                "instructions":msg.instructions.iter().map(|i|instruction(i.program_id_index,&i.accounts,&i.data)).collect::<Vec<_>>(),
                "addressTableLookups":msg.address_table_lookups.iter().map(|l|json!({"accountKey":b58(&l.account_key),"writableIndexes":l.writable_indexes,"readonlyIndexes":l.readonly_indexes})).collect::<Vec<_>>() }},
            "meta":{"err":meta.err.as_ref().map(|e|b58(&e.err)),"logMessages":if meta.log_messages_none{None}else{Some(&meta.log_messages)},
                "innerInstructions":inner,"preTokenBalances":balances(&meta.pre_token_balances)?,"postTokenBalances":balances(&meta.post_token_balances)?,
                "loadedAddresses":{"writable":meta.loaded_writable_addresses.iter().map(|a|b58(a)).collect::<Vec<_>>(),"readonly":meta.loaded_readonly_addresses.iter().map(|a|b58(a)).collect::<Vec<_>>()}}}});
    serde_json::to_vec(&capture).map_err(|_| Error("serialization_failed"))
}

pub struct Intake {
    log: LiveLog,
    store: Store,
}
impl Intake {
    pub fn open(database: &Database) -> Result<Self> {
        let store = Store::open(database)?;
        Ok(Self {
            log: LiveLog::open(database)?,
            store,
        })
    }
    /// The wire payload commits before normalization/decoding; a crash leaves retryable pending input.
    pub fn retain(&self, update: &SubscribeUpdateTransaction) -> Result<i64> {
        Ok(self.retain_batch(std::slice::from_ref(update))?[0])
    }
    /// Retains wire payloads in one statement, before any normalization or decoding.
    pub fn retain_batch(&self, updates: &[SubscribeUpdateTransaction]) -> Result<Vec<i64>> {
        let mut rows = Vec::with_capacity(updates.len());
        for update in updates {
            if update.encoded_len() > decoder::MAX_CAPTURE_BYTES || update.slot > i64::MAX as u64 {
                return Err(Error("live_input_limit"));
            }
            rows.push((update.slot as i64, update.encode_to_vec()));
        }
        self.log.retain_batch(&rows)
    }
    pub fn process(&mut self, id: i64, update: &SubscribeUpdateTransaction) -> Result<usize> {
        self.process_batch(&[(id, update)]).remove(0)
    }
    /// Decodes and stores retained inputs; their output and status commit in one transaction,
    /// so progress never runs ahead of durable output. Results follow input order.
    pub fn process_batch(
        &mut self,
        inputs: &[(i64, &SubscribeUpdateTransaction)],
    ) -> Vec<Result<usize>> {
        let mut results: Vec<Option<Result<usize>>> = vec![None; inputs.len()];
        let mut failed = Vec::new();
        let mut ready = Vec::new();
        for (position, (id, update)) in inputs.iter().enumerate() {
            match normalize(update).and_then(|raw| Ok((decoder::parse_capture(&raw)?, raw))) {
                Ok((capture, raw)) => {
                    // Persist the same complete record set as historical ingestion. Filter queries by mint.
                    let decoded = decoder::decode(&capture);
                    ready.push((position, *id, capture, raw, decoded));
                }
                Err(error) => {
                    failed.push((*id, error.0));
                    results[position] = Some(Err(error));
                }
            }
        }
        let items: Vec<BatchItem<'_>> = ready
            .iter()
            .map(|(_, _, capture, raw, decoded)| BatchItem {
                capture,
                raw,
                decoded,
            })
            .collect();
        let ids: Vec<i64> = ready.iter().map(|r| r.1).collect();
        let stored = self.store.persist_batch_acknowledged(
            &items,
            Some(&Acknowledge::Live {
                ids: &ids,
                failed: &failed,
            }),
        );
        for ((position, _, _, _, decoded), stored) in ready.iter().zip(stored) {
            results[*position] = Some(match (stored, decoded) {
                (Ok(count), Ok(_)) => Ok(count),
                (Ok(_), Err(error)) => Err(*error),
                (Err(error), _) => Err(error),
            });
        }
        results
            .into_iter()
            .map(|r| r.unwrap_or(Err(Error("storage_write_failed"))))
            .collect()
    }
    pub fn retry(&mut self) -> Result<Value> {
        let cursor = self.log.retry_cursor()?;
        let mut telemetry = crate::telemetry::Telemetry::default();
        telemetry.set_context("live", "laserstream", "recovery");
        telemetry.emit("retry_batch_started", "retry", "retry", None);
        let pending = self.log.pending_after(cursor, 100)?;
        // Fairness only: a crash can delay these rows until wraparound, never acknowledge them.
        if let Some(last) = pending.last() {
            self.log.advance_retry_cursor(*last)?;
        }
        let mut updates = Vec::with_capacity(pending.len());
        for id in &pending {
            let (raw, schema) = self.log.raw(*id)?;
            if schema != PROTO_SCHEMA {
                return Err(Error("unsupported_stored_proto_schema"));
            }
            if raw.len() > decoder::MAX_CAPTURE_BYTES {
                return Err(Error("stored_record_invalid"));
            }
            updates.push(
                SubscribeUpdateTransaction::decode(raw.as_slice())
                    .map_err(|_| Error("stored_record_invalid"))?,
            );
        }
        let inputs: Vec<(i64, &SubscribeUpdateTransaction)> =
            pending.iter().copied().zip(updates.iter()).collect();
        let success = self
            .process_batch(&inputs)
            .iter()
            .filter(|r| r.is_ok())
            .count();
        telemetry.emit(
            "retry_batch_completed",
            "retry",
            if success == pending.len() {
                "success"
            } else {
                "partial"
            },
            (success != pending.len()).then_some("unresolved_decode_items"),
        );
        Ok(
            json!({"attempted":pending.len(),"decoded":success,"failed":pending.len()-success,"retry_cursor":pending.last().copied().unwrap_or(cursor),"run_id":telemetry.run_id,"coverage":"unknown"}),
        )
    }
    pub fn report(database: &Database) -> Result<Value> {
        // Raw inputs by arrival time show flow during a cycle; attempts, not unique transfers.
        let snapshot = LiveLog::snapshot(database, now() - RECENT_SECONDS)?;
        let runs: Vec<Value> = snapshot.runs.iter().map(|run| {
            let (from, reference, reference_at, first, replay_at) = (run.from_slot, run.reference_slot, run.reference_at, run.replay_first, run.replay_at);
            json!({"run":run.id,"from_slot":from,"state":run.state,
                "mint":run.mint,"decoder_version":run.decoder,
                "reference_slot":reference,"reference_at":reference_at,
                "reference_age_seconds":reference_at.and_then(age),
                "replay_first":first,"replay_at":replay_at,"replay_info_age_seconds":replay_at.and_then(age),
                "replay_margin_slots":first.map(|n|from-n),
                "clock_skew":reference_at.is_some_and(|n|n>now())||replay_at.is_some_and(|n|n>now()),
                "unresolved_interval":{"from_slot":from,"through_slot":reference.filter(|n|*n>=from),"kind":"unverified"},
                "verified_boundary":null,"coverage":"unknown"})
        }).collect();
        let pending = snapshot.pending;
        let (received, decoded) = snapshot.recent;
        Ok(
            json!({"producer_cursors":snapshot.producer_cursors.iter().map(|(stream,slot)| json!({"stream":stream,"slot":slot,"resume_from":crate::kafka::resume_slot(*slot as u64)})).collect::<Vec<_>>(),"recent":{"window_seconds":RECENT_SECONDS,"received":received,"decoded":decoded,"pending":received-decoded},"last_received_slot":snapshot.last_received_slot,"retry_cursor":snapshot.retry_cursor,"retry_batch_limit":100,"oldest_unresolved_age_seconds":if pending==0{Some(0)}else{snapshot.oldest_pending_at.and_then(age)},"state":if runs.is_empty(){"uninitialized"}else{"partial"},"mint":runs.first().and_then(|r|r.get("mint")),"decoder_version":decoder::DECODER_VERSION,"verified_boundary":null,"coverage":"unknown","unresolved_items":pending,"runs":runs,"limit":100,"reason":"filtered_finalized_stream_has_no_validated_completion_evidence"}),
        )
    }
}
fn age(observed: i64) -> Option<i64> {
    now().checked_sub(observed).filter(|n| *n >= 0)
}

/// Only this explicitly opted-in command opens a provider connection.
pub fn validate_options(from: u64, seconds: u64) -> Result<()> {
    if !(seconds == CONTINUOUS || (1..=300).contains(&seconds)) || from > i64::MAX as u64 {
        return Err(Error("invalid_live_bounds"));
    }
    if std::env::var("INDEXER_LIVE_OPT_IN").as_deref() != Ok("1") {
        return Err(Error("live_opt_in_required"));
    }
    let api_key = std::env::var("HELIUS_API_KEY").map_err(|_| Error("missing_live_credentials"))?;
    if api_key.is_empty() {
        return Err(Error("missing_live_credentials"));
    }
    Ok(())
}
pub fn run(
    database: &Database,
    from: u64,
    seconds: u64,
    telemetry: &mut crate::telemetry::Telemetry,
) -> Result<Value> {
    run_mint(database, from, seconds, USDC, telemetry)
}

pub fn run_mint(
    database: &Database,
    from: u64,
    seconds: u64,
    mint: &str,
    telemetry: &mut crate::telemetry::Telemetry,
) -> Result<Value> {
    run_mint_admitted(
        database,
        from,
        seconds,
        mint,
        telemetry,
        &crate::coordinator::WriteGate::default(),
    )
}
pub fn run_admitted(
    database: &Database,
    from: u64,
    seconds: u64,
    telemetry: &mut crate::telemetry::Telemetry,
    gate: &crate::coordinator::WriteGate,
) -> Result<Value> {
    run_mint_admitted(database, from, seconds, USDC, telemetry, gate)
}

pub fn run_mint_admitted(
    database: &Database,
    from: u64,
    seconds: u64,
    mint: &str,
    telemetry: &mut crate::telemetry::Telemetry,
    gate: &crate::coordinator::WriteGate,
) -> Result<Value> {
    run_intake(database, from, seconds, mint, telemetry, gate, None)
}

pub(crate) fn run_replay(
    database: &Database,
    seconds: u64,
    telemetry: &mut crate::telemetry::Telemetry,
    gate: &crate::coordinator::WriteGate,
    replay: &mut crate::replay::ReplayRun,
) -> Result<Value> {
    let mint = replay.scope.mint.clone();
    // Re-reading from the job start would spend each grant on already-received slots.
    let start = crate::storage::replay::JobLog::open(database)?
        .resume_start(&replay.reservation.job, &replay.scope)?;
    run_intake(
        database,
        start,
        seconds,
        &mint,
        telemetry,
        gate,
        Some(replay),
    )
}

fn run_intake(
    database: &Database,
    from: u64,
    seconds: u64,
    mint: &str,
    telemetry: &mut crate::telemetry::Telemetry,
    gate: &crate::coordinator::WriteGate,
    mut replay: Option<&mut crate::replay::ReplayRun>,
) -> Result<Value> {
    let historical = replay.is_some();
    if historical && seconds == CONTINUOUS {
        return Err(Error("invalid_live_bounds"));
    }
    let deadline = Instant::now()
        + if seconds == CONTINUOUS {
            CONTINUOUS_DEADLINE
        } else {
            Duration::from_secs(seconds)
        };
    let admit = || {
        if historical {
            gate.historical_until(deadline)
        } else {
            gate.live()
        }
    };
    crate::backfill::scope(mint, from, from)?;
    telemetry.set_context(
        "live_ingestion",
        "laserstream",
        if historical { "historical" } else { "live" },
    );
    validate_options(from, seconds)?;
    let token = Token::from_env()?;
    let mut intake = {
        let _permit = admit()?;
        Intake::open(database)?
    };
    let run = if historical {
        None
    } else {
        let _permit = admit()?;
        Some(
            intake
                .log
                .start_run(from as i64, mint, decoder::DECODER_VERSION)?,
        )
    };
    if !historical {
        gate.live_started()?;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Error("runtime_failed"))?;
    let started = Instant::now();
    let mut stats = IntakeStats::default();
    let mut duplicate_receipts = 0usize;
    let mut bytes = 0usize;
    let mut reconnects = 0usize;
    let mut buffer = Vec::new();
    let mut buffered_bytes = 0usize;
    let mut buffered_since: Option<Instant> = None;
    let mut last_slot: Option<u64> = None;
    let mut metrics_at = Instant::now();
    // Historical input keeps its own durable receipts; only live intake publishes to Kafka.
    let mut producer = if historical { None } else { live_producer()? };
    let result = (|| {
        for attempt in 0..=2 {
            if Instant::now() >= deadline {
                return Ok("time_limit");
            }
            reconnects = attempt;
            event(
                telemetry,
                "connection_attempt",
                if attempt == 0 { "startup" } else { "retry" },
            );
            if let Some(job) = replay.as_mut() {
                let _permit = admit()?;
                job.reservation.connection()?;
            }
            // A reconnect resumes from delivered input, not from the run's first slot.
            let start = if historical {
                from
            } else {
                resume_start(from, producer.as_ref().map_or(last_slot, |p| p.cursor()))
            };
            let mut client = connect(&runtime, deadline, &token)?;
            let (first_available, reference) = availability(&runtime, deadline, &mut client)?;
            {
                let _permit = admit()?;
                if let Some(job) = replay.as_mut() {
                    job.reservation
                        .availability(&job.scope, first_available, reference)?;
                } else {
                    let first = first_available.ok_or(Error("replay_info_unavailable"))?;
                    check_replay(start, first)?;
                    if reference > i64::MAX as u64 || first > i64::MAX as u64 {
                        return Err(Error("unsupported_slot"));
                    }
                    intake.log.record_reference(
                        run.ok_or(Error("live_run_missing"))?,
                        reference as i64,
                        first as i64,
                    )?;
                }
            }
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            network(&runtime, deadline, async {
                sender
                    .send(request_mint(start, mint))
                    .await
                    .map_err(|_| Error("subscription_failed"))
            })?;
            let mut stream = network(&runtime, deadline, async {
                Ok(client
                    .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
                    .await
                    .map_err(|_| Error("subscription_failed"))?
                    .into_inner())
            })?;
            loop {
                if Instant::now() >= deadline {
                    stats.flush_live(
                        &mut intake,
                        &mut buffer,
                        &mut producer,
                        mint,
                        gate,
                        telemetry,
                    )?;
                    return Ok("time_limit");
                }
                // Live intake has no message cap; only historical replay is message-bounded.
                if historical && stats.count >= MAX_MESSAGES {
                    return Ok("message_limit");
                }
                if !historical && metrics_at.elapsed().as_secs() >= RECENT_SECONDS as u64 {
                    metrics_at = Instant::now();
                    let elapsed = started.elapsed().as_secs_f64();
                    diagnostic(
                        telemetry,
                        &metrics(&stats, bytes, reconnects, elapsed, historical, &intake),
                    );
                }
                if std::env::var_os("BACKFILL_CANCEL_FILE")
                    .is_some_and(|p| std::path::Path::new(&p).exists())
                    && historical
                {
                    return Ok("cancelled");
                }
                if let Some(job) = replay.as_mut() {
                    let _permit = admit()?;
                    job.reservation.allowance()?;
                }
                // A buffered batch times out at its flush window, which is not idleness.
                let idle_timeout = buffered_since.is_none();
                let update = match network(
                    &runtime,
                    deadline,
                    next_update(
                        &mut stream,
                        buffered_since.map_or(Duration::from_secs(10), |since| {
                            BATCH_WINDOW.saturating_sub(since.elapsed())
                        }),
                    ),
                ) {
                    Ok(Some(u)) => u,
                    Ok(None) => {
                        stats.flush_live(
                            &mut intake,
                            &mut buffer,
                            &mut producer,
                            mint,
                            gate,
                            telemetry,
                        )?;
                        (buffered_bytes, buffered_since) = (0, None);
                        event(telemetry, "connection_ended", "provider_unavailable");
                        break;
                    }
                    Err(Error("stream_inactive")) => {
                        stats.flush_live(
                            &mut intake,
                            &mut buffer,
                            &mut producer,
                            mint,
                            gate,
                            telemetry,
                        )?;
                        (buffered_bytes, buffered_since) = (0, None);
                        if idle_timeout {
                            telemetry.emit("stream_idle", "intake", "waiting", None);
                        }
                        continue;
                    }
                    Err(Error("intake_time_limit")) => {
                        stats.flush_live(
                            &mut intake,
                            &mut buffer,
                            &mut producer,
                            mint,
                            gate,
                            telemetry,
                        )?;
                        return Ok("time_limit");
                    }
                    Err(e) => {
                        stats.flush_live(
                            &mut intake,
                            &mut buffer,
                            &mut producer,
                            mint,
                            gate,
                            telemetry,
                        )?;
                        (buffered_bytes, buffered_since) = (0, None);
                        event(telemetry, "connection_failed", e.0);
                        break;
                    }
                };
                if matches!(
                    update.update_oneof,
                    Some(subscribe_update::UpdateOneof::Ping(_))
                ) {
                    network(&runtime, deadline, async {
                        sender
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await
                            .map_err(|_| Error("subscription_failed"))
                    })?;
                }
                if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = update.update_oneof {
                    if tx.slot < start && !historical {
                        return Err(Error("unexpected_slot"));
                    }
                    telemetry.slot = Some(tx.slot);
                    bytes = bytes.saturating_add(tx.encoded_len());
                    if !historical {
                        buffered_bytes = buffered_bytes.saturating_add(tx.encoded_len());
                        buffered_since.get_or_insert_with(Instant::now);
                        last_slot = last_slot.max(Some(tx.slot));
                        buffer.push(tx);
                        if buffer.len() >= BATCH_INPUTS || buffered_bytes >= BATCH_BYTES {
                            stats.flush_live(
                                &mut intake,
                                &mut buffer,
                                &mut producer,
                                mint,
                                gate,
                                telemetry,
                            )?;
                            (buffered_bytes, buffered_since) = (0, None);
                        }
                        continue;
                    }
                    let _permit = admit()?;
                    let operation = Instant::now();
                    stats.count += 1;
                    let id = if let Some(job) = replay.as_mut() {
                        match job.input(&tx)? {
                            crate::storage::replay::Retained::PastEnd => {
                                return Ok("end_observed");
                            }
                            crate::storage::replay::Retained::BeforeStart => continue,
                            crate::storage::replay::Retained::AlreadyDecoded => {
                                duplicate_receipts += 1;
                                continue;
                            }
                            crate::storage::replay::Retained::Pending(id) => id,
                        }
                    } else {
                        intake.retain(&tx)?
                    };
                    stats.raw_writes += 1;
                    stats.raw_seconds += operation.elapsed().as_secs_f64();
                    let operation = Instant::now();
                    let processed = intake.process(id, &tx);
                    if let Some(job) = replay.as_mut() {
                        job.reservation.acknowledge(id, &processed)?;
                    }
                    stats.output_seconds += operation.elapsed().as_secs_f64();
                    match processed {
                        Ok(n) => {
                            stats.output_successes += 1;
                            stats.inserted += n;
                        }
                        Err(e) => {
                            event(telemetry, "decode_or_write_failed", e.0);
                            if e.0.starts_with("storage_") {
                                return Err(e);
                            }
                            stats.failures += 1;
                        }
                    }
                    if stats.count >= MAX_MESSAGES {
                        return Ok("message_limit");
                    }
                }
            }
        }
        Err(Error("provider_unavailable"))
    })();
    let result = match result {
        Err(Error("intake_time_limit")) => stats
            .flush_live(
                &mut intake,
                &mut buffer,
                &mut producer,
                mint,
                gate,
                telemetry,
            )
            .map(|_| "time_limit"),
        Err(Error("historical_admission_timeout")) if Instant::now() >= deadline => {
            Ok("time_limit")
        }
        other => other,
    };
    let result = match producer.as_mut() {
        Some(producer) => result.and_then(|status| {
            if let Some(slot) = producer.flush(Duration::from_secs(30))? {
                let _permit = gate.live()?;
                intake.log.advance_producer_cursor(mint, slot as i64)?;
            }
            Ok(status)
        }),
        None => result,
    };
    let status = result.as_ref().map_or_else(|e| e.0, |s| *s);
    if let Some(run) = run {
        let _permit = gate.live()?;
        intake.log.finish_run(run, status)?;
    }
    event(telemetry, "shutdown", status);
    let elapsed = started.elapsed().as_secs_f64();
    let coverage = if historical {
        json!({"coverage":"unknown","reason":"filtered_replay_has_no_validated_slot_completion_evidence"})
    } else {
        Intake::report(database)?
    };
    let report = json!({"status":status,"raw_write_successes":stats.raw_writes,"duplicate_receipts":duplicate_receipts,"mode":if historical {"historical"}else{"live"},"received":stats.count,"unresolved_attempts":stats.failures,"input_protobuf_bytes":bytes,"elapsed_seconds":elapsed,"reconnect_attempts":reconnects,"inserted_transfers":stats.inserted,"input_transactions_per_second":stats.count as f64/elapsed,"decoded_write_successes":stats.output_successes,"coverage_valid":false,"verified_boundary":null,"freshness_seconds":null,"storage":intake.store.stats(),"kafka":producer.as_ref().map(|p| json!({"topic":crate::kafka::TOPIC,"producer_cursor":p.cursor(),"late_beyond_reorder_margin":p.late_beyond_margin,"reorder_margin_slots":crate::kafka::REORDER_MARGIN_SLOTS})),"coverage":coverage});
    diagnostic(
        telemetry,
        &metrics(&stats, bytes, reconnects, elapsed, historical, &intake),
    );
    result?;
    Ok(report)
}

/// The live subscription start: the run's first slot, or the reorder-margin resume point
/// of input already delivered, whichever is later.
fn resume_start(from: u64, delivered: Option<u64>) -> u64 {
    delivered.map_or(from, |slot| crate::kafka::resume_slot(slot).max(from))
}

/// Cumulative counters since the run started; a continuous run emits them every minute.
fn metrics(
    stats: &IntakeStats,
    bytes: usize,
    reconnects: usize,
    elapsed: f64,
    historical: bool,
    intake: &Intake,
) -> Value {
    json!({"event":"live_metrics","mode":if historical {"historical"}else{"live"},"source_kind":"laserstream","indexer_transactions_total":stats.count,"indexer_decode_failures_total":stats.failures,"indexer_input_bytes_total":bytes,"indexer_reconnects_total":reconnects,"indexer_coverage_valid":0,"indexer_live_freshness_seconds":null,"indexer_live_slot_gap":null,
        "stage_metrics":[{"stage":"raw_write","success":stats.raw_writes,"duration_seconds_sum":stats.raw_seconds},
            {"stage":"decode_write","success":stats.output_successes,"unsupported":stats.failures,"duration_seconds_sum":stats.output_seconds},
            {"stage":"transfer_write","storage":intake.store.stats()}],
        "elapsed_seconds":elapsed,"input_transactions_per_second":stats.count as f64/elapsed})
}
#[derive(Default)]
struct IntakeStats {
    count: usize,
    raw_writes: usize,
    failures: usize,
    raw_seconds: f64,
    output_seconds: f64,
    output_successes: usize,
    inserted: usize,
}
impl IntakeStats {
    fn flush_live(
        &mut self,
        intake: &mut Intake,
        buffer: &mut Vec<SubscribeUpdateTransaction>,
        producer: &mut Option<crate::kafka::IntakeProducer>,
        mint: &str,
        gate: &crate::coordinator::WriteGate,
        telemetry: &crate::telemetry::Telemetry,
    ) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        if let Some(producer) = producer.as_mut() {
            let operation = Instant::now();
            for tx in buffer.iter() {
                let late = producer.late_beyond_margin;
                producer.send(tx.slot, &tx.encode_to_vec(), PROTO_SCHEMA)?;
                if producer.late_beyond_margin > late {
                    event(
                        telemetry,
                        "late_slot_beyond_reorder_margin",
                        "late_slot_beyond_reorder_margin",
                    );
                }
            }
            self.count += buffer.len();
            if let Some(slot) = producer.poll(Duration::ZERO)? {
                let _permit = gate.live()?;
                intake.log.advance_producer_cursor(mint, slot as i64)?;
            }
            self.raw_seconds += operation.elapsed().as_secs_f64();
            buffer.clear();
            return Ok(());
        }
        let _permit = gate.live()?;
        let operation = Instant::now();
        let ids = intake.retain_batch(buffer)?;
        self.raw_writes += ids.len();
        self.raw_seconds += operation.elapsed().as_secs_f64();
        self.count += buffer.len();
        let operation = Instant::now();
        let inputs: Vec<(i64, &SubscribeUpdateTransaction)> =
            ids.iter().copied().zip(buffer.iter()).collect();
        let results = intake.process_batch(&inputs);
        self.output_seconds += operation.elapsed().as_secs_f64();
        let mut storage_error = None;
        for result in results {
            match result {
                Ok(n) => {
                    self.output_successes += 1;
                    self.inserted += n;
                }
                Err(e) => {
                    event(telemetry, "decode_or_write_failed", e.0);
                    if e.0.starts_with("storage_") {
                        storage_error = Some(e);
                    } else {
                        self.failures += 1;
                    }
                }
            }
        }
        buffer.clear();
        storage_error.map_or(Ok(()), Err)
    }
}

fn live_producer() -> Result<Option<crate::kafka::IntakeProducer>> {
    match std::env::var("INDEXER_KAFKA_BROKERS") {
        Ok(brokers) => {
            crate::kafka::require_loopback(&brokers)?;
            crate::kafka::ensure_topic(&brokers, crate::kafka::TOPIC, crate::kafka::PARTITIONS)?;
            Ok(Some(crate::kafka::IntakeProducer::new(
                &brokers,
                crate::kafka::TOPIC,
            )?))
        }
        Err(_) => Ok(None),
    }
}

/// Helius credential sent as `x-token`; loaded from the environment only.
#[derive(Clone)]
pub(crate) struct Token(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);
impl Token {
    pub(crate) fn from_env() -> Result<Self> {
        let api_key =
            std::env::var("HELIUS_API_KEY").map_err(|_| Error("missing_live_credentials"))?;
        Ok(Self(
            api_key
                .parse()
                .map_err(|_| Error("invalid_live_credentials"))?,
        ))
    }
}
impl tonic::service::Interceptor for Token {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, tonic::Status> {
        req.metadata_mut().insert("x-token", self.0.clone());
        Ok(req)
    }
}
pub(crate) type Client = geyser_client::GeyserClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Token>,
>;

pub(crate) fn connect(
    runtime: &tokio::runtime::Runtime,
    deadline: Instant,
    token: &Token,
) -> Result<Client> {
    // Fixed canonical mainnet endpoint prevents credential-bearing URLs entering diagnostics/config.
    let channel = network(runtime, deadline, async {
        Endpoint::from_static("https://laserstream-mainnet-ewr.helius-rpc.com")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .initial_stream_window_size(RECEIVE_WINDOW)
            .initial_connection_window_size(RECEIVE_WINDOW)
            .http2_adaptive_window(false)
            .buffer_size(1)
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|_| Error("tls_failed"))?
            .connect()
            .await
            .map_err(|_| Error("provider_unavailable"))
    })?;
    Ok(
        geyser_client::GeyserClient::with_interceptor(channel, token.clone())
            .max_decoding_message_size(decoder::MAX_CAPTURE_BYTES)
            .max_encoding_message_size(65536),
    )
}

/// Actual earliest replayable slot (None means unavailable, not zero) and the finalized reference.
pub(crate) fn availability(
    runtime: &tokio::runtime::Runtime,
    deadline: Instant,
    client: &mut Client,
) -> Result<(Option<u64>, u64)> {
    let first_available = network(runtime, deadline, async {
        Ok(client
            .subscribe_replay_info(SubscribeReplayInfoRequest {})
            .await
            .map_err(|_| Error("replay_info_unavailable"))?
            .into_inner()
            .first_available)
    })?;
    let reference = network(runtime, deadline, async {
        Ok(client
            .get_slot(GetSlotRequest {
                commitment: Some(CommitmentLevel::Finalized as i32),
            })
            .await
            .map_err(|_| Error("reference_unavailable"))?
            .into_inner()
            .slot)
    })?;
    Ok((first_available, reference))
}

/// Drive only network futures here. Synchronous PostgreSQL operations run after
/// block_on returns, outside Tokio's enter guard (the postgres client owns a runtime).
pub(crate) fn network<T>(
    runtime: &tokio::runtime::Runtime,
    deadline: Instant,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error("intake_time_limit"));
    }
    runtime.block_on(async {
        tokio::time::timeout(remaining, future)
            .await
            .map_err(|_| Error("intake_time_limit"))?
    })
}

fn diagnostic(telemetry: &crate::telemetry::Telemetry, metrics: &Value) {
    let mut event = telemetry.event("live_metrics", "metrics", "success", None);
    event["metrics"] = metrics.clone();
    write_diagnostic(std::io::stderr().lock(), &event);
}
fn write_diagnostic(mut output: impl std::io::Write, value: &Value) {
    let _ = writeln!(output, "{value}");
}
fn event(telemetry: &crate::telemetry::Telemetry, event: &'static str, outcome: &'static str) {
    let error = if matches!(
        outcome,
        "startup" | "retry" | "time_limit" | "message_limit" | "end_observed" | "cancelled"
    ) {
        None
    } else {
        Some(outcome)
    };
    telemetry.emit(
        event,
        "intake",
        if error.is_some() { "failed" } else { outcome },
        error,
    );
}

async fn next_update<S, E>(stream: &mut S, idle: Duration) -> Result<Option<SubscribeUpdate>>
where
    S: futures_util::Stream<Item = std::result::Result<SubscribeUpdate, E>> + Unpin,
{
    tokio::time::timeout(idle, stream.next())
        .await
        .map_err(|_| Error("stream_inactive"))?
        .transpose()
        .map_err(|_| Error("provider_unavailable"))
}

fn check_replay(from: u64, first: u64) -> Result<()> {
    if first > from {
        Err(Error("outside_replay_window"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::pg::TestDatabase;
    use laserstream_core_proto::solana::storage::confirmed_block as wire;
    #[test]
    fn a_reconnect_resumes_from_delivered_input_within_the_reorder_margin() {
        let margin = crate::kafka::REORDER_MARGIN_SLOTS;
        assert_eq!(resume_start(1_000, None), 1_000);
        assert_eq!(resume_start(1_000, Some(5_000)), 5_000 - margin);
        // Never earlier than the run's own first slot.
        assert_eq!(resume_start(1_000, Some(1_000 + margin - 1)), 1_000);
    }
    #[test]
    fn shared_loop_flush_keeps_live_output_batched_and_atomically_acknowledged() {
        let db = TestDatabase::new();
        let mut intake = Intake::open(&db.0).unwrap();
        let mut other = update();
        let info = other.transaction.as_mut().unwrap();
        info.signature = vec![10; 64];
        info.transaction.as_mut().unwrap().signatures = vec![vec![10; 64]];
        let mut buffer = vec![update(), other];
        let mut stats = IntakeStats::default();
        stats
            .flush_live(
                &mut intake,
                &mut buffer,
                &mut None,
                USDC,
                &crate::coordinator::WriteGate::default(),
                &crate::telemetry::Telemetry::default(),
            )
            .unwrap();
        assert!(buffer.is_empty());
        assert_eq!(stats.count, 2);
        assert_eq!(stats.raw_writes, 2);
        assert_eq!(stats.output_successes, 2);
        assert_eq!(intake.store.stats().batches, 1);
        assert_eq!(Intake::report(&db.0).unwrap()["unresolved_items"], 0);
        let rows = Store::query(&db.0, Some(USDC), 100).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.amount_raw == Some(42) && row.fee_raw == Some(0))
        );
    }

    #[test]
    fn shared_loop_publishes_live_to_kafka_while_replay_keeps_its_own_receipts() {
        use crate::{
            kafka::{
                self,
                tests::{TestTopic, test_brokers},
            },
            storage::replay::JobLog,
        };
        let db = TestDatabase::new();
        let topic = TestTopic::new(kafka::PARTITIONS);
        let brokers = test_brokers();
        crate::replay::create(&db.0, "recent", USDC, 100, 149).unwrap();
        let mut attempt = crate::replay::ReplayRun {
            scope: JobLog::open(&db.0).unwrap().scope("recent").unwrap(),
            reservation: JobLog::reserve(&db.0, "recent", 5).unwrap(),
        };
        let replay_tx = update();
        let crate::storage::replay::Retained::Pending(id) = attempt.input(&replay_tx).unwrap()
        else {
            panic!("replay raw missing")
        };
        let mut intake = Intake::open(&db.0).unwrap();
        let result = intake.process(id, &replay_tx);
        attempt.reservation.acknowledge(id, &result).unwrap();
        result.unwrap();
        let mut live_tx = update();
        live_tx.slot = 150;
        let info = live_tx.transaction.as_mut().unwrap();
        info.signature = vec![10; 64];
        info.transaction.as_mut().unwrap().signatures = vec![vec![10; 64]];
        let mut buffer = vec![live_tx];
        let mut producer = Some(kafka::IntakeProducer::new(&brokers, &topic.0).unwrap());
        let mut stats = IntakeStats::default();
        stats
            .flush_live(
                &mut intake,
                &mut buffer,
                &mut producer,
                USDC,
                &crate::coordinator::WriteGate::default(),
                &crate::telemetry::Telemetry::default(),
            )
            .unwrap();
        producer
            .as_mut()
            .unwrap()
            .flush(Duration::from_secs(20))
            .unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.raw_writes, 0);
        assert_eq!(stats.output_successes, 0);
        assert_eq!(Store::query(&db.0, Some(USDC), 100).unwrap().len(), 1);
        kafka::consume(&db.0, &brokers, &topic.0, 6).unwrap();
        assert_eq!(Store::query(&db.0, Some(USDC), 100).unwrap().len(), 2);
        assert_eq!(
            crate::replay::status(&db.0, "recent").unwrap()["receipts"]["unique_inputs"],
            1
        );
    }

    #[test]
    fn async_receive_returns_before_synchronous_postgres_write_and_deadline_is_bounded() {
        let db = TestDatabase::new();
        let mut intake = Intake::open(&db.0).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tx = network(&runtime, Instant::now() + Duration::from_secs(1), async {
            tokio::task::yield_now().await;
            Ok(update())
        })
        .unwrap();
        assert!(tokio::runtime::Handle::try_current().is_err());
        let raw = intake.retain(&tx).unwrap();
        assert_eq!(intake.process(raw, &tx).unwrap(), 1);
        assert_eq!(Store::query(&db.0, Some(USDC), 100).unwrap().len(), 1);
        assert_eq!(
            network(
                &runtime,
                Instant::now() + Duration::from_millis(20),
                std::future::pending::<Result<()>>()
            )
            .unwrap_err()
            .0,
            "intake_time_limit"
        );
    }
    pub(crate) fn update() -> SubscribeUpdateTransaction {
        let mut data = vec![12];
        data.extend(42u64.to_le_bytes());
        data.push(6);
        SubscribeUpdateTransaction {
            bank_id: 0,
            slot: 123,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![9; 64],
                transaction: Some(wire::Transaction {
                    signatures: vec![vec![9; 64]],
                    message: Some(wire::Message {
                        account_keys: vec![
                            bs58::decode(decoder::SPL_TOKEN).into_vec().unwrap(),
                            vec![1; 32],
                            bs58::decode(USDC).into_vec().unwrap(),
                            vec![2; 32],
                            vec![3; 32],
                        ],
                        instructions: vec![wire::CompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![1, 2, 3, 4],
                            data,
                        }],
                        ..Default::default()
                    }),
                }),
                meta: Some(wire::TransactionStatusMeta::default()),
                ..Default::default()
            }),
        }
    }
    // Only the spawned test process exits; no production fault-injection switch exists.
    #[test]
    #[ignore = "subprocess helper for process-exit recovery tests"]
    fn process_exit_boundary_child() {
        let session = std::env::var("INDEXER_TEST_RECOVERY_DB").unwrap();
        let path = &Database::new(TestDatabase::url(), &session).unwrap();
        let phase = std::env::var("INDEXER_TEST_RECOVERY_PHASE").unwrap();
        let mut intake = Intake::open(path).unwrap();
        let tx = update();
        if phase == "before_raw" {
            std::process::exit(77);
        }
        let id = intake.retain(&tx).unwrap();
        if phase == "after_retry_cursor" {
            path.connect()
                .unwrap()
                .execute("INSERT INTO live_retry_cursor VALUES(1,$1)", &[&id])
                .unwrap();
            std::process::exit(77);
        }
        if phase == "broken_diagnostics" {
            std::io::Read::read_exact(&mut std::io::stdin(), &mut [0]).unwrap();
            assert_eq!(intake.retry().unwrap()["decoded"], 1);
            std::process::exit(77);
        }
        if phase == "after_raw" {
            std::process::exit(77);
        }
        if phase == "after_output" {
            let raw = normalize(&tx).unwrap();
            let capture = decoder::parse_capture(&raw).unwrap();
            Store::open(path)
                .unwrap()
                .persist(&capture, &raw, &decoder::decode(&capture))
                .unwrap();
            std::process::exit(77);
        }
        assert_eq!(phase, "after_ack");
        intake.process(id, &tx).unwrap();
        std::process::exit(77);
    }

    #[test]
    fn process_exit_recovery_preserves_raw_output_and_acknowledgment_boundaries() {
        for phase in [
            "before_raw",
            "after_raw",
            "after_output",
            "after_ack",
            "after_retry_cursor",
        ] {
            let test = TestDatabase::new();
            let path = &test.0;
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "live::tests::process_exit_boundary_child",
                    "--ignored",
                ])
                .env("INDEXER_TEST_RECOVERY_DB", path.session())
                .env("INDEXER_TEST_RECOVERY_PHASE", phase)
                .output()
                .unwrap()
                .status;
            assert_eq!(status.code(), Some(77), "{phase}");
            let before = Store::query(path, Some(USDC), 100).unwrap();
            assert_eq!(
                before.len(),
                usize::from(matches!(phase, "after_output" | "after_ack"))
            );
            let mut db = test.sql();
            let received: Option<i64> = db
                .query_one("SELECT MIN(received_at) FROM live_raw", &[])
                .unwrap()
                .get(0);
            assert_eq!(received.is_some(), phase != "before_raw");
            let report = Intake::report(path).unwrap();
            assert_eq!(
                report["unresolved_items"],
                usize::from(matches!(
                    phase,
                    "after_raw" | "after_output" | "after_retry_cursor"
                ))
            );
            assert!(report["verified_boundary"].is_null());
            let mut intake = Intake::open(path).unwrap();
            intake.retry().unwrap();
            // Unacknowledged input before raw persistence must arrive again from a source.
            let tx = update();
            let id = intake.retain(&tx).unwrap();
            intake.process(id, &tx).unwrap();
            assert_eq!(Store::summary(path, USDC).unwrap()["transfer_count"], 1);
            assert_eq!(
                Store::summary(path, USDC).unwrap()["known_volume_raw"],
                "42"
            );
            assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 0);
            if let Some(received) = received {
                assert_eq!(
                    db.query_one("SELECT received_at FROM live_raw ORDER BY id LIMIT 1", &[])
                        .unwrap()
                        .get::<_, i64>(0),
                    received
                );
            }
            assert!(Intake::report(path).unwrap()["verified_boundary"].is_null());
        }
    }

    #[test]
    fn unresolved_prefix_cannot_starve_later_retryable_input_after_restart() {
        let test = TestDatabase::new();
        let path = &test.0;
        let mut intake = Intake::open(path).unwrap();
        let mut unsupported = update();
        unsupported.transaction.as_mut().unwrap().meta = None;
        for _ in 0..100 {
            intake.retain(&unsupported).unwrap();
        }
        intake.retain(&update()).unwrap();
        test.sql()
            .execute("UPDATE live_raw SET received_at=1", &[])
            .unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 0);
        drop(intake);
        let mut intake = Intake::open(path).unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Store::summary(path, USDC).unwrap()["transfer_count"], 1);
        assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 100);
        assert_eq!(
            test.sql()
                .query_one("SELECT MIN(received_at) FROM live_raw", &[])
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        drop(intake);
        test.sql()
            .execute("DELETE FROM live_retry_cursor", &[])
            .unwrap();
        assert!(Intake::report(path).unwrap()["retry_cursor"].is_null());
        Intake::open(path).unwrap();
        assert_eq!(Store::summary(path, USDC).unwrap()["transfer_count"], 1);
    }

    #[test]
    fn broken_diagnostics_pipe_does_not_block_durable_retry() {
        use std::{io::Write, process::Stdio};
        let test = TestDatabase::new();
        let path = &test.0;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "live::tests::process_exit_boundary_child",
                "--ignored",
            ])
            .env("INDEXER_TEST_RECOVERY_DB", path.session())
            .env("INDEXER_TEST_RECOVERY_PHASE", "broken_diagnostics")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        drop(child.stderr.take());
        child.stdin.take().unwrap().write_all(&[1]).unwrap();
        assert_eq!(child.wait().unwrap().code(), Some(77));
        assert_eq!(Store::summary(path, USDC).unwrap()["transfer_count"], 1);
        assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 0);
    }

    #[test]
    fn finalized_mint_request_and_version_flags() {
        let req = request(123);
        assert_eq!(req.commitment, Some(2));
        assert_eq!(req.from_slot, Some(123));
        assert!(check_replay(123, 123).is_ok());
        assert_eq!(
            check_replay(122, 123).unwrap_err().0,
            "outside_replay_window"
        );
        let custom = request_mint(42, "So11111111111111111111111111111111111111112");
        assert_eq!(
            custom.transactions["usdc"].account_include,
            ["So11111111111111111111111111111111111111112"]
        );
        assert!(custom.transactions["usdc"].match_mints);
        assert!(req.transactions["usdc"].match_mints);
        assert_eq!(req.transactions["usdc"].account_include, [USDC]);
        let mut tx = update();
        let raw = normalize(&tx).unwrap();
        let capture = decoder::parse_capture(&raw).unwrap();
        assert_eq!(decoder::decode(&capture).unwrap()[0].amount_raw, Some(42));
        let message = tx
            .transaction
            .as_mut()
            .unwrap()
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.versioned = true;
        message.config = Some(wire::TransactionConfig::default());
        assert_eq!(
            decoder::parse_capture(&normalize(&tx).unwrap())
                .unwrap()
                .result
                .version,
            1
        );
        tx.transaction
            .as_mut()
            .unwrap()
            .meta
            .as_mut()
            .unwrap()
            .inner_instructions_none = true;
        assert_eq!(
            decoder::decode(&decoder::parse_capture(&normalize(&tx).unwrap()).unwrap())
                .unwrap_err()
                .0,
            "missing_inner_instructions"
        );
    }
    #[test]
    fn durable_pending_restart_retry_duplicates_and_failed_metadata() {
        let test = TestDatabase::new();
        let path = &test.0;
        let tx = update();
        let intake = Intake::open(path).unwrap();
        let id = intake.retain(&tx).unwrap();
        drop(intake);
        let mut intake = Intake::open(path).unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Store::query(path, Some(USDC), 100).unwrap().len(), 1);
        assert_eq!(intake.process(id, &tx).unwrap(), 0);
        let report = Intake::report(path).unwrap();
        assert!(report["verified_boundary"].is_null());
        assert_eq!(report["unresolved_items"], 0);
        let mut broken = update();
        broken.transaction.as_mut().unwrap().meta = None;
        let id = intake.retain(&broken).unwrap();
        assert!(intake.process(id, &broken).is_err());
        assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 1);
        let wire: Vec<u8> = test
            .sql()
            .query_one(
                "SELECT protobuf FROM live_raw WHERE id=$1 AND octet_length(protobuf)<=16777216",
                &[&id],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            SubscribeUpdateTransaction::decode(wire.as_slice()).unwrap(),
            broken
        );
        test.sql()
            .execute(
                "UPDATE live_raw SET proto_schema='future-schema' WHERE id=$1",
                &[&id],
            )
            .unwrap();
        assert_eq!(
            intake.retry().unwrap_err().0,
            "unsupported_stored_proto_schema"
        );
        assert_eq!(Store::query(path, Some(USDC), 100).unwrap().len(), 1);
    }
    #[test]
    fn reports_preserve_the_stored_run_decoder_scope() {
        let test = TestDatabase::new();
        let path = &test.0;
        let intake = Intake::open(path).unwrap();
        drop(intake);
        test.sql().execute("INSERT INTO live_runs(from_slot,mint,decoder,status) VALUES(123,$1,'previous-decoder','time_limit')",&[&USDC]).unwrap();
        let report = Intake::report(path).unwrap();
        assert_eq!(report["runs"][0]["decoder_version"], "previous-decoder");
        assert_eq!(report["runs"][0]["mint"], USDC);
        assert!(report["runs"][0]["verified_boundary"].is_null());
    }
    #[test]
    fn failed_output_write_preserves_retryable_raw_without_transfers() {
        let test = TestDatabase::new();
        let path = &test.0;
        let mut intake = Intake::open(path).unwrap();
        test.sql().batch_execute("CREATE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_transfers BEFORE INSERT ON transfers FOR EACH ROW EXECUTE FUNCTION fail();").unwrap();
        let tx = update();
        let id = intake.retain(&tx).unwrap();
        assert!(intake.process(id, &tx).is_err());
        assert!(Store::query(path, None, 100).unwrap().is_empty());
        assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 1);
        let recent = &Intake::report(path).unwrap()["recent"];
        assert_eq!(
            (recent["received"].as_i64(), recent["pending"].as_i64()),
            (Some(1), Some(1))
        );
        test.sql()
            .batch_execute("DROP TRIGGER fail_transfers ON transfers;")
            .unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Intake::report(path).unwrap()["recent"]["decoded"], 1);
        assert_eq!(Store::query(path, None, 100).unwrap().len(), 1);
    }
    #[test]
    fn batch_progress_commits_only_with_its_output() {
        let test = TestDatabase::new();
        let path = &test.0;
        let mut intake = Intake::open(path).unwrap();
        let mut other = update();
        let info = other.transaction.as_mut().unwrap();
        info.signature = vec![8; 64];
        info.transaction.as_mut().unwrap().signatures = vec![vec![8; 64]];
        let mut broken = update();
        broken.transaction.as_mut().unwrap().meta = None;
        let status = |test: &TestDatabase| -> Vec<String> {
            test.sql()
                .query("SELECT status FROM live_raw ORDER BY id", &[])
                .unwrap()
                .iter()
                .map(|r| r.get(0))
                .collect()
        };
        // Output fails for the whole batch: no input may be acknowledged.
        test.sql().batch_execute("CREATE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_transfers BEFORE INSERT ON transfers FOR EACH ROW EXECUTE FUNCTION fail();").unwrap();
        let batch = [update(), broken.clone(), other.clone()];
        let ids = intake.retain_batch(&batch).unwrap();
        let inputs: Vec<_> = ids.iter().copied().zip(batch.iter()).collect();
        let results = intake.process_batch(&inputs);
        assert_eq!(results[0], Err(Error("storage_write_failed")));
        assert_eq!(results[2], Err(Error("storage_write_failed")));
        assert!(Store::query(path, None, 100).unwrap().is_empty());
        assert!(status(&test).iter().all(|s| s != "decoded"));
        test.sql()
            .batch_execute("DROP TRIGGER fail_transfers ON transfers;")
            .unwrap();
        // The same retained rows succeed together; the broken one stays retryable.
        let results = intake.process_batch(&inputs);
        assert_eq!(results[0], Ok(1));
        assert_eq!(results[1], Err(Error("missing_execution_metadata")));
        assert_eq!(results[2], Ok(1));
        assert_eq!(
            status(&test),
            ["decoded", "missing_execution_metadata", "decoded"]
        );
        assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 1);
        assert_eq!(Store::query(path, None, 100).unwrap().len(), 2);
    }
    #[test]
    fn produced_input_advances_a_monotonic_replay_cursor() {
        use crate::kafka::{
            IntakeProducer, require_loopback,
            tests::{TestTopic, test_brokers},
        };
        let test = TestDatabase::new();
        let topic = TestTopic::new(2);
        let intake = Intake::open(&test.0).unwrap();
        let mut producer = IntakeProducer::new(&test_brokers(), &topic.0).unwrap();
        for slot in [123, 124, 125] {
            let mut tx = update();
            tx.slot = slot;
            producer
                .send(slot, &tx.encode_to_vec(), PROTO_SCHEMA)
                .unwrap();
        }
        assert_eq!(producer.late_beyond_margin, 0, "123..125 are in order");
        assert_eq!(producer.flush(Duration::from_secs(20)).unwrap(), Some(125));
        intake.log.advance_producer_cursor(USDC, 125).unwrap();
        // A stale producer reporting an older slot cannot rewind the stored position.
        intake.log.advance_producer_cursor(USDC, 120).unwrap();
        assert_eq!(
            Intake::report(&test.0).unwrap()["producer_cursors"],
            json!([{"stream":USDC,"slot":125,"resume_from":125u64.saturating_sub(crate::kafka::REORDER_MARGIN_SLOTS)}])
        );
        // A slot far behind the newest one sent is outside the resume margin: counted.
        producer.send(1_000, b"newest", PROTO_SCHEMA).unwrap();
        producer.send(10, b"late", PROTO_SCHEMA).unwrap();
        assert_eq!(producer.late_beyond_margin, 1);
        assert_eq!(
            producer.flush(Duration::from_secs(20)).unwrap(),
            Some(1_000)
        );
        let oversize = vec![0; crate::kafka::MAX_MESSAGE_BYTES + 1];
        assert_eq!(
            producer.send(126, &oversize, PROTO_SCHEMA).unwrap_err().0,
            "live_input_limit"
        );
        for brokers in ["db.example.test:9092", "127.0.0.1:59092,10.0.0.5:9092", ""] {
            assert_eq!(
                require_loopback(brokers).unwrap_err().0,
                "kafka_tls_required"
            );
        }
        assert!(require_loopback("localhost:59092,[::1]:9092").is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transport_end_failure_and_inactivity_are_distinct() {
        let mut empty = futures_util::stream::empty::<std::result::Result<SubscribeUpdate, ()>>();
        assert!(
            next_update(&mut empty, Duration::from_millis(10))
                .await
                .unwrap()
                .is_none()
        );
        let mut failed = futures_util::stream::iter([Err::<SubscribeUpdate, _>(
            "secret credential-bearing error",
        )]);
        assert_eq!(
            next_update(&mut failed, Duration::from_millis(10))
                .await
                .unwrap_err()
                .0,
            "provider_unavailable"
        );
        let mut idle = futures_util::stream::pending::<std::result::Result<SubscribeUpdate, ()>>();
        assert_eq!(
            next_update(&mut idle, Duration::from_millis(10))
                .await
                .unwrap_err()
                .0,
            "stream_inactive"
        );
        let mut idle = futures_util::stream::pending::<std::result::Result<SubscribeUpdate, ()>>();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                next_update(&mut idle, Duration::from_secs(10))
            )
            .await
            .is_err()
        );
    }
    #[test]
    fn diagnostics_failure_and_unknown_ages_are_safe() {
        struct Broken;
        impl std::io::Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        write_diagnostic(Broken, &json!({"coverage_valid":0,"freshness":null}));
        assert!(age(now() + 100).is_none());
    }

    #[test]
    fn bounds_checked_before_environment_or_network() {
        // Zero seconds is a continuous run, not a bounds error.
        assert_ne!(
            run(
                &Database::new("postgres://unused@127.0.0.1/x", "unused").unwrap(),
                1,
                CONTINUOUS,
                &mut crate::telemetry::Telemetry::default()
            )
            .unwrap_err()
            .0,
            "invalid_live_bounds"
        );
        assert_eq!(
            run(
                &Database::new("postgres://unused@127.0.0.1/x", "unused").unwrap(),
                1,
                301,
                &mut crate::telemetry::Telemetry::default()
            )
            .unwrap_err()
            .0,
            "invalid_live_bounds"
        );
    }
}
