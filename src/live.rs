//! Bounded LaserStream intake. Filtered transaction delivery never proves slot coverage.
use crate::storage::Database;
use crate::{
    Error, Result, decoder,
    storage::{
        Store,
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
const PROTO_SCHEMA: &str = "laserstream-core-proto/14.0.0";
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
        if update.encoded_len() > decoder::MAX_CAPTURE_BYTES || update.slot > i64::MAX as u64 {
            return Err(Error("live_input_limit"));
        }
        self.log.retain(update.slot as i64, &update.encode_to_vec())
    }
    pub fn process(&mut self, id: i64, update: &SubscribeUpdateTransaction) -> Result<usize> {
        let result = (|| {
            let raw = normalize(update)?;
            let capture = decoder::parse_capture(&raw)?;
            // Persist the same complete record set as historical ingestion. Filter queries by mint.
            let decoded = decoder::decode(&capture);
            let count = self.store.persist(&capture, &raw, &decoded)?;
            decoded?;
            Ok(count)
        })();
        let status = result.as_ref().map_or_else(|e: &Error| e.0, |_| "decoded");
        self.log.set_status(id, status)?;
        result
    }
    pub fn retry(&mut self) -> Result<Value> {
        let cursor = self.log.retry_cursor()?;
        let mut telemetry = crate::telemetry::Telemetry::default();
        telemetry.set_context("live", "laserstream", "recovery");
        telemetry.emit("retry_batch_started", "retry", "retry", None);
        let pending = self.log.pending_after(cursor, 100)?;
        let mut success = 0;
        for id in &pending {
            // Fairness only: a crash can delay this row until wraparound, never acknowledge it.
            self.log.advance_retry_cursor(*id)?;
            let (raw, schema) = self.log.raw(*id)?;
            if schema != PROTO_SCHEMA {
                return Err(Error("unsupported_stored_proto_schema"));
            }
            if raw.len() > decoder::MAX_CAPTURE_BYTES {
                return Err(Error("stored_record_invalid"));
            }
            let update = SubscribeUpdateTransaction::decode(raw.as_slice())
                .map_err(|_| Error("stored_record_invalid"))?;
            success += usize::from(self.process(*id, &update).is_ok());
        }
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
            json!({"recent":{"window_seconds":RECENT_SECONDS,"received":received,"decoded":decoded,"pending":received-decoded},"last_received_slot":snapshot.last_received_slot,"retry_cursor":snapshot.retry_cursor,"retry_batch_limit":100,"oldest_unresolved_age_seconds":if pending==0{Some(0)}else{snapshot.oldest_pending_at.and_then(age)},"state":if runs.is_empty(){"uninitialized"}else{"partial"},"mint":runs.first().and_then(|r|r.get("mint")),"decoder_version":decoder::DECODER_VERSION,"verified_boundary":null,"coverage":"unknown","unresolved_items":pending,"runs":runs,"limit":100,"reason":"filtered_finalized_stream_has_no_validated_completion_evidence"}),
        )
    }
}
fn age(observed: i64) -> Option<i64> {
    now().checked_sub(observed).filter(|n| *n >= 0)
}

/// Only this explicitly opted-in command opens a provider connection.
pub fn validate_options(from: u64, seconds: u64) -> Result<()> {
    if !(1..=300).contains(&seconds) || from > i64::MAX as u64 {
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
    crate::backfill::scope(mint, from, from)?;
    telemetry.set_context("live_ingestion", "laserstream", "live");
    validate_options(from, seconds)?;
    let api_key = std::env::var("HELIUS_API_KEY").map_err(|_| Error("missing_live_credentials"))?;
    // Fixed canonical mainnet endpoint prevents credential-bearing URLs entering diagnostics/config.
    let token: tonic::metadata::MetadataValue<_> = api_key
        .parse()
        .map_err(|_| Error("invalid_live_credentials"))?;
    let mut intake = {
        let _permit = gate.live()?;
        Intake::open(database)?
    };
    let run = {
        let _permit = gate.live()?;
        intake
            .log
            .start_run(from as i64, mint, decoder::DECODER_VERSION)?
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Error("runtime_failed"))?;
    let started = Instant::now();
    let mut count = 0usize;
    let mut failures = 0usize;
    let mut bytes = 0usize;
    let mut reconnects = 0usize;
    let mut raw_seconds = 0.0;
    let mut output_seconds = 0.0;
    let mut output_successes = 0usize;
    let mut inserted = 0usize;
    let result = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(seconds), async {
            for attempt in 0..=2 {
                reconnects = attempt;
                event(
                    telemetry,
                    "connection_attempt",
                    if attempt == 0 { "startup" } else { "retry" },
                );
                let channel =
                    Endpoint::from_static("https://laserstream-mainnet-ewr.helius-rpc.com")
                        .connect_timeout(Duration::from_secs(5))
                        .timeout(Duration::from_secs(10))
                        .initial_stream_window_size(65536)
                        .initial_connection_window_size(65536)
                        .http2_adaptive_window(false)
                        .buffer_size(1)
                        .tls_config(ClientTlsConfig::new().with_native_roots())
                        .map_err(|_| Error("tls_failed"))?
                        .connect()
                        .await
                        .map_err(|_| Error("provider_unavailable"))?;
                let auth = token.clone();
                let mut client = geyser_client::GeyserClient::with_interceptor(
                    channel,
                    move |mut req: Request<()>| {
                        req.metadata_mut().insert("x-token", auth.clone());
                        Ok(req)
                    },
                )
                .max_decoding_message_size(decoder::MAX_CAPTURE_BYTES)
                .max_encoding_message_size(65536);
                let replay = client
                    .subscribe_replay_info(SubscribeReplayInfoRequest {})
                    .await
                    .map_err(|_| Error("replay_info_unavailable"))?
                    .into_inner()
                    .first_available
                    .ok_or(Error("replay_info_unavailable"))?;
                let reference = client
                    .get_slot(GetSlotRequest {
                        commitment: Some(CommitmentLevel::Finalized as i32),
                    })
                    .await
                    .map_err(|_| Error("reference_unavailable"))?
                    .into_inner()
                    .slot;
                check_replay(from, replay)?;
                if reference > i64::MAX as u64 || replay > i64::MAX as u64 {
                    return Err(Error("unsupported_slot"));
                }
                intake
                    .log
                    .record_reference(run, reference as i64, replay as i64)?;
                let (sender, receiver) = tokio::sync::mpsc::channel(1);
                sender
                    .send(request_mint(from, mint))
                    .await
                    .map_err(|_| Error("subscription_failed"))?;
                let mut stream = client
                    .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
                    .await
                    .map_err(|_| Error("subscription_failed"))?
                    .into_inner();
                loop {
                    if started.elapsed() >= Duration::from_secs(seconds) {
                        return Ok("time_limit");
                    }
                    let update = match next_update(&mut stream, Duration::from_secs(10)).await {
                        Ok(Some(u)) => u,
                        Ok(None) => {
                            event(telemetry, "connection_ended", "provider_unavailable");
                            break;
                        }
                        Err(e) => {
                            event(telemetry, "connection_failed", e.0);
                            break;
                        }
                    };
                    if matches!(
                        update.update_oneof,
                        Some(subscribe_update::UpdateOneof::Ping(_))
                    ) {
                        sender
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await
                            .map_err(|_| Error("subscription_failed"))?;
                    }
                    if let Some(subscribe_update::UpdateOneof::Transaction(tx)) =
                        update.update_oneof
                    {
                        if tx.slot < from {
                            return Err(Error("unexpected_slot"));
                        }
                        telemetry.slot = Some(tx.slot);
                        bytes = bytes.saturating_add(tx.encoded_len());
                        let _permit = gate.live()?;
                        let operation = Instant::now();
                        let id = intake.retain(&tx)?;
                        raw_seconds += operation.elapsed().as_secs_f64();
                        count += 1;
                        let operation = Instant::now();
                        let processed = intake.process(id, &tx);
                        output_seconds += operation.elapsed().as_secs_f64();
                        match processed {
                            Ok(n) => {
                                output_successes += 1;
                                inserted += n;
                            }
                            Err(e) => {
                                event(telemetry, "decode_or_write_failed", e.0);
                                if e.0.starts_with("storage_") {
                                    return Err(e);
                                }
                                failures += 1;
                            }
                        }
                        if count >= MAX_MESSAGES {
                            return Ok("message_limit");
                        }
                    }
                }
            }
            Err(Error("provider_unavailable"))
        })
        .await
        .unwrap_or(Ok("time_limit"))
    });
    let status = result.as_ref().map_or_else(|e| e.0, |s| *s);
    {
        let _permit = gate.live()?;
        intake.log.finish_run(run, status)?;
    }
    event(telemetry, "shutdown", status);
    let elapsed = started.elapsed().as_secs_f64();
    let report = json!({"status":status,"received":count,"unresolved_attempts":failures,"input_protobuf_bytes":bytes,"elapsed_seconds":elapsed,"reconnect_attempts":reconnects,"inserted_transfers":inserted,"input_transactions_per_second":count as f64/elapsed,"decoded_write_successes":output_successes,"coverage_valid":false,"verified_boundary":null,"freshness_seconds":null,"storage":intake.store.stats(),"coverage":Intake::report(database)?});
    diagnostic(
        telemetry,
        &json!({"event":"live_metrics","mode":"live","source_kind":"laserstream","indexer_transactions_total":count,"indexer_decode_failures_total":failures,"indexer_input_bytes_total":bytes,"indexer_reconnects_total":reconnects,"indexer_coverage_valid":0,"indexer_live_freshness_seconds":null,"indexer_live_slot_gap":null,
            "stage_metrics":[{"stage":"raw_write","success":count,"duration_seconds_sum":raw_seconds},
                {"stage":"decode_write","success":output_successes,"unsupported":failures,"duration_seconds_sum":output_seconds},
                {"stage":"transfer_write","storage":intake.store.stats()}],
            "elapsed_seconds":elapsed,"input_transactions_per_second":count as f64/elapsed}),
    );
    result?;
    Ok(report)
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
        "startup" | "retry" | "time_limit" | "message_limit"
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
mod tests {
    use super::*;
    use crate::storage::pg::TestDatabase;
    use laserstream_core_proto::solana::storage::confirmed_block as wire;
    fn update() -> SubscribeUpdateTransaction {
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
        assert_eq!(decoder::decode(&capture).unwrap()[0].amount_raw, 42);
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
        assert_eq!(
            run(
                &Database::new("postgres://unused@127.0.0.1/x", "unused").unwrap(),
                1,
                0,
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
