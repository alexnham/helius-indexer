//! Bounded LaserStream intake. Filtered transaction delivery never proves slot coverage.
use crate::{Error, Result, decoder, storage::Store};
use futures_util::StreamExt;
use laserstream_core_proto::geyser::*;
use prost::Message;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::{
    Request,
    transport::{ClientTlsConfig, Endpoint},
};

pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MAX_MESSAGES: usize = 10_000;
const PROTO_SCHEMA: &str = "laserstream-core-proto/14.0.0";

pub fn request(from: u64) -> SubscribeRequest {
    SubscribeRequest {
        commitment: Some(CommitmentLevel::Finalized as i32),
        from_slot: Some(from),
        transactions: [(
            "usdc".into(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![USDC.into()],
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
    db: Connection,
    store: Store,
}
impl Intake {
    pub fn open(path: &Path) -> Result<Self> {
        let store = Store::open(path)?;
        let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error("storage_configuration_failed"))?;
        db.execute_batch("PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS live_raw(id INTEGER PRIMARY KEY, slot INTEGER NOT NULL, protobuf BLOB NOT NULL, proto_schema TEXT NOT NULL DEFAULT 'laserstream-core-proto/14.0.0', status TEXT NOT NULL, received_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS live_runs(id INTEGER PRIMARY KEY, from_slot INTEGER NOT NULL, mint TEXT NOT NULL, decoder TEXT NOT NULL, status TEXT NOT NULL, reference_slot INTEGER, reference_at INTEGER, replay_first INTEGER, replay_at INTEGER);")
            .map_err(|_|Error("storage_configuration_failed"))?;
        Ok(Self { db, store })
    }
    /// The wire payload commits before normalization/decoding; a crash leaves retryable pending input.
    pub fn retain(&self, update: &SubscribeUpdateTransaction) -> Result<i64> {
        if update.encoded_len() > decoder::MAX_CAPTURE_BYTES || update.slot > i64::MAX as u64 {
            return Err(Error("live_input_limit"));
        }
        self.db
            .execute(
                "INSERT INTO live_raw(slot,protobuf,status,received_at) VALUES(?1,?2,'pending',?3)",
                params![update.slot as i64, update.encode_to_vec(), now()],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        Ok(self.db.last_insert_rowid())
    }
    pub fn process(&mut self, id: i64, update: &SubscribeUpdateTransaction) -> Result<usize> {
        let result = (|| {
            let raw = normalize(update)?;
            let capture = decoder::parse_capture(&raw)?;
            let decoded = decoder::decode(&capture).map(|records| {
                records
                    .into_iter()
                    .filter(|t| t.mint == USDC)
                    .collect::<Vec<_>>()
            });
            let count = self.store.persist(&capture, &raw, &decoded)?;
            decoded?;
            Ok(count)
        })();
        let status = result.as_ref().map_or_else(|e: &Error| e.0, |_| "decoded");
        self.db
            .execute(
                "UPDATE live_raw SET status=?1 WHERE id=?2",
                params![status, id],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        result
    }
    pub fn retry(&mut self) -> Result<Value> {
        let pending: Vec<i64> = {
            let mut s = self
                .db
                .prepare("SELECT id FROM live_raw WHERE status!='decoded' ORDER BY id LIMIT 100")
                .map_err(|_| Error("storage_read_failed"))?;
            s.query_map([], |r| r.get(0))
                .map_err(|_| Error("storage_read_failed"))?
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| Error("storage_read_failed"))?
        };
        let mut success = 0;
        for id in &pending {
            let (raw, schema): (Vec<u8>, String) = self
                .db
                .query_row(
                    "SELECT protobuf,proto_schema FROM live_raw WHERE id=?1 AND length(protobuf)<=16777216",
                    [id],
                    |r| Ok((r.get(0)?,r.get(1)?)),
                )
                .map_err(|_| Error("storage_read_failed"))?;
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
        Ok(json!({"attempted":pending.len(),"decoded":success,"coverage":"unknown"}))
    }
    pub fn report(path: &Path) -> Result<Value> {
        if !path.exists() {
            return Err(Error("storage_open_failed"));
        }
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let exists: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='live_runs')",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        if !exists {
            return Ok(json!({"state":"uninitialized","verified_boundary":null}));
        }
        let mut stmt=db.prepare("SELECT id,from_slot,status,reference_slot,reference_at,replay_first,replay_at FROM live_runs ORDER BY id DESC LIMIT 100").map_err(|_|Error("storage_read_failed"))?;
        let rows=stmt.query_map([],|r| {
            let from: i64=r.get(1)?;
            let reference: Option<i64>=r.get(3)?;
            let reference_at: Option<i64>=r.get(4)?;
            let first: Option<i64>=r.get(5)?;
            let replay_at: Option<i64>=r.get(6)?;
            Ok(json!({"run":r.get::<_,i64>(0)?,"from_slot":from,"state":r.get::<_,String>(2)?,
                "reference_slot":reference,"reference_at":reference_at,
                "reference_age_seconds":reference_at.and_then(age),
                "replay_first":first,"replay_at":replay_at,"replay_info_age_seconds":replay_at.and_then(age),
                "replay_margin_slots":first.map(|n|from-n),
                "clock_skew":reference_at.is_some_and(|n|n>now())||replay_at.is_some_and(|n|n>now()),
                "unresolved_interval":{"from_slot":from,"through_slot":reference.filter(|n|*n>=from),"kind":"unverified"},
                "verified_boundary":null,"coverage":"unknown"}))
        }).map_err(|_|Error("storage_read_failed"))?;
        let runs = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error("storage_read_failed"))?;
        let pending: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM live_raw WHERE status!='decoded'",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        Ok(
            json!({"state":if runs.is_empty(){"uninitialized"}else{"partial"},"mint":USDC,"decoder_version":decoder::DECODER_VERSION,"verified_boundary":null,"coverage":"unknown","unresolved_items":pending,"runs":runs,"limit":100,"reason":"filtered_finalized_stream_has_no_validated_completion_evidence"}),
        )
    }
}
fn age(observed: i64) -> Option<i64> {
    now().checked_sub(observed).filter(|n| *n >= 0)
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

/// Only this explicitly opted-in command opens a provider connection.
pub fn run(
    path: &Path,
    from: u64,
    seconds: u64,
    telemetry: &mut crate::telemetry::Telemetry,
) -> Result<Value> {
    telemetry.set_context("live_ingestion", "laserstream", "live");
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
    // Fixed canonical mainnet endpoint prevents credential-bearing URLs entering diagnostics/config.
    let token: tonic::metadata::MetadataValue<_> = api_key
        .parse()
        .map_err(|_| Error("invalid_live_credentials"))?;
    let mut intake = Intake::open(path)?;
    intake
        .db
        .execute(
            "INSERT INTO live_runs(from_slot,mint,decoder,status) VALUES(?1,?2,?3,'running')",
            params![from as i64, USDC, decoder::DECODER_VERSION],
        )
        .map_err(|_| Error("storage_write_failed"))?;
    let run = intake.db.last_insert_rowid();
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
    let result=runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(seconds),async {
            for attempt in 0..=2 {
                reconnects=attempt;
                event(telemetry,"connection_attempt",if attempt==0{"startup"}else{"retry"});
                let channel=Endpoint::from_static("https://laserstream-mainnet-ewr.helius-rpc.com")
                    .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(10))
                    .initial_stream_window_size(65536).initial_connection_window_size(65536)
                    .http2_adaptive_window(false).buffer_size(1)
                    .tls_config(ClientTlsConfig::new().with_native_roots()).map_err(|_|Error("tls_failed"))?
                    .connect().await.map_err(|_|Error("provider_unavailable"))?;
                let auth=token.clone();
                let mut client=geyser_client::GeyserClient::with_interceptor(channel,move |mut req: Request<()>| {
                    req.metadata_mut().insert("x-token",auth.clone()); Ok(req)
                }).max_decoding_message_size(decoder::MAX_CAPTURE_BYTES).max_encoding_message_size(65536);
                let replay=client.subscribe_replay_info(SubscribeReplayInfoRequest{}).await.map_err(|_|Error("replay_info_unavailable"))?.into_inner().first_available.ok_or(Error("replay_info_unavailable"))?;
                let reference=client.get_slot(GetSlotRequest{commitment:Some(CommitmentLevel::Finalized as i32)}).await.map_err(|_|Error("reference_unavailable"))?.into_inner().slot;
                check_replay(from,replay)?;
                if reference>i64::MAX as u64 || replay>i64::MAX as u64 {return Err(Error("unsupported_slot"));}
                intake.db.execute("UPDATE live_runs SET reference_slot=?1,reference_at=?2,replay_first=?3,replay_at=?2 WHERE id=?4",params![reference as i64,now(),replay as i64,run]).map_err(|_|Error("storage_write_failed"))?;
                let (sender,receiver)=tokio::sync::mpsc::channel(1);
                sender.send(request(from)).await.map_err(|_|Error("subscription_failed"))?;
                let mut stream=client.subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver)).await.map_err(|_|Error("subscription_failed"))?.into_inner();
                loop {
                    if started.elapsed()>=Duration::from_secs(seconds) {return Ok("time_limit");}
                    let update=match next_update(&mut stream,Duration::from_secs(10)).await {
                        Ok(Some(u))=>u,
                        Ok(None)=>{event(telemetry,"connection_ended","provider_unavailable");break;},
                        Err(e)=>{event(telemetry,"connection_failed",e.0);break;}
                    };
                    if matches!(update.update_oneof,Some(subscribe_update::UpdateOneof::Ping(_))) {
                        sender.send(SubscribeRequest{ping:Some(SubscribeRequestPing{id:1}),..Default::default()}).await.map_err(|_|Error("subscription_failed"))?;
                    }
                    if let Some(subscribe_update::UpdateOneof::Transaction(tx))=update.update_oneof {
                        if tx.slot<from {return Err(Error("unexpected_slot"));}
                        telemetry.slot=Some(tx.slot);
                        bytes=bytes.saturating_add(tx.encoded_len());
                        let operation=Instant::now();
                        let id=intake.retain(&tx)?;
                        raw_seconds+=operation.elapsed().as_secs_f64();
                        count+=1;
                        let operation=Instant::now();
                        let processed=intake.process(id,&tx);
                        output_seconds+=operation.elapsed().as_secs_f64();
                        match processed {
                            Ok(n)=>{output_successes+=1;inserted+=n;},
                            Err(e)=>{
                                event(telemetry,"decode_or_write_failed",e.0);
                                if e.0.starts_with("storage_"){return Err(e);}
                                failures+=1;
                            }
                        }
                        if count>=MAX_MESSAGES{return Ok("message_limit");}
                    }
                }
            }
            Err(Error("provider_unavailable"))
        }).await.unwrap_or(Ok("time_limit"))
    });
    let status = result.as_ref().map_or_else(|e| e.0, |s| *s);
    intake
        .db
        .execute(
            "UPDATE live_runs SET status=?1 WHERE id=?2",
            params![status, run],
        )
        .map_err(|_| Error("storage_write_failed"))?;
    event(telemetry, "shutdown", status);
    let elapsed = started.elapsed().as_secs_f64();
    let report = json!({"status":status,"received":count,"unresolved_attempts":failures,"input_protobuf_bytes":bytes,"elapsed_seconds":elapsed,"reconnect_attempts":reconnects,"inserted_transfers":inserted,"input_transactions_per_second":count as f64/elapsed,"decoded_write_successes":output_successes,"coverage_valid":false,"verified_boundary":null,"freshness_seconds":null,"coverage":Intake::report(path)?});
    diagnostic(
        telemetry,
        &json!({"event":"live_metrics","mode":"live","source_kind":"laserstream","indexer_transactions_total":count,"indexer_decode_failures_total":failures,"indexer_input_bytes_total":bytes,"indexer_reconnects_total":reconnects,"indexer_coverage_valid":0,"indexer_live_freshness_seconds":null,"indexer_live_slot_gap":null,
            "stage_metrics":[{"stage":"raw_write","success":count,"duration_seconds_sum":raw_seconds},
                {"stage":"decode_write","success":output_successes,"unsupported":failures,"duration_seconds_sum":output_seconds}],
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.sqlite");
        let tx = update();
        let intake = Intake::open(&path).unwrap();
        let id = intake.retain(&tx).unwrap();
        drop(intake);
        let mut intake = Intake::open(&path).unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Store::query(&path, Some(USDC), 100).unwrap().len(), 1);
        assert_eq!(intake.process(id, &tx).unwrap(), 0);
        let report = Intake::report(&path).unwrap();
        assert!(report["verified_boundary"].is_null());
        assert_eq!(report["unresolved_items"], 0);
        let mut broken = update();
        broken.transaction.as_mut().unwrap().meta = None;
        let id = intake.retain(&broken).unwrap();
        assert!(intake.process(id, &broken).is_err());
        assert_eq!(Intake::report(&path).unwrap()["unresolved_items"], 1);
        let wire: Vec<u8> = intake
            .db
            .query_row(
                "SELECT protobuf FROM live_raw WHERE id=?1 AND length(protobuf)<=16777216",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            SubscribeUpdateTransaction::decode(wire.as_slice()).unwrap(),
            broken
        );
        intake
            .db
            .execute(
                "UPDATE live_raw SET proto_schema='future-schema' WHERE id=?1",
                [id],
            )
            .unwrap();
        assert_eq!(
            intake.retry().unwrap_err().0,
            "unsupported_stored_proto_schema"
        );
        assert_eq!(Store::query(&path, Some(USDC), 100).unwrap().len(), 1);
    }
    #[test]
    fn failed_output_write_preserves_retryable_raw_without_transfers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.sqlite");
        let mut intake = Intake::open(&path).unwrap();
        intake.db.execute_batch("CREATE TRIGGER fail_transfers BEFORE INSERT ON transfers BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        let tx = update();
        let id = intake.retain(&tx).unwrap();
        assert!(intake.process(id, &tx).is_err());
        assert!(Store::query(&path, None, 100).unwrap().is_empty());
        assert_eq!(Intake::report(&path).unwrap()["unresolved_items"], 1);
        intake
            .db
            .execute_batch("DROP TRIGGER fail_transfers;")
            .unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Store::query(&path, None, 100).unwrap().len(), 1);
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
                Path::new("unused"),
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
                Path::new("unused"),
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
