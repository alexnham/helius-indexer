use helius_indexer::{
    Error, Result,
    backfill::{History, RpcSource},
    decoder::{self, MAX_CAPTURE_BYTES},
    storage::Store,
    telemetry::Telemetry,
};
use serde_json::json;
use std::{
    env,
    fs::File,
    io::{Read, Write},
    path::Path,
    process::ExitCode,
    time::Instant,
};

const USAGE: &str = "usage: helius-indexer ingest DB CAPTURE [--metrics FILE] | query DB [MINT] | backfill DB MINT START END [--metrics FILE] | coverage DB MINT START END";

fn ingest(db: &str, input: &str, telemetry: &mut Telemetry) -> Result<serde_json::Value> {
    let started = Instant::now();
    let parsed: Result<_> = (|| {
        let file = File::open(input).map_err(|_| Error("capture_open_failed"))?;
        let mut raw = Vec::new();
        file.take(MAX_CAPTURE_BYTES as u64 + 1)
            .read_to_end(&mut raw)
            .map_err(|_| Error("capture_read_failed"))?;
        telemetry.input_bytes = raw.len();
        let capture = decoder::parse_capture(&raw)?;
        Ok((capture, raw))
    })();
    telemetry.observe(
        "input",
        if parsed.is_ok() {
            "success"
        } else {
            "rejected"
        },
        started.elapsed().as_secs_f64(),
        parsed.as_ref().err().map(|e| e.0),
    );
    let (capture, raw) = parsed?;
    telemetry.slot = Some(capture.result.slot);
    let started = Instant::now();
    let decoded = decoder::decode(&capture);
    telemetry.decode_failure = decoded.as_ref().err().map(|e| e.0);
    telemetry.transfers_decoded = decoded.as_ref().map_or(0, |t| t.len());
    telemetry.observe(
        "decode",
        if decoded.is_ok() { "success" } else { "failed" },
        started.elapsed().as_secs_f64(),
        telemetry.decode_failure,
    );
    let started = Instant::now();
    let stored =
        Store::open(Path::new(db)).and_then(|mut store| store.persist(&capture, &raw, &decoded));
    telemetry.observe(
        "storage",
        if stored.is_ok() { "success" } else { "failed" },
        started.elapsed().as_secs_f64(),
        stored.as_ref().err().map(|e| e.0),
    );
    let inserted = stored?;
    let transfers = decoded?;
    Ok(
        json!({"inserted":inserted,"decoded":transfers.len(),"coverage":"unknown",
        "transaction_failed":!capture.result.meta.err.is_null()}),
    )
}

fn run(args: &[String], telemetry: &mut Telemetry) -> Result<serde_json::Value> {
    match args {
        [command, db, mint, start, end] if command == "coverage" => {
            let start = start
                .parse()
                .map_err(|_| Error("invalid_historical_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_historical_range"))?;
            Ok(
                serde_json::to_value(History::open(Path::new(db))?.coverage(mint, start, end)?)
                    .map_err(|_| Error("serialization_failed"))?,
            )
        }
        [command, db, mint, start, end, rest @ ..] if command == "backfill" => {
            let metrics_file = match rest {
                [] => None,
                [flag, file] if flag == "--metrics" => Some(file),
                _ => return Err(Error(USAGE)),
            };
            let start = start
                .parse()
                .map_err(|_| Error("invalid_historical_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_historical_range"))?;
            telemetry.set_context("backfill", "rpc", "historical");
            let mut history = History::open(Path::new(db))?;
            telemetry.run_id = history.run_id().to_owned();
            history.coverage(mint, start, end)?;
            let endpoint = env::var("SOLANA_RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
            let cancel = env::var_os("BACKFILL_CANCEL_FILE");
            let mut source = RpcSource::new(endpoint)?;
            let (coverage, metrics) = history.run(mint, start, end, &mut source, || {
                cancel.as_ref().is_some_and(|p| Path::new(p).exists())
            })?;
            if let Some(file) = metrics_file
                && std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(file)
                    .and_then(|mut f| f.write_all(metrics.prometheus(&coverage).as_bytes()))
                    .is_err()
            {
                telemetry.emit(
                    "metrics_export_failed",
                    "metrics_export",
                    "failed",
                    Some("metrics_export_failed"),
                );
            }
            Ok(json!({"coverage":coverage,"metrics":metrics}))
        }
        [command, db, input] if command == "ingest" => ingest(db, input, telemetry),
        [command, db, input, flag, _] if command == "ingest" && flag == "--metrics" => {
            ingest(db, input, telemetry)
        }
        [command, db] if command == "query" => query(db, None),
        [command, db, mint] if command == "query" => query(db, Some(mint)),
        _ => Err(Error(USAGE)),
    }
}

fn query(db: &str, mint: Option<&str>) -> Result<serde_json::Value> {
    let transfers = Store::query(Path::new(db), mint, 1000)?;
    Ok(json!({"coverage":"unknown","limit":1000,"transfers":transfers}))
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut telemetry = Telemetry::default();
    let result = run(&args, &mut telemetry);
    if let [command, _, _, flag, file] = args.as_slice()
        && command == "ingest"
        && flag == "--metrics"
        && std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(file)
            .and_then(|mut file| file.write_all(telemetry.prometheus().as_bytes()))
            .is_err()
    {
        // Diagnostics failure is reported, but cannot undo successful ingestion.
        telemetry.emit(
            "metrics_export_failed",
            "metrics_export",
            "failed",
            Some("metrics_export_failed"),
        );
    }
    match result {
        Ok(value) => {
            if writeln!(std::io::stdout().lock(), "{value}").is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            telemetry.emit("command_failed", "command", "failed", Some(error.0));
            ExitCode::FAILURE
        }
    }
}
