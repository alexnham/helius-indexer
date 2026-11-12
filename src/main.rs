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

const USAGE: &str = "usage: helius-indexer ingest DB CAPTURE [--metrics FILE] | query DB [MINT] | backfill DB MINT START END [--metrics FILE] | coverage DB MINT START END | live DB FROM_SLOT SECONDS | live-status DB | live-retry DB | summary DB MINT | overlap DB START END SECONDS | repair DB START END | repair-status DB START END | repair-history DB MINT START END | [feature archive] archive/archive-replay/archive-overlap DB CACHE MINT START END | bridge-plan DB CACHE MINT START | bridge DB CACHE MINT START | bridge-status DB MINT START | bridge-live DB CACHE START SECONDS | archive-tip CACHE | archive-live DB CACHE START END SECONDS";

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
        #[cfg(feature = "archive")]
        [command, db, cache, mint, start] if command == "bridge-plan" => {
            helius_indexer::coordinator::bridge::plan(
                Path::new(db),
                Path::new(cache),
                mint,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, cache, mint, start] if command == "bridge" => {
            helius_indexer::coordinator::bridge::run(
                Path::new(db),
                Path::new(cache),
                mint,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
                None,
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, mint, start] if command == "bridge-status" => {
            helius_indexer::coordinator::bridge::status(
                Path::new(db),
                mint,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, cache, start, seconds] if command == "bridge-live" => {
            helius_indexer::coordinator::bridge::run(
                Path::new(db),
                Path::new(cache),
                helius_indexer::live::USDC,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
                Some(seconds.parse().map_err(|_| Error("invalid_live_bounds"))?),
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, cache] if command == "archive-tip" => {
            helius_indexer::archive::tip(Path::new(cache), telemetry)
        }
        #[cfg(feature = "archive")]
        [command, db, cache, mint, start, end]
            if matches!(
                command.as_str(),
                "archive" | "archive-replay" | "archive-overlap"
            ) =>
        {
            let start = start.parse().map_err(|_| Error("invalid_archive_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_archive_range"))?;
            if command == "archive-overlap" {
                helius_indexer::archive::overlap(
                    Path::new(db),
                    Path::new(cache),
                    mint,
                    start,
                    end,
                    telemetry,
                )
            } else {
                helius_indexer::archive::run(
                    Path::new(db),
                    Path::new(cache),
                    mint,
                    start,
                    end,
                    command == "archive",
                    telemetry,
                )
            }
        }
        #[cfg(feature = "archive")]
        [command, db, cache, start, end, seconds] if command == "archive-live" => {
            helius_indexer::archive::live(
                Path::new(db),
                Path::new(cache),
                start.parse().map_err(|_| Error("invalid_archive_range"))?,
                end.parse().map_err(|_| Error("invalid_archive_range"))?,
                seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
                telemetry,
            )
        }
        [command, db, mint, start, end] if command == "repair-history" => {
            let start = start.parse().map_err(|_| Error("invalid_repair_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_repair_range"))?;
            let cancel = env::var_os("REPAIR_CANCEL_FILE");
            helius_indexer::repair::history(Path::new(db), telemetry, mint, start, end, || {
                cancel.as_ref().is_some_and(|p| Path::new(p).exists())
            })
        }
        [command, db, start, end] if command == "repair" || command == "repair-status" => {
            let start = start.parse().map_err(|_| Error("invalid_repair_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_repair_range"))?;
            if command == "repair-status" {
                helius_indexer::repair::status(Path::new(db), start, end)
            } else {
                let cancel = env::var_os("REPAIR_CANCEL_FILE");
                helius_indexer::repair::run(Path::new(db), start, end, telemetry, || {
                    cancel.as_ref().is_some_and(|p| Path::new(p).exists())
                })
            }
        }
        [command, db, mint] if command == "summary" => Store::summary(Path::new(db), mint),
        [command, db, start, end, seconds] if command == "overlap" => {
            helius_indexer::coordinator::run(
                Path::new(db),
                start
                    .parse()
                    .map_err(|_| Error("invalid_historical_range"))?,
                end.parse().map_err(|_| Error("invalid_historical_range"))?,
                seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
                telemetry,
            )
        }
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
        [command, db, from, seconds] if command == "live" => helius_indexer::live::run(
            Path::new(db),
            from.parse().map_err(|_| Error("invalid_live_bounds"))?,
            seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
            telemetry,
        ),
        [command, db] if command == "live-status" => {
            helius_indexer::live::Intake::report(Path::new(db))
        }
        [command, db] if command == "live-retry" => {
            helius_indexer::live::Intake::open(Path::new(db))?.retry()
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
    Ok(
        json!({"coverage":"unknown","limit":1000,"transfers":transfers,"live":helius_indexer::live::Intake::report(Path::new(db))?}),
    )
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
