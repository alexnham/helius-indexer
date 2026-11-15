use helius_indexer::{
    Error, Result,
    backfill::{History, RpcSource},
    decoder::{self, MAX_CAPTURE_BYTES},
    storage::{
        Database, Store,
        window::{self, Window},
    },
    telemetry::Telemetry,
};
use serde_json::json;
use std::{
    env,
    fs::File,
    io::{Read, Write},
    path::Path,
    process::ExitCode,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const USAGE: &str = "usage: DATABASE_URL=postgres://... helius-indexer COMMAND, where SESSION is a schema name (^[a-z][a-z0-9_]*$), not a file path: replay-create SESSION JOB MINT START END | replay-status/replay-retry/replay-grant SESSION JOB | replay-route SESSION JOB CACHE | archive-grant SESSION JOB | replay SESSION JOB SECONDS | replay-monitor SESSION JOB LIVE_FROM SECONDS | health SESSION | migrate SESSION | consume SESSION SECONDS (uses INDEXER_KAFKA_BROKERS) | comparison-input SESSION MINT [--sample] | monitor SESSION MINT LIVE_FROM START END SECONDS | query-recent SESSION [MINT] [WINDOW] [--limit N] [--cursor C] [--all-stored] | ingest SESSION CAPTURE [--metrics FILE] | query SESSION [MINT] [WINDOW] [--limit N] [--cursor C] [--all-stored] | backfill SESSION MINT START END [--metrics FILE] | coverage SESSION MINT START END | live SESSION FROM_SLOT SECONDS [MINT] | prune SESSION KEEP | live-status SESSION | live-retry SESSION | summary SESSION MINT [WINDOW] [--all-stored] | overlap SESSION START END SECONDS | repair SESSION START END | repair-status SESSION START END | repair-history-archive SESSION MINT START END | repair-history SESSION MINT START END | [feature archive] replay-archive SESSION JOB CACHE | archive/archive-replay/archive-overlap SESSION CACHE MINT START END | bridge-plan SESSION CACHE MINT START | bridge SESSION CACHE MINT START | bridge-status SESSION MINT START | bridge-live SESSION CACHE START SECONDS | archive-overlap-replay SESSION MINT START END | archive-tip CACHE | archive-live SESSION CACHE START END SECONDS";

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
    let stored = Store::open(&Database::from_env(db)?)
        .and_then(|mut store| store.persist(&capture, &raw, &decoded));
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
        [command, session] if command == "health" => Database::from_env(session)?.health(),
        [command, session, id, mint, start, end] if command == "replay-create" => {
            helius_indexer::replay::create(
                &Database::from_env(session)?,
                id,
                mint,
                start.parse().map_err(|_| Error("invalid_replay_bounds"))?,
                end.parse().map_err(|_| Error("invalid_replay_bounds"))?,
            )
        }
        [command, session, id] if command == "replay-status" => {
            helius_indexer::replay::status(&Database::from_env(session)?, id)
        }
        [command, session, id, cache] if command == "replay-route" => {
            helius_indexer::replay::route::measure(
                &Database::from_env(session)?,
                id,
                Path::new(cache),
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, session, id, cache] if command == "replay-archive" => {
            helius_indexer::replay::archive::run(
                &Database::from_env(session)?,
                id,
                Path::new(cache),
                telemetry,
            )
        }
        [command, session, id] if command == "archive-grant" => {
            helius_indexer::replay::archive::grant(&Database::from_env(session)?, id, telemetry)
        }
        [command, session, id] if command == "replay-grant" => {
            helius_indexer::replay::grant(&Database::from_env(session)?, id, telemetry)
        }
        [command, session, id] if command == "replay-retry" => {
            helius_indexer::replay::retry(&Database::from_env(session)?, id, telemetry)
        }
        [command, session, id, from, seconds] if command == "replay-monitor" => {
            helius_indexer::replay::monitor(
                &Database::from_env(session)?,
                id,
                from.parse().map_err(|_| Error("invalid_replay_bounds"))?,
                seconds
                    .parse()
                    .map_err(|_| Error("invalid_replay_bounds"))?,
                telemetry,
            )
        }
        [command, session, id, seconds] if command == "replay" => helius_indexer::replay::run(
            &Database::from_env(session)?,
            id,
            seconds
                .parse()
                .map_err(|_| Error("invalid_replay_bounds"))?,
            telemetry,
        ),
        #[cfg(feature = "archive")]
        [command, db, mint, start, end] if command == "archive-overlap-replay" => {
            helius_indexer::archive::replay_overlap(
                &Database::from_env(db)?,
                mint,
                start.parse().map_err(|_| Error("invalid_archive_range"))?,
                end.parse().map_err(|_| Error("invalid_archive_range"))?,
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, cache, mint, start] if command == "bridge-plan" => {
            helius_indexer::coordinator::bridge::plan(
                &Database::from_env(db)?,
                Path::new(cache),
                mint,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
                telemetry,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, cache, mint, start] if command == "bridge" => {
            helius_indexer::coordinator::bridge::run(
                &Database::from_env(db)?,
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
                &Database::from_env(db)?,
                mint,
                start.parse().map_err(|_| Error("invalid_bridge_plan"))?,
            )
        }
        #[cfg(feature = "archive")]
        [command, db, cache, start, seconds] if command == "bridge-live" => {
            helius_indexer::coordinator::bridge::run(
                &Database::from_env(db)?,
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
                    &Database::from_env(db)?,
                    Path::new(cache),
                    mint,
                    start,
                    end,
                    telemetry,
                )
            } else {
                helius_indexer::archive::run(
                    &Database::from_env(db)?,
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
                &Database::from_env(db)?,
                Path::new(cache),
                start.parse().map_err(|_| Error("invalid_archive_range"))?,
                end.parse().map_err(|_| Error("invalid_archive_range"))?,
                seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
                telemetry,
            )
        }
        [command, db, mint, start, end]
            if command == "repair-history" || command == "repair-history-archive" =>
        {
            let start = start.parse().map_err(|_| Error("invalid_repair_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_repair_range"))?;
            let cancel = env::var_os("REPAIR_CANCEL_FILE");
            if command == "repair-history-archive" {
                helius_indexer::repair::archive_history(
                    &Database::from_env(db)?,
                    telemetry,
                    mint,
                    start,
                    end,
                    || cancel.as_ref().is_some_and(|p| Path::new(p).exists()),
                )
            } else {
                helius_indexer::repair::history(
                    &Database::from_env(db)?,
                    telemetry,
                    mint,
                    start,
                    end,
                    || cancel.as_ref().is_some_and(|p| Path::new(p).exists()),
                )
            }
        }
        [command, db, start, end] if command == "repair" || command == "repair-status" => {
            let start = start.parse().map_err(|_| Error("invalid_repair_range"))?;
            let end = end.parse().map_err(|_| Error("invalid_repair_range"))?;
            if command == "repair-status" {
                helius_indexer::repair::status(&Database::from_env(db)?, start, end)
            } else {
                let cancel = env::var_os("REPAIR_CANCEL_FILE");
                helius_indexer::repair::run(&Database::from_env(db)?, start, end, telemetry, || {
                    cancel.as_ref().is_some_and(|p| Path::new(p).exists())
                })
            }
        }
        [command, db, mint, from, start, end, seconds] if command == "monitor" => {
            helius_indexer::coordinator::monitor(
                &Database::from_env(db)?,
                helius_indexer::coordinator::MonitorConfig {
                    mint,
                    live_from: from.parse().map_err(|_| Error("invalid_live_bounds"))?,
                    start: start
                        .parse()
                        .map_err(|_| Error("invalid_historical_range"))?,
                    end: end.parse().map_err(|_| Error("invalid_historical_range"))?,
                    seconds: seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
                },
                telemetry,
            )
        }
        [command, db] if command == "migrate" => Store::migrate(&Database::from_env(db)?),
        [command, db, seconds] if command == "consume" => {
            let seconds: u64 = seconds.parse().map_err(|_| Error("invalid_live_bounds"))?;
            if !(1..=300).contains(&seconds) {
                return Err(Error("invalid_live_bounds"));
            }
            helius_indexer::kafka::consume(
                &Database::from_env(db)?,
                &helius_indexer::kafka::brokers()?,
                helius_indexer::kafka::TOPIC,
                seconds,
            )
        }
        [command, db, mint] if command == "comparison-input" => {
            Store::comparison_input(&Database::from_env(db)?, mint, 33)
        }
        [command, db, mint, flag] if command == "comparison-input" && flag == "--sample" => {
            Store::comparison_input(&Database::from_env(db)?, mint, 16)
        }
        [command, rest @ ..]
            if matches!(command.as_str(), "query" | "query-recent" | "summary") =>
        {
            windowed(command, rest)
        }
        [command, db, start, end, seconds] if command == "overlap" => {
            helius_indexer::coordinator::run(
                &Database::from_env(db)?,
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
            Ok(serde_json::to_value(
                History::open(&Database::from_env(db)?)?.coverage(mint, start, end)?,
            )
            .map_err(|_| Error("serialization_failed"))?)
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
            let mut history = History::open(&Database::from_env(db)?)?;
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
            &Database::from_env(db)?,
            from.parse().map_err(|_| Error("invalid_live_bounds"))?,
            seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
            telemetry,
        ),
        [command, db, from, seconds, mint] if command == "live" => helius_indexer::live::run_mint(
            &Database::from_env(db)?,
            from.parse().map_err(|_| Error("invalid_live_bounds"))?,
            seconds.parse().map_err(|_| Error("invalid_live_bounds"))?,
            mint,
            telemetry,
        ),
        [command, db, keep] if command == "prune" => {
            telemetry.set_context("storage", "local", "retention");
            let report = Store::prune(
                &Database::from_env(db)?,
                keep.parse().map_err(|_| Error("invalid_retention_limit"))?,
            )?;
            telemetry.emit("retention_pruned", "prune", "success", None);
            Ok(report)
        }
        [command, db] if command == "live-status" => {
            helius_indexer::live::Intake::report(&Database::from_env(db)?)
        }
        [command, db] if command == "live-retry" => {
            helius_indexer::live::Intake::open(&Database::from_env(db)?)?.retry()
        }
        [command, db, input] if command == "ingest" => ingest(db, input, telemetry),
        [command, db, input, flag, _] if command == "ingest" && flag == "--metrics" => {
            ingest(db, input, telemetry)
        }
        _ => Err(Error(USAGE)),
    }
}

/// `query`, `query-recent` and `summary` take positionals, then `--window-seconds N` or
/// `--from T --to T` (block times, `[from, to)`), plus `--limit N` and `--cursor C` for listings.
/// `--all-stored` is local inspection of every retained row (pre-window output shape); it
/// excludes the window flags and is not a served API mode.
fn windowed(command: &str, args: &[String]) -> Result<serde_json::Value> {
    let mut positional = Vec::new();
    let (mut seconds, mut from, mut to, mut limit, mut cursor) = (None, None, None, None, None);
    let mut all_stored = false;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let Some(flag) = arg.strip_prefix("--") else {
            positional.push(arg.as_str());
            continue;
        };
        if flag == "all-stored" {
            all_stored = true;
            continue;
        }
        let value = rest.next().ok_or(Error(USAGE))?;
        match flag {
            "window-seconds" => seconds = Some(value.parse().map_err(|_| Error("invalid_window"))?),
            "from" => from = Some(value.parse().map_err(|_| Error("invalid_window"))?),
            "to" => to = Some(value.parse().map_err(|_| Error("invalid_window"))?),
            "limit" => limit = Some(value.parse().map_err(|_| Error("invalid_query_limit"))?),
            "cursor" => cursor = Some(value.as_str()),
            _ => return Err(Error(USAGE)),
        }
    }
    if all_stored && (seconds.is_some() || from.is_some() || to.is_some() || cursor.is_some()) {
        return Err(Error("all_stored_excludes_window_and_cursor"));
    }
    let explicit = match (from, to) {
        (Some(from), Some(to)) => Some((from, to)),
        (None, None) => None,
        _ => return Err(Error("invalid_window")),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .ok_or(Error("clock_failed"))?;
    let window = Window::resolve(now, seconds, explicit)?;
    let [session, mint @ ..] = positional.as_slice() else {
        return Err(Error(USAGE));
    };
    let database = Database::from_env(session)?;
    if all_stored {
        return stored(command, &database, mint, limit.unwrap_or(1000));
    }
    if command == "summary" {
        let [mint] = mint else {
            return Err(Error(USAGE));
        };
        return window::window_summary(&database, mint, window);
    }
    if mint.len() > 1 {
        return Err(Error(USAGE));
    }
    let mut out = window::listing(
        &database,
        mint.first().copied(),
        window,
        command == "query-recent",
        limit.unwrap_or(1000),
        cursor,
    )?;
    out["live"] = helius_indexer::live::Intake::report(&database)?;
    Ok(out)
}

/// Pre-window output over every stored row, including unknown block time: no clock, cutoff,
/// window, pagination or coverage object. `summary` is `Store::summary`'s all-stored aggregate.
fn stored(
    command: &str,
    database: &Database,
    mint: &[&str],
    limit: usize,
) -> Result<serde_json::Value> {
    if command == "summary" {
        let [mint] = mint else {
            return Err(Error(USAGE));
        };
        return Store::summary(database, mint);
    }
    if mint.len() > 1 {
        return Err(Error(USAGE));
    }
    let mint = mint.first().copied();
    let recent = command == "query-recent";
    let transfers = if recent {
        Store::query_recent(database, mint, limit)?
    } else {
        Store::query(database, mint, limit)?
    };
    let mut out = json!({"coverage":"unknown","limit":limit,"transfers":transfers,
        "live":helius_indexer::live::Intake::report(database)?});
    if recent {
        out["order"] = json!("newest_first");
    }
    Ok(out)
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
