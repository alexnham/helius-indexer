use helius_indexer::{
    Error, Result,
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

const USAGE: &str = "usage: helius-indexer ingest DB CAPTURE [--metrics FILE] | query DB [MINT]";

fn ingest(db: &str, input: &str, telemetry: &mut Telemetry) -> Result<serde_json::Value> {
    let started = Instant::now();
    let parsed = (|| {
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
    );
    let started = Instant::now();
    let stored =
        Store::open(Path::new(db)).and_then(|mut store| store.persist(&capture, &raw, &decoded));
    telemetry.observe(
        "storage",
        if stored.is_ok() { "success" } else { "failed" },
        started.elapsed().as_secs_f64(),
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
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            json!({"event":"metrics_export_failed","severity":"error"})
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
            let _ = writeln!(
                std::io::stderr().lock(),
                "{}",
                json!({"event":"command_failed","severity":"error","error_class":error.0})
            );
            ExitCode::FAILURE
        }
    }
}
