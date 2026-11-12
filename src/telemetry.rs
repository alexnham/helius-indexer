use serde_json::json;
use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

/// One bounded offline invocation; metrics are attempts, not distinct stored transfers.
pub struct Telemetry {
    component: &'static str,
    source_kind: &'static str,
    mode: &'static str,
    stages: Vec<(&'static str, &'static str, f64)>,
    pub run_id: String,
    pub slot: Option<u64>,
    pub input_bytes: usize,
    pub transfers_decoded: usize,
    pub decode_failure: Option<&'static str>,
}

impl Default for Telemetry {
    fn default() -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            component: "offline_indexer",
            source_kind: "capture",
            mode: "offline",
            stages: Vec::new(),
            run_id: format!("{}-{time}", std::process::id()),
            slot: None,
            input_bytes: 0,
            transfers_decoded: 0,
            decode_failure: None,
        }
    }
}

impl Telemetry {
    pub fn set_context(
        &mut self,
        component: &'static str,
        source_kind: &'static str,
        mode: &'static str,
    ) {
        self.component = component;
        self.source_kind = source_kind;
        self.mode = mode;
    }
    pub fn observe(
        &mut self,
        stage: &'static str,
        outcome: &'static str,
        seconds: f64,
        error_class: Option<&'static str>,
    ) {
        self.stages.push((stage, outcome, seconds));
        self.emit("stage_completed", stage, outcome, error_class);
    }

    pub fn event(
        &self,
        event: &'static str,
        operation: &'static str,
        outcome: &'static str,
        error_class: Option<&'static str>,
    ) -> serde_json::Value {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        json!({"timestamp_unix_ms":timestamp,"severity":if matches!(outcome,"success"|"startup"|"retry"|"time_limit"|"message_limit") {"info"} else {"error"},
            "component":self.component,"operation":operation,"outcome":outcome,"error_class":error_class,
            "run_id":self.run_id,"slot":self.slot,"decoder_version":crate::decoder::DECODER_VERSION,
            "source_kind":self.source_kind,"mode":self.mode,"event":event})
    }

    pub fn emit(
        &self,
        event: &'static str,
        operation: &'static str,
        outcome: &'static str,
        error_class: Option<&'static str>,
    ) {
        // A broken diagnostics pipe must not interrupt a durable transaction write.
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            self.event(event, operation, outcome, error_class)
        );
    }

    pub fn prometheus(&self) -> String {
        let mut output = format!(
            "# TYPE indexer_input_bytes_total counter\nindexer_input_bytes_total{{source_kind=\"capture\"}} {}\n# TYPE indexer_transfers_decoded_total counter\nindexer_transfers_decoded_total {}\n# TYPE indexer_transactions_total counter\n# TYPE indexer_stage_duration_seconds histogram\n",
            self.input_bytes, self.transfers_decoded
        );
        output.push_str(&format!("# TYPE indexer_decode_failures_total counter\nindexer_decode_failures_total{{failure_class=\"invalid_or_unsupported\"}} {}\n",usize::from(self.decode_failure.is_some())));
        for (stage, outcome, elapsed) in &self.stages {
            output.push_str(&format!(
                "indexer_transactions_total{{stage=\"{stage}\",outcome=\"{outcome}\"}} 1\n"
            ));
            for bound in [0.001, 0.01, 0.1, 1.0, 10.0] {
                let count = usize::from(*elapsed <= bound);
                output.push_str(&format!("indexer_stage_duration_seconds_bucket{{operation=\"{stage}\",le=\"{bound}\"}} {count}\n"));
            }
            output.push_str(&format!("indexer_stage_duration_seconds_bucket{{operation=\"{stage}\",le=\"+Inf\"}} 1\nindexer_stage_duration_seconds_sum{{operation=\"{stage}\"}} {elapsed}\nindexer_stage_duration_seconds_count{{operation=\"{stage}\"}} 1\n"));
        }
        output
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;
    #[test]
    fn pipeline_events_keep_correlation_and_bounded_error_context() {
        let mut telemetry = Telemetry::default();
        telemetry.set_context("live_ingestion", "laserstream", "live");
        telemetry.slot = Some(123);
        let stage = telemetry.event(
            "stage_completed",
            "decode",
            "failed",
            Some("missing_execution_metadata"),
        );
        let terminal = telemetry.event(
            "command_failed",
            "command",
            "failed",
            Some("missing_execution_metadata"),
        );
        assert_eq!(stage["run_id"], terminal["run_id"]);
        for event in [stage, terminal] {
            assert_eq!(event["component"], "live_ingestion");
            assert_eq!(event["source_kind"], "laserstream");
            assert_eq!(event["mode"], "live");
            assert_eq!(event["slot"], 123);
            assert_eq!(event["error_class"], "missing_execution_metadata");
            assert!(event["timestamp_unix_ms"].is_number());
        }
    }
}
