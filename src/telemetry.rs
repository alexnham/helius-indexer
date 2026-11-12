use serde_json::json;
use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

/// One bounded offline invocation; metrics are attempts, not distinct stored transfers.
pub struct Telemetry {
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
    pub fn observe(&mut self, stage: &'static str, outcome: &'static str, seconds: f64) {
        self.stages.push((stage, outcome, seconds));
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let event = json!({"timestamp_unix_ms":timestamp,"severity":if outcome=="success" {"info"} else {"error"},
            "component":"offline_indexer","operation":stage,"outcome":outcome,
            "run_id":self.run_id,"slot":self.slot,"decoder_version":crate::decoder::DECODER_VERSION,
            "source_kind":"capture","mode":"offline","event":"stage_completed"});
        // A broken diagnostics pipe must not interrupt a durable transaction write.
        let _ = writeln!(std::io::stderr().lock(), "{event}");
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
