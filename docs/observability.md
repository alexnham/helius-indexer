# Observability contract

Status: the capture and bounded historical slices implement structured stage events and per-invocation metric snapshots. Long-running collection, tracing, dashboards, and alerts remain pending. This contract supplements the correctness requirements in `../PROPOSAL.md`. Monitoring backends remain an architecture choice.

## Operational questions

An operator must be able to determine whether input is arriving, which data is durable, what has been decoded, which ranges are verified complete, and what prevents recovery. A healthy process or advancing input cursor alone is not evidence of complete indexing.

Show live and historical work separately. Show per-configured-mint coverage in a diagnostic API or coverage report. Quiet mints must not look broken merely because no transfers occurred.

## Metrics and definitions

Names below describe the intended exported contract. Choose histogram buckets from benchmark measurements and the operating targets before production. Counters reset on process restart; durable progress is reconstructed from persisted state. Include units in metric names.

| Metric | Type | Definition |
| --- | --- | --- |
| `indexer_transactions_total` | Counter | Transactions reaching a named stage, with bounded outcomes such as success, retry, rejected, or unsupported. Counts attempts, not unique transfers. |
| `indexer_transfers_decoded_total` | Counter | Transfer records emitted by decoding attempts; replay can increase this count without increasing unique stored transfers. |
| `indexer_input_bytes_total` | Counter | Bytes received at the documented transport/normalization boundary, by source. Label the measurement basis in dashboard descriptions; this is not automatically provider-billed bytes. |
| `indexer_stage_duration_seconds` | Histogram | Elapsed time per named operation, including raw writes, decoding, publication, and transfer writes. Document batch versus item operations. |
| `indexer_live_freshness_seconds` | Gauge | Age of the block time at the latest contiguous verified live coverage boundary, measured against current UTC time. Export only with known boundary time; pair with reference freshness and coverage validity. |
| `indexer_live_slot_gap` | Gauge | Finalized reference slot minus latest contiguous verified live slot, clamped at zero. This is a slot-distance indicator, not a missing-block count. |
| `indexer_reference_age_seconds` | Gauge | Time since the finalized reference was last successfully refreshed. A stale reference invalidates claims that lag is healthy. |
| `indexer_coverage_valid` | Gauge | One only when the reported boundary is known for the active mint/filter set and decoder revision; otherwise zero. Diagnostic coverage reports retain the detailed scope. |
| `indexer_missing_ranges` | Gauge | Number of known unresolved ranges in the retained query window, not their slot span. |
| `indexer_backlog_items` | Gauge | Pending work at a bounded stage/queue, distinguishing live and historical work. |
| `indexer_oldest_pending_age_seconds` | Gauge | Age since first durable enqueue of the oldest pending item. Retries preserve original age. Zero means an observed empty queue; an unavailable measurement is not zero. |
| `indexer_retries_total` | Counter | Retry attempts by operation and bounded error class. |
| `indexer_decode_failures_total` | Counter | Failed/unsupported decode attempts by bounded failure class. |
| `indexer_unresolved_decode_items` | Gauge | Durable candidate transactions awaiting successful decoding or a supported resolution. These block affected coverage. |
| `indexer_reconnects_total` | Counter | Stream reconnection attempts by bounded outcome. |
| `indexer_replay_margin_slots` | Gauge | Requested next recovery slot minus provider's earliest available replay slot. Negative means that position is already outside replay availability. Include availability-check freshness separately. |
| `indexer_replay_info_age_seconds` | Gauge | Time since the last successful provider replay-availability check. |
| `indexer_api_requests_total` | Counter | Requests by route template, method, and bounded status class. |
| `indexer_api_request_duration_seconds` | Histogram | API request duration by route template and status class. |
| `indexer_telemetry_dropped_total` | Counter | Logs/spans discarded because bounded telemetry buffers or exporters could not keep up. Export independently where practical. |

Expose process memory/CPU, disk capacity, and selected queue/database/object-store health using appropriate backend instrumentation. Record retained raw bytes, decoded storage, historical progress/rate, and recovery throughput in benchmark and operational reports. Avoid adding a custom exporter for metrics already supplied by a selected service.

Coverage is based on verified processed ranges, including empty produced blocks and accounted-for skipped slots, not the last observed transfer. Unknown boundary time, missing reference, or an uninitialized checkpoint must appear as unknown/unready rather than a zero lag. Report clock skew or negative time differences instead of silently claiming healthy freshness.

Provider usage reports must distinguish measured transport bytes, authoritative billed usage, and estimates. Record pricing basis, verification date, report delay, and uncertainty for projected spend. Report initial backfill costs separately from recurring operation.

## Cardinality and sensitive data

Metric labels use bounded enums: component, stage, source kind, live/historical mode, operation, outcome, failure class, route template, and status class. Keep a documented label allowlist.

Signatures, slots, account addresses, request IDs, raw URLs, error messages, and arbitrary mint/filter/decoder revision values belong in diagnostic events or reports rather than metric labels. Use build metadata and coverage reports to identify versions. Keep the default metric series count independent of indexed transaction count and mint-list growth.

Redact credentials and credential-bearing URLs before formatting logs or spans. Payload dumps are opt-in local diagnostics with bounded size and explicit retention. Never log encryption secrets or claim encrypted amounts have been recovered.

## Structured logs and tracing

Emit structured events for startup/shutdown, configuration changes, source connection changes, retries/exhaustion, failed decoding, durable range completion, coverage gaps, recovery transitions, and decoder correction outcomes. A completion event occurs only after its required durable acknowledgments.

Include UTC timestamp, severity, component, operation, outcome, source kind, live/historical mode, and applicable slot/range, decoder revision, filter-set revision, error class, and retry attempt. Include signature only when needed for a transaction diagnostic; keep successful hot-path logs sampled or aggregated.

Carry correlation IDs across normalization, durable publication, decoding, and persistence. Preserve message identity separately from attempt/span identity so replay attempts can be correlated. Instrument batch operations without generating a span for every instruction by default. Use sampled traces with bounded queues; record errors and recovery transitions in structured logs even when traces are not sampled. A trace exporter outage must not lose chain data or prevent durable writes.

## Dashboard views

- **Live health:** reference freshness, coverage validity, verified freshness/slot gap, input/decode/write rates, stage errors, and write latency.
- **Recovery:** missing ranges, historical progress, oldest pending age, backlog, unresolved decoding, replay margin/check freshness, and correction status.
- **API correctness and latency:** route latency/errors plus coverage/unknown-amount indicators from representative query checks. Returned coverage remains the source of truth for a specific query.
- **Capacity and cost:** CPU/memory/disk, retained data size, buffer pressure, authoritative usage where available, and labeled cost estimates.

## Alerts and first actions

Before production, choose an owner, notification destination, thresholds, sustained evaluation windows, and recovery conditions for each rule. Store those values with the deployed alert configuration. Validate delivery and resolution using injected failures. Numeric thresholds remain unresolved until benchmarks and operating targets are agreed.

| Condition | First response / runbook |
| --- | --- |
| No verified progress while finalized reference advances, or freshness exceeds target | Check reference health, connection, backlog, writes, and unresolved decoding. Preserve checkpoints; repair from durable inputs or a verified source. |
| Reference or replay-availability information is stale | Restore provider observation; mark lag/replay safety unknown. Do not suppress this condition because derived lag looks small. |
| Pending work approaches replay availability limits | Prioritize durable live intake, bound historical work, check actual replay availability, and plan archive/RPC gap repair. Never skip a range to make a gauge healthy. |
| Coverage gaps or unresolved decode items persist | Inspect durable failure records, identify affected scope, fix/retry, and independently verify the repaired range before marking complete. |
| Backlog age grows or durable writes repeatedly fail | Inspect downstream capacity and storage health; apply bounded backpressure. Confirm retained-input coverage before restart or replay. |
| Disk/raw retention capacity or projected provider usage exceeds agreed limits | Identify growth source and estimate remaining headroom. Follow approved retention; obtain approval before paid capacity changes. |
| API failures or latency breach targets | Check storage health, query cost, deduplication/correction behavior, and incomplete-coverage responses; preserve correctness while recovering service. |
| Telemetry exports drop data or monitoring disappears | Restore collection/export and check local health. Treat missing monitoring as unknown, not a healthy service. |

Group related alerts by component and incident cause to limit repeated notifications. Notify on actionable sustained failures and recovery. Health/readiness checks must distinguish a running process, dependencies ready for its role, and complete dataset coverage; historical backfill does not by itself make a usable live API unready.

## Ticket acceptance and verification

Include applicable telemetry in each behavior ticket rather than deferring it to a final monitoring ticket:

- **First offline slice:** structured events, stage metrics, sanitized failures, and deterministic checks that processing one fixture emits the expected outcomes. Establish an export/inspection path usable in local validation.
- **Live and historical sources:** input rates, reference/availability freshness, progress and failure events; prove no-match ranges and source outages are distinguishable.
- **Overlap and recovery:** backlog/age, retry metrics, durable progress and coverage validity; crash injection must show incomplete work and eventual repair accurately.
- **Fees and confidentiality:** bounded unsupported/failure classes and query checks that hidden values remain unknown. No secret/payload leakage or high-cardinality labels.
- **Decoder correction and new mints:** diagnostic scope/revision and progress reports that distinguish existing coverage from pending work.
- **API and retention:** templated-route metrics and checks for coverage disclosure, unknown amounts, exact cutoff, and raw expiration behavior.
- **Benchmark/readiness:** exported measurements, dashboard evidence, measured telemetry overhead, label-cardinality checks, tested alerts and recovery actions, plus chosen owners/destinations/thresholds.

Tests verify counter/gauge semantics, retry-versus-unique counts, unknown-state handling, and redaction. Simulate a telemetry backend outage and buffer exhaustion to verify bounded resource use and continued ingestion correctness. Documentation-only changes need link/content checks, not a live monitoring deployment.

## Bounded historical snapshots

The historical CLI exports metrics for its explicitly requested mint/decoder/slot
scope, identified in the accompanying coverage report rather than metric labels.
`indexer_missing_ranges` counts unresolved contiguous intervals in that scope;
it does not describe the full retained window. `indexer_backlog_items` counts
unverified slots. Pending age uses the first durable scheduling time across
retries, is zero for an observed empty backlog, and is omitted for unscheduled
work. Live freshness/reference/replay gauges are not fabricated by this worker.

`indexer_stage_duration_seconds` observes one `historical_range` operation per
invocation (sum/count and the infinite bucket); finite buckets await measurements.
The bounded diagnostic additions `indexer_historical_source_errors_total`,
`indexer_historical_write_errors_total`, `indexer_historical_elapsed_seconds`, and
`indexer_historical_transactions_per_second` have no unbounded labels. The rate
counts successful decode attempts divided by invocation elapsed time, including
source/storage time, and is not a benchmark. Failed candidates are retained in
historical raw blocks with scoped durable error/retry state. Snapshot counters
are attempts; replay does not imply new unique query results.
