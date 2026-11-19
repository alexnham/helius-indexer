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

## Query window API and retention

The rolling-window query CLI (`query`, `query-recent`, `summary`) applies the cutoff
`now - window` from an injected clock (the CLI passes system time; tests pass a fixed
time). Rows with `block_time >= cutoff` and `< to` are eligible; the boundary row at
exactly the cutoff is included. Rows with unknown block time are counted in
`null_block_time` and never listed or summed. Storage tests cover exact cutoff,
null-time exclusion, exact sums above u64, keyset ties and coverage states against
real PostgreSQL.

Route request/error/latency metrics (`indexer_api_requests_total`,
`indexer_api_request_duration_seconds`) are not exported yet: there is no HTTP server,
so no route template exists to label. They are deferred to the HTTP route layer and
will not be approximated from CLI invocations. Raw-expiration checks remain with
the retention work and are not part of this query surface.

Migration 8 adds nullable `block_time`, `amount_raw` and `fee_raw` columns and fills
them from `record_json` in the migration transaction, so a failure leaves the session
at version 7. `record_json` is unchanged and NULL stays unknown. The fill updates every
`transfers` row while holding its locks, so a large session is blocked for its duration. `record_json` remains the source of truth, so the
columns can be recomputed from it with the migration's own `UPDATE`. Rollback follows
the general policy in `docs/replay-jobs.md`: a pre-migration binary refuses the
upgraded session (`storage_schema_newer_than_binary`), so restore the pre-upgrade
backup into a separate session and use its matching binary. Do not drop the columns
or decrement the schema version by hand.

### Query window coverage

`coverage` on a window response is the coverage of the slot span of the window's
known-block-time transfers for the mint and decoder. The span is the minimum to
maximum slot of those rows (`slot_range`). Coverage describes only that span: it does
not prove slots at the window edges before the first or after the last transfer were
processed, so a clean `complete` is not a claim about the whole window.

- `complete`: every slot in the span is `complete` or `skipped` in `historical_slots`.
- `partial`: some slot in the span is missing or not complete; `missing_ranges` gives
  bounded contiguous ranges (first 100, `missing_range_count` is exact).
- `unknown` with `no_coverage_evidence`: transfers exist but no historical slot record
  covers the span (for example live-only ingestion).
- `unknown` with `no_known_time_transfers_in_window`: no known-time transfer falls in
  the window, so there is no slot span to evaluate. This is not an observed-empty range.
- `unknown` with `mint_required`: a mint-less listing has no per-mint coverage.
- `unknown` with `all_stored` (`query --all-stored`, `summary --all-stored`): inspection
  output has no window, so no coverage is computed and `coverage` is the literal
  `"unknown"`. This is never a window coverage claim.

An observed-empty window (covered, zero transfers) is not produced: deriving a slot
span from time without transfers would need block times from historical slots, which
this slice does not use. Coverage is one aggregate over `historical_slots` (status
counts, covered runs and missing runs), not a per-slot loop.

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
source/storage time, and is not a benchmark. `indexer_historical_source_seconds` is the part of
elapsed time spent in source `block` calls: fetching and, for the archive, decoding. Failed candidates are retained in
historical raw blocks with scoped durable error/retry state. Snapshot counters
are attempts; replay does not imply new unique query results.

Range progress uses the gauges `indexer_historical_range_slots_requested`,
`indexer_historical_range_slots_completed` (complete or skipped), and
`indexer_historical_range_first_slot`/`last_slot`; the coverage JSON carries the
same values as `progress`. `historical_range_completed` is emitted once per
invocation, only when the re-read coverage of the whole requested range is complete
and the run was not cancelled. It carries `mint`, `slot_start`, `slot_end` and the
decoder version. The mint is the filter-set identity; no separate filter-set
revision exists yet. The per-slot `durable_range_completed` event remains and means
only that one slot's status was acknowledged.

## Bounded live slice status

The opt-in LaserStream command now emits connection/retry/end/failure events and
a bounded JSON invocation snapshot with protobuf input bytes, transaction attempts,
decode failures, raw/decode-write duration sums, output successes, reconnect
attempts and input rate. `live-status` provides observed reference/replay ages
and margin; verified freshness/slot gap remain unknown and coverage-valid zero.
`stream_idle` (info) means no update arrived for 10 s with nothing buffered; the
200 ms batch-flush timeout does not emit it.
Stderr write failure cannot undo durable input. Continuous refresh, histograms,
backlog-age metrics and independent finalized completion evidence remain pending;
see [live validation](live-ingestion.md).

## Combined source validation

The overlap command includes a terminal admission-gate snapshot: whether any write
is active, `active_writes` against `write_limit` (one permit per configured historical
worker), waiting live/historical operations, oldest wait age, admission counts and elapsed
wait sums. A gate operation is not a unique transaction; deduplicated counts and
volume come from the storage summary. Existing source reports retain separate
durable input, decoded output, and verified coverage progress. The public gate
snapshot is inspectable during embedded runs; no continuous monitoring endpoint
is added. See [overlap validation](overlap.md) for scope and proof limits.

### Unknown transfer fees

Decoder v4 indexes ordinary Token-2022 checked transfers whose gross amount is
known even when the historical withheld fee is unavailable. Such rows are decoded,
not unresolved decode items. Query/summary diagnostics preserve null fees and
report `known_fees_raw` with `unknown_fee_count`; zero known fees with a nonzero
unknown count is not a zero-fee total. Range coverage describes instruction
indexing, not completeness of optional fee/owner metadata. Execution ambiguity and
unsupported instructions still block verified range coverage.

### Retained-input retry diagnostics

`live-retry` exposes `attempted`, `decoded`, `failed`, `retry_cursor`, `run_id`,
and unknown coverage per bounded batch. Its start/completion events use
component `live`, source `laserstream`, and mode `recovery`; partial batches retain
an unresolved error class. `live-status` separately exposes `retry_cursor` and
`retry_batch_limit`. The cursor describes scheduling only, not durable chain
progress or verified coverage. Input age is preserved through retries. Invalid
stored payload/schema can terminate a batch; the command error and retained
unresolved input remain visible. See [recovery](recovery.md) for tested boundaries
and remaining monitoring gaps.

### Decoder revision repair

`repair-status` separates target revision, retained capture scope, remaining
other-revision captures, corrected/unresolved counts, original unresolved time/age,
and up to 16 failure-class counts. Capture batch reports expose attempts,
corrected/unresolved outcomes, replaced/written rows, cancellation, and run ID.
`decoder_correction` and `decoder_repair_batch_finished` events use source
`retained_capture`, mode `repair`, bounded classes, and run correlation. Offline
block re-verification uses source `retained_block` and shared run correlation.
`indexer_historical_cached_blocks_total` counts reused blocks separately from
source requests. `indexer_historical_unrelated_transactions_total` (report field
`unrelated_transactions`) counts block transactions that do not touch the job's mint:
read for slot evidence, neither decoded nor stored. `indexer_historical_retained_bytes_total`
(report field `retained_bytes`) counts block bytes written to `historical_inputs` after
unrelated transactions are pruned to their signatures; compare it with
`indexer_input_bytes_total`. No per-signature metric labels are introduced.

`repair_complete` never means verified full-slot coverage. Failed corrections hide
prior output from normal reads while preserving it physically. Summary fields
`target_decoder` and `other_decoder_transfer_count` expose mixed-revision visible
output during a partial repair. Current-revision historical completion is
invalidated by correction and must be reverified from complete retained blocks.
See [decoder repair](decoder-repair.md) for commands and limits.

## Optional archive source

Archive runs reuse historical telemetry with `archive` input-byte labels. The
final report includes bounded HTTP request/body-byte/cache-hit counts, configured
limits, pinned reader revision and source diagnostics. Cache write failures count
as write failures; corrupt CAR/metadata, HTTP and budget failures retain explicit
error classes. Capture provenance includes epoch, slot range and raw SHA-256.
Archive/RPC reports separate `overlap_verified` from the last source coverage;
unknown live coverage remains visible. Tip snapshots report probe states and
freshness gap, never infer absence from an unknown response or claim measured
publication delay. See [archive validation](archive.md) for actual results and
remaining decoder gaps. Archive unit tests check zero-network replay, both source
orders, duplicate replay, integrity failure and conflicting immutable block
identity including migration from retained historical inputs.

## Frozen source-boundary batches

`bridge-status` separates the scheduling position from verified contiguous
historical coverage. It reports pending slots, source-split missing intervals,
bounded interval samples/truncation, boundary measurement age, generation-bound
overlap proof and an explicitly unknown live tail. Pending age starts at the
original durable plan measurement and is not reset by retries or cursor movement.
Completed plans report zero pending age. Batch reports retain stage attempt
metrics, source diagnostics and the unique persisted storage summary separately.
Archive diagnostics count transport bytes; historical input metrics count retained
normalized bytes. RPC source diagnostics now count consumed response-body bytes
and request/read/oversize failures; unavailable source diagnostics stay null. `bridge_batch_finished` includes run, mint/range/tip and pending
scope in the diagnostic event, with no new high-cardinality metric labels.
Tests distinguish interrupted proof acknowledgment, a failed source, scheduler
rotation and durable completion. See [source-boundary operation](source-boundary.md).


### Decoder v5 maintenance and archive repair

Base authority changes and withheld-fee maintenance no longer create unsupported
failures for recognized layouts. Malformed layouts retain the bounded classes
`invalid_authority_instruction` and `invalid_fee_maintenance_instruction`; unknown
extension authority/configuration types remain unsupported. Owner uncertainty is
represented by null, never by a guessed authority. Unknown ordinary transfer fees
remain null after fee maintenance. Captured and synthetic telemetry/storage
checks preserve attempts-versus-unique semantics and source failure visibility.

`repair-history-archive` uses the existing retained-block repair context and
revision-scoped coverage. Reports add `source_kind` (`archive` or `rpc`) alongside
`retained_blocks_only`; cached blocks and zero source requests distinguish offline
reverification from fetching. Original repair age is preserved. No new metric
labels are introduced. See [decoder maintenance evidence](validation/2026-11-12-decoder-maintenance.md).

## Corroborated RPC execution evidence

Recovery keeps native archive/RPC completion, recovery attempts/errors, repaired
transaction counts and generation-bound readiness separate. Native RPC failures
remain observable even after the corroborated input succeeds. The bounded metric
source label `corroborated` measures reconstructed normalized bytes and repeated
decoding attempts, not provider-billed/downloaded bytes or unique transfers.
Capture provenance retains both input SHA-256 references; raw logs and endpoint
URLs never become diagnostic/metric labels. Frozen-plan status distinguishes
`overlap_verified`, `overlap_recovered` and `boundary_ready`; the live tail stays
unknown. SQLite crash/restart tests verify the proof boundary and no duplicate
query output. See [evidence recovery](rpc-evidence-recovery.md).


## RPC response-body accounting

`RpcSource` diagnostics report `requests`, `request_failures`, `read_failures`,
`oversized_responses` and `response_body_bytes_read`. They cover genesis checks,
finalized reference/time responses and full block responses. Counts advance at
the reader boundary before parsing or propagating a partial-read failure; the
one-byte size-limit sentinel is included. A deadline rejected before sending
counts no request. HTTP/connection failures count as request failures; bodies not
read by the client are not included. This measures consumed response-body bytes,
not request/header/TLS bytes, packets on the wire or provider-billed usage.

Historical and bridge reports receive the snapshot through the existing source
interface. Archive overlap/tip reports additionally expose `rpc_transport`; their
older successful-block byte field remains narrower. Retained-input replay makes
no RPC request. No endpoint, credential, raw payload or unbounded error string is
included. Offline tests cover successful and oversized reads, bytes retained on
read failure and diagnostic redaction. No live-provider check is required.

## Container console health

`/healthz` reports console availability and a readable existing transfer table,
with captured-data mode and unknown coverage. Failure returns unavailable without
creating a database or exposing a path/error payload. It never substitutes for
verified coverage/freshness gauges. Persistent initialization publishes its receipt
only after replay checks pass; startup/restart and native CLI event logs remain
separate. See [Docker operation](docker.md) for scope and validation.

## Provider-backed local console

The opt-in runtime exposes `/api/status` with separate supervisor state, combined
worker report, per-mint unique storage summary, live raw/replay diagnostics and
scoped historical coverage. The newest-first transfer query uses the same storage
repair visibility rules as ordinary queries. Polling is every five seconds only
while the page is visible. Supervisor failure/timeout cannot fabricate a successful
source report. Availability remains separate from source connection and verified
coverage. See [runtime testing](runtime-testing.md) for limits and manual correctness checks.

Continuous runtime cycles record `cycle`, the last report and the prune result in
`worker.json` (`state` is `running`, `retrying` or `stopped`). Each prune emits a
`retention_pruned` event, and its report gives removed transfer/input row counts
and the oldest retained slot. `live-status` exposes `last_received_slot`, the
resume position for the next cycle. Pruning invalidates historical/source/overlap completion proofs for a partially
retained boundary slot in the same transaction as deletion. Shutdown tracks both
intake and prune subprocesses; timeout kills and reaps the current child. Pruning
keeps coverage `unknown`.

### Console event feed

`/api/events` returns up to 100 events, newest first, from the last 256 KiB of the
session's `events.jsonl`, with an optional `category` of `live`, `backfill`, `storage`
or `errors`. Category comes from the event's `mode`, component/name and severity.
`stage_completed` and `durable_range_completed` are omitted as hot-path volume. Only
scalar fields pass: strings must be lowercase identifiers or base58 chain values, so
URLs, credentials and free-form text never reach the page. The response reports
whether the log exists, whether older events were cut off and how many non-JSON lines
were skipped. Supervisor transitions `runtime_started` and `worker_retrying` use the
CLI event schema with component `runtime`. The feed is inspection, not an exporter.

## PostgreSQL readiness and identity integrity

The console health path uses `health SESSION`, a read-only schema-version/table
probe with a three-second subprocess limit. It does not create a schema or run
migrations, and availability never implies verified chain coverage. Block identity
checks and inserts hold a transaction advisory lock scoped to the session,
including absent parent/child anchors. Conflicting concurrent observations fail
closed. Summary rows stream through checked u128 accumulation instead of loading
a mint's history into a client-side vector. Existing bounded error classes and
credential redaction apply.


## Confidential transfer diagnostics

Decoder v6 preserves hidden amounts/fees as null. Storage summaries report
`unknown_amount_count` and `confidential_transfer_count` independently of known
volume, unknown fees and indexing coverage. Unknown amounts do not become decode
failures when the instruction and mint are identifiable. Unsupported variants and
malformed layouts use bounded `unsupported_confidential_instruction` and
`invalid_confidential_instruction` classes through the existing decode events,
metrics and durable historical retry state. No ciphertext, secret or new metric
label is exported. Synthetic fixture/integration checks cover null serialization,
duplicate replay, optional historical metadata, non-transfer activity, failed
execution evidence, sanitized failure events and retries blocking coverage.

## Replay jobs and source separation

The replay job report includes frozen range/revision, consumed allowance and
unique/decoded/unresolved/retained-raw counts in a repeatable-read snapshot.
Attempt history includes availability observation time/age and margin, terminal
classes and later-slot observations. Failures and pending age remain visible
after restart. Queryable transfers in scope are separate from processing receipts;
coverage and verified boundary remain unknown.

The paired worker shares live-priority admission, bounds historical waiting and
starts history after live initialization rather than after its cycle. Raw-input
`ingestion_mode` keeps replay out of live arrival/freshness and resume counters.
The shared intake emits connection/retry/end/failure events and stage duration,
transaction/input-byte/write diagnostics with mode `historical` for replay.
`replay_attempt_finished`, retry and explicit budget-grant events use the replay
component with bounded failure classes. Retrying retained input makes no provider
request. Telemetry writes remain best-effort and cannot undo durable input/output.
No mint/slot/signature metric labels are added.

Source routing (#31) appends a revision per measurement. The report's `routing`
block shows the latest revision, measurement age, replay first-available and
finalized reference, whether the archive was probed and its last available slot
or failure class, and the routed segments (`archive`, `replay`, `overlap`,
`uncovered`) marked `routed_not_processed`. Unknown availability appears as null
and routes to `uncovered`. Each measurement emits `route_measured` with outcome
`incomplete` when any segment is uncovered, plus revision, whether the archive was
probed and the uncovered segment count; a failed replay measurement emits it with
outcome `failed` and records no revision.

Each archive batch emits `archive_attempt_finished` (component `replay_archive`) with
outcome `success`, `incomplete`, `cancelled` or the error class, and the observed
requests, response bytes and cached chunks. The status `archive` block reports
attempts and response bytes by status, remaining batches and per-segment pending
slots with oldest pending age; `archive_budget_granted` records an explicit grant.
These are JSON inspection snapshots like the replay report, with no job, mint or
slot metric labels.

Each RPC gap batch emits `gap_attempt_finished` (component `replay_gap`, source
`rpc`) with the same outcomes and observed requests and response bytes, and
`gap_budget_granted` records an explicit gap grant. The status `gap` block mirrors
`archive`; `verified` adds `gap_slots` and `uncovered_unverified_slots`.

`replay-overlap` emits `overlap_reconciled` (component `replay_overlap`) with outcome
`success` or `conflict` (error class `overlap_conflict`), the route revision and
per-outcome slot counts. The report's `verified` block gives `verified_through`,
verified archive and overlap slot counts, unknown slots, latest outcome counts and
up to 32 conflict slots with their reason; the console shows the boundary, counts
and conflicts. A `replay-monitor` cycle reports each step's outcome or error class.

The Docker runtime's `/api/status` adds `configuration.archive` (archive batches
enabled) and `configuration.binary` (`fresh`, `current`, `upgraded` or
`upgrade_required`). Worker state `binary_upgrade_required` means the image binary
differs from the session copy and nothing is ingested until an explicit upgrade.
Route measurements append their `route_measured` events to the session's
`events.jsonl`, and the last route report is kept as `route.json`. Readiness still
means the process and its dependencies are available, not source completeness.

The console shows remaining budgets, processing versus raw retention and unknown
coverage, alongside live reports and actual query output. Current JSON snapshots
are inspection telemetry, not a production continuous exporter or alert system.
See [replay jobs](replay-jobs.md) for bounds and operating commands.


### Replay integration with batched writes and Kafka

The shared network driver runs synchronous PostgreSQL and Kafka operations after
leaving Tokio's network future. Live input retains the 100-input/16-MiB/200-ms
flush bounds, atomic batch output/status acknowledgment and storage write metrics.
With Kafka configured, only live input publishes there; delivery-confirmed cursors
retain the existing reorder margin. Historical replay keeps separate durable raw
receipts and charged allowances, and cannot advance the live producer cursor.
The runtime resumes Kafka from the stored `resume_from`, never its maximum raw
slot. Deterministic tests exercise direct batch acknowledgment and Kafka publication
alongside a replay receipt using real local PostgreSQL/Kafka, without provider calls.

The Kafka consumer report adds `storage_write_seconds` and `prefetch_peak_bytes`
(from librdkafka statistics, sampled every second); producer and consumer buffer
bounds and their backpressure are listed in [recovery](recovery.md).

For the #36 metrics, the consumer report and events give:

- **Consume rate:** `consumed_per_second` over `elapsed_seconds`. The produce rate is
  `input_transactions_per_second` in the producer's `live_metrics` event.
- **Consumer lag:** `lag_messages` at exit and `lag_peak_messages`, measured as the broker
  high watermark minus the position stored with the output, summed over assigned
  partitions. Fetched but unwritten input therefore counts. It is measured at most once a
  second before a write, within a one-second request budget; a failed or slow request,
  or no assignment, gives `null` (unknown), never zero. While the broker is unreachable each
  measurement spends its whole budget, so writes can slow by up to one second per second,
  and the exit report can take one more second.
- **Rebalances:** `rebalances.assigned` and `rebalances.revoked`, plus a
  `consumer_rebalanced` event (operation `assign` or `revoke`) for each one.
- **Retries:** `batch_retries` counts batches that failed storage and were retried
  input by input. The idempotent producer's own resends inside librdkafka are not counted.
- **Decode failures and oldest pending age:** `retained_failures` in the report, and
  `live-status` `unresolved_items` and `oldest_unresolved_age_seconds`. Retained Kafka
  inputs are part of those.

These are JSON inspection fields and events, not labelled metric series.

## Sampled demo datasets

A sampled dataset (#72) exposes its immutable policy, inclusion fraction and mint
through `sample-status` and in `query`, `query-recent` and `summary` output, with
actual retained counts and no extrapolation. Existing input, decode and coverage
signals are unchanged; storage write counts describe retained sampled rows, not
all decoded transfers. Historical coverage remains independent of selection.
Kept-versus-excluded counts are not yet exported as a metric. A sampled runtime
retains every raw input and skips pruning, so disk usage must be monitored for the
lifetime of the demo.

## Large sampled archive plans

The standalone archive backfill wrapper preserves the bridge's frozen range,
independent pending/verified counts and overlap proof. Its fsynced manifest
records each batch's pessimistic byte reservation before starting the native
worker. A batch that reports its source response bytes is then charged those bytes;
interrupted or failed work retains the worst-case charge with unknown actual bytes.
Charged bytes are a scheduling allowance, distinct from billing. A failed or timed-out
native command appends its redacted stderr events to the session's `events.jsonl` and
reports only the CLI's fixed error class.
Cancellation, low disk and exhausted allowances stop scheduling without advancing
coverage. Time probes are bounded, preserve null/skipped/unavailable time as
unknown, and explicitly label the selected start as unverified temporal coverage.
The loopback console polls while visible and separates actual sampled counts
from historical slot evidence. No credential-bearing endpoint, raw payload or
provider error text is added to its public status. See
[30-day operation](30-day-backfill.md) for limits and source verification.

Backfill JSON run settings are retained in the manifest's `run_config` and exposed
by `status`. `bridge_batch_finished` includes `batch_limits`, matching the native
report's configured slot ceiling and conservative RPC-response bound. Settings
are diagnostic fields, not metric labels. The wrapper derives its crash-safe
reservation from the configured slot count; changing settings never resets consumed
allowances. Offline checks cover config precedence, rejected settings, propagation,
and bounded native historical subranges.
