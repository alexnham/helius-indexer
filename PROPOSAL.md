# Solana token transfer index: design proposal

Status: requirements agreed; the first offline captured-transfer slice is implemented locally. Production infrastructure selection and sizing remain pending validation.

## Purpose and scope

Build a custom index of successful, finalized token transfer instructions for an explicitly maintained list of Solana mint addresses. Serve an exact rolling 30-day window, primarily for analytics by mint and time. Wallet history is secondary. This document defines the proposal and validation work; it does not authorize implementation or production provisioning.

Include outer and inner transfer instructions. Exclude minting, burning, and native SOL movements. Wrapped SOL token transfers are included when its mint is configured. Preserve successful zero-value and self transfers in the underlying index; analytical queries may explicitly exclude them.

Use `finalized` consistently for live and historical ingestion. Current Helius documentation describes `confirmed` as a compatibility alias under Alpenglow; the original brief's distinction between confirmed and finalized must not drive the design. See [delivery guarantees](https://www.helius.dev/docs/laserstream/delivery-guarantees).

## Transfer representation

Each transfer record contains:

| Field | Meaning |
| --- | --- |
| `transfer_id` | Deterministic identity shared by live ingestion, historical ingestion, and replay |
| `signature`, `slot`, `block_time` | Transaction identity and chain position/time |
| `instruction_index`, `inner_instruction_index` | Position of the transfer instruction; inner index absent for outer instructions |
| `mint`, `token_program` | Token and owning token program |
| `source_token_account`, `destination_token_account` | Accounts participating in the transfer |
| `source_owner`, `destination_owner` | Wallet owners at execution time where historical evidence permits resolution; otherwise null |
| `amount_raw`, `decimals` | Instructed transfer amount in base units and token precision; amount null if unavailable |
| `fee_raw` | Separate Token-2022 withheld transfer fee in base units; null if unavailable |
| `confidential` | Whether the transfer uses confidential semantics |
| `decoder_version` | Decoder revision used to derive the record |
| `ingested_at` | Operational ingestion timestamp |

Distinguish an observed zero fee from an unknown fee. Default volume uses the instructed transfer amount, with fees reported separately. Derive recipient proceeds only when the relevant semantics and values are supported; document the calculation.

Recognizable confidential transfer instructions are indexed when the mint is identifiable, with a confidential tag and null unavailable amounts or fees. Mint involvement alone does not establish a transfer. Keep other mint-associated activity distinct from transfer records. Do not substitute zero for hidden values. Solana documents encrypted amounts and balances in its [confidential transfer integration guide](https://solana.com/docs/tokens/extensions/confidential-transfer/integration-guide).

Publish a support matrix for relevant SPL Token and Token-2022 transfer variants/extensions. Unrecognized candidate cases remain available for investigation and prevent affected coverage from being declared complete. Net balance changes alone are insufficient to reconstruct individual transfer edges.

The precise identity encoding remains an implementation detail to validate. It must uniquely identify every supported outer or inner transfer and remain stable across ingestion sources and decoder revisions. Decoder corrections must support deterministic replacement and removal of prior records, including records a corrected decoder no longer emits.

## Logical pipeline

```text
Live finalized stream ──┐
                       ├─> normalize + durably retain raw transactions
Historical source ─────┘                │
                                        v
                             durable processing buffer
                                        │
                                        v
                                 shared decoder
                                        │
                                        v
                             transfer storage + queries

Coordinator: schedule ranges, track coverage, retry failures, reconcile gaps
```

These are responsibilities, not mandatory separate services or technologies. Both ingestion paths must preserve the metadata needed by the same decoder and produce identical transfer identities and semantics.

Start live ingestion immediately while historical backfill fills the initial window. Expose partial historical readiness. After an outage, restore live ingestion first and repair history concurrently with bounded resources so recovery does not repeatedly delay live data.

Adding a mint triggers its own historical backfill. Coverage must be tracked against the applicable mint set or filter revision; coverage for an earlier mint set does not prove coverage for a newly added mint.

## Durability, coverage, and recovery

Distinguish progress at three stages:

1. Raw input durably retained or published.
2. Candidate transactions decoded and resulting records durably written.
3. A range verified complete and safe to expose as complete.

A received transaction or Kafka delivery acknowledgment does not by itself establish slot completeness. Persist coverage only after the selected source's completion evidence and all required acknowledgments are accounted for. Validate completion semantics for the actual subscription and commitment level; do not assume a notification for another commitment establishes completeness.

Distinguish skipped slots, produced slots with no matching transfers, and unknown or missing data. Parallel completion must not advance a contiguous checkpoint over an unfinished range. Coverage includes the decoder revision where corrections are underway.

Consumer progress advances only after durable storage acknowledgment. Replay after a crash may repeat work; database queries must still return each logical transfer once immediately, including during overlapping historical and live ingestion.

Retain undecodable candidate transactions with failure details and retry state. Continue unrelated work, but keep affected ranges incomplete. Durable raw storage and processing state must allow retries without relying exclusively on an in-memory stream cursor.

LaserStream replay is documented as roughly 48 hours and honors mint filtering. Check actual availability using `SubscribeReplayInfo`; do not rely solely on a fixed slot subtraction. Older gaps require another historical source. See [historical replay](https://www.helius.dev/docs/laserstream/historical-replay).

## Retention and query behavior

Every user query applies an explicit block-time cutoff for the rolling 30-day window. Database TTL handles eventual physical cleanup and does not define query eligibility.

Provisionally retain matched raw transactions for 37 days: 30 days of query history plus a seven-day repair margin. Compressed object storage is the candidate raw store. Preserve instructions, inner instructions, relevant execution metadata, source provenance, and schema/version information needed to repeat decoding. Size and validate this policy before production.

The initial API provides:

- Transfer listings filtered by mint and time, with deterministic ordering and pagination.
- Transfer counts and volume aggregates by mint and time.
- Coverage boundaries and missing ranges relevant to the requested dataset.

Responses must disclose incomplete coverage. Null confidential amounts must not silently disappear into an aggregate presented as total token volume: report known volume with unknown-amount counts or another explicit completeness indicator. Distinguish incomplete indexing from indexed transfers whose amounts are confidential.

## Candidate infrastructure and selection criteria

| Responsibility | Candidate | What validation must establish |
| --- | --- | --- |
| Live source | Helius LaserStream gRPC | Mint-filter coverage, completion signals, replay behavior, SDK compatibility, workload cost |
| Historical source | Old Faithful with Jetstreamer; RPC fallback | Required metadata, archive availability, sustainable throughput, bridge to recent data, operational effort |
| Processing buffer | Kafka or a simpler durable alternative | Recovery, backpressure, retention, throughput, and operating burden |
| Checkpoints and coordination | PostgreSQL or a simpler durable store | Atomic progress transitions, range scheduling, concurrency, restart behavior |
| Transfer analytics | ClickHouse | Required query patterns, immediate logical deduplication, corrections/deletions, retention, cost |
| Raw transaction retention | Compressed object storage | Complete replay inputs, expiration, repair speed, storage and request costs |

None of these choices is committed. In particular, a `ReplacingMergeTree` declaration alone is not an acceptance demonstration for immediate duplicate-free results. Specify and test the query/write strategy before accepting ClickHouse. Evaluate sorting and partitioning against mint/time queries, corrections, and retention rather than adopting the initial example unchanged.

Treat archive publication lag, Jetstreamer interfaces, and compatibility with required transaction metadata as facts to verify. The original brief's 10–20 hour archive lag is not a validated recovery guarantee. Historical and live sources need a verified overlap with no uncovered interval.

Target operation by one engineer. Prefer managed services where their cost is justified, and avoid separate services or databases without demonstrated need.

## Rust package organization

Start with one Rust package. Modules express responsibilities; extract crates only when independent builds or concrete reuse justify them. Separate commands or binaries can run workers independently without introducing a crate per worker.

```text
solana-transfer-indexer/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── model.rs
│   ├── decoder/
│   ├── producer/
│   ├── consumer/
│   ├── backfill/
│   ├── coordinator/
│   ├── storage/
│   └── api/
├── migrations/
│   ├── postgres/
│   └── clickhouse/
├── infra/
│   ├── docker-compose.yml
│   └── prometheus.yml
├── tests/
│   ├── fixtures/
│   ├── integration.rs
│   ├── integration/
│   ├── recovery.rs
│   └── recovery/
└── README.md
```

`model.rs` owns explicit domain types instead of an unrestricted `common` module. `storage` owns persistence and query correctness. The coordinator schedules and verifies work; producers retain/publish input; consumers decode and persist output. Migration and infrastructure files are created only for selected technologies.

## Validation before infrastructure commitment

Use USDC, USDT, wrapped SOL, and a representative Token-2022 mint verified during research. Verify canonical mint addresses and relevant extension behavior rather than selecting by ticker alone. This is the benchmark set, not the final production list.

Exercise a representative end-to-end slice covering:

1. Historical ingestion and concurrent live ingestion through the shared decoder.
2. An overlap that proves stable identities and immediate duplicate-free listings and aggregates.
3. Crashes around raw persistence, publication, database acknowledgment, and progress commits.
4. Restart and recovery both inside and outside the available stream replay window.
5. An undecodable candidate, bounded retry, and accurate incomplete-coverage reporting.
6. Decoder corrections that change values, add transfers, and remove incorrect transfers.
7. Outer/inner transfers, multiple transfers in one transaction, zero/self transfers, Token-2022 fees, and confidential cases where suitable data is available.
8. A new mint added while existing mints remain live.
9. Exact query-time retention with physical TTL cleanup delayed.

Compare selected ranges against independently fetched finalized blocks. Reconcile candidate transactions and individual transfer instructions, not just the pipeline's own counts. Record the limits of this comparison: a shared provider or parser can introduce correlated omissions. Use separate verification logic and sources where practical.

Measure matched transactions/second, bytes/transaction, raw and decoded storage, durable write throughput, live indexing lag, historical catch-up rate, query latency, and resource/cost usage. Verify bounded memory and backpressure under downstream slowdown. Report benchmark conditions and uncertainty.

## Cost and outstanding decisions

Operational telemetry follows [the observability contract](docs/observability.md). Each implementation ticket includes the applicable metrics, structured events, and verification criteria from that contract. Instrument the first working slice; extend coverage with each feature. Production readiness includes tested alerts, recovery runbooks, and explicit measured thresholds.

Helius Business is listed at **USD $499/month**, including mainnet LaserStream access. The current plan table lists 100 million monthly credits; streaming is metered at **2 credits per 0.1 MB of uncompressed data**. This is a plan price and allowance, not the system's total operating cost. See [plans](https://www.helius.dev/docs/billing/plans) and [stream metering](https://www.helius.dev/docs/billing/credits#laserstream-grpc).

Before approving production infrastructure, produce a measured estimate covering provider usage and overages, compute, databases, queue, raw storage, requests, networking, monitoring, and initial backfill. Separate one-time initialization from recurring operation.

The total acceptable monthly budget remains unresolved. Production mint selection, numerical freshness/query/recovery targets, backfill deadline, deployment topology, exact storage schemas, and technology selection must be decided using validation results. Do not present unmeasured capacity, cost, latency, or archive availability as guarantees.

## Next review

Use this proposal as the requirements baseline. The first captured-transfer slice now provides local decoding, provisional SQLite persistence, a bounded query CLI, and basic telemetry; see `README.md` for commands and limitations. Continue the approved tickets toward the representative end-to-end validation and use its evidence for production architecture and targets. No production infrastructure has been provisioned.

The bounded opt-in live transport slice is documented in
[the live validation notes](docs/live-ingestion.md). It preserves raw protobuf
before decoding; finalized filtered delivery leaves slot coverage unknown pending
independent completion corroboration. This does not resolve production choices.

The offline Token-2022 fee slice supports execution-validated explicit fees and
known-zero cases. Ordinary checked transfers retain their known instructed
amount with a null fee when historical fee evidence is unavailable; summaries
count those unknown fees separately. See the [support matrix](docs/token-2022-support.md)
for exact variants and validation limits.

The bounded overlap validation combines the two source workers with live-priority
write admission and immediate compatible-observation coalescing. Its historical
coverage is scoped to the requested finite range, while the stream tail remains
unverified. See [overlap validation](docs/overlap.md) for measured evidence, storage
semantics, and the bounded credentialed check with its remaining coverage gaps. This does not select
a production architecture or establish production capacity.

Explicit offline decoder revision repair now replaces each transaction's complete
output atomically, with retained inputs, original pending age, and revision-scoped
diagnostics. Failed corrections preserve but hide stale prior rows. Range repair
is incremental, and historical completion requires full-block re-verification;
retained-block replay can recover candidates rejected by an earlier revision
without fetching the same history again. See [decoder repair](docs/decoder-repair.md)
for compatibility, backup/rollback requirements, and validation limits.

## Bounded archive validation (2026-11-12)

The optional maintained Jetstreamer firehose adapter now normalizes Old Faithful
CAR ranges into the shared decoder, SQLite storage and coverage contracts.
[Measured results and limits](docs/archive.md) include legacy/v0/v1 mainnet input
and an archive freshness snapshot. RPC truncation and unsupported instructions
prevent verified bridge coverage in some sampled ranges; slot 468265451 passed
both sources with 51 deduplicated transfer records. This does not select
production infrastructure or authorize a full-window backfill.

The bounded source-boundary coordinator now persists a measured archive tip and
finalized reference, routes finite batches to archive/recent RPC, and reports
unfinished source intervals separately from the live tail. Scheduling progress
is independent of verified contiguous coverage; boundary readiness requires
comparison and both durable observations. Plans freeze their endpoint and do not
claim ongoing live completeness. See [boundary coordination](docs/source-boundary.md)
for operation, limits, recovery checks and remaining production work.


Decoder v5 recognizes base authority operations and withheld-fee collection as
non-transfer activity while preserving ordinary transfer instructions in the same
transaction. Account-owner changes clear affected owner fields conservatively,
even if transaction-boundary owners agree. Fee collection does not establish an
ordinary transfer fee or a new transfer edge. The [support matrix](docs/token-2022-support.md)
and [retained-input validation](docs/validation/2026-11-12-decoder-maintenance.md)
record exact supported variants, explicit repair requirements and remaining gaps.
