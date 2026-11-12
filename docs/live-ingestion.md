# Bounded live validation

Verified 2026-11-11 against primary documentation and the published Rust sources.
This is a local validation slice for issue #2; production readiness is pending.

## Provider contract

- Circle's [USDC addresses](https://developers.circle.com/stablecoins/usdc-contract-addresses)
  identify mainnet Solana USDC as
  `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`.
- Helius [mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering)
  requires `match_mints=true`: plain account inclusion misses unchecked transfers
  whose account lists do not mention the mint. Matching includes pre/post token
  balance mints and is honored in replay. It delivers activity candidates,
  including non-transfer activity; the shared decoder decides transfer semantics.
- [Current SDKs](https://www.helius.dev/docs/laserstream/clients) support this flag
  in Rust 0.6.4+. The inspected current `helius-laserstream` 2.0.0 uses
  [`laserstream-core-proto` 14.0.0](https://docs.rs/crate/laserstream-core-proto/14.0.0/source/proto/solana-storage.proto). Its protobuf `Message.config` identifies v1;
  `versioned` without config identifies v0; otherwise legacy. Known protobuf fields
  survive retention even when normalization/decoding fails; unknown fields are
  not preserved by Prost re-encoding.
- [Delivery guarantees](https://www.helius.dev/docs/laserstream/delivery-guarantees)
  and [the FAQ](https://www.helius.dev/docs/faqs/laserstream) do not supply a
  validated per-filter finalized completion count. Do not assume finalized slot
  notifications prove transaction delivery complete. The FAQ and ordering table
  differ in their wording; this implementation requires neither ordering claim.
- [Replay](https://www.helius.dev/docs/laserstream/historical-replay) is approximately
  48 hours; actual `SubscribeReplayInfo.first_available` is checked before every
  subscription. A requested position older than availability is rejected rather
  than silently skipped. `getSlot(finalized)` records an independent reference.
- [Plans](https://www.helius.dev/docs/billing/plans) and
  [credits](https://www.helius.dev/docs/billing/credits#laserstream-grpc) describe
  paid mainnet access and metered streaming. A bounded credentialed check is
  recorded in the overlap validation results; provider price/capacity was not
  benchmarked. Measured protobuf bytes are not billed bytes.

The implementation uses the generated Helius protobuf client directly with
Tonic TLS, rather than the SDK's automatic reconnect loop: SDK 2.0.0 resets its
retry counter after a successful connection and can reconnect silently. Owning
three explicit subscription attempts gives bounded retry events and replays from
the requested start, never an in-memory cursor. `convert`/cuckoo features are
excluded; no Solana runtime or database service is introduced. Prost re-encodes
known wire fields with an explicitly stored schema revision, Tokio supplies deadlines, and a capacity-one request channel
handles subscription/ping messages. H2 receive windows are 64 KiB, adaptive growth
disabled, and decoded messages are capped at 16 MiB. Intake processes one message
at a time; there is no unbounded transaction/telemetry queue.

## Opt-in command

```sh
# Set HELIUS_API_KEY securely in the process environment; do not put it in argv.
INDEXER_LIVE_OPT_IN=1 cargo run --locked -- live data/live.sqlite START_SLOT 30
cargo run --locked -- live-status data/live.sqlite
cargo run --locked -- live-retry data/live.sqlite
cargo run --locked -- query data/live.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

Compatible sources can now share a validation database; use the combined overlap
command when coordinated write priority is needed.
Replace START_SLOT with an explicitly chosen replayable slot. Time must be 1–300
seconds; intake stops at 10,000 received transactions even if time remains.
Subscription attempts stop at three; connection timeout is five seconds,
unary request timeout ten seconds, and stream inactivity timeout ten seconds.
The overall asynchronous deadline also covers connect/replay/reference work.
A synchronous SQLite operation can extend shutdown by its bounded five-second
busy wait. Ctrl-C/process termination closes the connection; committed raw inputs
remain retryable. No network is opened by ordinary commands/tests. A fixed
mainnet endpoint avoids accepting credential-bearing URLs or arbitrary networks.

Each decoded wire transaction is re-encoded with the pinned protobuf schema and
commits to `live_raw` (including the schema identifier) with status `pending` before parsing.
Normalized capture and decoded transfers then use the existing atomic Store.
Only afterward does raw status become `decoded`. A crash between those commits
repeats decoding and existing stable transfer IDs deduplicate results immediately.
Failures retain raw bytes and bounded classes; `live-retry` retries at most 100
unresolved items without a provider. A durable scheduling cursor rotates through
unresolved IDs across calls/restarts so an unsupported oldest batch cannot hide
later recoverable input. It is manual bounded recovery, not a durable supervisor.
See [the recovery runbook](recovery.md) for restart order, cursor semantics, and
verified failure boundaries. A stored schema identifier that differs from the pinned decoder is
rejected without interpreting the payload. Preserve the SQLite database; the stream cursor alone is insufficient.
The additive `live_raw`/`live_runs` tables preserve existing offline tables;
rollback consists of using the previous binary (which ignores the new tables).
No automatic deletion or production migration policy is added.

## Coverage and telemetry limits

`query` includes a `live` diagnostic report; `live-status` shows up to 100 recent
runs with start slot, scoped mint/decoder, replay availability, finalized reference
and observation times, unresolved raw items, and termination reason. A run left
`running` after a crash is unfinished, never complete. Filtered output cannot
prove an empty slot or distinguish a skipped slot from a missed transaction.
Accordingly verified boundary and freshness are null, coverage is unknown, and
coverage-valid is zero even when transactions were successfully stored. A quiet
stream and provider failure have distinct transport events, but neither is
reported as an observed empty range. This acceptance criterion requires future
independent finalized block corroboration (historical verification), and remains
open rather than guessed. There is no verified contiguous checkpoint to advance.

Stream transactions have no block time. Normalized captures retain null time;
raw protobuf remains sufficient to re-normalize after corroboration supplies
historical block time. Compatible RPC observations now coalesce and can enrich the null timestamp;
contradictory known identity or effect values remain quarantined conflicts.
See [overlap validation](overlap.md) for exact compatibility and scope. Raw source
inputs remain available. This does not establish full live-window coverage or
automatically change decoder revisions. Explicit
[offline decoder repair](decoder-repair.md) replaces retained capture outputs;
raw protobuf acknowledgments are retried separately.

Both sources persist the same full supported transaction record set, including
related mints; the USDC subscription selects candidates and query filters select
returned assets. This avoids source-dependent record sets.

JSON stderr events share run correlation, source context, timestamps and bounded
error classes with terminal command failures. They cover startup/retry, connection end/error, unresolved decoding
or writes, shutdown, and bounded metric snapshots. Snapshots expose input protobuf
bytes, intake attempts, unresolved attempts, explicit reconnect attempts, elapsed
seconds and coverage validity. Reports include reference/replay observation ages and replay margin; freshness and verified slot-gap remain unknown. No raw
provider errors, credentials, payloads, signatures, or URLs enter diagnostics.
No exporter/backend is required and no telemetry queue grows with transaction
count. Full stage histograms, continuously refreshed reference/replay gauges,
backlog age, dashboards/alerts and independent completeness checks remain pending.
