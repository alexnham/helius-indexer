# helius-indexer

Offline validation slice for a Solana token transfer index. It decodes compiled
SPL Token instructions, atomically stores captured inputs and transfers in
SQLite, and queries persisted results from a separate process. Implementation
starts with [ticket #1](https://github.com/alexnham/helius-indexer/issues/1).

The [proposal](PROPOSAL.md) defines the full index. Production backfill,
full-window coverage, confidential transfers, rolling
retention, and production analytics are subsequent tickets. This CLI is a bounded
inspection tool, not a production 30-day API. Captured-query responses report coverage as
`unknown`; processing one transaction never establishes complete slot coverage.

## Run the captured example

Requires Rust/Cargo (declared minimum Rust 1.89) and a C compiler for bundled
SQLite. No provider account, API key, Docker, or running database is needed.
The first build fetches dependencies; the captured commands and default tests make no network
requests. The historical command below explicitly requests bounded RPC data.

```sh
cargo build --locked
mkdir -p data
cargo run --locked -- ingest data/index.sqlite tests/fixtures/mainnet-transfer.json --metrics data/first-run.prom
cargo run --locked -- query data/index.sqlite
cargo run --locked -- query data/index.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

The captured example decodes two inner USDC transfers of `23360000` base units
(23.36 USDC each). The first ingest reports `inserted: 2`; replaying the same
capture reports `inserted: 0` and `decoded: 2`. Distinct instruction positions
remain distinct even when their amounts match.

CLI syntax:

```text
helius-indexer ingest DB CAPTURE [--metrics NEW_FILE]
helius-indexer query DB [MINT]
```

`query` opens existing storage read-only and returns up to 1,000 records, sorted
by slot, signature, outer position, and inner position (outer before inner).
Pagination, time filtering, and aggregates arrive in the analytics ticket. Amounts
and fees are decimal JSON strings so clients preserve the full unsigned 64-bit
range. Internally they are integers. Missing block time or historically unresolved
owners remain null; an instruction's signing authority is not assumed to be the
wallet owner.

## Capture contract and support boundary

Input is one UTF-8 JSON capture, limited to 16 MiB, with this envelope around the
`result` from a `getTransaction` call using `encoding: json` and finalized
commitment:

```json
{
  "schema_version": 1,
  "cluster": "mainnet-beta",
  "commitment": "finalized",
  "source": "solana-mainnet-rpc",
  "result": { "...": "complete getTransaction result" }
}
```

The example above shows the envelope, not a runnable transaction. Source may also
be `fixture`. The envelope attests to capture provenance; the offline tool does
not independently verify consensus finality, signatures, or transaction validity.
Use trustworthy captures. Public source provenance and sanitization are documented
in the [fixture notes](tests/fixtures/README.md).

Supported: legacy and version 0/1 compiled JSON transaction shapes; address lookup
keys resolved as static + loaded writable + loaded readonly; classic SPL Token
`Transfer` and `TransferChecked` at outer/inner positions. Unchecked transfers
require consistent mint/decimal information from token balance metadata. Checked
transfers supply their own mint/decimals, checked against available balances.
Amounts come from instructions, never inferred balance deltas. Failed transactions
produce no transfers. Zero/self transfers are preserved. Classic SPL transfers
have a known zero transfer fee and `confidential: false`.

Inner instruction metadata must be present. Transactions containing inner SPL
Token instructions also require matching runtime invocation/success evidence.
Truncated/missing evidence or any caught CPI failure leaves the transaction
unsupported rather than incorrectly indexing rolled-back effects. Unknown SPL opcodes and authority-change transactions are explicitly unsupported
in this slice. Owners are reported only when both pre/post observations agree;
otherwise they are null. The decoder makes no network or database calls.

A structurally valid capture that fails decoding is retained with a bounded error
class, with no partial transfer records, and the command exits unsuccessfully.
Malformed envelopes fail before persistence. Decoder correction/retry scheduling
is a later ticket; retained failure records do not imply verified coverage.

## Provisional persistence

SQLite is an embedded local validation store, chosen to demonstrate real durable
writes and restart behavior without committing to production infrastructure.
Bundled SQLite keeps local and CI behavior consistent. Raw capture, decode status,
and all transfers commit in one transaction, with WAL and `synchronous=FULL`.
Writes wait at most five seconds for contention.

Compatible same-revision observations coalesce immediately by deterministic
transfer identity. Unknown metadata may be enriched; contradictory known values
or decoder revisions are quarantined/rejected. Raw source observations are
retained in an additive audit table. See [overlap validation](docs/overlap.md)
for compatibility, rollback, and storage-overhead limits. Decoder corrections
that add/remove/change semantic transfers remain ticket #8. Keep operational
databases outside version control.

## Telemetry

[The observability contract](docs/observability.md) governs implementation.
Ingestion emits JSON stage events to stderr with UTC epoch-millisecond timestamps,
run correlation ID, stage, outcome, decoder version, and slot when known. Errors
use bounded classes rather than payloads, supplied paths, or raw database errors.

Optional `--metrics` writes a Prometheus text snapshot for this invocation:
input bytes, decoded transfer attempts, stage transaction outcomes, decode failures,
and duration histograms. Counts include replay attempts, not unique stored rows.
Input bytes measure capture-file bytes, not provider-billed bandwidth. Run IDs,
slots, signatures, mints, and owners are excluded from metric labels.

Snapshots use a **new file** per invocation and never overwrite existing files,
including captures/databases. Export failure is logged and does not undo successful
ingestion. Stderr failure is best-effort and cannot abort durable writes. Histogram
buckets are preliminary local diagnostics, not measured production targets. A
long-running exporter, trace backend, live lag gauges, dashboards, and operational
alerts arrive with the corresponding pipeline stages.

## Verify

```sh
cargo fetch --locked
cargo fmt --check
cargo clippy --offline --locked --all-targets -- -D warnings
cargo test --offline --locked
```

CI performs these checks without provider credentials. Tests cover the captured
transaction, synthetic boundary cases, exact u64 serialization, malformed data,
unsupported execution, versioned addresses, duplicate ingestion, separate-process
querying, atomic rollback under a failed database write, retained decode failures,
and telemetry/redaction/export failures.

## Bounded historical range

```sh
# Public mainnet RPC is the default; SOLANA_RPC_URL optionally selects an existing HTTPS endpoint.
# Keep credential-bearing URLs in environment variables, never arguments or committed files.
cargo run --locked -- backfill data/history.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447 --metrics data/history.prom
cargo run --locked -- coverage data/history.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447
cargo run --locked -- query data/history.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

Compatible stream/RPC observations can share a validation database. Use the
combined command below when write priority matters; contradictory semantic
values remain errors rather than being silently overwritten.

`backfill DB MINT START END [--metrics NEW_FILE]` processes an inclusive range of
at most 128 slots with one blocking request/transaction at a time. Each response
is limited to 64 MiB, each normalized transaction to 16 MiB, and each request to
20 seconds. JSON parsing adds bounded expansion beyond response byte size; no
production memory or throughput target is claimed. HTTPS is required, redirects
are disabled, and the endpoint's genesis hash must identify mainnet. Each unfinished
slot gets at most one attempt per invocation; rerun the same command to retry.
Create the file named by `BACKFILL_CANCEL_FILE` to stop between slots or
transaction writes. The first slot can perform two requests (genesis and block),
so cancellation can wait up to 40 seconds initially, then 20 seconds per fetch.
Process termination
also leaves unfinished work recoverable. Already completed slots are not fetched.
A cancelled run or an incomplete range returns a diagnostic JSON report with
`coverage.complete: false`; successful command execution does not imply readiness.

Historical coverage is separate from transaction intake and is scoped to mint,
decoder revision, and slot. `coverage DB MINT START END` reports every requested
slot, errors, attempts, block time, pending count/age and the contiguous verified
boundary. `complete` means a full finalized block was decoded and all output
writes acknowledged; it includes no-match blocks. `skipped` requires a finalized
child's parent-slot evidence. Missing blocks and RPC skipped/snapshot errors stay
`unknown`. Trailing skipped slots need a produced child in the requested range
before they can be verified. Quiet mints are therefore distinguishable from
unavailable history. New mint scopes start unknown. The generic transaction
`query` command still reports global coverage unknown; use the explicit scoped
`coverage` report for this bounded historical dataset.

All block transactions are decoded and successful results are stored, including
other mints; mint selection scopes readiness and query filtering, not RPC traffic.
An unsupported unrelated transaction conservatively blocks that entire slot.
The historical tables retain full raw responses, receive times, source kind,
attempt numbers and unresolved slot error classes. Failed candidates remain in
those raw blocks rather than `captures`; enriched source metadata can repair them
on retry. Successful effects keep their identity and known semantic values; compatible
unknown metadata can be enriched. Decoder revision correction/removal semantics
remain outside this slice.
SQLite acknowledgment precedes coverage acknowledgment. A crash after raw/output
writes leaves coverage unknown; restart reuses durable raw input after interruption
and deduplicates output. Missing/unsupported metadata is refetched on retry.
Storage schema additions are compatible with existing captures/transfers; older
binaries ignore them. Recovery is rerunning the bounded range, with no destructive
migration. Raw block cleanup remains manual in this validation slice.

Historical JSON events report retries, gaps and durable completion after writes.
Metric snapshots report measured input bytes (not billed bytes), decode attempt
counts, failures, retries, source/write errors, elapsed time, observed decode rate,
scoped coverage validity and durable pending backlog/age. An unscheduled pending
age is omitted rather than reported as zero. No slot, mint or signature is a metric
label; telemetry failure cannot roll back data writes. Rates describe this invocation
and are not a benchmark. Source/overlap evidence and limitations are recorded in
[historical source validation](docs/historical-source.md). All default tests remain
offline; commands above explicitly opt into a bounded RPC request.

## Bounded live intake

A credential-configured opt-in LaserStream command now retains raw protobufs and
decodes USDC candidates. See [live validation](docs/live-ingestion.md) for bounds,
commands, verified provider sources, retry behavior and acceptance gaps. Queries
include live-run diagnostics; finalized filtered intake alone leaves coverage
unknown. A bounded credentialed check and its coverage limitations are recorded
in [validation results](docs/validation/2026-11-12-overlap.md); production readiness
is not established.

Token-2022 supported transfers and historical fee evidence are described in
[the support matrix](docs/token-2022-support.md).

## Combined overlap validation

`overlap DB START_SLOT END_SLOT SECONDS` runs the bounded USDC live and historical
workers with shared live-priority write admission. `summary DB MINT` reports exact
unique-row counts and known volume/fees for all stored data. It does not apply the
future production rolling window. Read [the overlap contract](docs/overlap.md)
for opt-in commands, verified scope, tests, and remaining coverage gaps.
