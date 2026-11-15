# helius-indexer

## Docker testing

```sh
docker compose up --build
```

Open **http://127.0.0.1:8765**. A dedicated named volume keeps the same captured
session across restarts; the image contains the compiled CLI, so Rust/Python do
not need to be installed on the host. No ingestion or credentials are enabled.
If the native console is already using 8765, set `INDEXER_CONSOLE_PORT=8766` before
the command. See [Docker operation and data reset](docs/docker.md).

## Start testing

```sh
python3 scripts/local-test.py
```

Open **http://127.0.0.1:8765** for the local captured-transfer console. Startup
seeds 303 unique transfers from 50 captures, checks replay, and keeps all test
data in a fresh ignored session. No provider credentials or Docker needed.
See [the local testing guide](docs/local-testing.md) for prerequisites, checks,
restart instructions and current limits. This is a read-only captured-data test
console; live and 30-day coverage remain unverified.


Offline validation slice for a Solana token transfer index. It decodes compiled
SPL Token instructions, atomically stores captured inputs and transfers in
PostgreSQL, and queries persisted results from a separate process. Implementation
starts with [ticket #1](https://github.com/alexnham/helius-indexer/issues/1).

The [proposal](PROPOSAL.md) defines the full index. Production backfill,
full-window coverage, rolling
retention, and production analytics are subsequent tickets. This CLI is a bounded
inspection tool, not a production 30-day API. Captured-query responses report coverage as
`unknown`; processing one transaction never establishes complete slot coverage.

## Recent replay and live testing

The opt-in runtime now uses LaserStream historical replay for a configurable
recent range, with durable job scope and budgets. Live intake has priority;
processing receipts survive the local newest-1,000-transfer cap. Query progress
and failures through `/api/status` and actual rows through `/api/transfers`.
Filtered replay does not prove complete slots: verified coverage remains unknown.
See [runtime testing](docs/runtime-testing.md) and [replay jobs](docs/replay-jobs.md).
Old Faithful routing and production 30-day retention remain pending.

## Run the captured example

Requires Rust/Cargo (declared minimum Rust 1.89) and PostgreSQL. The bundled
loopback container is enough (`docker compose up -d postgres`). No provider account
or API key is needed. Every command takes a SESSION, a schema name such as `local`,
in place of the former SQLite file; the server comes only from `DATABASE_URL`, never
an argument. Existing SQLite files are not migrated: re-ingest or refetch them.
The first build fetches dependencies; the captured commands and default tests make no network
requests. The historical command below explicitly requests bounded RPC data.

```sh
docker compose up -d postgres
export DATABASE_URL=postgres://indexer:indexer@127.0.0.1:55432/indexer
cargo build --locked
mkdir -p data
cargo run --locked -- ingest local tests/fixtures/mainnet-transfer.json --metrics data/first-run.prom
cargo run --locked -- query local
cargo run --locked -- query local EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

The captured example decodes two inner USDC transfers of `23360000` base units
(23.36 USDC each). The first ingest reports `inserted: 2`; replaying the same
capture reports `inserted: 0` and `decoded: 2`. Distinct instruction positions
remain distinct even when their amounts match.

CLI syntax:

```text
helius-indexer ingest DB CAPTURE [--metrics NEW_FILE]
helius-indexer query DB [MINT] [WINDOW] [--limit N] [--cursor C]
helius-indexer query-recent DB [MINT] [WINDOW] [--limit N] [--cursor C]
helius-indexer summary DB MINT [WINDOW]
helius-indexer query DB [MINT] --all-stored     (inspection only; see below)
helius-indexer query-recent DB [MINT] --all-stored
helius-indexer summary DB MINT --all-stored
WINDOW = --window-seconds N | --from T --to T   (block times, [from, to))
```

`--all-stored` is local inspection of captured data, not a query mode: it returns
every retained row, including rows older than the 30-day window (up to 37 days old)
and rows with unknown block time, with the pre-window output shape and
`coverage: "unknown"`. It cannot be combined with `WINDOW` or `--cursor`. Served APIs
must not expose it; the local console and smoke scripts use it.

`query` and `query-recent` open existing storage read-only and return up to
`--limit` records (1 to 1,000, default 1,000), sorted by slot, signature, outer
position, and inner position (outer before inner); `query-recent` is newest first.
Without a window the rolling 30-day window applies. The server computes the cutoff
as now minus the window and returns `now`, `cutoff` and `window`. Only rows with a
known block time inside `[from, to)` are listed or summed; rows with unknown block
time are never listed and appear only as `null_block_time`. Each page returns
`next_cursor` (null on the last page) for keyset pagination; pass it back with
`--cursor`. Each response carries a `coverage` object for the window (see
[the coverage mapping](docs/observability.md#query-window-coverage)). Amounts and
fees are decimal JSON strings so clients preserve the full unsigned 64-bit range;
`summary` sums them exactly in SQL. Missing block time or historically unresolved
owners remain null; an instruction's signing authority is not assumed to be the
wallet owner. In live-only operation most rows have null block time until history
or reconcile enriches them.

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
unsupported rather than incorrectly indexing rolled-back effects. Unknown SPL opcodes and unsupported extension authority types remain unresolved.
Base authority operations and withheld-fee maintenance are recognized; see the
[Token-2022 support matrix](docs/token-2022-support.md). Owners are reported only
when both pre/post observations agree and no account-owner change makes them ambiguous;
otherwise they are null. The decoder makes no network or database calls.

A structurally valid capture that fails decoding is retained with a bounded error
class, with no partial transfer records, and the command exits unsuccessfully.
Malformed envelopes fail before persistence. Explicit [decoder repair](docs/decoder-repair.md) is available; retained failure
records do not imply verified coverage.

## Provisional persistence

PostgreSQL is the selected store (see PROPOSAL.md, 2026-11-13); SQLite was removed.
Each SESSION is its own schema on the `DATABASE_URL` server, created and migrated on
first connection (`src/storage/migrations`). Raw capture, decode status and all of a
transaction's transfers commit in one transaction at READ COMMITTED: a first
observation inserts without reading, and a repeat waits on that capture's row lock.
Transfers are range-partitioned by slot. Plaintext connections are accepted only
over loopback or a Unix socket. Any other host requires TLS (`sslmode=disable` is
refused and `prefer` is upgraded to required) with an authenticated server: a trusted
chain and matching hostname (system roots, or `INDEXER_DB_TLS_ROOT`), or, for
self-signed servers, an operator-pinned leaf fingerprint in `INDEXER_DB_TLS_SHA256`.
There is no unauthenticated mode: unlike libpq, URL `sslmode=require` here still means
authenticated TLS, and verification is configured only through `INDEXER_DB_TLS_ROOT`
and `INDEXER_DB_TLS_SHA256`. A pin is never learned automatically; certificate
rotation fails closed until the pin is updated. See `.env.example`. The opt-in
`cargo test --test hosted -- --ignored` check (needs `INDEXER_HOSTED_DATABASE_URL`)
measures latency and throughput against a hosted server in a throwaway session.

Compatible same-revision observations coalesce immediately by deterministic
transfer identity. Unknown metadata may be enriched; contradictory known values
or decoder revisions are quarantined/rejected. Raw source observations are
retained in an additive audit table. See [overlap validation](docs/overlap.md)
for compatibility, rollback, and storage-overhead limits. Explicit
[decoder revision repair](docs/decoder-repair.md) can add, change, or remove
semantic transfers from retained inputs. Keep operational databases outside
version control.

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

Kafka tests need the local broker (loopback port 59092; each test uses its own topic).
Building needs `cmake`, which compiles the vendored librdkafka. Storage tests need the local PostgreSQL container (loopback port 55432; each test
uses its own schema). Set `INDEXER_TEST_DATABASE_URL` to use another local server.
This makes no provider calls; never point tests at a hosted database.

```sh
docker compose up -d postgres kafka
cargo fetch --locked
cargo fmt --check
cargo clippy --offline --locked --all-targets -- -D warnings
cargo test --offline --locked
```

CI performs these checks without provider credentials, using a PostgreSQL service container. Tests cover the captured
transaction, synthetic boundary cases, exact u64 serialization, malformed data,
unsupported execution, versioned addresses, duplicate ingestion, separate-process
querying, atomic rollback under a failed database write, retained decode failures,
and telemetry/redaction/export failures.

## Bounded historical range

```sh
# Public mainnet RPC is the default; SOLANA_RPC_URL optionally selects an existing HTTPS endpoint.
# Keep credential-bearing URLs in environment variables, never arguments or committed files.
cargo run --locked -- backfill history EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447 --metrics data/history.prom
cargo run --locked -- coverage history EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447
cargo run --locked -- query history EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
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
Durable output acknowledgment precedes coverage acknowledgment. A crash after raw/output
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

Recovery commands, process-exit validation, and remaining outage-repair limits are
documented in [the recovery runbook](docs/recovery.md).

Decoder revisions can be repaired offline with `repair DB START END`, inspected
with `repair-status DB START END`, and historical coverage reverified with
`repair-history DB MINT START END` using retained full blocks only. Back up before
in-place correction; successful replacement removes obsolete output. See
[decoder repair](docs/decoder-repair.md) for the full procedure, partial-query
semantics, input bounds, and rollback requirements.

## Archive adapter

The optional `archive` feature provides bounded Old Faithful reads, offline CAR
replay, archive/RPC overlap checks and a measured tip snapshot. See
[commands, limits, provenance and actual validation gaps](docs/archive.md).
The archive feature requires Rust 1.98.1+ and adds substantial Agave dependencies;
the default build remains the smaller offline validation tool.

The [source-boundary coordinator](docs/source-boundary.md) adds `bridge-plan`,
`bridge`, `bridge-status` and `bridge-live`: a frozen measured boundary, eight-slot
resumable batches, source-specific unfinished intervals and an unverified live
tail. Create a plan explicitly before running either worker command.


Decoder v5 supports base authority changes and withheld-fee collection without
losing unrelated transfers. Use `repair-history-archive SESSION MINT START END` for
explicit offline re-verification of normalized archive receipts already retained
in the session. It needs no CAR cache or optional SDK feature. See the
[decoder maintenance validation](docs/validation/2026-11-12-decoder-maintenance.md)
and back up the database before in-place repair.

The optional archive feature also supports
`archive-overlap-replay DB MINT START END` for bounded offline recovery of explicit
RPC log truncation from complete matching archive evidence. Both originals remain
retained; native RPC stays incomplete and reports expose corroborated readiness.
See [RPC evidence recovery](docs/rpc-evidence-recovery.md) for limits and rollback.

RPC source reports now include consumed response-body bytes and request/read/
oversize failures, separately from normalized input and stored transfer counts.
These measurements include small reference/genesis responses and partial reads;
they are not provider-billed or full wire bytes. See [telemetry](docs/observability.md#rpc-response-body-accounting).

For **real provider-backed live intake and configurable backfill**, use the
[opt-in runtime console](docs/runtime-testing.md):
`docker compose --profile live up --build -d runtime`, then open
http://127.0.0.1:8767. It needs a local Helius secret, starts a 50-slot backfill
alongside live intake, then continues live intake in 60-second cycles, and keeps only the newest 1,000
transfers; it does not seed fixtures.


Decoder v6 supports identifiable current-layout confidential Token-2022 transfers
with null hidden amounts/fees and optional decimals. Summary responses disclose
unknown-amount and confidential-transfer counts. See the [support boundary and
reader/repair compatibility](docs/token-2022-support.md#revision-v6-confidential-transfer-queries).
