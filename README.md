# helius-indexer

Offline validation slice for a Solana token transfer index. It decodes compiled
SPL Token instructions, atomically stores captured inputs and transfers in
SQLite, and queries persisted results from a separate process. Implementation
starts with [ticket #1](https://github.com/alexnham/helius-indexer/issues/1).

The [proposal](PROPOSAL.md) defines the full index. Live ingestion, historical
backfill, verified slot coverage, Token-2022, confidential transfers, rolling
retention, and production analytics are subsequent tickets. This CLI is a bounded
inspection tool, not a production 30-day API. Its responses report coverage as
`unknown`; processing one transaction never establishes complete slot coverage.

## Run the captured example

Requires Rust/Cargo (declared minimum Rust 1.89) and a C compiler for bundled
SQLite. No provider account, API key, Docker, or running database is needed.
The first build fetches dependencies; the application and tests make no network
requests.

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
unsupported rather than incorrectly indexing rolled-back effects. Token-2022,
unknown SPL opcodes, and authority-change transactions are explicitly unsupported
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

Identical result replay is a no-op, independent of capture source metadata.
Conflicting results or decoder revisions for the same signature are rejected;
the later correction ticket supplies deliberate replacement/removal semantics.
No record silently overwrites a different transaction result. Current capture and
transfer tables are the initial schema; production migration/version policy is
not established. Database files and raw captures may contain public account data;
keep operational databases outside version control.

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
