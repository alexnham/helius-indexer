# Test provider-backed indexing

The `runtime` service ingests real finalized mainnet transactions. It never seeds
fixtures and uses its own named `runtime-data` volume, separate from the captured
console. Each intake cycle is bounded; the service repeats cycles until stopped.
This is a local test runtime, not an unattended production indexer.

## Start

Put `HELIUS_API_KEY=...` in an ignored local secret file. Do not put credentials in
commands or commit them. The existing untracked `.env` is the default secret file;
only the key is read. Compose grants that file only to the runtime service, never
to the captured console or build. The key is supplied to the child process via
its environment, never argv. The runtime uses Helius mainnet RPC and LaserStream.

```sh
# Starts only the provider-backed service; does not replace the captured console.
docker compose --profile live up --build -d runtime
# Open http://127.0.0.1:8767
docker compose logs --tail 30 runtime
```

Defaults: USDC, 60 seconds of live intake, 50 consecutive finalized historical
slots ending at an independently observed mainnet finalized reference. A slot is
a position in the chain, not one transaction; 50 slots represent roughly 20 seconds.
The stream begins four slots before that reference. The two workers share the
existing write admission gate so waiting live writes take priority.

Configure environment variables in the ignored `.env` or command environment:

```sh
INDEXER_MINT=So11111111111111111111111111111111111111112 \
INDEXER_LIVE_SECONDS=120 INDEXER_BACKFILL_SLOTS=50 \
  docker compose --profile live up -d --force-recreate runtime

# Explicit inclusive history, independent of the recent stream starting position:
INDEXER_BACKFILL_START=START_SLOT INDEXER_BACKFILL_END=END_SLOT \
  docker compose --profile live up -d --force-recreate runtime
```

Mint addresses must decode to 32 bytes. Duration is 1–300 seconds; history is
1–128 slots. An explicit start overrides the slot count. End defaults to the
finalized reference and must not be newer. `INDEXER_LIVE_FROM` optionally selects
a replay position; LaserStream validates actual replay availability. Historical
fetching is serial, with at most one request per requested slot plus genesis
validation, 64 MiB per response, existing 20-second request deadlines and no
perpetual retry loop. At most 10,000 stream transactions are received per cycle. Each intake process
has a hard process timeout at duration + 60 seconds; synchronous in-flight work
can exceed the nominal duration. Fifty large blocks can consume substantial
bandwidth/storage; the theoretical response ceiling is 3.125 GiB, not an expected
usage estimate. Container memory/CPU/process limits remain explicit in Compose.

Ingestion is continuous until the service is stopped. The first cycle runs the
live stream plus the configured backfill; each later cycle runs live intake only
for `INDEXER_LIVE_SECONDS`, resuming from the last received slot (LaserStream
replay covers the reconnect). A failed cycle waits 30 seconds, then retries.
Counts can temporarily exceed the cap while a cycle is ingesting.
After every cycle the runtime keeps only the newest `INDEXER_KEEP_TRANSFERS`
(default 1,000) transfers across all mints and deletes raw inputs, captures and
historical progress for older slots. Pruned slots, including a partially retained boundary slot, report `unknown`,
never `complete`. Older deleted raw data needs backup or refetch for repair.
Evidence still retained at the boundary can be reverified, potentially restoring
rows that a later prune removes again. At USDC volume 1,000 transfers is only a few seconds of chain activity, so
the initial backfill is usually pruned after the first cycle. SQLite reuses freed
pages rather than shrinking the file. The transfer count is not a hard disk-byte cap;
lower `INDEXER_LIVE_SECONDS` to lower that peak, or `down --volumes` to reclaim it.
Continuous LaserStream usage is metered by Helius. The initial sample retained
about 54 MB of protobuf in a minute; this is not billed bandwidth or an authoritative
credit measurement. Check actual usage in the provider dashboard.
`docker compose --profile live restart runtime` starts again from a newly
measured tip. To reopen existing data without any provider request, use
`INDEXER_RUN=0 docker compose --profile live up -d --force-recreate runtime`.
This inspection mode needs an existing runtime session and retains its original
configuration/report. Return to `INDEXER_RUN=1` explicitly to ingest again. Retained output is reused and deduplicated; cycles reconnect from an observed
slot, which does not prove complete delivery. Unattended historical gap repair is
not supplied.
Restart defaults to a newly measured recent range; it does not silently claim
that the interval while stopped was indexed. Use explicit range configuration to
repair that interval. Larger/older backfills use the existing bounded archive/
bridge CLI documented in [archive](archive.md) and [source-boundary](source-boundary.md),
not an unbounded RPC scan or a full 30-day job started by this service.

## Query and check correctness

The browser refreshes every five seconds while visible. It displays the newest
1,000 rows (optionally filtered by mint), exact raw amounts, separate fees, worker
state, unique persisted count, unresolved live input and the scoped history report.
Use the address filter to locate a signature among loaded rows; it is not a search
of the whole database. Export downloads the current loaded query.

```sh
curl -fsS http://127.0.0.1:8767/api/transfers
curl -fsS 'http://127.0.0.1:8767/api/transfers?mint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
curl -fsS http://127.0.0.1:8767/api/status
# Same shared Rust query/storage contracts, without HTTP:
docker compose exec runtime /app/data/runtime/helius-indexer summary /app/data/runtime/index.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
docker compose exec runtime /app/data/runtime/helius-indexer live-status /app/data/runtime/index.sqlite
```

Check these separately:

1. **Intake:** the worker is running and retained/decoded attempts arrive. A process
   being healthy is not proof that the provider subscription succeeded. Inspect
   `worker.report.live` and `live.runs` for connection failures and termination.
2. **Output:** the unique count and newest slots advance for an active mint. Counts
   are by the configured mint; candidates may include supported related mints.
   `summary` spans all stored history, not just loaded rows or a rolling window.
3. **Failures:** `live.unresolved_items` and historical slot error classes remain
   visible. A worker can finish with an incomplete historical report.
4. **Coverage:** only complete/skipped states in `historical` verify that bounded
   mint/decoder/range. Missing/unknown slots remain incomplete. Live tail coverage
   and verified freshness stay unknown even when transfers arrive.
5. **Values:** compare sampled signatures, instruction positions, mint, amounts
   and fees against full finalized transaction data in an explorer/RPC. Unknown
   fees stay null. A live block time may remain null until compatible historical
   evidence enriches it. This manual spot-check is not an independent full-range
   reconciliation.
6. **Deduplication:** rerun the exact backfill range. Existing transfer identities
   must remain unique; later live transactions may legitimately raise totals.

An independent classic SPL USDC instruction parser can spot-check up to the first
16 retained block observations (64 MiB maximum), without another provider call:

```sh
docker compose exec runtime python3 /app/scripts/compare-captured-usdc.py /app/data/runtime/index.sqlite /app/data/runtime/index.sqlite --sample
```

It prints `expected`, `stored`, `missing`, `extra`, `wrong` and `blocks`. A successful
sample has zero missing/extra/wrong rows. This checks exact amounts and endpoints
against retained block instructions; it does not certify Token-2022, other mints,
provider completeness or the unverified live tail. A bound exceeded or no retained
blocks fails explicitly instead of reporting success.

`/healthz` checks the console and existing transfer table; it deliberately does
not claim live connectivity, zero unresolved input, or complete coverage. The raw
worker report and sanitized events are retained in the volume. No secret-bearing
provider exception or endpoint is printed by the Python supervisor.

## Stop, backup and upgrades

`docker compose stop runtime` terminates the child and preserves committed SQLite
input/output. An interrupted worker remains unverified; restart explicitly. Never
back up a live SQLite file by copying only its main file: stop the service and
preserve the whole session including WAL/SHM and raw provenance, following the
[Docker backup instructions](docker.md). Runtime data lives under `runtime/`
in the `runtime-data` volume. Do not run `down --volumes` unless deleting both
console and runtime volumes is intentional.

The session retains the CLI that created it. It is architecture specific; move
SQLite/raw data with a deliberate compatible-binary upgrade, not by overwriting
a running session. Retention deletion is intentional and irreversible without
a backup or refetch; no automatic decoder migration or production capacity claim
is added.

Provider contracts rechecked 2026-11-13 using the Helius documentation MCP:
[mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering),
[historical replay](https://www.helius.dev/docs/laserstream/historical-replay),
and [replay availability](https://www.helius.dev/docs/api-reference/laserstream/grpc/subscribereplayinfo).

See [measured runtime validation](validation/2026-11-13-docker-runtime.md) for the
initial incomplete 50-slot result, independent comparison gaps and offline checks.
