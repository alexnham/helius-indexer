# Test live ingestion and configurable replay

The default `runtime` service (`docker compose up`) uses PostgreSQL, live finalized LaserStream intake
and an independent historical worker in the same native process: LaserStream
replay for recent slots and, in the default `console-archive` image, Old Faithful
archive batches for older routed slots (#31, #32). It never seeds fixtures or
performs a full-window backfill.

## Start and configure

Keep `HELIUS_API_KEY` in the ignored secret file (default `.env`); it is mounted
only into the runtime and passed to the child via its environment. It is never
put in argv, images or logs.

```sh
docker compose up --build -d
# Open http://127.0.0.1:8765
```

Compose also starts Kafka and a `consumer` service. Live input is
published to Kafka (`kafka:9092` on the private Compose network, plaintext under
`INDEXER_KAFKA_PRIVATE_NETWORK=1`) and the consumer writes it to the same PostgreSQL
session; historical replay keeps writing directly. Outside this Compose network, brokers
must still be loopback. Check consumer progress with `docker compose logs consumer`.

Defaults are USDC, 60-second live cycles, an initial frozen 50-slot finalized
range and job ID `recent`. Set `INDEXER_MINT`, `INDEXER_BACKFILL_SLOTS` or explicit
`INDEXER_BACKFILL_START` / `INDEXER_BACKFILL_END` before first startup. Each scope
is limited to 128 slots. `INDEXER_REPLAY_JOB` identifies the durable replay job.
`INDEXER_LIVE_SECONDS` accepts 1–300; historical network intake is capped at 60
seconds per attempt. Actual replay availability is checked before each subscription.
The small RPC genesis/reference probe chooses the initial scope; RPC is not the
normal historical transaction source.

The session directory is `/app/data/runtime-replay`, separate from legacy
`/app/data/runtime`. Old directories are preserved and not silently converted.
Existing copied CLI binaries are preserved on restart. A rebuilt image is not
adopted implicitly: when its binary differs from the session's copy, the runtime
serves the session for inspection only and reports worker state
`binary_upgrade_required`, without migrating or ingesting. To upgrade, stop the
runtime, back up PostgreSQL (`pg_dump` of the session schema), then start once with
`INDEXER_UPGRADE_BINARY=1`: the old copy is kept as `helius-indexer.backup-<ns>`, the
new one replaces it atomically, and migrations run. The database schema is not
rolled back with the binary; see the rollback note below. Changing an
existing job's mint/range/ID is rejected. To choose a different scope, use a new
`INDEXER_SESSION` directory and a new `INDEXER_REPLAY_JOB` (or database session).
After upgrade, older binaries refuse schema 5; rollback restores a pre-upgrade
backup into a separate schema with its matching binary. See [replay jobs](replay-jobs.md).

## Archive routing and batches

With archive enabled, on its first cycle and later only when replay fell out of its
window, the archive tip was unknown, or uncovered slots are still unverified (a newly
published archive epoch may now serve them), the runtime runs `replay-route` to
measure replay availability and, if replay cannot serve the whole job, the archive
tip. Attempts, including failed ones, are at most hourly. A rebuilt binary is
detected before any credential read or provider call. Each `replay-monitor` cycle then runs retry, replay, one archive batch and
an RPC fill of the measured archive-to-replay gap, then overlap reconciliation
under live priority ([replay jobs](replay-jobs.md)). Set
`INDEXER_ARCHIVE=0`, or build `INDEXER_RUNTIME_TARGET=console`, for replay only.

The archive cache is `archive-cache` in the session directory (mode 0700, on the
`runtime-data` volume). It holds downloaded archive ranges bounded by the per-batch
limits; deleting it loses only cached downloads, never output, receipts or charged
budgets. Archive work stops at its allowance (4 batches per grant) until an explicit
`archive-grant`. This session layout is testing state; PostgreSQL (#35) holds the
durable records.

The console shows the archive as its own source beside live and replay: route
revision and measurement age, uncovered intervals, archive probe failures, complete
and pending archive slots, remaining batches and attempts by status. The replay
source line shows the verified-through slot and archive/overlap/unknown counts.

The Pipeline tab's **Live health** panel shows the mint, worker state and cycle, the
latest received slot, the finalized reference and its age, the received-slot distance
to that reference, the live input rate over the stated window, unresolved inputs,
replay margin and the dataset's sampling. Received-slot distance is not verified lag;
verified freshness and slot gap stay unknown because no live slot is verified complete.

The **Events** tab lists the newest 100 meaningful events (connection, backfill,
restart, storage and error transitions) from the last 256 KiB of the session's
`events.jsonl`, filtered by live, backfill, storage or errors. Per-slot and per-stage
records are omitted. The runtime appends `runtime_started` on each start and
`worker_retrying` before its backoff. Transfer rows link to Solana Explorer (mainnet).

## Restart, budgets and retention

Restart retains the job's original range and consumed allowance. Live intake starts
near the current finalized tip unless `INDEXER_LIVE_FROM` is explicitly provided;
subsequent cycles resume inclusively from durable live input. This live scheduling
position is not a complete checkpoint. The stopped interval is not automatically
certified or repaired by the old job's scope.

One grant permits 60 reserved network seconds, 10,000 transaction messages, 64 MiB
of processed replay wire payload and three connection attempts. It is never reset
by a live cycle or process restart. Expired availability, exhausted budgets and
decode failures remain visible while live continues. Explicitly authorize another
bounded allowance with `replay-grant`; the runtime does not auto-refill it.

After both workers end a cycle, pruning keeps the newest `INDEXER_KEEP_TRANSFERS`
(default 1,000) across all mints. Counts can exceed the cap during a cycle. Compact
job/processing receipts survive raw/output deletion; retained coverage stays
unknown. This is not a disk-byte cap or the production 30-day retention policy.
A live-source failure backs off 30 seconds. History failure alone does not delay
the next live cycle. Shutdown terminates and, if needed, kills/reaps its child.

`INDEXER_RUN=0 docker compose up -d --force-recreate runtime` reopens
an existing session for inspection without provider requests. Set it back to `1`
explicitly to ingest again. No new runtime has been deployed by these code changes.

## Query and check

```sh
curl -fsS http://127.0.0.1:8765/api/status
curl -fsS 'http://127.0.0.1:8765/api/transfers?mint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
docker compose exec runtime /app/data/runtime-replay/helius-indexer replay-status runtime recent
docker compose exec runtime /app/data/runtime-replay/helius-indexer live-status runtime
```

Check these independently:

- `worker.report.live` and `live.runs`: actual live connection/termination state.
  Process health and raw arrival do not prove verified freshness. Live counters
  exclude historical replay input.
- `historical`: frozen start/end, attempt history, availability age/margin, failures,
  pending age, remaining allowance and unique/decoded/unresolved receipts. A
  later-slot observation is not range completion.
- `historical.retained`: actual queryable transfer count in that mint/range, separate
  from processing receipts. Zero retained rows does not mean an empty indexed range.
- `summary` and `/api/transfers`: deduplicated retained output, exact integer amounts,
  separate fees and null unknowns. Summary currently spans stored history rather
  than a production rolling time window.

Both live and replay verified coverage remain unknown without independent full-slot
evidence. Replaying the same input must not increase unique transfer identities;
new live transactions can raise totals. Block times and historical owner/fee state
may remain unknown. A manual signature/position/amount comparison is a spot check,
not proof that the complete range was received.

Use [replay job commands](replay-jobs.md) for offline retry/refill and the separate
[archive validation](archive.md) for older data. The former independent block
parser requires retained full block observations; filtered replay does not fabricate
those inputs.

## Isolated sampled demo

`INDEXER_SAMPLE_PERCENT=N` (1–100) makes a fresh runtime directory and PostgreSQL
session (`INDEXER_DB_SESSION`) a single-mint dataset keeping N% of transfers; see
`PROPOSAL.md`. The
policy is set before any input is admitted and saved with the session, so a
restart keeps it. Startup refuses to convert a populated exact session, change the
sampled mint or percentage, or restart a sampled session as exact. Query, summary and console
output label the policy and show the actual sampled count; coverage still
describes source evidence. Sampling runs after decoding: every raw input is kept
for retries and decoder corrections and the newest-transfer prune does not run, so
only transfer-row storage shrinks.
