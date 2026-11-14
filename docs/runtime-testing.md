# Test live ingestion and configurable replay

The opt-in `runtime` service uses PostgreSQL, live finalized LaserStream intake
and an independent recent-history replay worker in the same native process. It
never seeds fixtures. Old Faithful routing is pending in #31; this image does not
perform full-window archive backfill.

## Start and configure

Keep `HELIUS_API_KEY` in the ignored secret file (default `.env`); it is mounted
only into the runtime and passed to the child via its environment. It is never
put in argv, images or logs.

```sh
docker compose --profile live up --build -d runtime
# Open http://127.0.0.1:8767
```

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
Existing copied CLI binaries are preserved on restart; use a fresh directory
for this schema upgrade and back up PostgreSQL before migrating. Changing an
existing job's mint/range/ID is rejected. To choose a different scope, use a new
`INDEXER_SESSION` directory and a new `INDEXER_REPLAY_JOB` (or database session).
After upgrade, older binaries refuse schema 5; rollback restores a pre-upgrade
backup into a separate schema with its matching binary. See [replay jobs](replay-jobs.md).

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

`INDEXER_RUN=0 docker compose --profile live up -d --force-recreate runtime` reopens
an existing session for inspection without provider requests. Set it back to `1`
explicitly to ingest again. No new runtime has been deployed by these code changes.

## Query and check

```sh
curl -fsS http://127.0.0.1:8767/api/status
curl -fsS 'http://127.0.0.1:8767/api/transfers?mint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
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
those inputs. Full archive/replay handoff remains #31/#32.
