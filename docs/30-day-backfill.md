# USDC sampled archive backfill

This is a 10% transfer sample for USDC, authorized for a 30-day historical target.
The frozen slot plan can remain incomplete. It is separate from the exact index
and from the console runtime's 128-slot replay jobs.

Build the archive feature and start the existing local PostgreSQL. Use a fresh
session schema. `scripts/archive-backfill.py` wraps the existing `bridge-plan`,
`bridge` and `bridge-status` commands; it does not replace their independent
coverage acknowledgments or move their contiguous checkpoint itself.

```
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build --locked --features archive
docker compose up -d postgres
export DATABASE_URL=postgres://indexer:indexer@127.0.0.1:55432/indexer
python3 scripts/archive-backfill.py --help
```

The runner's plan command accepts a supplied start slot, or `--days 30` to probe
mainnet `getBlockTime`. Credentials and optional `SOLANA_RPC_URL` come from the
environment, never command arguments or logs. Time discovery checks the mainnet
genesis and finalized reference, then probes epoch boundaries backward until one
has a known timestamp older than the cutoff. Work starts a full epoch earlier.
Null/skipped times remain unknown, failed or exhausted probes stop discovery, and
no fixed slots-per-day calculation is used. This is conservative scheduling,
not proof of complete temporal coverage. Windowed queries still apply exact
known block-time eligibility and disclose null block times.

Configure runs with `run --config config/backfill.example.json` alongside the
usual `--binary`, `--directory`, `--session` and provider opt-in arguments. The
JSON accepts integer `slots_per_batch`, `max_batches`, `max_reserved_bytes` and
`min_free_bytes`; explicit CLI flags override file values. Unknown keys and invalid
limits fail before provider access. `max_batches` is required in the file or CLI.
The checked-in example preserves the existing defaults and requests 100 batches.
Credentials remain in environment variables, never this file.

`slots_per_batch` defaults to eight and accepts 1–10,000. For example, set it to
1,000 and `max_reserved_bytes` to at least 68,224,548,864 bytes (63.54 GiB) to
admit one worst-case batch. This reservation is deliberately pessimistic, not an
expected download size or measured recommendation. A batch of N slots may download N MiB in N/2
archive requests (at least 128 MiB and 128 requests, at most 2 GiB and 2048) within the
120-second cooperative deadline; it can stop or fail before reaching the slot maximum.
Inside a batch, the next 128-slot window downloads while the current one is written. Native historical operations remain in subranges of at most 128 slots,
with the same per-slot durable writes and independent coverage checks.

The native `bridge` worker also accepts `INDEXER_BRIDGE_BATCH_SLOTS`; the wrapper
sets it from the effective config, overriding an inherited value. Run settings
are saved as `run_config` in the manifest, while native reports/events expose the
effective batch limits. Existing sessions can resume with changed limits without
changing their frozen range, sampling policy, or binary. A session frozen with an
older binary needs a new session to use the native batch-size setting.

Each bridge batch processes at most the configured positions. The wrapper reserves the
archive limit above + (`slots_per_batch` + 1) × 64 MiB before
each batch so a crash cannot reset its charge. When the batch finishes and reports
its source response bytes, the charge drops to those bytes; failed, timed-out and
interrupted batches keep the full reservation. After a rate-limited batch (an Old Faithful
429), `run` waits 30 s, doubling to 15 minutes, or longer if `Retry-After` asks, and then
continues. It stops with `archive_rate_limited` after 12 rate-limited batches in a row.
The wait shows as `rate_limit` in `status` and on the dashboard. `run` stops at `--max-reserved-bytes`
(default 2 GiB), low free disk, a `CANCEL` file in the session directory, or a batch
that verified nothing new. Charged bytes are a scheduling allowance, not billing.
Each native command may take up to 360 s. The native worker shares a cooperative
120-second deadline with bounded 20-second RPC requests; its stderr events go to the session's `events.jsonl`.

`status` takes no lock, so it works while `run` is active. Starting `run` deletes a
`CANCEL` file left from an earlier stop and reports `cleared_stale_cancel`; create
the file again to stop a run in progress. After each finished batch `run` deletes the downloaded blocks from `archive-cache`;
captures stay in PostgreSQL, and each block keeps only its CAR receipt (repair reads the
block again from Old Faithful: `repair-history-archive ... CACHE`). A failed or
interrupted batch keeps its cache. Each block's decoded transactions are written in batches of up to 1,000
(16 MiB), not one PostgreSQL transaction each.

`INDEXER_HISTORICAL_WORKERS` (1–16, default 1; the wrapper passes its environment
through) runs that many block workers per historical run, each on its own PostgreSQL
connection. The run's thread still fetches blocks and records slot status; workers
retain the raw block (transactions that do not touch the mint are reduced to their
signatures), decode it and write its output. Slots can complete out of
order; the contiguous checkpoint covers only slots whose output is durable. At most
two blocks per worker are held in memory, so allow 128 MiB per worker on top of the
batch reservation. Revision repair always uses one worker. More workers help only
when PostgreSQL has spare CPUs: the Compose database is limited to one. A 10% transfer sample saves no raw-input bytes or network/decoding work.
Monitor the PostgreSQL disk as well as the cache filesystem, especially if they
are on different hosts or volumes. The local dashboard shows actual sample
counts, remaining scope and uncertainty; it does not extrapolate a full total.

Verification: `python3 -m unittest tests.archive_window_test tests.archive_backfill_test`
and `node tests/archive_dashboard_test.js`, plus the Rust default/archive suites.
No fixture test contacts a provider. Operational measurements belong in the
validation report and must not be inferred from the configured ceilings.

Primary sources checked 2026-11-15: [Old Faithful](https://docs.old-faithful.net/)
describes completed-epoch archives and expected publication delay;
[Solana getBlockTime](https://solana.com/docs/rpc/http/getblocktime) describes
estimated timestamps and unknown/unavailable responses; the
[Triton streaming guide](https://blog.triton.one/streaming-solanas-history-the-open-path-to-backfill-indexing/)
describes whole-ledger archive ingestion. Archive access is currently public;
hosting and provider costs have not been measured for this full window.
