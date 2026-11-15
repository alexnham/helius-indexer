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

Each bridge batch processes at most eight positions, under its existing 128 MiB
archive and nine 64 MiB RPC response ceilings. The wrapper reserves 704 MiB before
each batch, retains that charge across crashes, and stops at its total allowance
or low free disk. A reservation is a conservative upper bound, not observed
traffic or billing; actual diagnostics and an unknown failed-attempt result stay
separate. Raw archive cache, normalized blocks and captures are all retained for
repair. A 10% transfer sample saves no raw-input bytes or network/decoding work.
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
