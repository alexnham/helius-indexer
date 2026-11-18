# Capture payload stored once

Measured 2026-11-18 on an Apple Silicon laptop (8 CPUs available to Docker) against a
throwaway local PostgreSQL 18.6 container with no CPU limit, over loopback. These are
single opt-in runs, not a hosted or production benchmark.

## Scope

Branch `codex/payload-once` against `main` at 3328154. Before this change, a first
observation's raw payload was written to `captures` and then copied into
`capture_inputs`. Now the `capture_inputs` row references the `captures` payload with
NULL columns (migration 0013), and any write that changes a `captures` payload first
copies the old bytes back into those rows.

This is the `captures`/`capture_inputs` part of #65. Not changed:
- the protobuf copy in `live_raw`;
- the raw block in `historical_inputs`;
- long-term retention (#40).

## Command and dataset

```
INDEXER_TEST_DATABASE_URL=postgres://indexer:<password>@127.0.0.1:<port>/indexer \
INDEXER_HISTORICAL_WORKERS=8 \
<release backfill test binary> historical_block_throughput --ignored --nocapture
```

Each checkout's binary was built in its own `CARGO_TARGET_DIR`, and runs alternated
base, branch, base, branch on the same database. WAL is the change in
`pg_stat_wal.wal_bytes` across one invocation, which covers both mixes. The dataset is
32 consecutive slots of 1,000 transactions per mix, built from the captured mainnet
USDC transfer with generated signatures. In the "1 in 20" mix, the other transactions
are failed copies with the mint removed.

## Results

| Measure | `main` (2 runs) | Branch (2 runs) |
| --- | --- | --- |
| Every transaction transfers the mint, tx/s | 1,661 / 1,700 | 2,076 / 2,231 |
| 1 in 20 transfers the mint, tx/s | 10,925 / 9,128 | 11,927 / 11,825 |
| WAL per invocation (both mixes), MiB | 652 / 645 | 460 / 534 |

WAL written falls by 18–29%. Throughput rises by about 25–30% when every transaction
transfers the mint, and 10–30% on the 1-in-20 mix, where fewer payloads are stored.
