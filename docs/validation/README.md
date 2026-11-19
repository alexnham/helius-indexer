# Run log

Every validation, provider run and measurement, newest first. Each row links to a
dated report that holds the command, dataset, environment and full results.
**Source** says what the run touched: *provider* (LaserStream, RPC or Old Faithful),
*hosted* (remote infrastructure), *local* (containers on a laptop) or *offline*.

## Planned runs toward the 30-day backfill

| # | Run | Ticket | Proves | Status |
| --- | --- | --- | --- | --- |
| 1 | Live demo (`docker compose up`, defaults) | [#73](https://github.com/alexnham/helius-indexer/issues/73) | Live intake → Kafka → consumer → dashboard, end to end | [Done](2026-11-16-live-demo.md) |
| 2 | Bounded archive/replay overlap | [#31](https://github.com/alexnham/helius-indexer/issues/31) | Old Faithful data and the archive-to-live seam are correct | [Done](2026-11-17-provider-overlap.md): 50 of 50 slots `consistent` under decoder v7 |
| 3 | One-day 10% sample | [#72](https://github.com/alexnham/helius-indexer/issues/72) | Throughput, disk and cost per day, to size run 4 | Planned |
| 4 | 30-day backfill, one mint | [#70](https://github.com/alexnham/helius-indexer/issues/70) | The full deliverable | Needs spend approval |

## Completed runs

| Date | Run | Source | Headline result | Report |
| --- | --- | --- | --- | --- |
| 2026-11-18 | Pruned historical blocks | local | 1 in 20 transactions transfer the mint: retained raw block bytes 120 → 17 MiB (−86%), `historical_inputs` ~47 → 7 MiB on disk; throughput change within noise on a loaded laptop | [report](2026-11-18-pruned-historical-blocks.md) |
| 2026-11-18 | Capture payload stored once (#65) | local | 8 workers, uncapped local PostgreSQL: WAL −18–29%; ~1,700 → ~2,150 tx/s when every transaction transfers the mint, ~10,000 → ~11,900 tx/s when 1 in 20 does | [report](2026-11-18-capture-payload-once.md) |
| 2026-11-18 | Parallel historical block workers | local | 8 workers on uncapped local PostgreSQL: ~800 → ~1,600 tx/s when every transaction transfers the mint, ~3,300 → ~8,500 tx/s when 1 in 20 does | [report](2026-11-18-parallel-historical-workers.md) |
| 2026-11-18 | Historical block write throughput | local | Batched block writes: 97 → 723–995 tx/s when every transaction transfers the mint, 1,223 → 2,721–3,650 tx/s when 1 in 20 does | [report](2026-11-18-historical-block-throughput.md) |
| 2026-11-18 | Archive/replay overlap under v7 (#31) | provider | 50 of 50 slots `consistent`, 0 conflicts, verified through 470015999 | [report](2026-11-17-provider-overlap.md#final-run-under-decoder-v7-2026-11-18-0245-utc) |
| 2026-11-17 | Archive/replay overlap (#31) | provider | 47 of 50 slots reconciled after #105, 0 conflicts; 3 slots unknown (unsupported instruction); omissions are failed transactions (checked on one slot) | [report](2026-11-17-provider-overlap.md) |
| 2026-11-16 | Live demo, first run (#73) | provider + local | 93–117 tx/s live intake; consumer kept up at 91/s; 1.9% of replay inputs unresolved; found #99 and #100 | [report](2026-11-16-live-demo.md) |
| 2026-11-14 | Kafka consumer-group throughput | local + hosted | Loopback 1,339 tx/s with one consumer; hosted PostgreSQL 65–72 tx/s, below the ~80 tx/s USDC load | [report](2026-11-14-kafka-consumer-throughput.md) |
| 2026-11-14 | Hosted PostgreSQL TLS and latency | hosted | One writer 0.98 tx/s; batches of 100 reach 45 tx/s | [report](2026-11-14-hosted-postgres-tls.md) |
| 2026-11-14 | Archive/replay routing and overlap | offline | Routing, archive batches and reconciliation pass offline; no provider measured | [report](2026-11-14-archive-routing.md) |
| 2026-11-14 | Bounded replay jobs | offline | 121 tests passed | [report](2026-11-14-replay-jobs.md) |
| 2026-11-13 | USDC live load | provider | Mean ~80 matched tx/s (p90 ~117) over 25,445 slots; ~2 USDC transfers per tx | [report](2026-11-13-usdc-load.md) |
| 2026-11-13 | PostgreSQL review fixes | local | Default and archive suites and Docker smoke passed | [report](2026-11-13-postgres-review-fixes.md) |
| 2026-11-13 | Docker runtime, first bounded source test | provider | 60 s live intake plus a 50-slot finalized range through the shared decoder | [report](2026-11-13-docker-runtime.md) |
| 2026-11-12 | In-place revision repair | offline | v3 database repaired to v4 from a backup, no provider calls | [report](2026-11-12-revision-repair.md) |
| 2026-11-12 | Decoder v5 maintenance | offline | 82 default and 103 archive tests passed | [report](2026-11-12-decoder-maintenance.md) |
| 2026-11-12 | Decoder v4 coverage | provider | Stored edges matched independent expectations: 97/97 and 65/65 | [report](2026-11-12-decoder-coverage.md) |
| 2026-11-12 | Bounded overlap (decoder v3) | provider | 5 s LaserStream plus a 3-slot RPC range; 18 stored USDC edges | [report](2026-11-12-overlap.md) |

## Recording a run

Add `YYYY-MM-DD-<topic>.md` here and a row above, in the same PR. The report states:

- **Scope:** ticket, commit, mint, slot range or duration, and what was *not* run.
- **Command and environment:** exact command (no keys or credential-bearing URLs),
  machine, container versions, local or hosted storage.
- **Results:** a table of measured numbers. Keep unknown distinct from zero and say
  whether coverage was verified.
- **Cost:** provider credits or spend, when the run used any.
- **Gaps:** what the run does not establish.
