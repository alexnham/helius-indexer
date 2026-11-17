# Run log

Every validation, provider run and measurement, newest first. Each row links to a
dated report that holds the command, dataset, environment and full results.
**Source** says what the run touched: *provider* (LaserStream, RPC or Old Faithful),
*hosted* (remote infrastructure), *local* (containers on a laptop) or *offline*.

## Planned runs toward the 30-day backfill

| # | Run | Ticket | Proves | Status |
| --- | --- | --- | --- | --- |
| 1 | Live demo (`docker compose up`, defaults) | [#73](https://github.com/alexnham/helius-indexer/issues/73) | Live intake → Kafka → consumer → dashboard, end to end | Next |
| 2 | Bounded archive/replay overlap | [#31](https://github.com/alexnham/helius-indexer/issues/31) | Old Faithful data and the archive-to-live seam are correct | Planned |
| 3 | One-day 10% sample | [#72](https://github.com/alexnham/helius-indexer/issues/72) | Throughput, disk and cost per day, to size run 4 | Planned |
| 4 | 30-day backfill, one mint | [#70](https://github.com/alexnham/helius-indexer/issues/70) | The full deliverable | Needs spend approval |

## Completed runs

| Date | Run | Source | Headline result | Report |
| --- | --- | --- | --- | --- |
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
