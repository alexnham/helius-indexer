# One-hour 10% USDC sample: 2026-11-18

Issue [#72](https://github.com/alexnham/helius-indexer/issues/72), first slice of run 3 in
the [planned runs](README.md#planned-runs-toward-the-30-day-backfill). One hour of chain
(9,000 slots) through LaserStream replay and Old Faithful, with a 10% transfer sample. It
was run to size the full day before committing to it.

## Scope

- USDC, slots 470439000–470447999 (the last 9,000 slots Old Faithful published), session
  `sample72_20261118`, `sample-init … 10` (policy `sha256-transfer-id-v1`).
- 71 replay jobs of 128 slots (`MAX_SLOTS`), run one after another. Each job runs
  `replay-route`, then `replay` until `end_observed` (one 60 s grant was always enough),
  then `replay-retry`, then 8-slot `replay-archive` batches until nothing is pending or
  three batches leave the pending count unchanged, then `replay-overlap`.
- Decoder `token-transfers-v7` (main at 4e737ac).
- Not run: the rest of the day, the RPC gap lane, live intake, Kafka, hosted storage.

## Command and environment

Runtime image `console-archive` built from 4e737ac, run as one-off Compose containers
against local PostgreSQL 18.6 with `INDEXER_ROUTE_ARCHIVE=always`, `INDEXER_LIVE_OPT_IN=1`
and `INDEXER_ARCHIVE_OPT_IN=1`. Apple Silicon laptop, Docker Engine 29.9.2.
2026-11-18 06:26–09:41 UTC.

A host script ran the per-job sequence and stopped before any job when the disk had under
15 GiB free (the guard never triggered).

## Results

| Measure | Result |
| --- | --- |
| Wall time | 11,610 s (3 h 14 min) for 1 hour of chain: **3.2× slower than real time**. Median job about 2.5 min (15–351 s) |
| Replay | 73 attempts, ~126k messages, 1.10 GB protobuf, 1,855 s streaming; every job `end_observed` within its first grant |
| Archive | 1,512 batches, 400 grants, 16,956 requests, 4.99 GB of Old Faithful responses |
| Inputs | 122,901 unique, 122,531 decoded, **370 unresolved (0.30%)**; all raw inputs kept |
| Unresolved by class | 292 `unsupported_token_instruction`, 73 `incomplete_execution_evidence`, 4 `missing_token_metadata`, 1 `invalid_transfer_instruction` |
| Sampled transfers kept | 22,079 (about 220k USDC transfers in the hour before sampling) |
| Overlap outcomes | **8,282 `consistent`**, 334 `pending` (archive slot left `unknown`), **0 conflicts** |
| Not reconciled | 384 slots (3 jobs): the archive probe returned `invalid_rpc_response`, so those jobs were routed replay-only |
| Contiguous checkpoint | `verified_through` reached the job end in 2 of 71 jobs; one pending slot stops it, as designed |
| Job coverage | `unknown` in every job (by design for filtered replay) |
| Database | 12.64 GB, **about 1.4 MB per slot** |

### Where the storage goes

| Table | Size | Rows |
| --- | --- | --- |
| `historical_inputs` (full archive blocks) | 5.7 GB | 9,095 |
| `capture_inputs` | 3.4 GB | ~309k |
| `captures` | 2.2 GB | ~200k |
| `live_raw` (replay inputs) | 0.56 GB | 122,494 |
| `transfer_context` | 95 MB | ~67k |
| `transfers` (sampled) | 42 MB | 22,079 |

Sampled transfers are 0.3% of the database. Retained raw input dominates, mostly whole
archive blocks, so a 10% sample does not shrink storage.

## Projection for a full day (216,000 slots)

The figures below scale this hour by 24, on the same laptop with jobs run one at a time:

| Measure | Estimate |
| --- | --- |
| Wall time | about 77 h |
| Database | about 300 GB |
| Archive egress | about 120 GB, about 410k requests |
| Replay | about 3M messages, about 26 GB |

30 days (#70) at the same rate would take about 97 days and about 9 TB. Neither fits this
machine. The full day needs one of the following:

- parallel archive workers;
- not keeping whole archive blocks after reconciliation;
- a larger host.

## Cost

Replay and archive usage are given above. Provider credits were not read from the dashboard.
No hosted infrastructure was used.

## Findings and gaps

1. **Whole archive blocks dominate storage.** At 0.63 MB per slot, `historical_inputs` is
   most of the 1.4 MB per slot. Before a full day, decide whether those blocks must be kept
   once the slot is reconciled.
2. **Throughput.** Jobs ran one after another, and archive batches inside a job also run
   one at a time (about 1 slot/s). Running jobs in parallel is the obvious lever, and the
   replay and archive allowances leave room for it. Parallel runs were not measured.
3. **Unknown archive slots stop the checkpoint.** 334 slots, 3.7%, stayed `unknown`, which
   left 69 of 71 jobs short of their end. Of those failures, 292 were
   `unsupported_token_instruction`, which is #60 work. Which instructions they are has not
   been identified yet.
4. **The archive probe can fail transiently.** Three routes got `invalid_rpc_response` and
   fell back to replay-only, so those slots are stored but not verified. Rerouting those
   jobs (`replay-route` again) should recover them; that was not run.
5. **Replay usage in `replay-status` for those three jobs.** Their `budget.messages_used`
   reads 0 although `replay` received 2,371, 1,453 and 1,374 messages. The replay totals
   above come from the `replay` output. This was not investigated.
6. **Job 0 used up its archive allowance.** A driver bug retried cache-only batches until
   all 64 grants were spent (4 slots stay pending). The loop was fixed before job 1. Job 1
   was interrupted and resumed, and its second `replay` call exited 1; its first attempt's
   inputs were already stored.
7. Provider credits were not recorded, and the run is one hour of one mint, not a day.
