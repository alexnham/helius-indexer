# Provider-measured archive/replay overlap: 2026-11-17

Issue #31 criterion 4, run 2 of the [planned runs](README.md#planned-runs-toward-the-30-day-backfill).
The first bounded run against both live sources: Old Faithful archive batches and
LaserStream replay of the same 50 slots, reconciled by `replay-overlap`.

## Scope

- USDC, slots 470015950–470015999 (last 50 of epoch 1087), session
  `overlap31_20261117`, job `overlap_470015950`.
- One `replay-route`, one 60 s `replay` grant, `replay-retry`, seven archive batches
  (one extra `archive-grant`), `replay-overlap`. Archive limits unchanged: 8 slots,
  32 requests, 128 MiB and 120 s per batch.
- Not run: the RPC gap lane (no gap: archive and replay windows overlap by ~675k
  slots), live intake, more than 50 slots.

## Command and environment

Runtime image built from this branch (`console-archive` target), run as a one-off
Compose container against local PostgreSQL 18.6 with `INDEXER_ROUTE_ARCHIVE=always`,
`INDEXER_LIVE_OPT_IN=1` and `INDEXER_ARCHIVE_OPT_IN=1`. Apple Silicon, Docker Engine
29.9.2. 2026-11-17 18:37:59–18:39:58 UTC.

Without `INDEXER_ROUTE_ARCHIVE=always` a first probe of slots 470447950–470447999
routed all 50 as `replay` and never probed the archive, so no overlap could be
reconciled (see the PR that adds the setting).

## Results

| Check | Result |
| --- | --- |
| Route | Archive last available 470447999 (epoch 1088 published); replay first available 469772744; reference 470636898. All 50 slots `overlap`. |
| Replay | 705 received in 13.3 s (`end_observed`), 6.6 MB; 704 unique inputs, 701 decoded, 3 unresolved (`unsupported_token_instruction`, retry did not resolve); 3,256 transfers |
| Archive batch 1 | 18 requests, 6.7 MB; slots 470015950–957: 6 `complete`, 2 `unknown` (`unsupported_token_instruction`) |
| Archive batches 2–7 | Each re-selected the same eight slots, served from cache, 0 requests; no progress past slot 470015957 |
| Overlap outcomes | 6 `replay_omission`, 0 `conflict`, 44 `pending` (`archive_slot_incomplete`) |
| Delivered vs archive mint inputs (6 slots) | 107 replay, 284 archive |
| Verified slots / coverage | `verified.overlap_slots` 6; job coverage `unknown` |

Every replay-delivered transaction in the six reconciled slots was in the archive
block with identical decoded transfers: identity, raw amounts, fees, confidentiality
and instruction positions.

### What replay omitted

Slot 470015957 (46 delivered, 169 archive mint inputs) was compared against the same
block from public RPC `getBlock` (one request). Replay delivered **all 46 successful**
USDC transactions (legacy, v0 and v1, including balance-only matches) and **none of
the 123 failed** ones. Failed transactions move no tokens, so no transfer is missing;
the reconciler's mint-transaction predicate counts failed transactions, which is why
the outcome is `replay_omission` rather than `consistent`.

## Cost

One LaserStream replay grant (705 messages, 6.6 MB, one connection), one
`SubscribeReplayInfo` per route, two public RPC requests. Old Faithful responses
6.7 MB. Provider credits not read from the dashboard.

## Findings and gaps

1. **Archive head-of-line blocking.** A slot that stays `unknown` (here an
   unsupported token instruction) is re-selected by every batch, because a batch takes
   the first eight incomplete slots. Archive work cannot pass it and spends its
   allowance on cache-only retries. A 30-day backfill would stall at the first such
   slot.
2. **Failed transactions count as replay omissions.** The reconciler should exclude
   failed transactions from membership, or report them separately, so slots like these
   reconcile as `consistent`.
3. **Unsupported token instruction:** 3 replay inputs and 2 archive slots in 50 slots.
   Related to #60; which instruction it is was not identified.
4. Criterion 4 is measured on 6 slots only; the other 44 wait on finding 1.
