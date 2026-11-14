# Local provider-backed Docker runtime validation

Verified 2026-11-13 on Apple Silicon, Linux ARM64 container, Docker Engine 29.9.2
and Compose 5.6.1. This is a functional test, not a capacity benchmark or a
production-readiness claim. Provider credentials and retained data are not committed.

## First bounded source test

The runtime used USDC, 60 seconds of finalized LaserStream intake and an inclusive
50-slot finalized RPC range (469000717–469000766), through the shared decoder,
SQLite persistence and live-priority admission gate.

Observed terminal results:

- 5,902 received live transaction attempts; 5,821 successful decoded writes and
  81 unresolved live inputs. Intake ended at the 60-second limit, with no reconnect.
- 54,142,880 retained protobuf bytes. These are not billed bytes or measured credits.
- 10,600 unique stored USDC transfers across live and historical inputs before
  count-based pruning. Decode/write attempt counts include related mints and
  must not be interpreted as unique USDC counts.
- 23 historical slots verified complete, 27 pending/unknown. Recorded errors were
  18 `incomplete_execution_evidence`, four `unsupported_token_instruction` and
  one cancellation; other slots were unattempted when the budget expired.
- The independent classic SPL USDC parser sampled 16 retained block observations:
  705 expected versus 703 stored edges, two missing, zero extra and zero incorrect
  amount/account values. The comparison failed, accurately exposing missing rows.
  This does not validate Token-2022 or provider-wide completeness.

Commands: `docker compose --profile live up --no-build -d runtime`, inspection
of `/api/status` and `/api/transfers`, and
`docker compose exec -T runtime python3 /app/scripts/compare-captured-usdc.py /app/data/runtime/index.sqlite /app/data/runtime/index.sqlite --sample`.
Browser inspection confirmed 1,000 rendered newest-first rows, no UI error and
visible unique counts/unresolved inputs. The captured console's offline Docker
smoke also passed, including recreation, graceful stop and network-disabled replay.

## Continuous mode and retention

Continuous cycles and a default newest-1,000-transfer cap were explicitly approved.
The cap applies after each cycle across all mints; retained counts can exceed it
during intake. Older raw inputs and progress are pruned; the original 50-slot
backfill normally ages out quickly. A pruned comparison sample may therefore be
unavailable and is never presented as a passing reconciliation.

Review found that the cutoff slot could lose individual transfers while keeping
its complete status. A failing regression reproduced this. The fix invalidates
historical/source/overlap completion proofs for that boundary atomically with
deletion. SQLite failure injection after transfer deletion verifies rollback of
both rows and coverage. Input evidence at the retained boundary remains available;
older removed evidence needs backup or refetch. Reverification can reintroduce
previously pruned rows, which a later explicit prune caps again.

Final offline checks: formatting, Clippy with warnings denied, full default and
archive-feature Rust suites, and 11 Python console tests passed. The new Python
supervisor test verifies timeout/termination kills and reaps its child, including
the prune subprocess. Local ARM64 image build and captured-console Docker smoke
are checked separately; AMD64 execution remains CI evidence.

## Remaining gaps

The live tail has no validated completeness signal. Observed resume slots are
not verified checkpoints, and restarting from the recent reference does not repair
an outage automatically. Decoder/evidence failures remain visible and historical
coverage stays incomplete. The count cap does not implement the production
30-day query/37-day raw-retention requirements or a strict byte-level disk cap.
No full-window backfill, archive-container build, production deployment or
independent provider reconciliation was performed.
