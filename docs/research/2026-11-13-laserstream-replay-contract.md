# Finalized mint-filtered replay contract

Verified 2026-11-13 against current Helius primary documentation using the Helius
Docs MCP, and the repository's generated `laserstream-core-proto` 14.0.0 client.
This is the source-contract prerequisite for issue #30, not a completed replay job.

## Verified transport capabilities

- `from_slot` requests historical delivery forward toward current activity.
  Replay honors `match_mints`, so configured mint filtering remains applicable.
- Use finalized commitment consistently. Older replay data is finalized even if
  a different commitment was requested; this does not require changing our policy.
- Ask `SubscribeReplayInfo` for actual earliest availability before subscribing.
  Missing availability is unavailable evidence; a present zero is a valid slot.
  Availability can move between the check and the subscription. An out-of-window
  rejection must remain a visible recovery failure, never a silent scope change.
- The delivery guide describes finalized ordering and service-side deduplication.
  Its ordering guarantee must not be generalized into restart-safe application
  acknowledgment: application retries and inclusive resubscriptions still need
  stable transfer identity and durable, idempotent storage.

## Completion limitation and implementation decision

The delivery guide's explicit end-of-slot delivery statement concerns processed
slot notifications. It does not establish a finalized per-mint transaction count
or a finalized filtered-slot completion barrier. The FAQ also describes ordering
more broadly and conservatively than the delivery guide. The implementation must
not depend on an inferred cross-slot/notification guarantee to certify coverage.

Consequently a filtered transaction replay cannot, from absence of matching
messages alone, distinguish an empty produced slot, a skipped slot, an unobserved
interval or a failed subscription. Seeing a later transaction is an observation,
not proof that every candidate in earlier slots was received and persisted.

**Decision for #30:** extend the existing intake with a finite requested range
and durable job/receipt state. Report raw acknowledgment, decoded output, received
positions and unverified replay progress separately. Preserve the inclusive
requested recovery start unless independent completion evidence permits a change.
Do not advance a verified checkpoint from a maximum slot, SDK memory, a quiet
stream or a finalized slot notification. A finite replay budget can expire with
coverage still unknown; explicit replenishment must be possible without changing
job scope. A later-slot observation may describe why intake stopped, never why a
range is complete.

**Coverage evidence source:** use the existing full-finalized-block verification
contract: archive blocks where available, or separately bounded RPC corroboration
for an explicitly selected recent sample/exceptional repair. All required
candidate decoding and durable output acknowledgments must succeed; validated
parent evidence can account for skipped slots. These sources verify only their
actual scope. They are not a replacement for LaserStream as the normal recent
backfill input, and a sample does not certify the remainder of a replay range.
Without such evidence, the replay's verified and retained coverage stay unknown.
A future replay-specific completion mechanism requires its own documented contract
and tests before it can change that rule.

## Required negative checks in the implementation

Exercise out-of-order and duplicate candidates, no matching transactions,
notifications/later slots before a delayed earlier transaction, replay expiry
between availability checking and subscription, process death at raw/output/job
acknowledgment boundaries, and pruning after successful processing. None can
manufacture a complete range or erase unresolved work. Status must distinguish
received data, durable processing receipts and queryable retained output.

No provider subscription or paid usage was started for this contract review.
No runtime behavior, SDK version or production storage selection changed.

## Primary references

- [Historical replay](https://www.helius.dev/docs/laserstream/historical-replay):
  start position, approximate window, older finalized history and mint filters.
- [Replay availability](https://www.helius.dev/docs/api-reference/laserstream/grpc/subscribereplayinfo):
  earliest available slot, absent versus zero and check/subscribe race.
- [Delivery guarantees](https://www.helius.dev/docs/laserstream/delivery-guarantees):
  commitment ordering, delivery semantics and processed notification behavior.
- [LaserStream FAQ](https://www.helius.dev/docs/faqs/laserstream#order-of-messages):
  broader ordering caveat; retain conservative application assumptions.
- [Mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering): replay
  uses the same mint-matching semantics.
