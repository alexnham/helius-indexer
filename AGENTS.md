# Working on helius-indexer

## Start here

- Read `PROPOSAL.md` before changing domain behavior, pipeline architecture, persistence, recovery, or API semantics. It is the requirements baseline and identifies provisional choices.
- Inspect the current code, callers, configuration, and relevant tests before editing. The proposal's directory tree describes intended organization; create modules as working features need them.
- Deliver the smallest complete change within the user's request. A new request to implement a feature authorizes that feature; the proposal's original document-only scope is not a permanent implementation freeze.
- Keep requirements changes explicit. If a request changes agreed behavior, update the proposal alongside the implementation. Record unresolved decisions instead of silently resolving them through code.

## Architecture

- Start with one Rust package and explicit domain types. Extract a crate or service when a concrete dependency, build, or operational requirement warrants it.
- Normalize live and historical inputs into the same decoding contract. Keep source-specific transport details at the ingestion boundary.
- Keep decoding deterministic and independent of network calls, system time, and database access. Supply historical context explicitly when decoding needs it.
- Keep persistence and query deduplication in storage code; range scheduling and coverage verification belong in coordination code.
- Treat Kafka, PostgreSQL, ClickHouse, and archive tooling as candidates until the validation slice supplies evidence. Add their dependencies, migrations, and deployment configuration only when selected for the work at hand.

## Correctness when implementing

- Apply the transfer semantics, confidentiality rules, fee representation, retention, and coverage requirements in `PROPOSAL.md`. Changes to those behaviors need fixtures or integration checks that demonstrate the resulting contract.
- Represent raw token amounts and fees exactly with integer types. Use checked arithmetic and sufficiently wide aggregate types; preserve exact values across storage and API serialization.
- Derive stable transfer identity from chain/instruction position, independent of ingestion source and decoder revision. Preserve enough provenance to reproduce decoding and correct earlier output.
- Resolve historical ownership and extension state using evidence valid at execution time. Current account state is not a substitute for missing historical state.
- Separate durable input progress, durable decoded output, and verified coverage. A cursor or delivery acknowledgment alone does not prove a range complete.
- Exercise retries, duplicates, out-of-order completion, and crashes at acknowledgment boundaries. Advance a contiguous checkpoint only over verified completed work.
- Keep unknown values distinct from zero and missing coverage distinct from an observed empty range. Make those distinctions visible in API aggregates.

## External sources and credentials

- Verify provider capabilities, SDK interfaces, replay availability, transaction-format support, and pricing against current primary documentation when the change depends on them. Prefer connected Solana and Helius documentation tools when available; record source links and verification dates with research findings.
- Validate RPC, stream, archive, and persisted input at trust boundaries. Bound concurrency, buffers, retries, and batch sizes; make cancellation and backpressure explicit.
- Load credentials through environment variables or the chosen secret store. Keep `.env` files, API keys, credential-bearing URLs, and private payloads out of commits, fixtures, and logs. Commit sanitized configuration examples when configuration is introduced.
- Keep ordinary tests deterministic and offline. Make live-provider checks explicitly opt-in, document required credentials and expected usage, and use bounded slot ranges. Full backfills and paid infrastructure provisioning require task-specific authorization.
- Before adding or changing pipeline stages, API behavior, recovery, or instrumentation, read `docs/observability.md`. Ship the affected metrics, diagnostic events, and telemetry checks with the behavior they describe.

## Verification

- Discover required checks from the repository's manifest and CI. Documentation-only changes need relevant content/link checks rather than invented build checks.
- For Rust changes, once the package exists, run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and the relevant `cargo test` targets. Run the full offline suite before presenting a PR as ready; report any unavailable prerequisites or failing checks accurately.
- Use captured, sanitized transactions for decoder fixtures. Include expected transfer identities, amounts, fees, confidentiality, and instruction positions, with source and slot provenance.
- For persistence/recovery changes, use real selected storage in integration tests and inject failures around writes and progress commits. Check observable query results and coverage, not only internal method calls.
- For decoder corrections, test records that are added, changed, and removed. For migrations, state compatibility with existing data and the recovery or rollback approach.
- Measure performance claims using representative inputs and record the command, dataset, environment, and result. Use `PROPOSAL.md` for the end-to-end acceptance scenarios.

## Branches and pull requests

- Use `codex/<short-purpose>` for new branches unless the user specifies another convention.
- Prefer small independent PRs, each with one coherent behavior change and its checks. Use a short stack when a change genuinely requires an unmerged prerequisite; aim for two or three PRs and split longer work into milestones.
- In a stack, base each child on its immediate parent. Identify its parent, intended merge order, and the child's own changes in the PR description. Review the child against its parent so prerequisite changes do not obscure its diff.
- Keep each PR buildable and tested with its prerequisites. Put tests with the behavior they verify. Keep unrelated cleanup outside the stack.
- Merge prerequisites first. After a parent merges, reconcile the child with the updated target branch, verify its base and diff, and rerun affected checks. Coordinate before rewriting any shared branch; use `--force-with-lease` only when a rewrite is authorized.
- Lead PR descriptions with the problem and resulting behavior. Include validation evidence, relevant limitations, and migration or operating impact. Link dependencies instead of repeating their descriptions.
- Commit and push when requested or necessary to fulfill the authorized task. Merge, deploy, provision paid resources, and change repository visibility only when authorized for that action.

## Finish

- Summarize what changed, why, and what verification actually ran. Identify material gaps or pending decisions without presenting assumptions as measured results.
- Update documentation and configuration examples when the changed behavior makes them inaccurate. Keep domain requirements in `PROPOSAL.md` and working instructions here rather than duplicating them.

## Agent skills

### Issue tracker
Track issues in GitHub Issues for `alexnham/helius-indexer`.
Before creating, reading, or updating tickets, read `docs/agents/issue-tracker.md`.

### Triage labels
Use the five canonical triage labels.
Before triaging or labeling tickets, read `docs/agents/triage-labels.md`.

### Domain docs
Use a single root glossary and shared ADR directory.
Before domain exploration or design changes, read `docs/agents/domain.md`.
