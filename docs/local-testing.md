# Start testing locally

From the repository directory:

```sh
python3 scripts/local-test.py
```

Open **http://127.0.0.1:8765**. The script builds the existing default Rust CLI,
creates a new private session under ignored `data/local-test/`, ingests 50 captured
inputs, and replays them to check deduplication. No provider credentials, Docker,
Node installation or API calls are needed. It does not load `.env` or modify
existing databases. Python 3.10+, the installed Rust toolchain and a C compiler
for bundled SQLite are required. Build dependencies must already be cached;
if needed, run `cargo fetch --locked` once, then repeat the startup command.

The initial dataset contains **303 unique transfers across 38 mints**, including
USDC and Token-2022 cases with unknown fees. It is captured mainnet data, not a
live feed. It does not establish full-slot coverage or a rolling 30-day window.
The browser deliberately reports coverage as unknown.

## First checks

1. Select USDC. Inspect human-readable amounts and exact raw amounts underneath.
2. Filter loaded rows by a transaction signature or token-account address.
3. Inspect Token-2022 fees: unknown values must remain distinct from zero.
4. Export the loaded mint query as JSON. Amounts/fees are strings or null; the
   browser formats integers using BigInt rather than floating-point arithmetic.
5. Refresh the page. The same records should appear without duplicates.

Hover shortened public addresses to view the full value. The console is read-only:
no browser action starts ingestion, sends a transaction, changes credentials or
provisions infrastructure. Search covers the loaded query only, capped at 1,000
rows. Export includes the current mint query before the address search filter.
There is no pagination, query-time retention or production API in this console.
Fixture replay status and seeded counts describe the startup seed, not live health.

## Stop and reopen

Ctrl+C in the startup terminal stops the server. The session database, copied
binary, captures and structured events remain in its private directory. Startup
prints the directory and direct CLI query command. Reopen that exact session:

```sh
python3 scripts/local-test.py --session data/local-test/run-SESSION
```

A normal startup without `--session` always creates a new session; it never clears
or upgrades another database. If port 8765 is occupied, stop the previous console
or use `--port 8766`. The server binds only to `127.0.0.1`. It exposes the fixed
console page and two read-only JSON routes, not repository files or arbitrary
indexer commands. Keep it local; this development server is not production hosting.

Useful options:

```sh
# Seed and verify without starting a server:
python3 scripts/local-test.py --seed-only
# Use a previously built CLI, skipping Cargo:
python3 scripts/local-test.py --binary target/debug/helius-indexer
```

For query errors, inspect the printed session's `events.jsonl`. Session data and
logs stay out of commits. No raw payload, environment variable or credential is
returned by the HTTP interface. The CLI binary is copied into the session so
later builds with different optional features do not replace the running session's
executable.

## Developer verification

```sh
cargo build --offline --locked
python3 -m unittest discover -s tests -p local_console_test.py
```

The acceptance checks use the real CLI/SQLite and a loopback HTTP server. They
cover captured values, mint filtering, null fees, duplicate-free replay, read-only
queries, invalid request bounds, foreign Host rejection and blocked filesystem/
write routes. They run in the existing default CI job after Rust tests. No Rust
behavior or storage schema is changed by this testing wrapper.


## Container alternative

[Docker Compose](docker.md) provides the same console with a prebuilt CLI and a
dedicated persistent session. Native startup still defaults to loopback and fresh
sessions. `--persistent-session PATH` explicitly enables locked, resumable
initialization; `--bind 0.0.0.0` is reserved for deliberate container use. Neither
option changes transfer semantics or verifies chain coverage.
