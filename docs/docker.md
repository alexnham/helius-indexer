# Docker captured-data console

This packages the current captured-transfer test console. It does not start live
intake, backfill or a production API, and requires no provider credentials.

## Start

Install/start a current Docker engine (28+ for the documented localhost publishing
behavior) with Docker Compose. From the repository:

```sh
docker compose up --build
```

Open **http://127.0.0.1:8765** after startup reports ready. If the native console
already occupies that port, leave it running and use:

```sh
INDEXER_CONSOLE_PORT=8766 docker compose up --build
```

Then open **http://127.0.0.1:8766**. The same setting controls the container port,
published host port and health check, retaining Host-header validation. Optional
configuration is documented in `.env.example`; no `.env` file is required. Compose
passes only the console port to the container, not your provider keys.

The first build downloads pinned public base images, Debian runtime packages and
locked Rust dependencies. It can take several minutes and needs Docker build disk
space. Runtime includes Python, the release Rust CLI, CA certificates and captured
fixtures. Cargo/build tools and private host files are not included. The build
context is allowlisted: local `data/`, `.env` files, host binaries and Git metadata
are excluded.

## Persistent session

The dedicated `console-data` named volume holds the database, SQLite WAL files,
raw captures, copied binary, receipt and events under `/app/data/session`. It is
separate from the repository's existing native `data/` directory. First startup
seeds 50 captures, then verifies duplicate-free replay (303 unique transfers).
Later startups reopen that same session without reseeding or replacing its binary.
An interrupted first seed can resume idempotently; the completion receipt is
published only after checks pass. A local file lock prevents concurrent seeders.

```sh
docker compose stop
docker compose start
# Recreate containers while keeping data:
docker compose down
docker compose up --build
```

Rebuilding an image does not silently migrate existing data or replace a session's
copied CLI. To try a newer decoder against existing data, use an explicit backed-up
repair procedure in a later operating step. The copied binary is architecture-
specific; do not move an entire session between ARM64 and AMD64 and expect that
binary to run. Database portability and migrations need their own validation.

Only for an intentional reset of this Compose project's session:

```sh
docker compose down --volumes
```

That deletes the named volume and everything stored in it. Ordinary `down` preserves
it. Stop the service before copying/archiving the volume; never copy a live SQLite
main database file alone while its WAL is active. Ingestion profiles, credential
file support and a full backup/restore runbook belong to the second Docker phase.

## Health and limits

`/healthz` checks that the session receipt parses and the existing SQLite transfer
table is readable, using a read-only connection. It does not create missing data.
A green container health check means the console/database are accessible; it does
not mean chain coverage, freshness, live replay or 30-day readiness. Coverage stays
explicitly unknown.

The image runs as UID/GID 10001. Compose publishes only on host `127.0.0.1`, drops
capabilities, disables privilege escalation, makes the image filesystem read-only,
and provides writable data plus a 64 MiB temporary filesystem. The captured-data
service has one CPU, 512 MiB memory and 64 PIDs; these are local-demo caps, not
production sizing results. The single-request server retains bounded socket and
CLI deadlines. SIGTERM during serving closes the server; interrupted seed writes
are recovered through the existing SQLite/idempotent replay contract.

Use `docker compose ps` for health and `docker compose logs --tail 30 console` for
startup errors. Detailed stage events remain in the volume's `events.jsonl`. No
provider requests are made and no infrastructure or image registry is provisioned.
The host's current native console can keep running on a different port.

## Verification

```sh
# Requires the native default CLI already built:
python3 -m unittest discover -s tests -p '*console_test.py'
# Build once, then test an isolated disposable Compose project:
docker compose build
INDEXER_CONSOLE_PORT=8766 python3 scripts/docker-smoke.py
```

The smoke script checks non-root execution, absence of Cargo, HTTP availability,
unknown coverage, unchanged queries/session receipt/binary after recreation, and
successful seed/replay with networking disabled. It deletes only its randomly
named smoke project's volume. It refuses remote Docker contexts. The default
Compose project's data is untouched. CI performs Linux AMD64 builds/smoke checks;
local Apple Silicon verification is recorded with the implementation results.

Official references: [multi-stage builds](https://docs.docker.com/build/building/multi-stage/),
[localhost publishing](https://docs.docker.com/engine/network/port-publishing/),
and [Compose secrets](https://docs.docker.com/compose/how-tos/use-secrets/) for the
next ingestion phase. Base-image multi-platform manifests were verified on
2026-11-13 and pinned by digest in the Dockerfile.


Local validation on 2026-11-13: Docker Engine 29.9.2 / Compose 5.6.1, Linux ARM64
on Apple Silicon. The image built successfully at 224,970,732 bytes. All six
native console/persistence tests passed. Docker smoke checks passed for health,
non-root execution, no runtime compiler, clean SIGTERM exit, unchanged session
receipt/binary/queries after recreation, and network-disabled initialization.
The default Docker console is healthy on the separate test port 8766. AMD64
verification is configured in CI and remains pending until that job passes.
No Rust source changed; the native Rust suite was not rebuilt/rerun locally.

The separate [provider-backed runtime](runtime-testing.md) is now available under
`--profile live`: real live intake and configurable bounded recent RPC history,
a separate data volume, runtime-only secret and inspectable coverage. The default
captured console remains unchanged. Archive-enabled image packaging from #27 is
still separate work.
