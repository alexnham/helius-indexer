# syntax=docker/dockerfile:1
FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS build
WORKDIR /build
ENV CARGO_BUILD_JOBS=2
# Build stage only: cmake compiles the vendored librdkafka; the runtime image is unchanged.
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,id=helius-indexer-console-registry-v1,target=/usr/local/cargo/registry \
    --mount=type=cache,id=helius-indexer-console-git-v1,target=/usr/local/cargo/git \
    cargo build --release --locked

FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df AS console
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 indexer \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin indexer \
    && mkdir -p /app/data && chown 10001:10001 /app/data
WORKDIR /app
COPY --from=build /build/target/release/helius-indexer /usr/local/bin/helius-indexer
COPY scripts/local-test.py scripts/indexer-runtime.py scripts/compare-captured-usdc.py ./scripts/
COPY scripts/local-console ./scripts/local-console
COPY tests/fixtures ./tests/fixtures
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 INDEXER_CONSOLE_PORT=8765
USER 10001:10001
EXPOSE 8765
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=3 \
    CMD python3 -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ['INDEXER_CONSOLE_PORT']+'/healthz',timeout=3).read()"
ENTRYPOINT ["python3","/app/scripts/local-test.py","--binary","/usr/local/bin/helius-indexer","--persistent-session","/app/data/session","--bind","0.0.0.0"]
