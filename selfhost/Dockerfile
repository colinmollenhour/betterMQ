# Self-host BetterMQ (local or Slate storage, panel auth — no Postgres).
FROM rust:1.89.0-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev llvm-dev cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY engine ./engine
COPY tools ./tools
ARG CARGO_BUILD_JOBS=2
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release -p broker-server -p broker-storage --features broker-storage/s3 --bins \
    && mkdir -p /out \
    && cp target/release/bettermq target/release/bettermq-archive target/release/bettermq-wal-migrate /out/

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 bettermq \
    && useradd --system --uid 1000 --gid bettermq --home-dir /data --no-create-home bettermq \
    && mkdir -p /data /cluster-shared/meta \
    && chown bettermq:bettermq /data /cluster-shared/meta
COPY --from=builder /out/bettermq /usr/local/bin/bettermq
COPY --from=builder /out/bettermq-archive /out/bettermq-wal-migrate /usr/local/bin/
COPY engine/panel/dist /usr/share/bettermq/panel
ENV BETTERMQ_PANEL_DIR=/usr/share/bettermq/panel
EXPOSE 8080
USER bettermq
ENTRYPOINT ["bettermq"]
