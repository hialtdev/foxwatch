# ── Stage 1: Build ────────────────────────────────────────────────────────────
# rdkafka cmake-build compiles librdkafka from C source.
# Needs cmake, gcc, libssl-dev, and pkg-config.
FROM rust:slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    gcc \
    g++ \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency layer — only re-runs when Cargo.toml/Cargo.lock changes
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Build real source
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/foxwatch .

ENV RUST_LOG=info

ENTRYPOINT ["./foxwatch"]
