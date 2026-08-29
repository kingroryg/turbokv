# TurboKV Development Container
# Used for CI testing and development

FROM rust:1.85-slim

# The persisted Bloom mapping uses gxhash's hardware AES implementation. The
# resulting native-code image must run on this CPU model or a feature superset.
ENV RUSTFLAGS="-C target-cpu=native"

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/turbokv

# Copy manifests first for caching
COPY Cargo.toml ./

# Create dummy source for dependency caching
RUN mkdir -p src && \
    echo "pub fn main() {}" > src/lib.rs

# Build dependencies (cached layer)
RUN cargo build --release && \
    rm -rf src

# Copy actual source code
COPY . .

# Build and test
RUN cargo build --release && \
    cargo test --release

# Default command runs tests
CMD ["cargo", "test", "--release"]
