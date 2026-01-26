# TurboKV Development Container
# Used for CI testing and development

FROM rust:1.75-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/turbokv

# Copy manifests first for caching
COPY Cargo.toml Cargo.lock ./

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
