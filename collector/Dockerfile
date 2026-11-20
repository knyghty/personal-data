FROM rust:1-bookworm AS build
WORKDIR /src
# Build the dependencies alone first, so the layer is cached until Cargo.toml or Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
# Touch main.rs so cargo rebuilds it over the placeholder, whose build is newer than the copied file.
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/collector /usr/local/bin/collector
CMD ["collector"]
