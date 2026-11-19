FROM rust:1.95.0-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release --bins -j 4 && mkdir /out && cp target/release/fleetcore target/release/simulator target/release/acceptance target/release/benchmark /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* && mkdir /state && chown 10001:10001 /state
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
ENTRYPOINT ["fleetcore"]
