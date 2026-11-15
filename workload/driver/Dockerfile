FROM rust:1-trixie AS build
WORKDIR /src
COPY driver/Cargo.toml ./
COPY driver/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/driver /out/ \
 && cp /out/driver /symbols/

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl jq ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/driver /usr/local/bin/
COPY --from=build /symbols /symbols
COPY driver/first_start_workload /opt/antithesis/test/v1/main/
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
