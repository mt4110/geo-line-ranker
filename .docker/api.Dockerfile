FROM rust:1.82-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release -p api -p cli

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

WORKDIR /app
COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/cli /usr/local/bin/cli
COPY --from=builder /app/configs ./configs
COPY --from=builder /app/storage/fixtures ./storage/fixtures
COPY --from=builder /app/storage/migrations ./storage/migrations

USER appuser

CMD ["bash", "-lc", "cli migrate && api serve"]
