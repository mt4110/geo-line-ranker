FROM rust:1.82-bookworm

WORKDIR /app
COPY . .
RUN cargo build -p api -p cli

CMD ["bash", "-lc", "/app/target/debug/cli migrate && /app/target/debug/api serve"]
