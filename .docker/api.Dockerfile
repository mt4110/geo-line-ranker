FROM rust:1.82-bookworm

WORKDIR /app
COPY . .
RUN cargo build -p api

CMD ["bash", "-lc", "cargo run -p cli -- migrate && cargo run -p api -- serve"]
