FROM rust:1.82-bookworm

WORKDIR /app
COPY . .
RUN cargo build -p crawler

CMD ["cargo", "run", "-p", "crawler", "--", "serve"]
