FROM rust:1.82-bookworm

WORKDIR /app
COPY . .
RUN cargo build -p worker

CMD ["cargo", "run", "-p", "worker", "--", "serve"]

