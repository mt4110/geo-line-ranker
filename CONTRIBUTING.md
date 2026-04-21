# Contributing

Read [`AGENTS.md`](AGENTS.md) first, then use [`docs/CONTRIBUTING_LOCAL.md`](docs/CONTRIBUTING_LOCAL.md) for the local runbook and the current JP station conversion guidance.

1. Keep changes reviewable.
2. Preserve deterministic behavior for the same inputs.
3. Update docs and fixtures when contracts change.
4. Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace`.
