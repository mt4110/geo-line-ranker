set dotenv-load := true

setup:
  ./scripts/contributor_setup.sh

dev:
  ./scripts/contributor_dev.sh

smoke:
  ./scripts/contributor_smoke.sh

docs:
  ./scripts/docs_check.sh

eval:
  ./scripts/contributor_eval.sh

fmt:
  cargo fmt --all

lint:
  cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
  cargo test --workspace

up:
  docker compose -f .docker/docker-compose.yaml up -d postgres redis

down:
  docker compose -f .docker/docker-compose.yaml down

migrate:
  cargo run -p cli -- migrate

seed:
  cargo run -p cli -- seed example

generate-demo-jp:
  cargo run -p cli -- fixtures generate-demo-jp

import-demo-jp:
  cargo run -p cli -- import jp-rail --manifest storage/sources/jp_rail/example.yaml
  cargo run -p cli -- import jp-postal --manifest storage/sources/jp_postal/example.yaml
  cargo run -p cli -- import jp-school-codes --manifest storage/sources/jp_school/example.yaml
  cargo run -p cli -- import jp-school-geodata --manifest storage/sources/jp_school_geo/example.yaml
  cargo run -p cli -- derive school-station-links

api:
  cargo run -p api -- serve

worker:
  cargo run -p worker -- serve

crawler:
  cargo run -p crawler -- fetch --manifest configs/crawler/sources/custom_example.yaml
  cargo run -p crawler -- parse --manifest configs/crawler/sources/custom_example.yaml

crawler-health:
  cargo run -p crawler -- health --manifest configs/crawler/sources/custom_example.yaml

fixture-doctor:
  cargo run -p cli -- fixtures doctor --path storage/fixtures/minimal
  cargo run -p cli -- fixtures doctor --path storage/fixtures/demo_jp

mvp-env:
  [[ -f .env ]] || cp .env.example .env

mvp-up:
  docker compose -f .docker/docker-compose.yaml up -d postgres redis
  ./scripts/wait_for_postgres.sh

mvp-bootstrap:
  ./scripts/wait_for_postgres.sh
  cargo run -p cli -- migrate
  cargo run -p cli -- seed example
  cargo run -p cli -- snapshot refresh

mvp-down:
  docker compose -f .docker/docker-compose.yaml down

mvp-acceptance:
  ./scripts/mvp_acceptance.sh

post-launch-doctor:
  ./scripts/post_launch_doctor.sh

data-quality-doctor:
  ./scripts/data_quality_doctor.sh

release-readiness:
  ./scripts/release_readiness.sh

post-mvp-hardening:
  ./scripts/post_mvp_hardening.sh

optional-evidence-review:
  ./scripts/optional_evidence_review.sh

local-review-eval:
  python3 scripts/local_review_eval.py --self-test

local-review-inspect artifact_dir:
  python3 scripts/local_review_eval.py --inspect {{artifact_dir}}

local-review-triage artifact_dir:
  python3 scripts/local_review_eval.py --inspect {{artifact_dir}} --triage

local-review-inventory artifact_root:
  python3 scripts/local_review_eval.py --inventory {{artifact_root}}
