CREATE TABLE IF NOT EXISTS crawl_runs (
    id BIGSERIAL PRIMARY KEY,
    manifest_path TEXT NOT NULL REFERENCES source_manifests (manifest_path) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    parser_key TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL,
    fetched_targets BIGINT NOT NULL DEFAULT 0,
    parsed_rows BIGINT NOT NULL DEFAULT 0,
    imported_rows BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS crawl_runs_manifest_path_idx
    ON crawl_runs (manifest_path, id DESC);

CREATE TABLE IF NOT EXISTS crawl_fetch_logs (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs (id) ON DELETE CASCADE,
    logical_name TEXT NOT NULL,
    target_url TEXT NOT NULL,
    final_url TEXT,
    http_status INTEGER,
    checksum_sha256 TEXT,
    size_bytes BIGINT,
    staged_path TEXT,
    fetch_status TEXT NOT NULL,
    content_changed BOOLEAN,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (crawl_run_id, logical_name, target_url)
);

CREATE INDEX IF NOT EXISTS crawl_fetch_logs_checksum_idx
    ON crawl_fetch_logs (checksum_sha256, fetched_at DESC);

CREATE TABLE IF NOT EXISTS crawl_parse_reports (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs (id) ON DELETE CASCADE,
    logical_name TEXT,
    level TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    parsed_rows BIGINT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crawl_dedupe_reports (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs (id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL,
    kept_event_id TEXT NOT NULL,
    dropped_event_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
