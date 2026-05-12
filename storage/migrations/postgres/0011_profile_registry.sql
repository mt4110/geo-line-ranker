CREATE TABLE IF NOT EXISTS profile_registry (
    profile_id TEXT PRIMARY KEY CHECK (btrim(profile_id) <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    compatibility_level TEXT NOT NULL CHECK (btrim(compatibility_level) <> ''),
    active_manifest_lineage_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS profile_pack_manifest_lineage (
    id BIGSERIAL PRIMARY KEY,
    profile_id TEXT NOT NULL CHECK (btrim(profile_id) <> '') REFERENCES profile_registry (profile_id) ON DELETE CASCADE,
    manifest_path TEXT NOT NULL CHECK (btrim(manifest_path) <> ''),
    manifest_checksum_sha256 TEXT NOT NULL CHECK (manifest_checksum_sha256 ~ '^[0-9a-fA-F]{64}$'),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    manifest_kind TEXT NOT NULL CHECK (btrim(manifest_kind) <> ''),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    compatibility_level TEXT NOT NULL CHECK (btrim(compatibility_level) <> ''),
    default_locale TEXT,
    description TEXT,
    ranking_config_dir TEXT NOT NULL CHECK (btrim(ranking_config_dir) <> ''),
    reason_catalog_path TEXT NOT NULL CHECK (btrim(reason_catalog_path) <> ''),
    content_kind_registry JSONB NOT NULL
        CHECK (
            CASE
                WHEN jsonb_typeof(content_kind_registry) = 'array'
                THEN jsonb_array_length(content_kind_registry) > 0
                ELSE false
            END
        ),
    supported_content_kinds JSONB NOT NULL
        CHECK (
            CASE
                WHEN jsonb_typeof(supported_content_kinds) = 'array'
                THEN jsonb_array_length(supported_content_kinds) > 0
                ELSE false
            END
        ),
    context_inputs JSONB NOT NULL
        CHECK (
            CASE
                WHEN jsonb_typeof(context_inputs) = 'array'
                THEN jsonb_array_length(context_inputs) > 0
                ELSE false
            END
        ),
    placements JSONB NOT NULL
        CHECK (
            CASE
                WHEN jsonb_typeof(placements) = 'array'
                THEN jsonb_array_length(placements) > 0
                ELSE false
            END
        ),
    fallback_policy TEXT NOT NULL CHECK (btrim(fallback_policy) <> ''),
    fixture_count INTEGER NOT NULL DEFAULT 0 CHECK (fixture_count >= 0),
    connector_count INTEGER NOT NULL DEFAULT 0 CHECK (connector_count >= 0),
    evaluation_reference_count INTEGER NOT NULL DEFAULT 0 CHECK (evaluation_reference_count >= 0),
    manifest_payload JSONB NOT NULL CHECK (jsonb_typeof(manifest_payload) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (profile_id, id),
    UNIQUE (profile_id, manifest_checksum_sha256)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'profile_registry_active_manifest_lineage_fk'
    ) THEN
        ALTER TABLE profile_registry
            ADD CONSTRAINT profile_registry_active_manifest_lineage_fk
            FOREIGN KEY (active_manifest_lineage_id)
            REFERENCES profile_pack_manifest_lineage (id)
            ON DELETE SET NULL
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS profile_pack_manifest_lineage_profile_idx
    ON profile_pack_manifest_lineage (profile_id, id DESC);

CREATE TABLE IF NOT EXISTS profile_compatibility_status (
    profile_id TEXT PRIMARY KEY REFERENCES profile_registry (profile_id) ON DELETE CASCADE,
    compatibility_level TEXT NOT NULL CHECK (btrim(compatibility_level) <> ''),
    status TEXT NOT NULL CHECK (status IN ('valid', 'warning', 'blocked')),
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence) = 'object'),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS evaluation_runs (
    id BIGSERIAL PRIMARY KEY,
    profile_id TEXT REFERENCES profile_registry (profile_id) ON DELETE SET NULL,
    profile_manifest_lineage_id BIGINT,
    run_kind TEXT NOT NULL CHECK (run_kind IN ('golden')),
    scenario_source_kind TEXT NOT NULL CHECK (btrim(scenario_source_kind) <> ''),
    scenario_path TEXT NOT NULL CHECK (btrim(scenario_path) <> ''),
    pairwise_pack_path TEXT,
    algorithm_version TEXT NOT NULL CHECK (btrim(algorithm_version) <> ''),
    status TEXT NOT NULL CHECK (status IN ('passed', 'blocked', 'failed')),
    scenarios INTEGER NOT NULL DEFAULT 0 CHECK (scenarios >= 0),
    passed INTEGER NOT NULL DEFAULT 0 CHECK (passed >= 0),
    blocked INTEGER NOT NULL DEFAULT 0 CHECK (blocked >= 0),
    blockers INTEGER NOT NULL DEFAULT 0 CHECK (blockers >= 0),
    warnings INTEGER NOT NULL DEFAULT 0 CHECK (warnings >= 0),
    CHECK (passed + blocked = scenarios),
    CHECK (profile_manifest_lineage_id IS NULL OR profile_id IS NOT NULL),
    CHECK (status <> 'passed' OR (blocked = 0 AND blockers = 0)),
    CHECK (status <> 'blocked' OR (blocked > 0 OR blockers > 0)),
    summary_payload JSONB NOT NULL CHECK (jsonb_typeof(summary_payload) = 'object'),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (profile_id, profile_manifest_lineage_id)
        REFERENCES profile_pack_manifest_lineage (profile_id, id)
        ON DELETE SET NULL (profile_manifest_lineage_id)
);

CREATE INDEX IF NOT EXISTS evaluation_runs_profile_created_idx
    ON evaluation_runs (profile_id, completed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS evaluation_run_cases (
    id BIGSERIAL PRIMARY KEY,
    evaluation_run_id BIGINT NOT NULL REFERENCES evaluation_runs (id) ON DELETE CASCADE,
    case_id TEXT NOT NULL CHECK (btrim(case_id) <> ''),
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    path TEXT NOT NULL CHECK (btrim(path) <> ''),
    status TEXT NOT NULL CHECK (status IN ('passed', 'blocked')),
    expected_fallback_stage TEXT NOT NULL CHECK (btrim(expected_fallback_stage) <> ''),
    actual_fallback_stage TEXT,
    expected_order JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(expected_order) = 'array'),
    actual_order JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(actual_order) = 'array'),
    checks_payload JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(checks_payload) = 'array'),
    UNIQUE (evaluation_run_id, case_id)
);
