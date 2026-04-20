CREATE TABLE IF NOT EXISTS source_manifests (
    manifest_path TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    manifest_version INTEGER NOT NULL,
    parser_version TEXT NOT NULL,
    manifest_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS import_runs (
    id BIGSERIAL PRIMARY KEY,
    manifest_path TEXT NOT NULL REFERENCES source_manifests (manifest_path) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL,
    total_rows BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS import_runs_manifest_path_idx
    ON import_runs (manifest_path, started_at DESC);

CREATE TABLE IF NOT EXISTS import_run_files (
    id BIGSERIAL PRIMARY KEY,
    import_run_id BIGINT NOT NULL REFERENCES import_runs (id) ON DELETE CASCADE,
    logical_name TEXT NOT NULL,
    staged_path TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    row_count BIGINT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (import_run_id, logical_name)
);

CREATE TABLE IF NOT EXISTS import_reports (
    id BIGSERIAL PRIMARY KEY,
    import_run_id BIGINT NOT NULL REFERENCES import_runs (id) ON DELETE CASCADE,
    level TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    row_count BIGINT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jp_school_codes (
    school_code TEXT PRIMARY KEY,
    school_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    prefecture_name TEXT NOT NULL,
    city_name TEXT NOT NULL,
    school_type TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jp_school_geodata (
    school_code TEXT PRIMARY KEY,
    school_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    prefecture_name TEXT NOT NULL,
    city_name TEXT NOT NULL,
    address TEXT NOT NULL,
    school_type TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom geography(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    ) STORED,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jp_school_geodata_geom_idx
    ON jp_school_geodata USING GIST (geom);

CREATE TABLE IF NOT EXISTS jp_rail_stations (
    station_code TEXT PRIMARY KEY,
    station_id TEXT NOT NULL UNIQUE,
    station_name TEXT NOT NULL,
    line_name TEXT NOT NULL,
    prefecture_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom geography(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    ) STORED,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jp_rail_stations_geom_idx
    ON jp_rail_stations USING GIST (geom);

CREATE TABLE IF NOT EXISTS jp_postal_codes (
    postal_code TEXT NOT NULL,
    prefecture_name TEXT NOT NULL,
    city_name TEXT NOT NULL,
    town_name TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (postal_code, prefecture_name, city_name, town_name)
);
