DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'user_events'
          AND column_name = 'occurred_at'
          AND data_type <> 'timestamp with time zone'
    ) THEN
        ALTER TABLE user_events
            ALTER COLUMN occurred_at TYPE TIMESTAMPTZ
            USING occurred_at::timestamptz;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'events'
          AND column_name = 'starts_at'
          AND data_type <> 'timestamp with time zone'
    ) THEN
        ALTER TABLE events
            ALTER COLUMN starts_at TYPE TIMESTAMPTZ
            USING CASE
                WHEN NULLIF(starts_at::TEXT, '') IS NULL THEN NULL
                WHEN NULLIF(starts_at::TEXT, '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2}([.][0-9]+)?)?)?$'
                    THEN NULLIF(starts_at::TEXT, '')::TIMESTAMP AT TIME ZONE 'UTC'
                ELSE NULLIF(starts_at::TEXT, '')::TIMESTAMPTZ
            END;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS areas (
    area_id TEXT PRIMARY KEY,
    country_code TEXT NOT NULL DEFAULT 'JP',
    prefecture_code TEXT,
    prefecture_name TEXT,
    city_code TEXT,
    city_name TEXT,
    parent_area_id TEXT REFERENCES areas(area_id),
    centroid geography(Point, 4326),
    area_level TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lines (
    line_id TEXT PRIMARY KEY,
    line_name TEXT NOT NULL,
    operator_name TEXT,
    country_code TEXT NOT NULL DEFAULT 'JP',
    source_id TEXT,
    source_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE stations
    ADD COLUMN IF NOT EXISTS station_code TEXT,
    ADD COLUMN IF NOT EXISTS station_group_code TEXT,
    ADD COLUMN IF NOT EXISTS line_id TEXT REFERENCES lines(line_id),
    ADD COLUMN IF NOT EXISTS area_id TEXT REFERENCES areas(area_id),
    ADD COLUMN IF NOT EXISTS source_id TEXT,
    ADD COLUMN IF NOT EXISTS source_version TEXT;

CREATE TABLE IF NOT EXISTS user_profile_contexts (
    user_id TEXT PRIMARY KEY,
    area_id TEXT REFERENCES areas(area_id),
    line_id TEXT REFERENCES lines(line_id),
    station_id TEXT REFERENCES stations(id),
    context_source TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    consent_scope TEXT NOT NULL,
    retained_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS context_resolution_traces (
    id BIGSERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    user_id_hash TEXT,
    context_source TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    area_id TEXT,
    line_id TEXT,
    station_id TEXT,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    license_name TEXT,
    license_url TEXT,
    source_url TEXT,
    refresh_policy TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_source_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES data_sources(source_id),
    snapshot_version TEXT NOT NULL,
    checksum TEXT NOT NULL,
    fetched_at TIMESTAMPTZ,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    manifest_path TEXT
);

CREATE INDEX IF NOT EXISTS user_events_user_kind_occurred_idx
    ON user_events (user_id, event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_active_starts_at_idx
    ON events (is_active, starts_at);

CREATE INDEX IF NOT EXISTS stations_line_id_idx
    ON stations (line_id);

CREATE INDEX IF NOT EXISTS stations_area_id_idx
    ON stations (area_id);
