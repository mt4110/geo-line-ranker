ALTER TABLE user_events
    ADD COLUMN IF NOT EXISTS event_id TEXT REFERENCES events (id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS target_station_id TEXT REFERENCES stations (id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'user_events_event_type_check'
    ) THEN
        ALTER TABLE user_events
            ADD CONSTRAINT user_events_event_type_check
            CHECK (
                event_type IN (
                    'school_view',
                    'school_save',
                    'search_execute',
                    'event_view',
                    'apply_click',
                    'share'
                )
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS user_events_user_id_occurred_at_idx
    ON user_events (user_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS user_events_school_id_occurred_at_idx
    ON user_events (school_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS popularity_snapshots (
    school_id TEXT PRIMARY KEY REFERENCES schools (id) ON DELETE CASCADE,
    popularity_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_events BIGINT NOT NULL DEFAULT 0,
    school_view_count BIGINT NOT NULL DEFAULT 0,
    school_save_count BIGINT NOT NULL DEFAULT 0,
    event_view_count BIGINT NOT NULL DEFAULT 0,
    apply_click_count BIGINT NOT NULL DEFAULT 0,
    share_count BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_affinity_snapshots (
    user_id TEXT NOT NULL,
    school_id TEXT NOT NULL REFERENCES schools (id) ON DELETE CASCADE,
    affinity_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    event_count BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, school_id)
);

CREATE INDEX IF NOT EXISTS user_affinity_snapshots_user_id_idx
    ON user_affinity_snapshots (user_id, affinity_score DESC);

CREATE TABLE IF NOT EXISTS area_affinity_snapshots (
    area TEXT PRIMARY KEY,
    affinity_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    event_count BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_queue (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    last_error TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS job_queue_status_run_after_idx
    ON job_queue (status, run_after, created_at, id);

CREATE TABLE IF NOT EXISTS job_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES job_queue (id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    UNIQUE (job_id, attempt_number)
);
