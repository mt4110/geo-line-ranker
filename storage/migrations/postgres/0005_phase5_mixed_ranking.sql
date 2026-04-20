ALTER TABLE schools
    ADD COLUMN IF NOT EXISTS group_id TEXT;

UPDATE schools
SET group_id = id
WHERE group_id IS NULL OR group_id = '';

ALTER TABLE schools
    ALTER COLUMN group_id SET NOT NULL;

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS event_category TEXT NOT NULL DEFAULT 'general',
    ADD COLUMN IF NOT EXISTS is_featured BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS priority_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS starts_at TEXT,
    ADD COLUMN IF NOT EXISTS placement_tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'seed',
    ADD COLUMN IF NOT EXISTS source_key TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS events_school_id_active_idx
    ON events (school_id, is_active, id);

CREATE INDEX IF NOT EXISTS events_source_type_source_key_idx
    ON events (source_type, source_key, is_active, id);
