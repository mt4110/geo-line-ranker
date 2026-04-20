ALTER TABLE recommendation_traces
    ADD COLUMN IF NOT EXISTS trace_payload JSONB NOT NULL DEFAULT '{}'::jsonb;
